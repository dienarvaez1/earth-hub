"""Backfill and catch-up through the whole pipeline: mock FDSN and feeds -> real jobs -> Kafka
(backfill topic) -> separate batched consumer -> PostgreSQL -> API."""

import json
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from support.fdsn import event_at, fail_times
from support.feed import FeedServer, revised

from earth_hub.database.progress import Progress
from earth_hub.ingestion.catchup import CatchUp
from earth_hub.ingestion.fdsn import FdsnClient
from earth_hub.ingestion.producer import EventPublisher, make_producer
from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client

pytestmark = [pytest.mark.e2e, pytest.mark.req("REQ-QA-002")]

JAN_1 = datetime(2026, 1, 1, tzinfo=UTC)
SUN_4 = datetime(2026, 1, 4, tzinfo=UTC)
FEB_15 = datetime(2026, 2, 15, tzinfo=UTC)  # Sunday: 7 windows from Jan 1
MS = timedelta(milliseconds=1)


def total(pipeline, **params) -> int:
    return pipeline.get("/earthquakes/stats", **params)["count"]


@pytest.mark.req("REQ-BF-002", "REQ-BF-010", "REQ-E2E-002")
def test_boundary_events_are_stored_exactly_once(pipeline, fdsn, feature_template):
    sundays = [SUN_4 + timedelta(days=7 * k) for k in range(6)]
    tricky = {}
    for k, sunday in enumerate(sundays):
        tricky |= {
            f"w{k}_before": sunday - MS,
            f"w{k}_exact": sunday,
            f"w{k}_after": sunday + MS,
            f"w{k}_last_second": sunday - timedelta(milliseconds=400),
        }
    fdsn.put(*[event_at(feature_template, name, when) for name, when in tricky.items()])
    report = pipeline.backfill(JAN_1, FEB_15, sweep=False)
    assert report.windows_done == 7
    pipeline.sync()

    served = pipeline.events()
    assert set(served) == set(tricky)  # every one present, none twice
    assert total(pipeline) == len(tricky)
    backfill = pipeline.reconcile(backfill=True)
    assert backfill.ok and backfill.lost == []
    # Exactly-midnight events were sent by both weeks; the second copy is a duplicate.
    assert backfill.outcomes == {"applied": len(tricky), "duplicate": 6}


@pytest.mark.req("REQ-BF-005", "REQ-BF-003")
def test_interrupted_backfill_resumes_with_exact_totals(pipeline, fdsn, feature_template):
    events = [
        event_at(feature_template, f"e{i:03d}", JAN_1 + timedelta(hours=7 * i)) for i in range(150)
    ]
    fdsn.put(*events)
    fourth_week = lambda r: r.url.params.get("starttime") == "2026-01-18T00:00:00"  # noqa: E731
    fdsn.interceptors.append(fail_times(3, 503, when=fourth_week))
    with pytest.raises(Exception, match="failed after 3 attempts"):
        pipeline.backfill(JAN_1, FEB_15, sweep=False)

    fdsn.requests.clear()
    report = pipeline.backfill(JAN_1, FEB_15, sweep=False)
    assert report.windows_skipped == 3  # Jan 1, Jan 4, Jan 11 were done
    pipeline.sync()
    assert set(pipeline.events()) == {e["id"] for e in events}
    assert pipeline.reconcile(backfill=True).outcomes == {"applied": 150}


@pytest.mark.req("REQ-BF-009", "REQ-E2E-003")
def test_backfill_and_live_feed_overlap_keeps_the_newest_once(pipeline, feed, fdsn, event):
    quake = event("us0001", minutes_ago=90, mag=4.0)
    newer = revised(quake, mag=4.4)
    feed.show(newer)  # the live feed already has the newer revision
    pipeline.poll()
    pipeline.sync()
    fdsn.put(quake)  # the backfill finds the older one
    now = datetime.now(UTC)
    pipeline.backfill(now - timedelta(days=1), now)
    pipeline.sync()

    body = pipeline.get("/earthquakes/us0001")
    assert (body["magnitude"], body["revision_count"]) == (4.4, 1)
    assert pipeline.messages_in_topic(pipeline.backfill_topic) == 0  # nothing newer to send


def catch_up_job(pipeline, month_feed):
    return CatchUp(
        UsgsClient(
            "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
            make_http_client(transport=month_feed.transport),
            sleep=lambda s: None,
        ),
        FdsnClient(
            "https://earthquake.usgs.gov/fdsnws/event/1",
            make_http_client(transport=pipeline.fdsn.transport),
            pause_s=0,
            sleep=lambda s: None,
        ),
        EventPublisher(
            make_producer(pipeline.settings.kafka_bootstrap_servers), pipeline.backfill_topic
        ),
        Progress(pipeline.db),
    )


@pytest.mark.req("REQ-CU-001")
def test_catch_up_recovers_a_revision_to_a_three_day_old_event(pipeline, feed, event):
    """Risk 1 from the plan: the live all_day feed no longer lists events this old."""
    old = event("us0001", minutes_ago=3 * 24 * 60, mag=3.0, status="automatic")
    feed.show(old)
    pipeline.poll()
    pipeline.sync()

    month_feed = FeedServer()
    month_feed.show(revised(old, by_s=3600, mag=3.3, status="reviewed"))
    assert catch_up_job(pipeline, month_feed).catch_up() == 1
    pipeline.sync()
    body = pipeline.get("/earthquakes/us0001")
    assert (body["magnitude"], body["review_status"], body["revision_count"]) == (
        3.3,
        "reviewed",
        2,
    )


@pytest.mark.req("REQ-CU-002", "REQ-E2E-011")
def test_deleted_check_hides_an_event_usgs_deleted(pipeline, feed, event):
    quake = event("us0001", minutes_ago=600)
    feed.show(quake)
    pipeline.poll()
    pipeline.sync()

    deletion = revised(quake, by_s=1800, status="deleted")
    pipeline.fdsn.put(deletion)
    job = catch_up_job(pipeline, FeedServer())
    Progress(pipeline.db).set_checkpoint("deleted", datetime.now(UTC) - timedelta(days=1))
    assert job.check_deleted() == 1
    pipeline.sync()
    assert pipeline.events() == {}
    assert pipeline.get("/earthquakes/us0001")["review_status"] == "deleted"


@pytest.mark.req("REQ-CU-003")
def test_the_history_pass_recovers_a_revision_to_an_event_older_than_the_month_feed(
    pipeline, feed, event
):
    """DEF-067: USGS revises an event 90 days old; no feed lists it any more."""
    old = event("us0002", minutes_ago=90 * 24 * 60, mag=3.0, status="automatic")
    feed.show(old)
    pipeline.poll()
    pipeline.sync()

    pipeline.fdsn.put(revised(old, by_s=90 * 86400 - 3600, mag=3.4, status="reviewed"))
    job = catch_up_job(pipeline, FeedServer())
    Progress(pipeline.db).set_checkpoint("revised", datetime.now(UTC) - timedelta(days=1))
    assert job.check_revised() == 1
    pipeline.sync()
    body = pipeline.get("/earthquakes/us0002")
    assert (body["magnitude"], body["review_status"]) == (3.4, "reviewed")


@pytest.mark.req("REQ-CU-004", "REQ-E2E-011")
def test_the_audit_hides_an_event_usgs_deleted_without_keeping_its_time(pipeline, feed, event):
    """DEF-066: FDSN keeps only the products' code and updateTime; no search by time finds it."""
    from earth_hub.ingestion.history import month_start

    quake = event("tx0003", minutes_ago=600)
    feed.show(quake)
    pipeline.poll()
    pipeline.sync()

    record = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "fixtures/usgs/deleted_without_time_tx2026pfurqq.json"
        ).read_text()
    )
    deleted_at = quake["properties"]["updated"] + 1800_000
    for kind in record["properties"]["products"].values():
        for product in kind:
            product.update(code="tx0003", updateTime=deleted_at)
    pipeline.fdsn.put(record)
    when = datetime.fromtimestamp(quake["properties"]["time"] / 1000, UTC)
    Progress(pipeline.db).set_checkpoint("audit_month", month_start(when))
    assert catch_up_job(pipeline, FeedServer()).audit() == 1
    pipeline.sync()
    assert pipeline.events() == {}
    body = pipeline.get("/earthquakes/tx0003")
    assert (body["review_status"], body["place"]) == ("deleted", quake["properties"]["place"])


@pytest.mark.req("REQ-BF-012", "REQ-BF-010")
def test_live_latency_is_unaffected_while_a_large_backfill_is_consumed(
    pipeline, feed, fdsn, event, feature_template
):
    """20,000 historical events queue in the backfill topic; a live event arriving meanwhile
    must reach the API in seconds, because it has its own topic and consumer."""
    history = [
        # 7 s past each 3-minute mark, so none sits exactly on a Sunday boundary.
        event_at(feature_template, f"h{i:05d}", JAN_1 + timedelta(minutes=3 * i, seconds=7))
        for i in range(20_000)
    ]
    fdsn.put(*history)
    pipeline.backfill(JAN_1, JAN_1 + timedelta(days=45), sweep=False)
    assert pipeline.messages_in_topic(pipeline.backfill_topic) == 20_000

    consumer = pipeline.backfill_consumer
    worker = threading.Thread(target=consumer.run_forever, daemon=True)
    worker.start()
    try:
        while pipeline.stored(pipeline.backfill_topic) == 0:
            time.sleep(0.05)  # the backfill consumer is busy before the live event arrives

        feed.show(event("live01", minutes_ago=1, mag=5.1))
        started = time.monotonic()
        pipeline.poll()
        while (
            "live01"
            not in pipeline.get("/earthquakes", limit=1, sort="event_time")["items"][0]["event_id"]
        ):
            pipeline.consumer.run_once(timeout_s=0.5)
        latency = time.monotonic() - started
        backlog_left = 20_000 - pipeline.stored(pipeline.backfill_topic)
    finally:
        consumer.stop_event.set()
        worker.join(timeout=120)
        pipeline._backfill_consumer = None

    print(f"\nlive event latency during backfill: {latency:.2f} s, backlog still {backlog_left}")
    assert latency < 5.0
    assert backlog_left > 0, "the backfill had already finished; the test proved nothing"


def location(pipeline, event_id) -> tuple:
    body = pipeline.get(f"/earthquakes/{event_id}")
    return body["city"], body["region"], body["country"]


@pytest.mark.req("REQ-DQ-PLACE-002")
def test_backfill_and_catch_up_store_city_region_and_country(
    pipeline, feed, fdsn, feature_template, event
):
    places = {
        "ak1": ("86 km E of Ugashik, Alaska", ("Ugashik", "Alaska", "United States")),
        "ci1": ("10 km N of Cobb, CA", ("Cobb", "California", "United States")),
        "mx1": ("10 km NE of Mexicali, B.C., MX", ("Mexicali", "Baja California", "Mexico")),
        "us1": ("south of the Fiji Islands", (None, "the Fiji Islands", None)),
        "us2": ("Fiji region", (None, None, None)),
        "us3": (None, (None, None, None)),
    }
    events = [
        event_at(feature_template, event_id, JAN_1 + timedelta(days=5 * k), place=place)
        for k, (event_id, (place, _)) in enumerate(places.items())
    ]
    georgia = event_at(feature_template, "us4", SUN_4, place="15 km WNW of Ambrolauri, Georgia")
    georgia["geometry"]["coordinates"][0] = 43.0
    fdsn.put(*events, georgia)
    pipeline.backfill(JAN_1, FEB_15, sweep=False)
    pipeline.sync()

    for event_id, (_, expected) in places.items():
        assert location(pipeline, event_id) == expected, event_id
    assert location(pipeline, "us4") == ("Ambrolauri", None, "Georgia")
    assert pipeline.reconcile(backfill=True).ok

    # A catch-up revision that moves the place moves the location with it.
    old = event("us0001", minutes_ago=3 * 24 * 60, place="86 km E of Ugashik, Alaska")
    feed.show(old)
    pipeline.poll()
    pipeline.sync()
    month_feed = FeedServer()
    month_feed.show(revised(old, by_s=3600, place="80 km E of Pilot Point, Alaska"))
    assert catch_up_job(pipeline, month_feed).catch_up() == 1
    pipeline.sync()
    assert location(pipeline, "us0001") == ("Pilot Point", "Alaska", "United States")
