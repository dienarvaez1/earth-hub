"""The plan's end-to-end scenarios, through the whole pipeline and checked via the API.

Each test gets a fresh topic, database and consumer group, and a mock feed it controls,
so results never depend on live earthquakes (REQ-QA-002).
"""

import json
from datetime import UTC, datetime, timedelta

import pytest
from support.feed import revised

from earth_hub.ingestion.state import read_topic_since

pytestmark = [pytest.mark.e2e, pytest.mark.req("REQ-QA-002")]


def assert_reconciled(pipeline, **outcomes):
    report = pipeline.reconcile()
    assert report.ok, report
    assert (report.lost, report.pending) == ([], 0)
    assert report.outcomes == outcomes


@pytest.mark.req("REQ-E2E-001")
def test_new_valid_event_becomes_retrievable(pipeline, feed, event):
    quake = event("us0001", mag=4.7, lon=142.3, lat=38.1, depth=24.0, magType="mww")
    feed.show(quake)
    assert pipeline.poll().published == 1
    pipeline.sync()

    body = pipeline.get("/earthquakes/us0001")
    assert (body["magnitude"], body["mag_type"]) == (4.7, "mww")
    assert (body["longitude"], body["latitude"], body["depth_km"]) == (142.3, 38.1, 24.0)
    assert body["ids"] == ["us0001"] and body["revision_count"] == 1
    assert list(pipeline.events()) == ["us0001"]
    assert pipeline.get("/earthquakes/stats")["count"] == 1
    assert_reconciled(pipeline, applied=1)


@pytest.mark.req("REQ-E2E-002", "REQ-PER-003")
def test_duplicate_delivery_creates_no_duplicate(pipeline, feed, event):
    feed.show(event("us0001"))
    pipeline.poll()
    pipeline.sync()
    # A producer retry or replay puts the same message on the topic again.
    [message] = _topic_values(pipeline)
    pipeline.publish_raw(b"us0001", message)
    pipeline.sync()

    assert list(pipeline.events()) == ["us0001"]
    assert pipeline.get("/earthquakes/us0001")["revision_count"] == 1
    assert_reconciled(pipeline, applied=1, duplicate=1)


@pytest.mark.req("REQ-E2E-003", "REQ-ING-004")
def test_revised_event_shows_latest_revision(pipeline, feed, event):
    quake = event("us0001", mag=4.0, status="automatic")
    feed.show(quake)
    pipeline.poll()
    feed.show(revised(quake, mag=4.3, status="reviewed"))
    assert pipeline.poll().published == 1
    pipeline.sync()

    body = pipeline.get("/earthquakes/us0001")
    assert (body["magnitude"], body["review_status"], body["revision_count"]) == (
        4.3,
        "reviewed",
        2,
    )
    assert_reconciled(pipeline, applied=2)


@pytest.mark.req("REQ-E2E-004", "REQ-DQ-GEO-002", "REQ-ING-007")
def test_invalid_event_is_never_served_and_is_auditable(pipeline, feed, event):
    feed.show(event("good01"), event("bad01", lat=95.0))
    # The poller publishes both: data-quality rules belong to the consumer.
    assert pipeline.poll().published == 2
    pipeline.sync()

    assert list(pipeline.events()) == ["good01"]
    pipeline.get("/earthquakes/bad01", expect=404)
    assert pipeline.get("/earthquakes/stats")["count"] == 1
    [rejected] = [r for r in pipeline.raw_outcomes() if r["outcome"] == "rejected"]
    assert (rejected["event_id"], rejected["rule"]) == ("bad01", "GEO-002")
    assert_reconciled(pipeline, applied=1, rejected=1)


@pytest.mark.req("REQ-E2E-006", "REQ-PER-007")
def test_consumer_restart_mid_batch(pipeline, feed, event):
    feed.show(*[event(f"us{i:04d}", minutes_ago=i) for i in range(20)])
    pipeline.poll()
    # Process part of the batch, then stop cleanly; a new consumer takes over.
    for _ in range(7):
        while not pipeline.consumer.run_once(timeout_s=1.0):
            pass
    pipeline.restart_consumer()
    pipeline.sync()

    assert len(pipeline.events()) == 20
    assert_reconciled(pipeline, applied=20)


@pytest.mark.req("REQ-E2E-006", "REQ-PER-002", "REQ-PER-007")
def test_crash_between_db_commit_and_offset_commit(pipeline, feed, event):
    feed.show(*[event(f"us{i:04d}", minutes_ago=i) for i in range(10)])
    pipeline.poll()
    # Store every message but "crash" before any offset is committed.
    pipeline.use_consumer_that_loses_commits()
    pipeline.sync()
    pipeline.restart_consumer()

    # After the restart all 10 are redelivered and recognized as already stored.
    redelivered = 0
    while redelivered < 10:
        redelivered += pipeline.consumer.run_once(timeout_s=1.0)
    assert pipeline.consumer.outcomes == {"already_processed": 10}
    assert len(pipeline.events()) == 10
    assert_reconciled(pipeline, applied=10)


@pytest.mark.req("REQ-E2E-007")
def test_empty_feed_and_database(pipeline, feed):
    feed.show()
    assert pipeline.poll().published == 0
    assert pipeline.messages_in_topic() == 0

    assert pipeline.get("/earthquakes") == {"items": [], "total": 0, "limit": 20, "offset": 0}
    stats = pipeline.get("/earthquakes/stats")
    assert (stats["count"], stats["magnitude"]) == (0, None)
    hourly = pipeline.get("/earthquakes/summary/hourly")
    assert len(hourly["buckets"]) == 24 and {b["count"] for b in hourly["buckets"]} == {0}
    buckets = pipeline.get("/earthquakes/summary/magnitude")["buckets"]
    assert len(buckets) == 10 and {b["count"] for b in buckets} == {0}
    assert pipeline.get("/health")["latest_event_time"] is None


@pytest.mark.req("REQ-E2E-008", "REQ-API-010")
def test_invalid_api_requests_get_consistent_client_errors(pipeline, feed, event):
    feed.show(event("us0001"))
    pipeline.poll()
    pipeline.sync()
    for params in (
        {"limit": 0},
        {"sort": "place"},
        {"start_time": "yesterday"},
        {"min_magnitude": 5, "max_magnitude": 1},
    ):
        error = pipeline.get("/earthquakes", expect=422, **params)["error"]
        assert error["code"] == "invalid_parameters" and error["details"]
    assert pipeline.get("/earthquakes/nope", expect=404)["error"]["code"] == "not_found"


@pytest.mark.req("REQ-E2E-009", "REQ-PER-013", "REQ-API-006", "REQ-API-008")
def test_batch_reconciles_with_outcomes_and_api(pipeline, feed, event):
    """55 messages: 45 feed events (40 valid, 5 invalid), 5 revisions, 5 replayed duplicates.
    The expected results are computed from the inputs, not from the database."""
    valid = [
        event(f"us{i:04d}", minutes_ago=i * 3, mag=round(-0.5 + i * 0.2, 2)) for i in range(40)
    ]
    invalid = [event(f"bad{i}", depth=2000.0) for i in range(5)]
    feed.show(*valid, *invalid)
    pipeline.poll()
    revisions = [revised(valid[i], mag=valid[i]["properties"]["mag"] + 1) for i in range(5)]
    feed.show(*revisions)
    pipeline.poll()
    pipeline.sync()
    # Replay 5 messages of events that were never revised: exactly 5 duplicates.
    replay = {f"us{i:04d}" for i in range(10, 15)}
    for value in _topic_values(pipeline):
        if json.loads(value)["event_id"] in replay:
            pipeline.publish_raw(json.loads(value)["event_id"].encode(), value)
    pipeline.sync()

    assert pipeline.messages_in_topic() == 55
    assert_reconciled(pipeline, applied=45, rejected=5, duplicate=5)

    final = {f["id"]: f["properties"]["mag"] for f in valid} | {
        f["id"]: f["properties"]["mag"] for f in revisions
    }
    served = pipeline.events()
    assert {k: v["magnitude"] for k, v in served.items()} == pytest.approx(final)
    stats = pipeline.get("/earthquakes/stats")
    assert stats["count"] == 40
    assert stats["magnitude"]["min"] == pytest.approx(min(final.values()))
    assert stats["magnitude"]["max"] == pytest.approx(max(final.values()))
    assert stats["magnitude"]["avg"] == pytest.approx(sum(final.values()) / 40, abs=1e-4)
    buckets = pipeline.get("/earthquakes/summary/magnitude")["buckets"]
    assert sum(b["count"] for b in buckets) == 40
    hourly = pipeline.get("/earthquakes/summary/hourly")["buckets"]
    assert sum(b["count"] for b in hourly) == 40


@pytest.mark.req("REQ-E2E-010", "REQ-PER-005", "REQ-PER-006", "REQ-API-005")
def test_merged_reports_are_one_event_under_either_id(pipeline, feed, event):
    feed.show(event("ak0001", mag=3.0, updated_s=30), event("us0001", mag=3.1, updated_s=40))
    pipeline.poll()
    pipeline.sync()
    assert set(pipeline.events()) == {"ak0001", "us0001"}

    # USGS associates the two reports; ak0001 disappears from the feed.
    merged = event("us0001", mag=3.4, ids=["us0001", "ak0001"], updated_s=600, magType="mww")
    feed.remove("ak0001")
    feed.show(merged)
    pipeline.poll()
    pipeline.sync()

    assert list(pipeline.events()) == ["us0001"]
    by_old, by_new = pipeline.get("/earthquakes/ak0001"), pipeline.get("/earthquakes/us0001")
    assert by_old == by_new
    assert (by_new["ids"], by_new["magnitude"]) == (["ak0001", "us0001"], 3.4)
    assert_reconciled(pipeline, applied=3)


@pytest.mark.req("REQ-E2E-011", "REQ-API-009")
def test_event_revised_to_deleted_is_hidden(pipeline, feed, event):
    quake = event("us0001")
    feed.show(quake, event("us0002"))
    pipeline.poll()
    feed.show(revised(quake, status="deleted"))
    pipeline.poll()
    pipeline.sync()

    assert list(pipeline.events()) == ["us0002"]
    assert set(pipeline.events(include_deleted=True)) == {"us0001", "us0002"}
    assert pipeline.get("/earthquakes/us0001")["review_status"] == "deleted"


@pytest.mark.req("REQ-E2E-012", "REQ-ING-008")
def test_poller_restart_resends_nothing(pipeline, feed, event):
    feed.show(*[event(f"us{i:04d}", minutes_ago=i) for i in range(8)])
    pipeline.poll()
    pipeline.sync()
    before = pipeline.events()

    pipeline.restart_poller()
    feed.show()  # a new feed version with nothing changed
    assert pipeline.poll().published == 0
    pipeline.sync()
    assert pipeline.messages_in_topic() == 8
    assert pipeline.events() == before


def _topic_values(pipeline) -> list[bytes]:
    return read_topic_since(
        pipeline.settings.kafka_bootstrap_servers,
        pipeline.topic,
        datetime.now(UTC) - timedelta(hours=1),
    )
