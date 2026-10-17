import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from support.fdsn import FdsnServer, event_at
from support.feed import FeedServer
from support.memory import MemoryProgress

from earth_hub.ingestion.catchup import FIRST_DELETED_CHECK, CatchUp
from earth_hub.ingestion.fdsn import FdsnClient
from earth_hub.ingestion.producer import EventPublisher
from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client

NOW = datetime(2026, 10, 17, 8, 0, tzinfo=UTC)
FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"


@pytest.fixture
def month_feed():
    return FeedServer()


@pytest.fixture
def fdsn_server():
    return FdsnServer()


@pytest.fixture
def make_job(month_feed, fdsn_server, fake_producer):
    def make(progress=None, clock=lambda: NOW, **kwargs):
        progress = progress or MemoryProgress()
        job = CatchUp(
            UsgsClient(
                FEED, make_http_client(transport=month_feed.transport), sleep=lambda s: None
            ),
            FdsnClient(
                "https://earthquake.usgs.gov/fdsnws/event/1",
                make_http_client(transport=fdsn_server.transport),
                pause_s=0,
                sleep=lambda s: None,
            ),
            EventPublisher(fake_producer, "earthquakes.backfill"),
            progress,
            clock=clock,
            **kwargs,
        )
        return job, progress

    return make


def published(producer):
    return [(topic, json.loads(value)) for topic, _, value in producer.produced]


@pytest.mark.req("REQ-CU-001", "REQ-BF-009", "REQ-BF-010")
def test_catch_up_publishes_only_revisions_newer_than_stored(
    make_job, month_feed, fake_producer, feature_template
):
    three_days_old = event_at(feature_template, "old_revised", NOW - timedelta(days=3))
    unchanged = event_at(feature_template, "unchanged", NOW - timedelta(days=10))
    month_feed.show(three_days_old, unchanged)
    stored = {
        "old_revised": three_days_old["properties"]["updated"] - 60_000,  # we hold an older version
        "unchanged": unchanged["properties"]["updated"],
    }
    job, progress = make_job(MemoryProgress(stored))
    assert job.catch_up() == 1
    [(topic, envelope)] = published(fake_producer)
    assert (topic, envelope["event_id"], envelope["source"]) == (
        "earthquakes.backfill",
        "old_revised",
        "catchup",
    )
    assert progress.points["catchup"] == NOW


def test_unchanged_month_feed_is_a_304(make_job, month_feed, feature_template, fake_producer):
    month_feed.show(event_at(feature_template, "a", NOW - timedelta(days=1)))
    job, _ = make_job()
    job.catch_up()
    fake_producer.produced.clear()
    assert job.catch_up() == 0 and fake_producer.produced == []


@pytest.mark.req("REQ-CU-002", "REQ-BF-008")
def test_deleted_check_asks_only_for_deletions_since_the_last_check(
    make_job, fdsn_server, fake_producer, feature_template
):
    last_check = NOW - timedelta(days=1)
    gone = event_at(
        feature_template,
        "gone",
        NOW - timedelta(days=40),
        status="deleted",
        updated=timedelta(days=39, hours=23),
    )  # deleted ~1 h ago
    old_deletion = event_at(
        feature_template,
        "old_gone",
        NOW - timedelta(days=60),
        status="deleted",
        updated=timedelta(days=1),
    )  # deleted long ago
    alive = event_at(feature_template, "alive", NOW - timedelta(days=5))
    fdsn_server.put(gone, old_deletion, alive)
    job, progress = make_job(MemoryProgress())
    progress.points["deleted"] = last_check

    assert job.check_deleted() == 1
    [(_, envelope)] = published(fake_producer)
    assert (
        envelope["event_id"] == "gone" and envelope["payload"]["properties"]["status"] == "deleted"
    )
    [query] = fdsn_server.requests_for("query")
    assert query["includedeleted"] == "only"
    assert query["updatedafter"] == last_check.strftime("%Y-%m-%dT%H:%M:%S")
    assert progress.points["deleted"] == NOW


def test_first_deleted_check_looks_back_30_days(make_job, fdsn_server):
    job, _ = make_job()
    job.check_deleted()
    [count] = fdsn_server.requests_for("count")
    assert count["updatedafter"] == (NOW - FIRST_DELETED_CHECK).strftime("%Y-%m-%dT%H:%M:%S")


@pytest.mark.req("REQ-CU-001")
def test_schedule_runs_catch_up_on_startup_and_then_hourly(make_job, month_feed, fdsn_server):
    clock = [NOW]
    job, progress = make_job(
        clock=lambda: clock[0], catchup_interval_s=3600, deleted_interval_s=86400
    )
    progress.points["deleted"] = NOW  # deletions were checked recently
    job.run_once()
    assert month_feed.requests == 1 and fdsn_server.requests == []  # catch-up runs on startup
    clock[0] += timedelta(minutes=30)
    job.run_once()
    assert month_feed.requests == 1  # not due yet
    clock[0] += timedelta(minutes=31)
    job.run_once()
    assert month_feed.requests == 2
    clock[0] += timedelta(days=1)
    job.run_once()
    assert len(fdsn_server.requests_for("query")) == 1  # the daily deleted check came due


@pytest.mark.req("REQ-OPS-005")
def test_heartbeat_only_after_successful_passes(make_job, month_feed, tmp_path):
    heartbeat = tmp_path / "hb"
    job, _ = make_job(heartbeat_path=heartbeat)
    month_feed.transport = httpx.MockTransport(lambda r: httpx.Response(503))
    job._month_feed._http = make_http_client(transport=month_feed.transport)
    job.run_once()
    assert not heartbeat.exists()


@pytest.mark.req("REQ-CU-002")
def test_a_sub_second_checkpoint_is_rounded_down_not_a_crash(make_job, fdsn_server):
    job, progress = make_job()
    progress.points["deleted"] = NOW - timedelta(days=1, microseconds=250_000)
    job.check_deleted()
    [query] = fdsn_server.requests_for("count")
    assert query["updatedafter"] == (NOW - timedelta(days=1, seconds=1)).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


@pytest.mark.req("REQ-CU-002", "REQ-BF-013")
def test_deleted_check_splits_when_usgs_is_too_slow(
    make_job, fdsn_server, fake_producer, feature_template
):
    gone = event_at(
        feature_template,
        "gone",
        NOW - timedelta(days=40, minutes=17),  # off the 08:00 split boundaries
        status="deleted",
        updated=timedelta(days=39, hours=23),
    )
    fdsn_server.put(gone)
    fdsn_server.interceptors.append(
        lambda r: (
            httpx.Response(504)
            if r.url.path.endswith("/query")
            and datetime.fromisoformat(r.url.params["endtime"])
            - datetime.fromisoformat(r.url.params["starttime"])
            > timedelta(days=60)
            else None
        )
    )
    job, progress = make_job()
    progress.points["deleted"] = NOW - timedelta(days=1)
    assert job.check_deleted() == 1
