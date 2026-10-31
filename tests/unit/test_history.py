"""Catch-up over all stored history: the history pass (DEF-067) and the audit (DEF-066)."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from support.fdsn import FdsnServer, at, event_at
from support.feed import FeedServer
from support.memory import MemoryProgress

from earth_hub.ingestion import history
from earth_hub.ingestion.catchup import AUDIT_LOOKUPS, FIRST_REVISED_CHECK, CatchUp
from earth_hub.ingestion.fdsn import FdsnClient
from earth_hub.ingestion.producer import EventPublisher
from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client
from earth_hub.validation.earthquake_rules import validate

NOW = datetime(2026, 10, 31, 8, 0, tzinfo=UTC)
# The real record FDSN gave for tx2026pfurqq on 2026-10-31: deleted, with no id, time, updated
# or ids; its products' code and updateTime are all that is left.
RECORD = json.loads(
    (
        Path(__file__).resolve().parents[1] / "fixtures/usgs/deleted_without_time_tx2026pfurqq.json"
    ).read_text()
)
# When USGS deleted it: the latest of its products' updateTime (here the phase data's).
DELETED_MS = max(p["updateTime"] for k in RECORD["properties"]["products"].values() for p in k)


def stored(key: str, when: datetime, *, ids=()) -> history.Stored:
    return history.Stored(
        key, key, frozenset({key, *ids}), at(when), -98.5, 31.2, 5.0, "earthquake", "Texas"
    )


@pytest.fixture
def fdsn_server():
    return FdsnServer()


@pytest.fixture
def make_job(fdsn_server, fake_producer):
    def make(progress=None, clock=lambda: NOW):
        progress = progress or MemoryProgress()
        job = CatchUp(
            UsgsClient(
                "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
                make_http_client(transport=FeedServer().transport),
                sleep=lambda s: None,
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
        )
        return job, progress

    return make


def published(producer):
    return [(topic, json.loads(value)) for topic, _, value in producer.produced]


# --- the deletion built from USGS's record --------------------------------------------------


@pytest.mark.req("REQ-CU-004")
def test_a_deletion_usgs_kept_no_time_for_becomes_an_ordinary_one_the_consumer_accepts():
    event = stored("tx2026pfurqq", datetime(2026, 8, 4, 3, 2, 1, tzinfo=UTC))
    message = history.deletion(RECORD, event, fetched_at=NOW)
    assert message.event_id == "tx2026pfurqq" and message.ids == ("tx2026pfurqq",)
    origin = RECORD["properties"]["products"]["origin"][0]["updateTime"]
    assert message.source_updated_ms == DELETED_MS > origin  # the latest, not the origin's
    assert message.event_time_ms == event.event_time_ms and message.source == "audit"
    props = message.payload["properties"]
    assert props["status"] == "deleted" and props["products"] == RECORD["properties"]["products"]
    assert message.payload["geometry"]["coordinates"] == [-98.5, 31.2, 5.0]
    valid = validate(json.loads(message.to_json()), now=NOW)  # the consumer's own rules
    assert valid.deleted and valid.fields["place"] == "Texas"


@pytest.mark.req("REQ-CU-004")
def test_only_a_deletion_without_time_is_built_from_the_stored_event(feature_template):
    event = stored("a", NOW - timedelta(days=90))
    live = event_at(feature_template, "a", NOW - timedelta(days=90))
    ordinary = event_at(feature_template, "a", NOW - timedelta(days=90), status="deleted")
    nameless = json.loads(json.dumps(RECORD))
    nameless["properties"]["products"] = {}
    for record in (live, ordinary, nameless, None):
        assert history.deletion(record, event, fetched_at=NOW) is None


# --- the history pass -------------------------------------------------------------------------


@pytest.mark.req("REQ-CU-003")
def test_the_history_pass_brings_revisions_older_than_the_month_feed(
    make_job, fdsn_server, fake_producer, feature_template
):
    old = NOW - timedelta(days=500)  # beyond the month feed, and beyond a year
    revised = event_at(feature_template, "old", old, updated=timedelta(days=499, hours=20))
    unchanged = event_at(feature_template, "same", old, updated=timedelta(days=1))
    fdsn_server.put(revised, unchanged)
    progress = MemoryProgress({"old": at(old) + 1, "same": at(old + timedelta(days=1))})
    progress.events = [stored("old", old), stored("same", old)]
    job, _ = make_job(progress)
    progress.points["revised"] = NOW - timedelta(days=1)

    assert job.check_revised() == 1
    [(topic, envelope)] = published(fake_producer)
    assert topic == "earthquakes.backfill" and envelope["event_id"] == "old"
    assert envelope["source"] == "revised"
    [query] = fdsn_server.requests_for("query")
    assert query["includedeleted"] == "false"
    assert query["starttime"] == f"{history.month_start(old):%Y-%m-%dT%H:%M:%S}"
    assert query["updatedafter"] == f"{NOW - timedelta(days=1):%Y-%m-%dT%H:%M:%S}"
    assert progress.points["revised"] == NOW


@pytest.mark.req("REQ-CU-003")
def test_the_first_history_pass_looks_back_30_days_over_at_least_a_year(make_job, fdsn_server):
    job, _ = make_job()  # nothing stored
    job.check_revised()
    [count] = fdsn_server.requests_for("count")
    assert count["updatedafter"] == f"{NOW - FIRST_REVISED_CHECK:%Y-%m-%dT%H:%M:%S}"
    assert count["starttime"] == f"{NOW - timedelta(days=366):%Y-%m-%dT%H:%M:%S}"


@pytest.mark.req("REQ-CU-002")
def test_the_deleted_check_covers_stored_history_older_than_a_year(
    make_job, fdsn_server, fake_producer, feature_template
):
    old = NOW - timedelta(days=600)
    fdsn_server.put(
        event_at(feature_template, "gone", old, status="deleted", updated=timedelta(days=599))
    )
    progress = MemoryProgress()
    progress.events = [stored("gone", old)]
    progress.points["deleted"] = NOW - timedelta(days=2)
    job, _ = make_job(progress)
    assert job.check_deleted() == 1


# --- the audit ----------------------------------------------------------------------------------


AUGUST = datetime(2026, 8, 1, tzinfo=UTC)


def august(feature_template, fdsn_server, *extra_stored):
    """Three stored August events USGS still lists, and what else is stored."""
    live = [
        event_at(feature_template, f"e{n}", AUGUST + timedelta(days=n, hours=6)) for n in (1, 2, 3)
    ]
    fdsn_server.put(*live)
    progress = MemoryProgress()
    progress.events = [
        stored(f"e{n}", AUGUST + timedelta(days=n, hours=6)) for n in (1, 2, 3)
    ] + list(extra_stored)
    return progress


@pytest.mark.req("REQ-CU-004", "REQ-BF-008")
def test_the_audit_finds_a_deletion_no_search_by_time_returns(
    make_job, fdsn_server, fake_producer, feature_template
):
    gone = stored("tx2026pfurqq", AUGUST + timedelta(days=3, hours=9))
    progress = august(feature_template, fdsn_server, gone)
    fdsn_server.put(RECORD)
    progress.points["audit_month"] = AUGUST
    job, _ = make_job(progress)

    assert job.audit() == 1
    [(_, envelope)] = published(fake_producer)
    assert envelope["event_id"] == "tx2026pfurqq" and envelope["source"] == "audit"
    assert envelope["payload"]["properties"]["status"] == "deleted"
    # A count per day of August; a listing and a lookup only for the day that differs.
    assert len(fdsn_server.requests_for("count")) == 31
    [listing] = [q for q in fdsn_server.requests_for("query") if "eventid" not in q]
    assert listing["starttime"] == "2026-08-04T00:00:00" and listing["includedeleted"] == "true"
    assert [q["eventid"] for q in fdsn_server.requests_for("query") if "eventid" in q] == [
        "tx2026pfurqq"
    ]
    assert progress.points["audit_month"] == datetime(2026, 9, 1, tzinfo=UTC)
    assert progress.points["audit:2026-08"] == NOW and progress.points["audit"] == NOW


@pytest.mark.req("REQ-CU-004")
def test_the_audit_publishes_nothing_when_the_month_matches(
    make_job, fdsn_server, fake_producer, feature_template
):
    progress = august(feature_template, fdsn_server)
    progress.points["audit_month"] = AUGUST
    job, _ = make_job(progress)
    assert job.audit() == 0 and fake_producer.produced == []
    assert all("eventid" not in q for q in fdsn_server.requests_for("query"))
    assert fdsn_server.requests_for("query") == []  # no day differed: nothing listed


@pytest.mark.req("REQ-CU-004")
def test_the_audit_goes_a_month_a_day_from_the_oldest_and_starts_again(
    make_job, fdsn_server, feature_template
):
    progress = august(feature_template, fdsn_server)  # the oldest stored month is August
    job, _ = make_job(progress)
    job.audit()  # no cursor yet: the oldest month
    assert progress.points["audit_month"] == datetime(2026, 9, 1, tzinfo=UTC)
    job.audit()
    job.audit()  # October, the current month, up to now
    assert progress.points["audit_month"] == datetime(2026, 11, 1, tzinfo=UTC)
    fdsn_server.requests.clear()
    job.audit()  # past now: August again
    [first, *_] = fdsn_server.requests_for("count")
    assert first["starttime"] == "2026-08-01T00:00:00"


@pytest.mark.req("REQ-CU-004")
def test_the_audit_looks_up_at_most_50_a_day_and_stays_on_the_month_until_done(
    make_job, fdsn_server, feature_template
):
    unlisted = [
        stored(f"x{n}", AUGUST + timedelta(days=10, minutes=n)) for n in range(AUDIT_LOOKUPS + 5)
    ]
    progress = august(feature_template, fdsn_server, *unlisted)
    progress.points["audit_month"] = AUGUST
    job, _ = make_job(progress)
    job.audit()
    lookups = [q for q in fdsn_server.requests_for("query") if "eventid" in q]
    assert len(lookups) == AUDIT_LOOKUPS
    assert progress.points["audit_month"] == AUGUST and "audit:2026-08" not in progress.points


# --- what is stored, in direct mode ------------------------------------------------------------


@pytest.mark.req("REQ-CU-003", "REQ-CU-004")
def test_the_bucket_answers_what_history_is_stored_as_postgresql_does(
    s3, tmp_path, make_envelope, make_feature
):
    from support.export import BUCKET, start_direct

    from earth_hub.direct.runner import BucketProgress, DirectRunner
    from earth_hub.export.bucket import Bucket
    from earth_hub.ingestion.windows import Window

    start_direct(s3)
    runner = DirectRunner(Bucket(s3, BUCKET), workdir=tmp_path, workers=2)
    progress = BucketProgress(runner)
    assert progress.oldest() is None and progress.stored_in(Window(AUGUST, NOW)) == []
    first, later = at(AUGUST + timedelta(days=3)), at(AUGUST + timedelta(days=40))
    runner.process(
        [
            make_envelope(make_feature("ak1", ids=["ak1", "us1"], time=first, updated=first)),
            make_envelope(make_feature("ak2", time=later, updated=later)),
            make_envelope(make_feature("ak3", time=later, updated=later + 1, status="deleted")),
        ],
        now=NOW,
    )
    assert progress.oldest() == AUGUST + timedelta(days=3)
    [one] = progress.stored_in(Window(AUGUST, AUGUST + timedelta(days=31)))
    assert (one.key, one.event_id, one.ids, one.event_time_ms) == (
        "ak1",
        "ak1",
        frozenset({"ak1", "us1"}),
        first,
    )
    # Deleted events are left out: the audit compares what USGS still lists.
    assert [e.key for e in progress.stored_in(Window(AUGUST, NOW))] == ["ak1", "ak2"]
