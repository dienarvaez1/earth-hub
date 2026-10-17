"""earth_hub.database.progress against real PostgreSQL."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError

from earth_hub.database.progress import Progress, WindowCounts
from earth_hub.database.repository import IncomingMessage, process_message
from earth_hub.ingestion.events import parse_feed
from earth_hub.ingestion.windows import Window

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)
SUN = datetime(2026, 1, 4, tzinfo=UTC)
WEEK = Window(SUN, SUN + timedelta(days=7))
COUNTS = WindowCounts(expected=3, fetched=3, published=2, skipped=1, rejected=0)


def messages(doc):
    return parse_feed(json.dumps(doc).encode(), fetched_at=NOW).messages


@pytest.mark.req("REQ-BF-009")
def test_needs_publish_compares_with_what_is_stored(db, feed_doc, make_envelope, make_feature):
    with db.begin() as conn:
        for i, feature in enumerate(feed_doc["features"][:3]):
            process_message(conn, IncomingMessage("t", 0, i, b"k", make_envelope(feature)), now=NOW)
        merged = make_feature("us77", ["us77", "ak77"], updated=1792137800000)
        process_message(conn, IncomingMessage("t", 0, 9, b"k", make_envelope(merged)), now=NOW)

    doc = json.loads(json.dumps(feed_doc))
    doc["features"][0]["properties"]["updated"] += 1  # newer than stored
    via_old_id = make_feature("ak77", ["ak77"], updated=1792137700000)  # older, via a former ID
    doc["features"].append(via_old_id)
    need = {m.event_id for m in Progress(db).needs_publish(messages(doc))}
    stored_unchanged = {f["id"] for f in feed_doc["features"][1:3]}
    assert need == {doc["features"][0]["id"]} | {f["id"] for f in feed_doc["features"][3:]}
    assert not need & stored_unchanged and "ak77" not in need


@pytest.mark.req("REQ-BF-005", "REQ-BF-001")
def test_done_windows_are_finished_and_open_ones_are_not(db):
    progress = Progress(db)
    progress.record("backfill", WEEK, COUNTS)
    current = Window(SUN + timedelta(days=7), SUN + timedelta(days=10), open=True)
    progress.record("backfill", current, COUNTS)
    assert progress.finished("backfill") == {(WEEK.start, WEEK.end)}
    # Re-recording the formerly open week as complete replaces its row.
    progress.record("backfill", Window(current.start, SUN + timedelta(days=14)), COUNTS)
    assert len(progress.finished("backfill")) == 2
    assert progress.finished("sweep") == set()


@pytest.mark.req("REQ-BF-003")
def test_the_database_refuses_a_window_marked_done_with_events_missing(db):
    with pytest.raises(IntegrityError, match="ck_backfill_windows_complete"):
        Progress(db).record(
            "backfill",
            WEEK,
            WindowCounts(expected=5, fetched=4, published=4, skipped=0, rejected=0),
        )


def test_checkpoints_are_upserted(db):
    progress = Progress(db)
    assert progress.checkpoint("catchup") is None
    progress.set_checkpoint("catchup", NOW)
    progress.set_checkpoint("catchup", NOW + timedelta(hours=1))
    assert progress.checkpoint("catchup") == NOW + timedelta(hours=1)
