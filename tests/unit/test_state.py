import json
from datetime import UTC, datetime, timedelta

import pytest

from earth_hub.ingestion.state import STATE_WINDOW, SentState, state_from_messages

NOW = datetime(2026, 10, 16, 8, 0, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)


def envelope(event_id, updated, time=NOW_MS) -> bytes:
    return json.dumps(
        {"event_id": event_id, "source_updated_ms": updated, "event_time_ms": time}
    ).encode()


def test_newer_revision_replaces_older_and_older_never_wins():
    state = SentState()
    state.mark_sent("us1", 200, NOW_MS)
    state.mark_sent("us1", 100, NOW_MS)  # late, older revision
    assert state.updated_ms("us1") == 200
    state.mark_sent("us1", 300, NOW_MS)
    assert state.updated_ms("us1") == 300


@pytest.mark.req("REQ-ING-008")
def test_rebuild_keeps_newest_revision_per_event():
    state = state_from_messages(
        [envelope("us1", 300), envelope("us1", 100), envelope("ak2", 50), envelope("us1", 200)]
    )
    assert len(state) == 2
    assert state.updated_ms("us1") == 300
    assert state.updated_ms("ak2") == 50


def test_rebuild_skips_unreadable_messages(caplog):
    values = [
        envelope("us1", 100),
        b"not json",
        json.dumps({"event_id": "x"}).encode(),
        json.dumps({"event_id": "y", "source_updated_ms": None, "event_time_ms": 1}).encode(),
        b"null",
    ]
    state = state_from_messages(values)
    assert len(state) == 1
    assert "skipped unreadable messages" in caplog.text


def test_rebuild_from_nothing_is_empty():
    assert len(state_from_messages([])) == 0


def test_prune_forgets_events_older_than_the_window():
    state = SentState()
    old = int((NOW - STATE_WINDOW - timedelta(minutes=1)).timestamp() * 1000)
    recent = int((NOW - timedelta(hours=23)).timestamp() * 1000)
    state.mark_sent("old", 1, old)
    state.mark_sent("recent", 1, recent)
    assert state.prune(NOW) == 1
    assert state.updated_ms("old") is None
    assert state.updated_ms("recent") == 1
