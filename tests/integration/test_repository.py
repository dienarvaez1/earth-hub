"""process_message against real PostgreSQL: one throwaway, fully migrated database per test."""

import itertools
import json
import threading
from datetime import UTC, datetime

import pytest
from sqlalchemy import insert, select, text
from sqlalchemy.exc import IntegrityError

from earth_hub.database.models import Earthquake, EarthquakeId, RawEvent
from earth_hub.database.repository import IncomingMessage, process_message, record_error

pytestmark = pytest.mark.integration

NOW_FIXTURE = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)


@pytest.fixture
def deliver(db):
    """Process a message value in its own transaction, at the next offset by default."""
    offsets = itertools.count()

    def deliver(value: bytes, *, offset: int | None = None, partition: int = 0):
        message = IncomingMessage(
            topic="earthquakes.raw",
            partition=partition,
            offset=next(offsets) if offset is None else offset,
            key=b"k",
            value=value,
            timestamp_ms=1792137522341,
        )
        with db.begin() as conn:
            return process_message(conn, message, now=NOW_FIXTURE)

    return deliver


def rows(db, table, *where, order_by=None):
    query = select(table).where(*where)
    if order_by is not None:
        query = query.order_by(order_by)
    with db.connect() as conn:
        return conn.execute(query).mappings().all()


def alias_map(db) -> dict[str, int]:
    return {r["event_id"]: r["earthquake_id"] for r in rows(db, EarthquakeId)}


def assert_invariants(db):
    """Every preferred ID maps to its own event; every alias points at an existing event."""
    aliases = alias_map(db)
    events = rows(db, Earthquake)
    for event in events:
        assert aliases[event["preferred_event_id"]] == event["id"]
    assert set(aliases.values()) <= {e["id"] for e in events}


def revise(feature, by_ms=60_000, **properties):
    revised = json.loads(json.dumps(feature))
    revised["properties"]["updated"] += by_ms
    revised["properties"].update(properties)
    return revised


# --- new events, redelivery, revisions --------------------------------------------------


@pytest.mark.req("REQ-PER-001")
def test_new_event_is_stored_with_normalized_fields(db, deliver, make_envelope, feed_doc):
    feature = feed_doc["features"][0]
    result = deliver(make_envelope(feature))
    assert result.outcome == "applied"

    [event] = rows(db, Earthquake)
    lon, lat, depth = feature["geometry"]["coordinates"]
    assert event["preferred_event_id"] == feature["id"]
    assert (event["longitude"], event["latitude"], event["depth_km"]) == (lon, lat, depth)
    assert event["magnitude"] == feature["properties"]["mag"]
    assert event["source_updated_ms"] == feature["properties"]["updated"]
    assert event["event_time"].utcoffset().total_seconds() == 0
    assert event["revision_count"] == 1
    assert alias_map(db) == {feature["id"]: event["id"]}

    [raw] = rows(db, RawEvent)
    assert raw["outcome"] == "applied" and raw["earthquake_id"] == event["id"]
    assert raw["envelope"]["payload"] == feature  # original payload preserved


def test_whole_fixture_feed_is_stored(db, deliver, make_envelope, feed_doc):
    for feature in feed_doc["features"]:
        assert deliver(make_envelope(feature)).outcome == "applied"
    assert len(rows(db, Earthquake)) == len(feed_doc["features"])
    assert_invariants(db)


@pytest.mark.req("REQ-PER-002")
def test_redelivered_offset_is_skipped(db, deliver, make_envelope, feed_doc):
    value = make_envelope(feed_doc["features"][0])
    deliver(value, offset=5)
    assert deliver(value, offset=5).outcome == "already_processed"
    assert len(rows(db, Earthquake)) == 1 and len(rows(db, RawEvent)) == 1


@pytest.mark.req("REQ-PER-003")
def test_same_revision_at_new_offset_is_duplicate(db, deliver, make_envelope, feed_doc):
    value = make_envelope(feed_doc["features"][0])
    deliver(value)
    assert deliver(value).outcome == "duplicate"
    [event] = rows(db, Earthquake)
    assert event["revision_count"] == 1
    assert [r["outcome"] for r in rows(db, RawEvent, order_by=RawEvent.id)] == [
        "applied",
        "duplicate",
    ]


@pytest.mark.req("REQ-PER-004")
def test_newer_revision_updates_the_event(db, deliver, make_envelope, feed_doc):
    feature = feed_doc["features"][0]
    deliver(make_envelope(feature))
    revised = revise(feature, mag=4.6, status="reviewed", place="Revised place")
    assert deliver(make_envelope(revised)).outcome == "applied"

    [event] = rows(db, Earthquake)
    assert (event["magnitude"], event["review_status"], event["place"]) == (
        4.6,
        "reviewed",
        "Revised place",
    )
    assert event["source_updated_ms"] == revised["properties"]["updated"]
    assert event["revision_count"] == 2


@pytest.mark.req("REQ-PER-004")
def test_older_revision_is_stale_and_changes_nothing(db, deliver, make_envelope, feed_doc):
    feature = feed_doc["features"][0]
    newer = revise(feature, mag=4.6)
    deliver(make_envelope(newer))
    assert deliver(make_envelope(feature)).outcome == "stale"
    [event] = rows(db, Earthquake)
    assert event["magnitude"] == 4.6 and event["revision_count"] == 1


# --- changing and merging IDs (risk 2) -------------------------------------------------


@pytest.mark.req("REQ-PER-005")
def test_preferred_id_change_keeps_one_record(db, deliver, make_envelope, make_feature):
    first = make_feature("ak0001", ["ak0001"], updated=1792137700000)
    deliver(make_envelope(first))
    # USGS associates the report with a US event, which becomes the preferred ID.
    renamed = make_feature("us0001", ["us0001", "ak0001"], updated=1792137760000, mag=4.0)
    assert deliver(make_envelope(renamed)).outcome == "applied"

    [event] = rows(db, Earthquake)
    assert event["preferred_event_id"] == "us0001" and event["magnitude"] == 4.0
    assert alias_map(db) == {"ak0001": event["id"], "us0001": event["id"]}
    assert_invariants(db)


@pytest.mark.req("REQ-PER-005", "REQ-PER-004")
def test_late_message_under_old_id_is_stale(db, deliver, make_envelope, make_feature):
    deliver(make_envelope(make_feature("us0001", ["us0001", "ak0001"], updated=1792137760000)))
    old = make_feature("ak0001", ["ak0001"], updated=1792137700000)
    assert deliver(make_envelope(old)).outcome == "stale"
    [event] = rows(db, Earthquake)
    assert event["preferred_event_id"] == "us0001"


@pytest.mark.req("REQ-PER-006")
def test_two_records_merge_into_the_smallest_key(db, deliver, make_envelope, make_feature):
    deliver(make_envelope(make_feature("ak0001", ["ak0001"], updated=1792137700000)))
    deliver(make_envelope(make_feature("us0001", ["us0001"], updated=1792137710000)))
    first_id, second_id = (e["id"] for e in rows(db, Earthquake, order_by=Earthquake.id))

    merged = make_feature("us0001", ["us0001", "ak0001"], updated=1792137800000, mag=4.2)
    result = deliver(make_envelope(merged))

    assert result.outcome == "applied" and result.earthquake_id == first_id
    [event] = rows(db, Earthquake)
    assert event["id"] == first_id and event["preferred_event_id"] == "us0001"
    assert event["magnitude"] == 4.2
    assert event["revision_count"] == 3  # 1 + 1 merged, + this revision
    assert alias_map(db) == {"ak0001": first_id, "us0001": first_id}
    assert {r["earthquake_id"] for r in rows(db, RawEvent)} == {first_id}
    assert second_id not in {e["id"] for e in rows(db, Earthquake)}
    assert_invariants(db)


@pytest.mark.req("REQ-PER-006")
def test_two_records_merge_into_the_smallest_key_even_if_it_is_newer(
    db, deliver, make_envelope, make_feature
):
    """DEF-050: the survivor's database id can be the larger one; its rows and IDs move to it."""
    deliver(make_envelope(make_feature("us0002", ["us0002"], updated=1792137700000)))
    deliver(make_envelope(make_feature("ak0002", ["ak0002"], updated=1792137710000)))
    first_id, second_id = (e["id"] for e in rows(db, Earthquake, order_by=Earthquake.id))

    merged = make_feature("us0002", ["us0002", "ak0002"], updated=1792137800000, mag=4.2)
    result = deliver(make_envelope(merged))

    assert result.outcome == "applied" and result.earthquake_id == second_id
    [event] = rows(db, Earthquake)
    assert event["id"] == second_id and event["key"] == "ak0002"
    assert event["preferred_event_id"] == "us0002" and event["revision_count"] == 3
    assert alias_map(db) == {"ak0002": second_id, "us0002": second_id}
    assert {r["earthquake_id"] for r in rows(db, RawEvent)} == {second_id}
    assert first_id not in {e["id"] for e in rows(db, Earthquake)}
    assert_invariants(db)


@pytest.mark.req("REQ-PER-006")
def test_merge_keeps_the_newest_data_even_from_the_removed_record(
    db, deliver, make_envelope, make_feature
):
    deliver(make_envelope(make_feature("ak0001", ["ak0001"], updated=1792137700000, mag=3.0)))
    deliver(make_envelope(make_feature("us0001", ["us0001"], updated=1792137900000, mag=4.4)))
    # This message is older than us0001's data, so it is stale, but it still links the IDs.
    linking = make_feature("ak0001", ["ak0001", "us0001"], updated=1792137800000, mag=3.5)
    assert deliver(make_envelope(linking)).outcome == "stale"

    [event] = rows(db, Earthquake)
    assert (event["preferred_event_id"], event["magnitude"]) == ("us0001", 4.4)
    assert_invariants(db)


@pytest.mark.req("REQ-PER-006")
def test_three_way_merge(db, deliver, make_envelope, make_feature):
    for i, eid in enumerate(["ak1", "us1", "hv1"]):
        deliver(make_envelope(make_feature(eid, [eid], updated=1792137700000 + i)))
    deliver(make_envelope(make_feature("us1", ["ak1", "us1", "hv1"], updated=1792137800000)))
    [event] = rows(db, Earthquake)
    assert set(alias_map(db)) == {"ak1", "us1", "hv1"}
    assert set(alias_map(db).values()) == {event["id"]}
    assert_invariants(db)


# --- rejections and errors -------------------------------------------------------------


@pytest.mark.req("REQ-PER-001")
def test_invalid_message_is_rejected_with_its_rule(db, deliver, make_envelope, feed_doc):
    feature = json.loads(json.dumps(feed_doc["features"][0]))
    feature["geometry"]["coordinates"][1] = 95.0
    result = deliver(make_envelope(feature))
    assert result.outcome == "rejected" and "GEO-002" in result.reason

    assert rows(db, Earthquake) == []
    [raw] = rows(db, RawEvent)
    assert (raw["outcome"], raw["rule"], raw["event_id"]) == ("rejected", "GEO-002", feature["id"])
    assert raw["earthquake_id"] is None
    assert raw["envelope"]["payload"]["geometry"]["coordinates"][1] == 95.0


@pytest.mark.req("REQ-PER-014")
@pytest.mark.parametrize(
    ("value", "stored_raw"),
    [
        (b"not json at all", "not json at all"),
        (b'{"a": NaN}', '{"a": NaN}'),
        (b'{"a": "nul\\u0000here"}', '{"a": "nul\\u0000here"}'),
        (b"null", "null"),
        (b"\xff\xfe binary", "�� binary"),
    ],
    ids=["text", "nan", "nul-char", "json-null", "invalid-utf8"],
)
def test_unstorable_json_is_kept_as_text_and_rejected(db, deliver, value, stored_raw):
    assert deliver(value).outcome == "rejected"
    [raw] = rows(db, RawEvent)
    assert raw["envelope"] is None and raw["raw_value"] == stored_raw
    assert raw["rule"] == "ENV-001"


@pytest.mark.req("REQ-PER-014")
def test_empty_value_is_rejected(db, deliver):
    assert deliver(None).outcome == "rejected"


@pytest.mark.req("REQ-PER-009")
def test_record_error_stores_the_message_and_reason(db):
    message = IncomingMessage("earthquakes.raw", 1, 9, b"us1", b'{"x": 1}', None)
    with db.begin() as conn:
        record_error(conn, message, "ValueError: boom")
    [raw] = rows(db, RawEvent)
    assert (raw["outcome"], raw["reason"], raw["raw_value"]) == (
        "error",
        "ValueError: boom",
        '{"x": 1}',
    )


# --- database-level protection ---------------------------------------------------------


@pytest.mark.req("REQ-PER-011")
@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("latitude", 91.0),
        ("longitude", -181.0),
        ("depth_km", 1001.0),
        ("magnitude", 11.0),
        ("review_status", "manual"),
        ("source_updated_ms", 1),
    ],
)
def test_check_constraints_hold_without_the_application(db, column, value):
    row = {
        "key": "x1",
        "preferred_event_id": "x1",
        "event_time": NOW_FIXTURE,
        "event_time_ms": 1000,
        "source_updated": NOW_FIXTURE,
        "source_updated_ms": 2000,
        "longitude": 0.0,
        "latitude": 0.0,
        "depth_km": 5.0,
        "event_type": "earthquake",
        "review_status": "reviewed",
        "tsunami": False,
        column: value,
    }
    with pytest.raises(IntegrityError, match="check constraint"), db.begin() as conn:
        conn.execute(insert(Earthquake), row)


@pytest.mark.req("REQ-PER-006")
def test_deleting_an_event_with_aliases_is_refused(db, deliver, make_envelope, feed_doc):
    deliver(make_envelope(feed_doc["features"][0]))
    with pytest.raises(IntegrityError, match="foreign key"), db.begin() as conn:
        conn.execute(text("DELETE FROM earthquakes"))


@pytest.mark.req("REQ-PER-010")
def test_concurrent_deliveries_of_one_event_create_one_record(db, make_envelope, make_feature):
    """Advisory locks stop two consumers from both inserting the same new event."""
    for round_ in range(10):
        value = make_envelope(make_feature(f"race{round_}", [f"race{round_}", f"alias{round_}"]))
        barrier = threading.Barrier(2)
        results, errors = [], []

        def worker(offset, value=value, barrier=barrier, results=results, errors=errors):
            message = IncomingMessage("earthquakes.raw", 0, offset, b"k", value)
            try:
                barrier.wait()
                with db.begin() as conn:
                    results.append(process_message(conn, message, now=NOW_FIXTURE).outcome)
            except Exception as exc:  # collected and asserted below
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(round_ * 10 + i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert sorted(results) == ["applied", "duplicate"]
    assert len(rows(db, Earthquake)) == 10
    assert_invariants(db)


@pytest.mark.req("REQ-PER-018")
def test_processing_times_come_from_the_time_given_not_the_database_clock(
    db, deliver, make_envelope, make_feature
):
    # The database's clock would differ by mode and by run (Milestone 8 parity).
    deliver(make_envelope(make_feature("us5", ["us5"])))
    (quake,) = rows(db, Earthquake)
    assert quake["first_seen_at"] == NOW_FIXTURE and quake["last_applied_at"] == NOW_FIXTURE
    assert {r["first_seen_at"] for r in rows(db, EarthquakeId)} == {NOW_FIXTURE}
