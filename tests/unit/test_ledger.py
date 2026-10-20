"""The decisions of persistence (processing/ledger.py), in memory: no database. The same rules
are checked against PostgreSQL in tests/integration/test_repository.py and test_batch.py."""

from datetime import UTC, datetime, timedelta

import pytest

from earth_hub.processing.ledger import Ledger, NewKey, Record
from earth_hub.validation.earthquake_rules import ValidEvent

T0 = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


def event(event_id, ids=None, updated=1000, *, status="reviewed", mag=3.0) -> ValidEvent:
    ids = tuple(ids or [event_id])
    fields = {"review_status": status, "magnitude": mag, "revision_ids": list(ids)}
    return ValidEvent(event_id, ids, updated, 0, fields)


def at(minutes: int) -> datetime:
    return T0 + timedelta(minutes=minutes)


def stored(preferred, ids, updated, *, count=1, seen=0, deleted=False) -> Record:
    return Record(
        preferred_event_id=preferred,
        source_updated_ms=updated,
        deleted=deleted,
        revision_ids=tuple(ids),
        revision_count=count,
        first_seen_at=at(seen),
        last_applied_at=at(seen),
        columns={"review_status": "deleted" if deleted else "reviewed", "magnitude": 1.0},
    )


def ledger(*records: tuple[int, Record]) -> Ledger:
    return Ledger(dict(records), {i: key for key, r in records for i in r.revision_ids})


@pytest.mark.req("REQ-PER-003", "REQ-PER-004", "REQ-PER-018")
def test_new_then_newer_then_same_then_older():
    book = Ledger({}, {})
    first = book.apply(event("us1", updated=1000, mag=3.0), at(0))
    assert (first.outcome, first.key) == ("applied", NewKey(0))
    assert book.apply(event("us1", updated=2000, mag=3.5), at(1)).outcome == "applied"
    assert book.apply(event("us1", updated=2000, mag=9.9), at(2)).outcome == "duplicate"
    assert book.apply(event("us1", updated=1500, mag=9.9), at(3)).outcome == "stale"
    record = book.records[NewKey(0)]
    assert (record.columns["magnitude"], record.revision_count) == (3.5, 2)
    assert (record.first_seen_at, record.last_applied_at) == (at(0), at(1))
    assert book.created == [NewKey(0)] and book.initial[NewKey(0)].columns["magnitude"] == 3.0


@pytest.mark.req("REQ-PER-005")
def test_a_changed_preferred_id_stays_one_record_and_keeps_the_old_id():
    book = ledger((7, stored("ak1", ["ak1"], 1000)))
    step = book.apply(event("us1", ["ak1", "us1"], updated=2000), at(5))
    assert (step.outcome, step.key) == ("applied", 7)
    assert book.records[7].preferred_event_id == "us1"
    assert book.new_ids == {"us1": 7} and book.event_of["ak1"] == 7


@pytest.mark.req("REQ-PER-004")
def test_a_stale_message_still_records_its_new_ids():
    book = ledger((7, stored("us1", ["us1"], 5000)))
    assert book.apply(event("us1", ["us1", "nc1"], updated=1000), at(1)).outcome == "stale"
    assert book.new_ids == {"nc1": 7} and not book.changed


@pytest.mark.req("REQ-PER-006")
def test_records_that_are_one_event_merge_into_the_oldest_with_the_newest_data():
    book = ledger(
        (3, stored("ak1", ["ak1"], 2000, count=2, seen=10)),
        (8, stored("us1", ["us1"], 1000, count=1, seen=5)),
    )
    step = book.apply(event("us1", ["us1", "ak1"], updated=1500), at(20))
    # The oldest record survives with the newest data (ak1's, updated 2000), so the merging
    # revision, older than that, is stale; counts add up, the earliest first_seen is kept.
    assert (step.outcome, step.key) == ("stale", 3)
    survivor = book.records[3]
    assert survivor.preferred_event_id == "ak1" and survivor.source_updated_ms == 2000
    assert survivor.revision_count == 3 and survivor.first_seen_at == at(5)
    assert 8 not in book.records and book.event_of["us1"] == 3
    (merge,) = book.merges
    assert (merge.survivor, merge.others) == (3, (8,)) and book.resolve(8) == 3


@pytest.mark.req("REQ-PER-006")
def test_a_three_way_merge_and_a_newer_merging_revision():
    book = ledger(
        (1, stored("a1", ["a1"], 1000)),
        (2, stored("b1", ["b1"], 3000)),
        (4, stored("c1", ["c1"], 2000)),
    )
    step = book.apply(event("b1", ["a1", "b1", "c1"], updated=4000, mag=6.1), at(9))
    assert (step.outcome, step.key) == ("applied", 1)
    survivor = book.records[1]
    assert survivor.preferred_event_id == "b1" and survivor.columns["magnitude"] == 6.1
    assert survivor.revision_count == 4 and survivor.last_applied_at == at(9)
    assert set(book.records) == {1} and book.merges[0].others == (2, 4)


@pytest.mark.req("REQ-PER-006", "REQ-PER-015")
def test_records_created_and_merged_within_one_batch():
    book = Ledger({}, {})
    book.apply(event("ak9", updated=1000), at(0))
    book.apply(event("us9", updated=1100), at(1))
    step = book.apply(event("us9", ["us9", "ak9"], updated=1200), at(2))
    # Both were created, so both are inserted, then the newer is merged away: identities
    # are assigned exactly as one-by-one processing assigns them.
    assert book.created == [NewKey(0), NewKey(1)]
    assert step.key == NewKey(0) and book.resolve(NewKey(1)) == NewKey(0)
    assert book.initial[NewKey(1)].preferred_event_id == "us9"
    assert book.records[NewKey(0)].revision_count == 3


@pytest.mark.req("REQ-DQ-DEL-002")
def test_a_duplicates_deletion_does_not_hide_the_event_it_was_merged_into():
    # aka2026jntkwa was merged into aka2026jntiye, then deleted on its own (DEF-026).
    book = ledger((5, stored("iye", ["iye", "kwa"], 1000)))
    step = book.apply(event("kwa", ["kwa"], updated=2000, status="deleted"), at(1))
    assert step.outcome == "stale" and "duplicate merged into iye" in step.reason
    assert not book.records[5].deleted and not book.changed


@pytest.mark.req("REQ-DQ-DEL-002")
def test_the_event_that_absorbed_a_stored_deletion_wins_whatever_its_updated():
    book = ledger((5, stored("kwa", ["kwa"], 2000, deleted=True)))
    step = book.apply(event("iye", ["iye", "kwa"], updated=1000), at(1))
    assert step.outcome == "applied" and not book.records[5].deleted


def test_resolve_follows_merges_in_order():
    book = ledger(
        (1, stored("a", ["a"], 1)), (2, stored("b", ["b"], 2)), (3, stored("c", ["c"], 3))
    )
    book.apply(event("c", ["b", "c"], updated=4), at(1))  # 3 into 2
    book.apply(event("a", ["a", "b"], updated=5), at(2))  # 2 into 1
    assert [book.resolve(k) for k in (1, 2, 3)] == [1, 1, 1]
