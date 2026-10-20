"""Batched persistence against real PostgreSQL."""

import json
import threading
import time
from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from support.streams import random_stream

from earth_hub.database.engine import make_engine
from earth_hub.database.models import Earthquake, EarthquakeId, RawEvent
from earth_hub.database.repository import IncomingMessage, process_batch, process_message

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)


@pytest.fixture
def second_db(make_database, make_alembic_config, settings):
    from alembic import command

    name = make_database()
    command.upgrade(make_alembic_config(settings.database_url(name)), "head")
    engine = make_engine(settings, name)
    yield engine
    engine.dispose()


def mixed_messages(make_envelope, make_feature, feed_doc) -> list[IncomingMessage]:
    """Every outcome, plus an ID change and a merge, in one ordered stream."""
    values = [make_envelope(f) for f in feed_doc["features"]]  # 6 new events
    values.append(values[0])  # duplicate
    revised = json.loads(json.dumps(feed_doc["features"][1]))
    revised["properties"]["updated"] += 60_000
    revised["properties"]["mag"] = 3.9
    values.append(make_envelope(revised))  # newer
    values.append(make_envelope(feed_doc["features"][1]))  # older again: stale
    values.append(make_envelope(make_feature("ak9", ["ak9"], updated=1792137700000)))
    values.append(make_envelope(make_feature("us9", ["us9"], updated=1792137710000)))
    values.append(
        make_envelope(make_feature("us9", ["us9", "ak9"], updated=1792137800000))
    )  # merge
    bad = json.loads(json.dumps(feed_doc["features"][2]))
    bad["geometry"]["coordinates"][1] = 95.0
    values.append(make_envelope(bad))  # rejected
    values.append(b"not json")  # unstorable, rejected
    deleted = json.loads(json.dumps(feed_doc["features"][3]))
    deleted["properties"].update(status="deleted", updated=deleted["properties"]["updated"] + 1)
    values.append(make_envelope(deleted))  # revised to deleted
    return [IncomingMessage("earthquakes.backfill", 0, i, b"k", v) for i, v in enumerate(values)]


def snapshot(engine) -> dict:
    with engine.connect() as conn:
        # Every column, first_seen_at included: both paths take it from `now`.
        quakes = [
            dict(row) for row in conn.execute(select(Earthquake).order_by(Earthquake.id)).mappings()
        ]
        ids = conn.execute(
            select(EarthquakeId.event_id, EarthquakeId.earthquake_id).order_by(
                EarthquakeId.event_id
            )
        ).all()
        raws = conn.execute(
            select(
                RawEvent.kafka_offset,
                RawEvent.outcome,
                RawEvent.rule,
                RawEvent.reason,
                RawEvent.earthquake_id,
                RawEvent.event_id,
            ).order_by(RawEvent.kafka_offset)
        ).all()
    return {"earthquakes": quakes, "ids": ids, "raw": raws}


@pytest.mark.req("REQ-PER-015")
def test_batch_gives_exactly_the_same_results_as_one_by_one(
    db, second_db, make_envelope, make_feature, feed_doc
):
    messages = mixed_messages(make_envelope, make_feature, feed_doc)

    one_by_one = []
    for message in messages:
        with db.begin() as conn:
            one_by_one.append(process_message(conn, message, now=NOW))
    with second_db.begin() as conn:
        batched = process_batch(conn, messages, now=NOW)

    assert batched == one_by_one
    assert {r.outcome for r in batched} == {"applied", "duplicate", "stale", "rejected"}
    assert snapshot(second_db) == snapshot(db)


@pytest.mark.req("REQ-PER-015", "REQ-PER-002")
def test_redelivered_batch_is_recognized(db, make_envelope, feed_doc):
    messages = [
        IncomingMessage("t", 0, i, b"k", make_envelope(f))
        for i, f in enumerate(feed_doc["features"])
    ]
    with db.begin() as conn:
        process_batch(conn, messages, now=NOW)
    with db.begin() as conn:
        again = process_batch(conn, messages, now=NOW)
    assert {r.outcome for r in again} == {"already_processed"}


@pytest.mark.req("REQ-PER-017", "REQ-PER-010")
def test_overlapping_batches_in_opposite_orders_never_deadlock(db, make_envelope, make_feature):
    """Two overlapping batches, in opposite orders, at the same moment, 20 times: both
    succeed and nothing is stored twice.

    A smoke test only. Each batch takes all its locks in one fast statement, and Python runs
    one thread at a time, so one thread usually finishes locking before the other starts; this
    test cannot reliably catch a wrong lock order. test_locks_are_taken_in_sorted_order does,
    deterministically."""
    size = 50
    for round_ in range(20):
        events = [make_feature(f"r{round_}e{i}", [f"r{round_}e{i}"]) for i in range(size)]
        forward = [
            IncomingMessage("t", 0, round_ * 1000 + i, b"k", make_envelope(e))
            for i, e in enumerate(events)
        ]
        backward = [
            IncomingMessage("t", 1, round_ * 1000 + i, b"k", make_envelope(e))
            for i, e in enumerate(reversed(events))
        ]
        barrier = threading.Barrier(2)
        errors, outcomes = [], []

        def run(batch, barrier=barrier, errors=errors, outcomes=outcomes):
            try:
                barrier.wait()
                with db.begin() as conn:
                    outcomes.extend(r.outcome for r in process_batch(conn, batch, now=NOW))
            except Exception as exc:  # collected and asserted below
                errors.append(exc)

        threads = [threading.Thread(target=run, args=(b,)) for b in (forward, backward)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert errors == [], errors
        assert sorted(outcomes) == ["applied"] * size + ["duplicate"] * size
    with db.connect() as conn:
        assert len(conn.execute(select(Earthquake.id)).all()) == 20 * size


@pytest.mark.req("REQ-PER-015")
@pytest.mark.parametrize("merges", [False, True], ids=["set-based-path", "with-merges"])
def test_random_streams_give_identical_results_batched_or_one_by_one(
    db, second_db, make_envelope, make_feature, monkeypatch, merges
):
    import random

    from earth_hub.database import repository

    fallbacks = [0]
    real = repository.process_message

    def counting(conn, message, **kwargs):
        if kwargs.get("locked"):
            fallbacks[0] += 1
        return real(conn, message, **kwargs)

    monkeypatch.setattr(repository, "process_message", counting)
    offset = 0
    for seed in range(20):
        rng = random.Random(seed)
        values = random_stream(rng, f"s{seed}", make_envelope, make_feature, merges)
        messages = [
            IncomingMessage("t", seed % 3, offset + i, b"k", v) for i, v in enumerate(values)
        ]
        offset += len(values)
        expected = []
        for message in messages:
            with db.begin() as conn:
                expected.append(real(conn, message, now=NOW))
        got, cursor = [], 0
        while cursor < len(messages):
            size = rng.randint(1, 40)
            with second_db.begin() as conn:
                got.extend(process_batch(conn, messages[cursor : cursor + size], now=NOW))
            cursor += size
        assert got == expected, f"seed {seed}"
    assert snapshot(second_db) == snapshot(db)
    kept = [r for r in snapshot(db)["raw"] if r.reason and "a duplicate merged into" in r.reason]
    assert kept, "no stream exercised a duplicate's deletion"
    # Everything goes through the set-based path, merges included (processing/ledger.py).
    assert fallbacks[0] == 0
    # Only a merge deletes a record, leaving a gap in the ids: the streams with merges merged.
    ids = [row["id"] for row in snapshot(db)["earthquakes"]]
    assert (ids != list(range(1, len(ids) + 1))) == merges


@pytest.mark.req("REQ-PER-017")
def test_locks_are_taken_in_sorted_order_whatever_order_they_are_given(db):
    """The property that rules out deadlocks: every transaction takes its locks in the same
    (sorted) order. Connection 1 holds "m"; connection 2 asks for z, m and a. In sorted order
    it takes "a", then waits for "m", and does not yet hold "z". Observed through pg_locks,
    which watches without taking any lock itself."""
    from sqlalchemy import text

    from earth_hub.database.repository import _lock

    advisory = text(
        """
        SELECT k.key, l.granted
        FROM pg_locks l
        JOIN unnest(CAST(:keys AS text[])) AS k(key)
          ON ((l.classid::bigint << 32) | l.objid::bigint) = hashtextextended(k.key, 0)
        WHERE l.locktype = 'advisory' AND l.pid = :pid
        """
    )
    keys = ["lock-a", "lock-m", "lock-z"]
    holder, watcher = db.connect(), db.connect()
    waiter_pid: list[int] = []
    try:
        holder_tx = holder.begin()
        _lock(holder, {"lock-m"})

        def wait_for_locks():
            with db.begin() as conn:
                waiter_pid.append(conn.execute(text("SELECT pg_backend_pid()")).scalar_one())
                _lock(conn, ["lock-z", "lock-m", "lock-a"])  # deliberately unsorted

        waiter = threading.Thread(target=wait_for_locks, daemon=True)
        waiter.start()
        deadline = time.monotonic() + 10
        while True:  # until the waiter is blocked on a lock
            state = (
                dict(watcher.execute(advisory, {"keys": keys, "pid": waiter_pid[0]}).all())
                if waiter_pid
                else {}
            )
            watcher.rollback()
            if False in state.values():
                break
            assert time.monotonic() < deadline, f"the waiter never blocked: {state}"
            time.sleep(0.05)
        assert state == {"lock-a": True, "lock-m": False}, f"wrong lock order: {state}"
        holder_tx.commit()  # release m; the waiter can take m and z and finish
        waiter.join(timeout=10)
        assert not waiter.is_alive()
    finally:
        holder.close()
        watcher.close()


@pytest.mark.req("REQ-MODE-010", "REQ-PER-018")
def test_messages_carrying_processing_times_are_processed_as_the_batches_they_were(
    db, second_db, make_envelope, make_feature
):
    """Replayed from direct mode: one consumer batch spans several of direct mode's rounds,
    each with its own processing time; the result is that of those rounds as batches."""
    import random
    from dataclasses import replace
    from datetime import timedelta

    rng = random.Random(3)
    values = random_stream(rng, "pt", make_envelope, make_feature, merges=True)
    rounds, cursor = [], 0
    while cursor < len(values):
        size = rng.randint(1, 9)
        rounds.append(values[cursor : cursor + size])
        cursor += size
    offset, messages = 0, []
    for number, chunk in enumerate(rounds):
        at = NOW + timedelta(minutes=number, microseconds=number)
        batch = [IncomingMessage("t", 0, offset + i, b"k", v) for i, v in enumerate(chunk)]
        offset += len(chunk)
        with db.begin() as conn:
            expected = process_batch(conn, batch, now=at)
        messages += [replace(m, processed_at=at) for m in batch]
        assert expected  # each round stored as its own batch
    with second_db.begin() as conn:
        process_batch(conn, messages, now=NOW + timedelta(days=9))  # the header wins
    assert snapshot(second_db) == snapshot(db)
