"""Persisting Kafka messages in PostgreSQL: audit row, validation, then the decisions of
processing/ledger.py (identity, merges, newer-only updates), written in one transaction.

Everything for a message, or a batch, happens in one transaction on the given connection, so
a failure leaves no partial state and the messages can simply be processed again.
"""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import Connection, bindparam, delete, func, insert, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from earth_hub.database.models import Earthquake, EarthquakeId, RawEvent
from earth_hub.processing.ledger import Key, Ledger, NewKey, Record
from earth_hub.validation.earthquake_rules import Rejected, ms_to_datetime, validate

logger = logging.getLogger(__name__)

earthquakes = Earthquake.__table__
earthquake_ids = EarthquakeId.__table__
raw_events = RawEvent.__table__


@dataclass(frozen=True)
class IncomingMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None = None


@dataclass(frozen=True)
class ProcessResult:
    outcome: str  # an OUTCOMES value, or "already_processed" for a redelivered offset
    earthquake_id: int | None = None
    reason: str | None = None


def _decode(value: bytes | None) -> tuple[object | None, str | None]:
    """(parsed JSON, None) when the value is JSON that jsonb can hold; else (None, raw text)."""
    text_value = value.decode("utf-8", errors="replace") if value is not None else None
    if text_value is None:
        return None, None
    try:
        # jsonb rejects NaN and Infinity, which Python's json accepts by default.
        parsed = json.loads(text_value, parse_constant=_reject_constant)
    except ValueError:
        return None, _storable(text_value)
    # jsonb cannot store the NUL character; JSON null would leave nothing to audit.
    if "\\u0000" in text_value or parsed is None:
        return None, _storable(text_value)
    return parsed, None


def _reject_constant(name: str):
    raise ValueError(f"{name} is not valid JSON")


def _storable(raw: str) -> str:
    return raw.replace("\x00", "\\u0000")


def _audit_row(message: IncomingMessage, envelope, raw_value) -> dict:
    is_dict = isinstance(envelope, dict)
    event_id = envelope.get("event_id") if is_dict else None
    updated = envelope.get("source_updated_ms") if is_dict else None
    return {
        "kafka_topic": message.topic,
        "kafka_partition": message.partition,
        "kafka_offset": message.offset,
        "kafka_timestamp": ms_to_datetime(message.timestamp_ms) if message.timestamp_ms else None,
        "message_key": message.key.decode("utf-8", errors="replace") if message.key else None,
        "envelope": envelope,
        "raw_value": raw_value,
        "event_id": event_id if isinstance(event_id, str) else None,
        "source_updated_ms": updated if _fits_bigint(updated) else None,
    }


def _fits_bigint(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and -(2**63) <= value < 2**63


def process_message(
    conn: Connection, message: IncomingMessage, *, now: datetime | None = None, locked: bool = False
) -> ProcessResult:
    """`locked`: the caller already holds the advisory locks for this message's IDs."""
    return _process(conn, [message], now or datetime.now(UTC), locked=locked)[0]


def process_batch(
    conn: Connection, messages: list[IncomingMessage], *, now: datetime | None = None
) -> list[ProcessResult]:
    """Process several messages in the caller's one transaction, with exactly the same results
    as processing them one by one in order.

    Done as a set, because the cost per message is round trips to the database, not work in
    it: read what the batch needs in a few queries, decide every outcome in memory in message
    order (processing/ledger.py), then write with a few bulk statements, in the order
    one-by-one processing would have written them.

    The advisory locks for every ID in the batch are taken first, in one sorted order. Taking
    them message by message instead could deadlock two consumers whose batches overlap: one
    holding A and waiting for B while the other holds B and waits for A.
    """
    return _process(conn, messages, now or datetime.now(UTC), locked=False)


def _process(
    conn: Connection, messages: list[IncomingMessage], now: datetime, *, locked: bool
) -> list[ProcessResult]:
    if not messages:
        return []
    decoded = [_decode(m.value) for m in messages]
    if not locked:
        # Serialize work on these IDs, in a fixed order to avoid deadlocks, so two consumers
        # can never create two records for one event or merge the same records twice.
        _lock(conn, {i for envelope, _ in decoded for i in _envelope_ids(envelope)})

    done = _stored_offsets(conn, messages)
    store = PostgresStore(conn)
    ledger = store.load(i for envelope, _ in decoded for i in _envelope_ids(envelope))
    raw_rows: list[tuple[dict, Key | None]] = []
    steps: list[tuple[str, Key | None, str | None] | None] = []
    for message, (envelope, raw_value) in zip(messages, decoded, strict=True):
        if (message.topic, message.partition, message.offset) in done:
            steps.append(None)
            continue
        audit = _audit_row(message, envelope, raw_value)
        try:
            event = validate(envelope, now=now)
        except Rejected as rejected:
            row = audit | {"outcome": "rejected", "rule": rejected.rule, "reason": rejected.reason}
            raw_rows.append((row, None))
            steps.append(("rejected", None, rejected.reason))
            continue
        step = ledger.apply(event, now)
        raw_rows.append(
            (audit | {"outcome": step.outcome, "rule": None, "reason": step.reason}, step.key)
        )
        steps.append((step.outcome, step.key, step.reason))

    ids = store.write(ledger, now)

    if raw_rows:
        # Where each message's record ended up: a later merge moves its rows to the survivor.
        conn.execute(
            pg_insert(raw_events).on_conflict_do_nothing(constraint="uq_raw_events_offset"),
            [
                row | {"earthquake_id": None if key is None else ids[ledger.resolve(key)]}
                for row, key in raw_rows
            ],
        )
    # Each result names the record as it was when that message was processed.
    return [
        ProcessResult("already_processed")
        if step is None
        else ProcessResult(step[0], None if step[1] is None else ids[step[1]], step[2])
        for step in steps
    ]


# The columns a revision sets, as ValidEvent.fields holds them.
_EVENT_COLUMNS = tuple(
    c.name
    for c in earthquakes.columns
    if c.name
    not in ("id", "key", "preferred_event_id", "revision_count", "first_seen_at", "last_applied_at")
)


class PostgresStore:
    """The EventStore (processing/ledger.py) of full mode, on the caller's connection and
    transaction. The caller holds the advisory locks for the IDs it loads."""

    def __init__(self, conn: Connection):
        self.conn = conn

    def load(self, ids) -> Ledger:
        return _load(self.conn, sorted(set(ids)))

    def write(self, ledger: Ledger, now: datetime) -> dict[Key, int]:
        return _write(self.conn, ledger, now)


def _load(conn: Connection, all_ids: list[str]) -> Ledger:
    """The records these IDs point to, with every column, in one query."""
    if not all_ids:
        return Ledger({}, {})
    rows = (
        conn.execute(
            select(earthquake_ids.c.event_id, earthquakes)
            .join(earthquakes, earthquakes.c.id == earthquake_ids.c.earthquake_id)
            .where(earthquake_ids.c.event_id.in_(all_ids))
        )
        .mappings()
        .all()
    )
    records = {
        r["id"]: Record(
            key=r["key"],
            preferred_event_id=r["preferred_event_id"],
            source_updated_ms=r["source_updated_ms"],
            deleted=r["review_status"] == "deleted",
            revision_ids=tuple(r["revision_ids"]),
            revision_count=r["revision_count"],
            first_seen_at=r["first_seen_at"],
            last_applied_at=r["last_applied_at"],
            columns={c: r[c] for c in _EVENT_COLUMNS},
        )
        for r in rows
    }
    return Ledger(records, {r["event_id"]: r["id"] for r in rows})


def _values(record: Record) -> dict:
    return {
        **record.columns,
        "preferred_event_id": record.preferred_event_id,
        "revision_count": record.revision_count,
        "first_seen_at": record.first_seen_at,
        "last_applied_at": record.last_applied_at,
    }


def _write(conn: Connection, ledger: Ledger, now: datetime) -> dict[Key, int]:
    """Write a ledger's decisions, in the order one-by-one processing would have: records
    created (each as it was created, so identities are assigned the same way), merges, then
    the final values, the new IDs. Returns each record's key -> its id."""
    ids: dict[Key, int] = {k: k for k in ledger.records if not isinstance(k, NewKey)}
    for merge in ledger.merges:
        ids.update({k: k for k in merge.others if not isinstance(k, NewKey)})
    if ledger.created:
        created = (
            conn.execute(
                insert(earthquakes).returning(earthquakes.c.id, sort_by_parameter_order=True),
                [
                    _values(ledger.initial[k]) | {"key": ledger.initial[k].key}
                    for k in ledger.created
                ],
            )
            .scalars()
            .all()
        )
        ids.update(zip(ledger.created, created, strict=True))
    for merge in ledger.merges:
        survivor, others = ids[merge.survivor], [ids[k] for k in merge.others]
        for table in (earthquake_ids, raw_events):
            conn.execute(
                update(table)
                .where(table.c.earthquake_id.in_(others))
                .values(earthquake_id=survivor)
            )
        conn.execute(delete(earthquakes).where(earthquakes.c.id.in_(others)))
        logger.info(
            "merged event records",
            extra={
                "stage": "persist",
                "event_id": merge.preferred_event_id,
                "survivor": survivor,
                "merged": others,
                "preferred_event_id": merge.preferred_event_id,
            },
        )
    if ledger.changed:
        columns = [
            *_EVENT_COLUMNS,
            "preferred_event_id",
            "revision_count",
            "first_seen_at",
            "last_applied_at",
        ]
        conn.execute(
            update(earthquakes)
            .where(earthquakes.c.id == bindparam("_id"))
            .values({c: bindparam(f"_{c}") for c in columns}),
            [
                {"_id": ids[k]} | {f"_{c}": v for c, v in _values(ledger.records[k]).items()}
                for k in sorted(ledger.changed, key=lambda k: ids[k])
            ],
        )
    if ledger.new_ids:
        conn.execute(
            pg_insert(earthquake_ids).on_conflict_do_nothing(index_elements=["event_id"]),
            [
                {"event_id": i, "earthquake_id": ids[ledger.resolve(k)], "first_seen_at": now}
                for i, k in ledger.new_ids.items()
            ],
        )
    return ids


def _stored_offsets(conn: Connection, messages) -> set[tuple[str, int, int]]:
    keys = [(m.topic, m.partition, m.offset) for m in messages]
    rows = conn.execute(
        select(
            raw_events.c.kafka_topic, raw_events.c.kafka_partition, raw_events.c.kafka_offset
        ).where(
            tuple_(
                raw_events.c.kafka_topic, raw_events.c.kafka_partition, raw_events.c.kafka_offset
            ).in_(keys)
        )
    ).all()
    return {tuple(r) for r in rows}


def _envelope_ids(envelope) -> set[str]:
    """The IDs a message would lock, read leniently: invalid messages are rejected later."""
    if not isinstance(envelope, dict):
        return set()
    ids = {i for i in envelope.get("ids") or [] if isinstance(i, str) and i}
    if isinstance(envelope.get("event_id"), str) and envelope["event_id"]:
        ids.add(envelope["event_id"])
    return ids


def _lock(conn: Connection, ids) -> None:
    """Advisory locks in Python's sorted order, which every caller uses, so no two
    transactions can wait on each other. One statement: ORDER BY the array position makes
    PostgreSQL take them in that order (volatile output is evaluated after the sort)."""
    ordered = sorted(ids)
    if not ordered:
        return
    conn.execute(
        text(
            "SELECT pg_advisory_xact_lock(hashtextextended(t.id, 0)) "
            "FROM unnest(CAST(:ids AS text[])) WITH ORDINALITY AS t(id, n) ORDER BY t.n"
        ),
        {"ids": ordered},
    )


def record_error(conn: Connection, message: IncomingMessage, error: str) -> None:
    """Store a message that kept failing for an unexpected reason, so the partition moves on."""
    raw = message.value.decode("utf-8", errors="replace") if message.value is not None else None
    row = _audit_row(message, None, _storable(raw) if raw is not None else None)
    conn.execute(
        pg_insert(raw_events)
        .values(**row, outcome="error", reason=error[:2000])
        .on_conflict_do_update(
            constraint="uq_raw_events_offset", set_={"outcome": "error", "reason": error[:2000]}
        )
    )


def ping(conn: Connection) -> None:
    conn.execute(select(func.now()))
