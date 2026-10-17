"""Persisting one Kafka message: audit row, validation, identity resolution, newer-only update.

Everything for one message happens in one transaction on the given connection, so a
failure leaves no partial state and the message can simply be processed again.
"""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import Connection, delete, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from earth_hub.database.models import Earthquake, EarthquakeId, RawEvent
from earth_hub.validation.earthquake_rules import Rejected, ValidEvent, ms_to_datetime, validate

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
    conn: Connection, message: IncomingMessage, *, now: datetime | None = None
) -> ProcessResult:
    now = now or datetime.now(UTC)
    envelope, raw_value = _decode(message.value)

    raw_id = conn.execute(
        pg_insert(raw_events)
        .values(**_audit_row(message, envelope, raw_value), outcome="applied")
        .on_conflict_do_nothing(constraint="uq_raw_events_offset")
        .returning(raw_events.c.id)
    ).scalar_one_or_none()
    if raw_id is None:
        return ProcessResult("already_processed")

    try:
        event = validate(envelope, now=now)
    except Rejected as rejected:
        _finish(conn, raw_id, "rejected", rule=rejected.rule, reason=rejected.reason)
        return ProcessResult("rejected", reason=rejected.reason)

    earthquake_id, outcome = _apply(conn, event, now)
    _finish(conn, raw_id, outcome, earthquake_id=earthquake_id)
    return ProcessResult(outcome, earthquake_id)


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


def _finish(conn, raw_id, outcome, *, rule=None, reason=None, earthquake_id=None) -> None:
    conn.execute(
        update(raw_events)
        .where(raw_events.c.id == raw_id)
        .values(outcome=outcome, rule=rule, reason=reason, earthquake_id=earthquake_id)
    )


def _apply(conn: Connection, event: ValidEvent, now: datetime) -> tuple[int, str]:
    # Serialize work on these IDs, in a fixed order to avoid deadlocks, so two consumers
    # can never create two records for one event or merge the same records twice.
    for event_id in sorted(event.ids):
        conn.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:id, 0))"), {"id": event_id}
        )

    matches = sorted(
        conn.execute(
            select(earthquake_ids.c.earthquake_id)
            .where(earthquake_ids.c.event_id.in_(event.ids))
            .distinct()
        ).scalars()
    )

    if not matches:
        earthquake_id = conn.execute(
            insert(earthquakes)
            .values(preferred_event_id=event.event_id, **event.fields, last_applied_at=now)
            .returning(earthquakes.c.id)
        ).scalar_one()
        _add_ids(conn, earthquake_id, event.ids)
        return earthquake_id, "applied"

    survivor, *others = matches
    if others:
        _merge(conn, survivor, others)

    current_updated = conn.execute(
        select(earthquakes.c.source_updated_ms)
        .where(earthquakes.c.id == survivor)
        .with_for_update()
    ).scalar_one()
    # IDs are identity facts, valid even when the data in this message is older.
    _add_ids(conn, survivor, event.ids)

    if event.source_updated_ms > current_updated:
        conn.execute(
            update(earthquakes)
            .where(earthquakes.c.id == survivor)
            .values(
                preferred_event_id=event.event_id,
                **event.fields,
                revision_count=earthquakes.c.revision_count + 1,
                last_applied_at=now,
            )
        )
        return survivor, "applied"
    return survivor, "duplicate" if event.source_updated_ms == current_updated else "stale"


def _add_ids(conn: Connection, earthquake_id: int, ids: tuple[str, ...]) -> None:
    conn.execute(
        pg_insert(earthquake_ids)
        .values([{"event_id": i, "earthquake_id": earthquake_id} for i in ids])
        .on_conflict_do_nothing(index_elements=["event_id"])
    )


def _merge(conn: Connection, survivor: int, others: list[int]) -> None:
    """Fold records that turned out to be the same event into the oldest one."""
    rows = (
        conn.execute(
            select(earthquakes).where(earthquakes.c.id.in_([survivor, *others])).with_for_update()
        )
        .mappings()
        .all()
    )
    by_id = {row["id"]: row for row in rows}
    newest = max(rows, key=lambda r: r["source_updated_ms"])
    total_revisions = sum(r["revision_count"] for r in rows)
    first_seen = min(r["first_seen_at"] for r in rows)

    conn.execute(
        update(earthquake_ids)
        .where(earthquake_ids.c.earthquake_id.in_(others))
        .values(earthquake_id=survivor)
    )
    conn.execute(
        update(raw_events)
        .where(raw_events.c.earthquake_id.in_(others))
        .values(earthquake_id=survivor)
    )
    conn.execute(delete(earthquakes).where(earthquakes.c.id.in_(others)))

    data = {k: v for k, v in newest.items() if k not in ("id", "first_seen_at", "revision_count")}
    if newest["id"] == survivor:
        data = {}
    conn.execute(
        update(earthquakes)
        .where(earthquakes.c.id == survivor)
        .values(**data, revision_count=total_revisions, first_seen_at=first_seen)
    )
    logger.info(
        "merged event records",
        extra={
            "survivor": survivor,
            "merged": others,
            "preferred_event_id": by_id[newest["id"]]["preferred_event_id"],
        },
    )


def ping(conn: Connection) -> None:
    conn.execute(select(func.now()))
