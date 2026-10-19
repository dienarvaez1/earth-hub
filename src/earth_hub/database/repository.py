"""Persisting one Kafka message: audit row, validation, identity resolution, newer-only update.

Everything for one message happens in one transaction on the given connection, so a
failure leaves no partial state and the message can simply be processed again.
"""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import Connection, bindparam, delete, func, insert, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from earth_hub.database.models import Earthquake, EarthquakeId, RawEvent
from earth_hub.database.revisions import Revision, compare
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
    conn: Connection, message: IncomingMessage, *, now: datetime | None = None, locked: bool = False
) -> ProcessResult:
    """`locked`: the caller already holds the advisory locks for this message's IDs."""
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

    earthquake_id, outcome, reason = _apply(conn, event, now, locked=locked)
    _finish(conn, raw_id, outcome, reason=reason, earthquake_id=earthquake_id)
    return ProcessResult(outcome, earthquake_id, reason)


def process_batch(
    conn: Connection, messages: list[IncomingMessage], *, now: datetime | None = None
) -> list[ProcessResult]:
    """Process several messages in the caller's one transaction, with exactly the same results
    as processing them one by one in order.

    Done as a set, because the cost per message is round trips to the database, not work in
    it: read what the batch needs in three queries, decide every outcome in memory in message
    order, then write with a few bulk statements. A batch in which any message would merge two
    records goes through process_message one by one instead; merges are rare.

    The advisory locks for every ID in the batch are taken first, in one sorted order. Taking
    them message by message instead could deadlock two consumers whose batches overlap: one
    holding A and waiting for B while the other holds B and waits for A.
    """
    now = now or datetime.now(UTC)
    if not messages:
        return []
    decoded = [_decode(m.value) for m in messages]
    _lock(conn, {i for envelope, _ in decoded for i in _envelope_ids(envelope)})

    done = _stored_offsets(conn, messages)
    known_ids, stored = _stored_events(conn, decoded)
    plan = _BatchPlan(known_ids, stored)
    for message, (envelope, raw_value) in zip(messages, decoded, strict=True):
        if (message.topic, message.partition, message.offset) in done:
            plan.results.append(ProcessResult("already_processed"))
            continue
        if not plan.add(message, envelope, raw_value, now):
            # A merge: replay the whole batch one message at a time. Nothing was written yet,
            # and the locks are already held.
            return [process_message(conn, m, now=now, locked=True) for m in messages]
    return plan.write(conn, now)


class _BatchPlan:
    """Outcomes of a batch, decided in memory exactly as process_message would decide them."""

    def __init__(self, known_ids: dict[str, int], stored: dict[int, dict]):
        self.event_of: dict[str, object] = dict(known_ids)  # USGS ID -> event key
        self.events: dict[object, dict] = {k: dict(v) for k, v in stored.items()}
        self.new_events: list[object] = []  # keys of events this batch creates, in order
        self.changed: set[object] = set()  # stored events this batch updates
        self.new_ids: dict[str, object] = {}  # USGS IDs to add -> event key
        self.raw_rows: list[tuple[dict, object]] = []
        self.results: list = []

    def add(self, message, envelope, raw_value, now) -> bool:
        """Plan one message. False if it would merge records (the caller falls back)."""
        audit = _audit_row(message, envelope, raw_value)
        try:
            event = validate(envelope, now=now)
        except Rejected as rejected:
            row = audit | {"outcome": "rejected", "rule": rejected.rule, "reason": rejected.reason}
            self.raw_rows.append((row, None))
            self.results.append(("rejected", None, rejected.reason))
            return True

        keys = {self.event_of[i] for i in event.ids if i in self.event_of}
        if len(keys) > 1:
            return False
        if not keys:
            key = ("new", len(self.new_events))
            self.new_events.append(key)
            self.events[key] = {
                "source_updated_ms": event.source_updated_ms,
                "preferred_event_id": event.event_id,
                "deleted": event.deleted,
                "revision_ids": event.ids,
                "fields": event.fields,
                "revision_count": 1,
            }
            outcome, reason = "applied", None
        else:
            (key,) = keys
            current = self.events[key]
            outcome, reason = compare(_stored_revision(current), _revision(event))
            if outcome == "applied":
                current.update(
                    source_updated_ms=event.source_updated_ms,
                    preferred_event_id=event.event_id,
                    deleted=event.deleted,
                    revision_ids=event.ids,
                    fields=event.fields,
                    revision_count=current["revision_count"] + 1,
                )
                if not isinstance(key, tuple):
                    self.changed.add(key)
        for event_id in event.ids:  # identity facts, recorded even for stale messages
            if event_id not in self.event_of:
                self.event_of[event_id] = key
                self.new_ids[event_id] = key
        self.raw_rows.append((audit | {"outcome": outcome, "rule": None, "reason": reason}, key))
        self.results.append((outcome, key, reason))
        return True

    def write(self, conn: Connection, now: datetime) -> list[ProcessResult]:
        ids: dict[object, int] = {k: k for k in self.events if not isinstance(k, tuple)}
        if self.new_events:
            rows = [
                {
                    "preferred_event_id": self.events[k]["preferred_event_id"],
                    **self.events[k]["fields"],
                    "revision_count": self.events[k]["revision_count"],
                    "last_applied_at": now,
                }
                for k in self.new_events
            ]
            # In message order, so identities are assigned as one-by-one processing would.
            created = (
                conn.execute(
                    insert(earthquakes).returning(earthquakes.c.id, sort_by_parameter_order=True),
                    rows,
                )
                .scalars()
                .all()
            )
            ids.update(zip(self.new_events, created, strict=True))
        if self.changed:
            conn.execute(
                update(earthquakes)
                .where(earthquakes.c.id == bindparam("_id"))
                .values({c: bindparam(f"_{c}") for c in _UPDATABLE}),
                [
                    {"_id": k}
                    | {
                        f"_{c}": v
                        for c, v in (
                            self.events[k]["fields"]
                            | {
                                "preferred_event_id": self.events[k]["preferred_event_id"],
                                "revision_count": self.events[k]["revision_count"],
                                "last_applied_at": now,
                            }
                        ).items()
                    }
                    for k in sorted(self.changed)
                ],
            )
        if self.new_ids:
            conn.execute(
                pg_insert(earthquake_ids).on_conflict_do_nothing(index_elements=["event_id"]),
                [{"event_id": i, "earthquake_id": ids[k]} for i, k in self.new_ids.items()],
            )
        if self.raw_rows:
            conn.execute(
                pg_insert(raw_events).on_conflict_do_nothing(constraint="uq_raw_events_offset"),
                [
                    row | {"earthquake_id": ids[k] if k is not None else None}
                    for row, k in self.raw_rows
                ],
            )
        return [
            r
            if isinstance(r, ProcessResult)
            else ProcessResult(r[0], ids[r[1]] if r[1] is not None else None, r[2])
            for r in self.results
        ]


_UPDATABLE = (
    "preferred_event_id",
    "event_time",
    "event_time_ms",
    "source_updated",
    "source_updated_ms",
    "magnitude",
    "mag_type",
    "place",
    "city",
    "region",
    "country",
    "longitude",
    "latitude",
    "depth_km",
    "event_type",
    "review_status",
    "tsunami",
    "felt",
    "cdi",
    "mmi",
    "alert",
    "sig",
    "net",
    "url",
    "title",
    "revision_ids",
    "revision_count",
    "last_applied_at",
)


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


def _stored_events(conn: Connection, decoded) -> tuple[dict[str, int], dict[int, dict]]:
    """USGS ID -> event id, and the stored state of those events, in one query."""
    all_ids = sorted({i for envelope, _ in decoded for i in _envelope_ids(envelope)})
    if not all_ids:
        return {}, {}
    rows = conn.execute(
        select(
            earthquake_ids.c.event_id,
            earthquakes.c.id,
            earthquakes.c.source_updated_ms,
            earthquakes.c.preferred_event_id,
            earthquakes.c.review_status,
            earthquakes.c.revision_ids,
            earthquakes.c.revision_count,
        )
        .join(earthquakes, earthquakes.c.id == earthquake_ids.c.earthquake_id)
        .where(earthquake_ids.c.event_id.in_(all_ids))
    ).all()
    known = {r.event_id: r.id for r in rows}
    stored = {
        r.id: {
            "source_updated_ms": r.source_updated_ms,
            "preferred_event_id": r.preferred_event_id,
            "deleted": r.review_status == "deleted",
            "revision_ids": tuple(r.revision_ids),
            "revision_count": r.revision_count,
            "fields": None,
        }
        for r in rows
    }
    return known, stored


def _revision(event: ValidEvent) -> Revision:
    return Revision(event.event_id, event.ids, event.source_updated_ms, event.deleted)


def _stored_revision(state) -> Revision:
    """From a planned event (dict) or a stored row (mapping) alike."""
    return Revision(
        state["preferred_event_id"],
        tuple(state["revision_ids"]),
        state["source_updated_ms"],
        state["deleted"],
    )


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


def _finish(conn, raw_id, outcome, *, rule=None, reason=None, earthquake_id=None) -> None:
    conn.execute(
        update(raw_events)
        .where(raw_events.c.id == raw_id)
        .values(outcome=outcome, rule=rule, reason=reason, earthquake_id=earthquake_id)
    )


def _apply(conn: Connection, event: ValidEvent, now: datetime, *, locked: bool = False):
    # Serialize work on these IDs, in a fixed order to avoid deadlocks, so two consumers
    # can never create two records for one event or merge the same records twice.
    if not locked:
        _lock(conn, event.ids)

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
        return earthquake_id, "applied", None

    survivor, *others = matches
    if others:
        _merge(conn, survivor, others)

    current = (
        conn.execute(
            select(
                earthquakes.c.preferred_event_id,
                earthquakes.c.revision_ids,
                earthquakes.c.source_updated_ms,
                (earthquakes.c.review_status == "deleted").label("deleted"),
            )
            .where(earthquakes.c.id == survivor)
            .with_for_update()
        )
        .mappings()
        .one()
    )
    # IDs are identity facts, valid even when the data in this message is older.
    _add_ids(conn, survivor, event.ids)

    outcome, reason = compare(_stored_revision(current), _revision(event))
    if outcome == "applied":
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
    return survivor, outcome, reason


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
            "stage": "persist",
            "event_id": by_id[newest["id"]]["preferred_event_id"],
            "survivor": survivor,
            "merged": others,
            "preferred_event_id": by_id[newest["id"]]["preferred_event_id"],
        },
    )


def ping(conn: Connection) -> None:
    conn.execute(select(func.now()))
