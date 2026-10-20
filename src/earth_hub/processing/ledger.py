"""The decisions of persistence, made in memory: for each validated event, in message order,
which record it belongs to, whether it is applied, which IDs it adds, and which records it
merges. A store loads what a batch of messages touches into a Ledger, applies the events, and
writes the result (database/repository.py for PostgreSQL).

The rules, the same whatever the store:
- An event matches the records of any of its IDs. None: a new record. More than one: they turn
  out to be one event, and are merged into the oldest record, which takes the data of the
  newest one (by `updated`), the sum of their revision counts and the earliest `first_seen_at`.
- Whether the event then replaces the stored revision is decided by `revisions.compare` only.
- IDs are identity facts: every ID of an event is recorded, even when its data is older.
"""

from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Protocol

from earth_hub.database.revisions import Revision, compare
from earth_hub.validation.earthquake_rules import ValidEvent

Key = Any  # a store's identity for a record; records created here get NewKey


@dataclass(frozen=True, order=True)
class NewKey:
    """A record created by this ledger, numbered in creation order."""

    n: int


@dataclass
class Record:
    """One stored event, as the decisions need it. `key` is its identity outside a store: the
    USGS ID it was created with, kept by the surviving record of a merge. `columns` are the
    values of the event's columns (those in ValidEvent.fields)."""

    key: str
    preferred_event_id: str
    source_updated_ms: int
    deleted: bool
    revision_ids: tuple[str, ...]
    revision_count: int
    first_seen_at: datetime
    last_applied_at: datetime
    columns: dict[str, Any] | None = None

    def revision(self) -> Revision:
        return Revision(
            self.preferred_event_id, self.revision_ids, self.source_updated_ms, self.deleted
        )


@dataclass(frozen=True)
class Step:
    outcome: str  # applied, duplicate or stale
    key: Key  # the record the event belongs to after this step
    reason: str | None = None


@dataclass(frozen=True)
class Merge:
    survivor: Key
    others: tuple[Key, ...]
    preferred_event_id: str  # of the newest of the merged records, for the log


def creation_order(key: Key) -> tuple:
    """Records stored before this ledger come first, in their own order; then new ones."""
    return (1, key.n) if isinstance(key, NewKey) else (0, key)


@dataclass
class Ledger:
    records: dict[Key, Record]
    event_of: dict[str, Key]  # USGS ID -> record
    created: list[NewKey] = field(default_factory=list)  # in creation order
    initial: dict[NewKey, Record] = field(default_factory=dict)  # each as it was created
    changed: set[Key] = field(default_factory=set)  # records whose values changed
    merges: list[Merge] = field(default_factory=list)  # in the order they happened
    new_ids: dict[str, Key] = field(default_factory=dict)  # IDs to record -> record

    def apply(self, event: ValidEvent, now: datetime) -> Step:
        matched = {self.event_of[i] for i in event.ids if i in self.event_of}
        keys = sorted(matched, key=creation_order)
        if not keys:
            key = NewKey(len(self.created))
            self.created.append(key)
            self.records[key] = Record(
                key=event.event_id,
                preferred_event_id=event.event_id,
                source_updated_ms=event.source_updated_ms,
                deleted=event.deleted,
                revision_ids=event.ids,
                revision_count=1,
                first_seen_at=now,
                last_applied_at=now,
                columns=dict(event.fields),
            )
            self.initial[key] = self.records[key]
            self._record_ids(event.ids, key)
            return Step("applied", key)

        survivor, *others = keys
        if others:
            self._merge(survivor, others)
        self._record_ids(event.ids, survivor)

        current = self.records[survivor]
        outcome, reason = compare(current.revision(), _revision(event))
        if outcome == "applied":
            self.records[survivor] = replace(
                current,
                preferred_event_id=event.event_id,
                source_updated_ms=event.source_updated_ms,
                deleted=event.deleted,
                revision_ids=event.ids,
                revision_count=current.revision_count + 1,
                last_applied_at=now,
                columns=dict(event.fields),
            )
            self.changed.add(survivor)
        return Step(outcome, survivor, reason)

    def resolve(self, key: Key) -> Key:
        """Where a record ended up: itself, or the record it was merged into."""
        for merge in self.merges:
            if key in merge.others:
                key = merge.survivor
        return key

    def _merge(self, survivor: Key, others: list[Key]) -> None:
        merged = [survivor, *others]
        # The first of equal `updated` times wins: the oldest record.
        newest = max(merged, key=lambda k: self.records[k].source_updated_ms)
        total = sum(self.records[k].revision_count for k in merged)
        first_seen = min(self.records[k].first_seen_at for k in merged)
        base = self.records[newest] if newest != survivor else self.records[survivor]
        self.records[survivor] = replace(
            base, key=self.records[survivor].key, revision_count=total, first_seen_at=first_seen
        )
        for key in others:
            del self.records[key]
        for event_id, key in self.event_of.items():
            if key in others:
                self.event_of[event_id] = survivor
        for event_id, key in self.new_ids.items():
            if key in others:
                self.new_ids[event_id] = survivor
        self.changed = {k for k in self.changed if k not in others} | {survivor}
        self.merges.append(
            Merge(survivor, tuple(others), self.records[survivor].preferred_event_id)
        )

    def _record_ids(self, ids: tuple[str, ...], key: Key) -> None:
        for event_id in ids:
            if event_id not in self.event_of:
                self.event_of[event_id] = key
                self.new_ids[event_id] = key


class EventStore(Protocol):
    """Where records live: PostgreSQL (database/repository.py:PostgresStore) or, in direct
    mode, the bucket. A store holds no decisions of its own; both give the same results."""

    def load(self, ids: Iterable[str]) -> Ledger:
        """The records these USGS IDs point to, with every column, and their IDs."""
        ...

    def write(self, ledger: Ledger, now: datetime) -> dict[Key, Any]:
        """Store a ledger's decisions; return each record's key -> its stored identity."""
        ...


def _revision(event: ValidEvent) -> Revision:
    return Revision(event.event_id, event.ids, event.source_updated_ms, event.deleted)
