"""Whether an incoming revision replaces the stored one. One rule, used by the consumer (one by
one and in batches) and by backfill and catch-up when they decide what to publish.

Normally the newer `updated` wins. Deletions need care because USGS deletes duplicates: when
two IDs turn out to be one event, the surviving event lists both IDs and the duplicate is
deleted on its own, with only its own ID (DEF-026, seen live on aka2026jntiye and
aka2026jntkwa). Both IDs point to one stored event, so the duplicate's deletion must not hide
the survivor. A deletion of the whole event lists the event's current ID, or is that ID.
"""

from collections.abc import Hashable
from dataclasses import dataclass

from earth_hub.ingestion.events import EventMessage
from earth_hub.validation.earthquake_rules import review_status

DUPLICATE_DELETION = "deletion of {event_id}, a duplicate merged into {preferred}; event kept"


@dataclass(frozen=True)
class Revision:
    event_id: str
    ids: tuple[str, ...]  # always includes event_id
    source_updated_ms: int
    deleted: bool


def compare(stored: Revision, incoming: Revision) -> tuple[str, str | None]:
    """(outcome, reason): "applied", "duplicate" or "stale"; a reason only when the outcome
    is not what the `updated` times alone would give."""
    if incoming.deleted and not stored.deleted and stored.event_id not in incoming.ids:
        return "stale", DUPLICATE_DELETION.format(
            event_id=incoming.event_id, preferred=stored.event_id
        )
    if (
        stored.deleted
        and not incoming.deleted
        and stored.event_id in incoming.ids
        and incoming.event_id not in stored.ids
    ):
        # What is stored is a duplicate's deletion, which arrived before (or instead of) the
        # event that absorbed it. That event wins whatever its `updated`.
        return "applied", None
    if incoming.source_updated_ms > stored.source_updated_ms:
        return "applied", None
    if incoming.source_updated_ms == stored.source_updated_ms:
        return "duplicate", None
    return "stale", None


def needed(
    messages: list[EventMessage],
    event_of: dict[str, Hashable],
    stored: dict[Hashable, Revision],
) -> list[EventMessage]:
    """Of these messages, those that are new, or that the consumer would apply to a stored
    event any of their IDs points to; one per event (the newest if listed twice). `event_of`
    maps known USGS IDs to stored events, `stored` each such event to its revision. Used by
    backfill and catch-up in both modes (database/progress.py, direct/), so both skip the same
    messages."""
    newest: dict[str, EventMessage] = {}
    for message in messages:
        incoming = Revision(
            message.event_id,
            tuple(dict.fromkeys([message.event_id, *message.ids])),
            message.source_updated_ms,
            review_status(message.payload.get("properties") or {}) == "deleted",
        )
        matched = {event_of[i] for i in incoming.ids if i in event_of}
        if matched and all(compare(stored[e], incoming)[0] != "applied" for e in matched):
            continue
        current = newest.get(message.event_id)
        if current is None or message.source_updated_ms > current.source_updated_ms:
            newest[message.event_id] = message
    return list(newest.values())
