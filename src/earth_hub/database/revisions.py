"""Whether an incoming revision replaces the stored one. One rule, used by the consumer (one by
one and in batches) and by backfill and catch-up when they decide what to publish.

Normally the newer `updated` wins. Deletions need care because USGS deletes duplicates: when
two IDs turn out to be one event, the surviving event lists both IDs and the duplicate is
deleted on its own, with only its own ID (DEF-026, seen live on aka2026jntiye and
aka2026jntkwa). Both IDs point to one stored event, so the duplicate's deletion must not hide
the survivor. A deletion of the whole event lists the event's current ID, or is that ID.
"""

from dataclasses import dataclass

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
