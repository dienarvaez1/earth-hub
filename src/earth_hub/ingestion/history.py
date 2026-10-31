"""What catch-up needs to keep stored history in line with USGS (DEF-066, DEF-067).

The audit checks one month of stored history a day: per UTC day, how many events USGS lists
against how many Earth-Hub stores. On a day that differs it lists the day, and looks up by ID
the stored events USGS no longer lists. USGS can delete an event so thoroughly that FDSN keeps
no `id`, `time` or `updated` for it, only its products' `code` and `updateTime` (2026-10-30,
`tx2026pfurqq`): no search by time can ever return such a deletion, only a lookup by ID.

As USGS sends it, that record cannot be stored: no ID to key it, no time to order it, a
geometry of `[0, 0, null]`. A deletion only has to identify and order the event, so the audit
publishes an ordinary one instead: the product's `code` as its ID, the latest product
`updateTime` as its `updated`, and the stored event's time, place and geometry, with USGS's
products kept as they came. The consumer's rules then decide as for any deletion, the
duplicates' rule included (DEF-026).
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from earth_hub.ingestion.events import EventMessage
from earth_hub.validation.earthquake_rules import review_status

DAY = timedelta(days=1)


@dataclass(frozen=True)
class Stored:
    """A stored event the audit compares with USGS (not deleted)."""

    key: str  # the USGS ID the record was created with
    event_id: str  # its preferred ID now
    ids: frozenset[str]  # every ID that points to it
    event_time_ms: int
    longitude: float
    latitude: float
    depth_km: float
    event_type: str
    place: str | None = None


def day_of(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d")


def month_start(moment: datetime) -> datetime:
    return moment.astimezone(UTC).replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def next_month(moment: datetime) -> datetime:
    return month_start(month_start(moment) + timedelta(days=32))


def days(start: datetime, end: datetime) -> list[datetime]:
    """The UTC days that start in [start, end)."""
    out, day = [], start
    while day < end:
        out.append(day)
        day += DAY
    return out


def without_time(record: Any) -> tuple[str, int] | None:
    """(the event's ID, when USGS deleted it) for a deletion that kept no time, else None."""
    if not isinstance(record, dict):
        return None
    props = record.get("properties")
    if not isinstance(props, dict) or review_status(props) != "deleted":
        return None
    if isinstance(props.get("time"), int) and isinstance(props.get("updated"), int):
        return None  # an ordinary deletion: the deleted-events check finds it by time
    products = [
        p
        for kind in (props.get("products") or {}).values()
        if isinstance(kind, list)
        for p in kind
        if isinstance(p, dict) and isinstance(p.get("code"), str) and p["code"]
    ]
    times = [p["updateTime"] for p in products if isinstance(p.get("updateTime"), int)]
    if not products or not times:
        return None  # nothing names the event or says when
    origins = [p for p in products if p.get("type") == "origin"] or products
    return origins[0]["code"], max(times)


def deletion(record: dict, stored: Stored, *, fetched_at: datetime) -> EventMessage | None:
    """The deletion to publish for a stored event USGS deleted without keeping its time."""
    found = without_time(record)
    if found is None:
        return None
    event_id, updated = found
    props = record["properties"]
    feature = {
        "type": "Feature",
        "id": event_id,
        "geometry": {
            "type": "Point",
            "coordinates": [stored.longitude, stored.latitude, stored.depth_km],
        },
        "properties": {
            **{k: v for k, v in props.items() if v is not None},
            "status": "deleted",
            "time": stored.event_time_ms,
            "updated": updated,
            "ids": f",{event_id},",
            "type": stored.event_type,
            "place": stored.place,
        },
    }
    return EventMessage(
        event_id=event_id,
        ids=(event_id,),
        source_updated_ms=updated,
        event_time_ms=stored.event_time_ms,
        source="audit",
        feed_generated_ms=None,
        fetched_at=fetched_at,
        payload=feature,
    )
