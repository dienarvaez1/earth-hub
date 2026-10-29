"""Queries behind the API. Every value reaches SQL as a bound parameter; sort columns come
from a fixed mapping, never from the request text."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import (
    Connection,
    and_,
    asc,
    case,
    cast,
    desc,
    func,
    literal,
    select,
)
from sqlalchemy import Integer as SqlInteger
from sqlalchemy.dialects.postgresql import aggregate_order_by

from earth_hub.database.models import Earthquake, EarthquakeId

earthquakes = Earthquake.__table__
earthquake_ids = EarthquakeId.__table__

SORT_COLUMNS = {
    "event_time": earthquakes.c.event_time,
    "magnitude": earthquakes.c.magnitude,
    "depth": earthquakes.c.depth_km,
}
# (label, inclusive min, exclusive max), in display order: one-unit buckets up to TOP_BUCKET.
TOP_BUCKET = 7
MAGNITUDE_BUCKETS = [
    ("<0", None, 0.0),
    *[(f"{n}-{n + 1}", float(n), float(n + 1)) for n in range(TOP_BUCKET)],
    (f">={TOP_BUCKET}", float(TOP_BUCKET), None),
    ("unknown", None, None),
]


@dataclass(frozen=True)
class Filters:
    min_magnitude: float | None = None
    max_magnitude: float | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    place: str | None = None
    event_type: str | None = None
    review_status: str | None = None
    include_deleted: bool = False

    def clauses(self) -> list:
        c = earthquakes.c
        clauses = []
        if self.min_magnitude is not None:
            clauses.append(c.magnitude >= self.min_magnitude)
        if self.max_magnitude is not None:
            clauses.append(c.magnitude <= self.max_magnitude)
        if self.start_time is not None:
            clauses.append(c.event_time >= self.start_time)
        if self.end_time is not None:
            clauses.append(c.event_time < self.end_time)
        if self.place:
            clauses.append(c.place.ilike(f"%{escape_like(self.place)}%", escape="\\"))
        if self.event_type is not None:
            clauses.append(c.event_type == self.event_type)
        if self.review_status is not None:
            clauses.append(c.review_status == self.review_status)
        elif not self.include_deleted:
            clauses.append(c.review_status != "deleted")
        return clauses


def escape_like(value: str) -> str:
    """Make %, _ and \\ match themselves in an ILIKE pattern."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _ids_column():
    return (
        select(
            func.array_agg(aggregate_order_by(earthquake_ids.c.event_id, earthquake_ids.c.event_id))
        )
        .where(earthquake_ids.c.earthquake_id == earthquakes.c.id)
        # Correlate with earthquakes only: the lookup query also joins earthquake_ids.
        .correlate(earthquakes)
        .scalar_subquery()
        .label("ids")
    )


_INTERNAL_COLUMNS = (
    "id",
    "preferred_event_id",
    "event_time_ms",
    "source_updated_ms",
    "revision_ids",
)


def _event_columns():
    c = earthquakes.c
    return [
        c.preferred_event_id.label("event_id"),
        _ids_column(),
        *[col for col in earthquakes.c if col.name not in _INTERNAL_COLUMNS],
    ]


def list_earthquakes(
    conn: Connection, filters: Filters, *, sort: str, order: str, limit: int, offset: int
) -> tuple[list[dict[str, Any]], int]:
    where = and_(true_(), *filters.clauses())
    total = conn.execute(select(func.count()).select_from(earthquakes).where(where)).scalar_one()
    direction = asc if order == "asc" else desc
    rows = conn.execute(
        select(*_event_columns())
        .where(where)
        # Missing magnitudes sort last either way; the id makes ties, and pages, stable.
        .order_by(direction(SORT_COLUMNS[sort]).nulls_last(), earthquakes.c.id)
        .limit(limit)
        .offset(offset)
    ).mappings()
    return [dict(r) for r in rows], total


def get_earthquake(conn: Connection, event_id: str) -> dict[str, Any] | None:
    """Look an event up by any of its IDs, current or former."""
    row = (
        conn.execute(
            select(*_event_columns())
            .select_from(
                earthquakes.join(earthquake_ids, earthquake_ids.c.earthquake_id == earthquakes.c.id)
            )
            .where(earthquake_ids.c.event_id == event_id)
        )
        .mappings()
        .one_or_none()
    )
    return dict(row) if row else None


def export_earthquakes(conn: Connection, *, after_id: int, limit: int) -> list[dict[str, Any]]:
    """Whole rows, every column, in id order. Paging by id (not offset) is stable while
    events are inserted or updated between pages."""
    rows = conn.execute(
        select(earthquakes)
        .where(earthquakes.c.id > after_id)
        .order_by(earthquakes.c.id)
        .limit(limit)
    ).mappings()
    return [dict(r) for r in rows]


def stats(conn: Connection, filters: Filters) -> dict[str, Any]:
    where = and_(true_(), *filters.clauses())
    c = earthquakes.c
    count, mag_min, mag_max, mag_avg = conn.execute(
        select(func.count(), func.min(c.magnitude), func.max(c.magnitude), func.avg(c.magnitude))
        .select_from(earthquakes)
        .where(where)
    ).one()
    by_type = conn.execute(
        select(c.event_type, func.count()).where(where).group_by(c.event_type)
    ).all()
    mag_type = func.coalesce(c.mag_type, literal("unknown"))
    by_mag_type = conn.execute(select(mag_type, func.count()).where(where).group_by(mag_type)).all()
    return {
        "count": count,
        "magnitude": None
        if mag_min is None
        else {"min": mag_min, "max": mag_max, "avg": round(float(mag_avg), 4)},
        "by_event_type": dict(sorted(by_type)),
        "by_magnitude_type": dict(sorted(by_mag_type)),
    }


def hour_starts(start: datetime, end: datetime) -> list[datetime]:
    """Every UTC hour from the one containing `start` up to (not including) `end`."""
    hour = start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    hours = []
    while hour < end:
        hours.append(hour)
        hour += timedelta(hours=1)
    return hours


def hourly_summary(
    conn: Connection, filters: Filters, start: datetime, end: datetime
) -> list[dict[str, Any]]:
    """Counts for every hour in the range, zeros included. `filters` must carry start/end."""
    # date_trunc on the UTC wall-clock time, so buckets never depend on the server timezone.
    hour = func.date_trunc("hour", func.timezone("UTC", earthquakes.c.event_time))
    counts = {
        h.replace(tzinfo=UTC): n
        for h, n in conn.execute(
            select(hour, func.count()).where(and_(true_(), *filters.clauses())).group_by(hour)
        ).all()
    }
    return [{"hour": h, "count": counts.get(h, 0)} for h in hour_starts(start, end)]


def magnitude_summary(conn: Connection, filters: Filters) -> list[dict[str, Any]]:
    m = earthquakes.c.magnitude
    floor_label = func.concat(
        cast(func.floor(m), SqlInteger), literal("-"), cast(func.floor(m), SqlInteger) + 1
    )
    label = case(
        (m.is_(None), literal("unknown")),
        (m < 0, literal("<0")),
        (m >= TOP_BUCKET, literal(f">={TOP_BUCKET}")),
        else_=floor_label,
    )
    counts = dict(
        conn.execute(
            select(label, func.count()).where(and_(true_(), *filters.clauses())).group_by(label)
        ).all()
    )
    return [
        {"label": name, "min": low, "max": high, "count": counts.get(name, 0)}
        for name, low, high in MAGNITUDE_BUCKETS
    ]


def latest_event_time(conn: Connection) -> datetime | None:
    return conn.execute(select(func.max(earthquakes.c.event_time))).scalar_one()


def true_():
    return literal(True)
