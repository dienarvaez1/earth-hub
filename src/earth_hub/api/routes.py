"""API v1 endpoints. Fixed paths are declared before /earthquakes/{event_id}, so that
'stats' and 'summary' are never taken for event IDs."""

import re
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Path, Query, Request
from fastapi.responses import JSONResponse
from pydantic import AfterValidator, BeforeValidator
from sqlalchemy import Engine

from earth_hub import __version__
from earth_hub.api import services
from earth_hub.api.errors import ERROR_RESPONSES, ApiError
from earth_hub.api.schemas import (
    Earthquake,
    EarthquakePage,
    Health,
    HourlySummary,
    MagnitudeSummary,
    Stats,
)
from earth_hub.validation.earthquake_rules import MAGNITUDE_RANGE

router = APIRouter(prefix="/api/v1")

MAX_LIMIT = 500
# Far beyond any realistic depth (USGS publishes ~110,000 events a year), and well inside
# PostgreSQL's bigint, which a larger value would overflow (a 500 found by Schemathesis).
MAX_OFFSET = 10_000_000
MAX_HOURLY_RANGE = timedelta(days=31)
DEFAULT_HOURLY_RANGE = timedelta(hours=24)


class SortField(StrEnum):
    event_time = "event_time"
    magnitude = "magnitude"
    depth = "depth"


class SortOrder(StrEnum):
    asc = "asc"
    desc = "desc"


def get_engine(request: Request) -> Engine:
    # Routes connect only inside their body. FastAPI resolves dependencies before it
    # validates query parameters, so connecting here would turn a bad request into a 503
    # whenever the database is down, instead of a 422.
    return request.app.state.engine


MIN_YEAR, MAX_YEAR = 1900, 2099
_DATE_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}")


def parse_date_time(value):
    """Accept ISO 8601 date-times only. Pydantic's lax mode would also accept numbers
    such as 0.5 as Unix timestamps, which the documented contract does not allow."""
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not _DATE_TIME.match(value):
        raise ValueError("must be an ISO 8601 date-time, e.g. 2026-10-16T08:00:00Z")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"must be an ISO 8601 date-time: {exc}") from None
    # Stored events start in 1900 (rule TIME-001). The bounds also keep window arithmetic
    # such as "24 hours before end_time" from overflowing datetime near year 1 or 9999.
    if not MIN_YEAR <= parsed.year <= MAX_YEAR:
        raise ValueError(f"year must be between {MIN_YEAR} and {MAX_YEAR}")
    return parsed


DateTime = Annotated[datetime, BeforeValidator(parse_date_time)]


def reject_nul(value: str | None) -> str | None:
    # PostgreSQL text cannot hold NUL; without this, %00 in a parameter is a 500.
    if value is not None and "\x00" in value:
        raise ValueError("must not contain NUL characters")
    return value


Text = Annotated[str, AfterValidator(reject_nul)]


def as_utc(value: datetime | None) -> datetime | None:
    """Times without a timezone are taken as UTC."""
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


Magnitude = Annotated[float | None, Query(ge=MAGNITUDE_RANGE[0], le=MAGNITUDE_RANGE[1])]


def filter_params(
    min_magnitude: Magnitude = None,
    max_magnitude: Magnitude = None,
    start_time: Annotated[
        DateTime | None,
        Query(description="Inclusive, ISO 8601. Without a timezone, UTC is assumed."),
    ] = None,
    end_time: Annotated[
        DateTime | None,
        Query(description="Exclusive, ISO 8601. Without a timezone, UTC is assumed."),
    ] = None,
    place: Annotated[
        Text | None,
        Query(min_length=1, max_length=200, description="Case-insensitive substring of place"),
    ] = None,
    event_type: Annotated[
        Text | None, Query(min_length=1, max_length=50, description="e.g. earthquake, explosion")
    ] = None,
    review_status: Literal["automatic", "reviewed", "deleted"] | None = None,
    include_deleted: Annotated[
        bool, Query(description="Include events USGS marked deleted (excluded by default)")
    ] = False,
) -> services.Filters:
    start, end = as_utc(start_time), as_utc(end_time)
    problems = []
    if min_magnitude is not None and max_magnitude is not None and min_magnitude > max_magnitude:
        problems.append(("min_magnitude", "must not be greater than max_magnitude"))
    if start and end and start >= end:
        problems.append(("start_time", "must be before end_time"))
    if problems:
        raise ApiError.invalid(problems)
    return services.Filters(
        min_magnitude=min_magnitude,
        max_magnitude=max_magnitude,
        start_time=start,
        end_time=end,
        place=place,
        event_type=event_type,
        review_status=review_status,
        include_deleted=include_deleted,
    )


Db = Annotated[Engine, Depends(get_engine)]
FilterSet = Annotated[services.Filters, Depends(filter_params)]


@router.get(
    "/health",
    response_model=Health,
    responses={503: {"model": Health, "description": "Database unavailable"}},
    summary="API and database health",
)
def health(db: Db):
    try:
        with db.connect() as conn:
            latest = services.latest_event_time(conn)
    except Exception:
        body = Health(
            status="unavailable",
            database="unavailable",
            latest_event_time=None,
            version=__version__,
        )
        return JSONResponse(status_code=503, content=body.model_dump(mode="json"))
    return Health(status="ok", database="ok", latest_event_time=latest, version=__version__)


@router.get(
    "/earthquakes",
    response_model=EarthquakePage,
    responses=ERROR_RESPONSES,
    summary="Paginated, filterable list of events",
)
def list_earthquakes(
    db: Db,
    filters: FilterSet,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 20,
    offset: Annotated[
        int, Query(ge=0, le=MAX_OFFSET, description="Use filters to narrow deeper result sets")
    ] = 0,
    sort: SortField = SortField.event_time,
    order: SortOrder = SortOrder.desc,
):
    with db.connect() as conn:
        items, total = services.list_earthquakes(
            conn, filters, sort=sort.value, order=order.value, limit=limit, offset=offset
        )
    return EarthquakePage(items=items, total=total, limit=limit, offset=offset)


@router.get(
    "/earthquakes/stats",
    response_model=Stats,
    responses=ERROR_RESPONSES,
    summary="Aggregate statistics over the filtered events",
)
def earthquake_stats(db: Db, filters: FilterSet):
    with db.connect() as conn:
        return services.stats(conn, filters)


@router.get(
    "/earthquakes/summary/hourly",
    response_model=HourlySummary,
    responses=ERROR_RESPONSES,
    summary="Event counts per UTC hour, empty hours included",
    description="Without start_time or end_time: the 24 hours up to the end of the current "
    "hour. With only one of them: the 24 hours after start_time, or before end_time. "
    "The range may cover at most 31 days.",
)
def hourly_summary(request: Request, db: Db, filters: FilterSet):
    start, end = filters.start_time, filters.end_time
    if start is None and end is None:
        now = request.app.state.clock()
        end = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    if start is None:
        start = end - DEFAULT_HOURLY_RANGE
    if end is None:
        end = start + DEFAULT_HOURLY_RANGE
    if start >= end:
        raise ApiError.invalid([("start_time", "must be before end_time")])
    if end - start > MAX_HOURLY_RANGE:
        raise ApiError.invalid([("end_time", "range must not exceed 31 days")])
    bounded = services.Filters(**{**filters.__dict__, "start_time": start, "end_time": end})
    with db.connect() as conn:
        buckets = services.hourly_summary(conn, bounded, start, end)
    return HourlySummary(start_time=start, end_time=end, buckets=buckets)


@router.get(
    "/earthquakes/summary/magnitude",
    response_model=MagnitudeSummary,
    responses=ERROR_RESPONSES,
    summary="Event counts per magnitude range, empty ranges included",
)
def magnitude_summary(db: Db, filters: FilterSet):
    with db.connect() as conn:
        return MagnitudeSummary(buckets=services.magnitude_summary(conn, filters))


@router.get(
    "/earthquakes/{event_id}",
    response_model=Earthquake,
    responses=ERROR_RESPONSES,
    summary="One event, by its current or any former USGS ID",
)
def get_earthquake(db: Db, event_id: Annotated[Text, Path(min_length=1, max_length=100)]):
    with db.connect() as conn:
        event = services.get_earthquake(conn, event_id)
    if event is None:
        raise ApiError(404, "not_found", f"no event with ID {event_id!r}")
    return event
