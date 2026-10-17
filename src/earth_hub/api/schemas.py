"""Response models. They define the OpenAPI contract, so field docs here are API docs."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class Earthquake(BaseModel):
    event_id: str = Field(description="Current preferred USGS event ID")
    ids: list[str] = Field(description="Every USGS ID this event has had, including event_id")
    event_time: datetime = Field(description="When the event happened (UTC)")
    source_updated: datetime = Field(description="When USGS last revised it (UTC)")
    magnitude: float | None
    mag_type: str | None = Field(description="Magnitude scale, e.g. ml, mb, mww")
    place: str | None
    longitude: float
    latitude: float
    depth_km: float = Field(description="Depth in km; negative means above sea level")
    event_type: str = Field(description="earthquake, explosion, quarry blast, ...")
    review_status: Literal["automatic", "reviewed", "deleted"]
    tsunami: bool
    felt: int | None = Field(description="Number of 'Did You Feel It?' reports")
    cdi: float | None = Field(description="Maximum reported intensity")
    mmi: float | None = Field(description="Maximum estimated instrumental intensity")
    alert: str | None = Field(description="PAGER alert level")
    sig: int | None = Field(description="USGS significance, 0 to about 1000")
    net: str | None
    url: str | None
    title: str | None
    revision_count: int = Field(description="Number of USGS revisions applied")
    first_seen_at: datetime = Field(description="When Earth-Hub first stored it (UTC)")
    last_applied_at: datetime = Field(description="When Earth-Hub last updated it (UTC)")


class EarthquakePage(BaseModel):
    items: list[Earthquake]
    total: int = Field(description="Number of events matching the filters, across all pages")
    limit: int
    offset: int


class Health(BaseModel):
    status: Literal["ok", "unavailable"]
    database: Literal["ok", "unavailable"]
    latest_event_time: datetime | None = Field(description="Newest stored event, if any")
    version: str


class MagnitudeStats(BaseModel):
    min: float
    max: float
    avg: float


class Stats(BaseModel):
    count: int
    magnitude: MagnitudeStats | None = Field(
        description="Over events with a magnitude; null when there are none. Mixes "
        "magnitude types; see by_magnitude_type."
    )
    by_event_type: dict[str, int]
    by_magnitude_type: dict[str, int] = Field(
        description="Event counts per magnitude scale; 'unknown' when not reported"
    )


class HourlyBucket(BaseModel):
    hour: datetime = Field(description="Start of the UTC hour")
    count: int


class HourlySummary(BaseModel):
    start_time: datetime
    end_time: datetime
    buckets: list[HourlyBucket] = Field(description="Every hour in the range, including empty ones")


class MagnitudeBucket(BaseModel):
    label: str = Field(description="'<0', '0-1', ..., '6-7', '>=7' or 'unknown'")
    min: float | None = Field(description="Inclusive lower bound; null when unbounded or unknown")
    max: float | None = Field(description="Exclusive upper bound; null when unbounded or unknown")
    count: int


class MagnitudeSummary(BaseModel):
    buckets: list[MagnitudeBucket] = Field(description="Every range, including empty ones")


class ErrorDetail(BaseModel):
    parameter: str | None = None
    message: str


class ErrorBody(BaseModel):
    code: str = Field(
        description="invalid_parameters, not_found, method_not_allowed, "
        "database_unavailable or internal_error"
    )
    message: str
    details: list[ErrorDetail] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    error: ErrorBody
