"""Table definitions. Migrations in migrations/versions must match these exactly; an
integration test compares them."""

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Double,
    ForeignKey,
    Identity,
    Index,
    Integer,
    MetaData,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

OUTCOMES = ("applied", "stale", "duplicate", "rejected", "error")
REVIEW_STATUSES = ("automatic", "reviewed", "deleted")


def _in(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_name)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class Earthquake(Base):
    """One row per physical event, whatever USGS IDs it has had."""

    __tablename__ = "earthquakes"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    preferred_event_id: Mapped[str] = mapped_column(Text, unique=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    event_time_ms: Mapped[int] = mapped_column(BigInteger)
    source_updated: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    source_updated_ms: Mapped[int] = mapped_column(BigInteger)
    magnitude: Mapped[float | None] = mapped_column(Double)
    mag_type: Mapped[str | None] = mapped_column(Text)
    place: Mapped[str | None] = mapped_column(Text)
    longitude: Mapped[float] = mapped_column(Double)
    latitude: Mapped[float] = mapped_column(Double)
    depth_km: Mapped[float] = mapped_column(Double)
    event_type: Mapped[str] = mapped_column(Text)
    review_status: Mapped[str] = mapped_column(Text)
    tsunami: Mapped[bool] = mapped_column(Boolean)
    felt: Mapped[int | None] = mapped_column(Integer)
    cdi: Mapped[float | None] = mapped_column(Double)
    mmi: Mapped[float | None] = mapped_column(Double)
    alert: Mapped[str | None] = mapped_column(Text)
    sig: Mapped[int | None] = mapped_column(Integer)
    net: Mapped[str | None] = mapped_column(Text)
    url: Mapped[str | None] = mapped_column(Text)
    title: Mapped[str | None] = mapped_column(Text)
    # The IDs listed by the revision currently stored; see database/revisions.py.
    revision_ids: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    revision_count: Mapped[int] = mapped_column(Integer, server_default="1")
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_applied_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        # The same limits as the validation rules, enforced again by the database.
        CheckConstraint("latitude BETWEEN -90 AND 90", name="latitude_range"),
        CheckConstraint("longitude BETWEEN -180 AND 180", name="longitude_range"),
        CheckConstraint("depth_km BETWEEN -10 AND 1000", name="depth_range"),
        CheckConstraint(
            "magnitude IS NULL OR magnitude BETWEEN -2 AND 10.5", name="magnitude_range"
        ),
        CheckConstraint(f"review_status IN ({_in(REVIEW_STATUSES)})", name="review_status"),
        CheckConstraint("source_updated_ms >= event_time_ms", name="updated_after_event"),
        CheckConstraint("revision_count >= 1", name="revision_count_positive"),
        Index("ix_earthquakes_event_time", "event_time"),
        Index("ix_earthquakes_magnitude", "magnitude"),
    )


class EarthquakeId(Base):
    """Every USGS ID ever seen, mapped to the event it belongs to."""

    __tablename__ = "earthquake_ids"

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    # No ON DELETE CASCADE: a merge must move IDs explicitly, so a bug fails loudly.
    earthquake_id: Mapped[int] = mapped_column(ForeignKey("earthquakes.id"))
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (Index("ix_earthquake_ids_earthquake_id", "earthquake_id"),)


class RawEvent(Base):
    """Every Kafka message received, with its outcome. Rejected messages live here too."""

    __tablename__ = "raw_events"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    kafka_topic: Mapped[str] = mapped_column(Text)
    kafka_partition: Mapped[int] = mapped_column(Integer)
    kafka_offset: Mapped[int] = mapped_column(BigInteger)
    kafka_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    message_key: Mapped[str | None] = mapped_column(Text)
    envelope: Mapped[Any | None] = mapped_column(JSONB)
    # Only set when the value could not be stored as JSON.
    raw_value: Mapped[str | None] = mapped_column(Text)
    event_id: Mapped[str | None] = mapped_column(Text)
    source_updated_ms: Mapped[int | None] = mapped_column(BigInteger)
    outcome: Mapped[str] = mapped_column(Text)
    rule: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    earthquake_id: Mapped[int | None] = mapped_column(ForeignKey("earthquakes.id"))
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "kafka_topic", "kafka_partition", "kafka_offset", name="uq_raw_events_offset"
        ),
        CheckConstraint(f"outcome IN ({_in(OUTCOMES)})", name="outcome"),
        Index("ix_raw_events_outcome", "outcome"),
        Index("ix_raw_events_earthquake_id", "earthquake_id"),
        Index("ix_raw_events_event_id", "event_id"),
    )


WINDOW_JOBS = ("backfill", "sweep")
WINDOW_STATUSES = ("done", "open")


class BackfillWindow(Base):
    """One fetched time window of a backfill, so an interrupted run resumes where it stopped.

    A window is `open` when it ended at "now" (the current week): it is fetched again on the
    next run. `expected` is what the FDSN count endpoint reported; `fetched` must not be lower.
    """

    __tablename__ = "backfill_windows"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    job: Mapped[str] = mapped_column(Text)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(Text)
    expected: Mapped[int] = mapped_column(Integer)
    fetched: Mapped[int] = mapped_column(Integer)
    published: Mapped[int] = mapped_column(Integer)
    skipped: Mapped[int] = mapped_column(Integer)
    rejected: Mapped[int] = mapped_column(Integer)
    finished_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("job", "window_start", name="uq_backfill_windows_job_start"),
        CheckConstraint(f"job IN ({_in(WINDOW_JOBS)})", name="job"),
        CheckConstraint(f"status IN ({_in(WINDOW_STATUSES)})", name="status"),
        CheckConstraint("window_end > window_start", name="window_order"),
        CheckConstraint("fetched >= expected", name="complete"),
    )


class IngestionCheckpoint(Base):
    """A named point in time, e.g. when catch-up or the deleted-events check last ran."""

    __tablename__ = "ingestion_checkpoints"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
