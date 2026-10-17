"""earthquake tables

earthquakes (one row per physical event), earthquake_ids (every USGS ID, mapped to its
event) and raw_events (every Kafka message with its outcome; rejected messages included).

Revision ID: 787bc8ddd60b
Revises: 3bffd05a48f6
Create Date: 2026-10-16 22:19:42.369024

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "787bc8ddd60b"
down_revision: str | Sequence[str] | None = "3bffd05a48f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "earthquakes",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("preferred_event_id", sa.Text(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_time_ms", sa.BigInteger(), nullable=False),
        sa.Column("source_updated", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_updated_ms", sa.BigInteger(), nullable=False),
        sa.Column("magnitude", sa.Double(), nullable=True),
        sa.Column("mag_type", sa.Text(), nullable=True),
        sa.Column("place", sa.Text(), nullable=True),
        sa.Column("longitude", sa.Double(), nullable=False),
        sa.Column("latitude", sa.Double(), nullable=False),
        sa.Column("depth_km", sa.Double(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("review_status", sa.Text(), nullable=False),
        sa.Column("tsunami", sa.Boolean(), nullable=False),
        sa.Column("felt", sa.Integer(), nullable=True),
        sa.Column("cdi", sa.Double(), nullable=True),
        sa.Column("mmi", sa.Double(), nullable=True),
        sa.Column("alert", sa.Text(), nullable=True),
        sa.Column("sig", sa.Integer(), nullable=True),
        sa.Column("net", sa.Text(), nullable=True),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("revision_count", sa.Integer(), server_default="1", nullable=False),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "last_applied_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "review_status IN ('automatic', 'reviewed', 'deleted')",
            name=op.f("ck_earthquakes_review_status"),
        ),
        sa.CheckConstraint(
            "depth_km BETWEEN -10 AND 1000", name=op.f("ck_earthquakes_depth_range")
        ),
        sa.CheckConstraint(
            "latitude BETWEEN -90 AND 90", name=op.f("ck_earthquakes_latitude_range")
        ),
        sa.CheckConstraint(
            "longitude BETWEEN -180 AND 180", name=op.f("ck_earthquakes_longitude_range")
        ),
        sa.CheckConstraint(
            "magnitude IS NULL OR magnitude BETWEEN -2 AND 10.5",
            name=op.f("ck_earthquakes_magnitude_range"),
        ),
        sa.CheckConstraint(
            "revision_count >= 1", name=op.f("ck_earthquakes_revision_count_positive")
        ),
        sa.CheckConstraint(
            "source_updated_ms >= event_time_ms", name=op.f("ck_earthquakes_updated_after_event")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_earthquakes")),
        sa.UniqueConstraint("preferred_event_id", name=op.f("uq_earthquakes_preferred_event_id")),
    )
    op.create_index("ix_earthquakes_event_time", "earthquakes", ["event_time"], unique=False)
    op.create_index("ix_earthquakes_magnitude", "earthquakes", ["magnitude"], unique=False)
    op.create_table(
        "earthquake_ids",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("earthquake_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["earthquake_id"],
            ["earthquakes.id"],
            name=op.f("fk_earthquake_ids_earthquake_id_earthquakes"),
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_earthquake_ids")),
    )
    op.create_index(
        "ix_earthquake_ids_earthquake_id", "earthquake_ids", ["earthquake_id"], unique=False
    )
    op.create_table(
        "raw_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("kafka_topic", sa.Text(), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        sa.Column("kafka_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("message_key", sa.Text(), nullable=True),
        sa.Column("envelope", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("raw_value", sa.Text(), nullable=True),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("source_updated_ms", sa.BigInteger(), nullable=True),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("rule", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("earthquake_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "outcome IN ('applied', 'stale', 'duplicate', 'rejected', 'error')",
            name=op.f("ck_raw_events_outcome"),
        ),
        sa.ForeignKeyConstraint(
            ["earthquake_id"],
            ["earthquakes.id"],
            name=op.f("fk_raw_events_earthquake_id_earthquakes"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_raw_events")),
        sa.UniqueConstraint(
            "kafka_topic", "kafka_partition", "kafka_offset", name="uq_raw_events_offset"
        ),
    )
    op.create_index("ix_raw_events_earthquake_id", "raw_events", ["earthquake_id"], unique=False)
    op.create_index("ix_raw_events_event_id", "raw_events", ["event_id"], unique=False)
    op.create_index("ix_raw_events_outcome", "raw_events", ["outcome"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_raw_events_outcome", table_name="raw_events")
    op.drop_index("ix_raw_events_event_id", table_name="raw_events")
    op.drop_index("ix_raw_events_earthquake_id", table_name="raw_events")
    op.drop_table("raw_events")
    op.drop_index("ix_earthquake_ids_earthquake_id", table_name="earthquake_ids")
    op.drop_table("earthquake_ids")
    op.drop_index("ix_earthquakes_magnitude", table_name="earthquakes")
    op.drop_index("ix_earthquakes_event_time", table_name="earthquakes")
    op.drop_table("earthquakes")
