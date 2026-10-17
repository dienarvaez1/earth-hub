"""backfill progress

backfill_windows (one row per fetched FDSN window, so a backfill resumes where it stopped)
and ingestion_checkpoints (when catch-up and the deleted-events check last ran).

Revision ID: 6f7955f3b0b2
Revises: 787bc8ddd60b
Create Date: 2026-10-17 01:27:56.464127

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "6f7955f3b0b2"
down_revision: str | Sequence[str] | None = "787bc8ddd60b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "backfill_windows",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("job", sa.Text(), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("expected", sa.Integer(), nullable=False),
        sa.Column("fetched", sa.Integer(), nullable=False),
        sa.Column("published", sa.Integer(), nullable=False),
        sa.Column("skipped", sa.Integer(), nullable=False),
        sa.Column("rejected", sa.Integer(), nullable=False),
        sa.Column(
            "finished_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("job IN ('backfill', 'sweep')", name=op.f("ck_backfill_windows_job")),
        sa.CheckConstraint("status IN ('done', 'open')", name=op.f("ck_backfill_windows_status")),
        sa.CheckConstraint("fetched >= expected", name=op.f("ck_backfill_windows_complete")),
        sa.CheckConstraint(
            "window_end > window_start", name=op.f("ck_backfill_windows_window_order")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_backfill_windows")),
        sa.UniqueConstraint("job", "window_start", name="uq_backfill_windows_job_start"),
    )
    op.create_table(
        "ingestion_checkpoints",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("value", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("name", name=op.f("pk_ingestion_checkpoints")),
    )


def downgrade() -> None:
    op.drop_table("ingestion_checkpoints")
    op.drop_table("backfill_windows")
