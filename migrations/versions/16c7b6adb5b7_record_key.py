"""record key

earthquakes.key: the record's identity outside the database (R2 object names, the snapshot),
the same in full and direct mode, unlike the identity column `id` (Milestone 8). New records
get the USGS ID they are created with, and a merge keeps the surviving record's. Existing rows
get the earliest-seen of their IDs, the closest record of the ID they were created with.

Revision ID: 16c7b6adb5b7
Revises: 7f09df13c9d4
Create Date: 2026-10-19 22:21:43.273842

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "16c7b6adb5b7"
down_revision: str | Sequence[str] | None = "7f09df13c9d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("earthquakes", sa.Column("key", sa.Text(), nullable=True))
    op.execute(
        """
        UPDATE earthquakes AS e SET key = first.event_id
        FROM (
            SELECT DISTINCT ON (earthquake_id) earthquake_id, event_id
            FROM earthquake_ids
            ORDER BY earthquake_id, first_seen_at, event_id
        ) AS first
        WHERE first.earthquake_id = e.id
        """
    )
    # A row without any ID cannot exist (reconcile's invariants), but would block NOT NULL.
    op.execute("UPDATE earthquakes SET key = preferred_event_id WHERE key IS NULL")
    op.alter_column("earthquakes", "key", nullable=False)
    op.create_unique_constraint(op.f("uq_earthquakes_key"), "earthquakes", ["key"])


def downgrade() -> None:
    op.drop_constraint(op.f("uq_earthquakes_key"), "earthquakes", type_="unique")
    op.drop_column("earthquakes", "key")
