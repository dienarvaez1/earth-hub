"""revision ids

earthquakes.revision_ids: the IDs listed by the revision currently stored, so a duplicate's
deletion can be told apart from the deletion of the whole event (DEF-026). Existing rows are
filled from the applied message that holds their current revision.

Revision ID: 1f84493aeaa9
Revises: 6f7955f3b0b2
Create Date: 2026-10-17 12:01:39.447750

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "1f84493aeaa9"
down_revision: str | Sequence[str] | None = "6f7955f3b0b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "earthquakes",
        sa.Column("revision_ids", postgresql.ARRAY(sa.Text()), server_default="{}", nullable=False),
    )
    op.execute(
        """
        UPDATE earthquakes e SET revision_ids = ARRAY(
            SELECT DISTINCT i FROM (
                SELECT e.preferred_event_id AS i
                UNION ALL
                SELECT jsonb_array_elements_text(r.envelope -> 'ids')
                FROM (
                    SELECT envelope FROM raw_events
                    WHERE earthquake_id = e.id AND outcome = 'applied'
                      AND source_updated_ms = e.source_updated_ms
                      AND envelope ->> 'event_id' = e.preferred_event_id
                    ORDER BY id DESC LIMIT 1
                ) r
            ) ids
            ORDER BY i
        )
        """
    )


def downgrade() -> None:
    op.drop_column("earthquakes", "revision_ids")
