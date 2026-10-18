"""place location

earthquakes.city, region and country, derived from place by validation/places.py. Existing
rows are filled with the same function the consumer uses for new revisions, so both agree.

Revision ID: 7f09df13c9d4
Revises: 1f84493aeaa9
Create Date: 2026-10-17 14:23:38.786732

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from earth_hub.validation.places import parse_place

revision: str = "7f09df13c9d4"
down_revision: str | Sequence[str] | None = "1f84493aeaa9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CHUNK = 5000


def upgrade() -> None:
    op.add_column("earthquakes", sa.Column("city", sa.Text(), nullable=True))
    op.add_column("earthquakes", sa.Column("region", sa.Text(), nullable=True))
    op.add_column("earthquakes", sa.Column("country", sa.Text(), nullable=True))

    conn = op.get_bind()
    rows = conn.execute(
        sa.text("SELECT id, place, longitude FROM earthquakes WHERE place LIKE '% of %'")
    ).all()
    updates = [
        {"id": id_, "city": loc.city, "region": loc.region, "country": loc.country}
        for id_, place, longitude in rows
        if (loc := parse_place(place, longitude)) != (None, None, None)
    ]
    statement = sa.text(
        "UPDATE earthquakes SET city = :city, region = :region, country = :country WHERE id = :id"
    )
    for start in range(0, len(updates), CHUNK):
        conn.execute(statement, updates[start : start + CHUNK])


def downgrade() -> None:
    op.drop_column("earthquakes", "country")
    op.drop_column("earthquakes", "region")
    op.drop_column("earthquakes", "city")
