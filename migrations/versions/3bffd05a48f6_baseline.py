"""baseline

Empty starting point so the migration workflow exists from Milestone 1.
The first tables arrive in Milestone 3.

Revision ID: 3bffd05a48f6
Revises:
Create Date: 2026-10-16 02:01:38.186556

"""

from collections.abc import Sequence

revision: str = "3bffd05a48f6"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
