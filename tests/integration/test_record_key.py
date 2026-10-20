"""earthquakes.key: the record's identity outside the database, the USGS ID it was created
with, kept by the surviving record of a merge (Milestone 8)."""

from datetime import UTC, datetime, timedelta

import pytest
from alembic import command
from sqlalchemy import select, text

from earth_hub.database.models import Earthquake
from earth_hub.database.repository import IncomingMessage, process_message

pytestmark = [pytest.mark.integration, pytest.mark.req("REQ-EXP-012")]

T0 = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)


def deliver(db, make_envelope, features) -> None:
    """One message per transaction, a minute apart, so each ID has its own first_seen_at."""
    for offset, feature in enumerate(features):
        message = IncomingMessage("earthquakes.raw", 0, offset, b"k", make_envelope(feature))
        with db.begin() as conn:
            process_message(conn, message, now=T0 + timedelta(minutes=offset))


def keys(db) -> dict[str, str]:
    """preferred_event_id -> key."""
    with db.connect() as conn:
        rows = conn.execute(select(Earthquake.preferred_event_id, Earthquake.key)).all()
    return dict(rows)


def stream(make_feature) -> list[dict]:
    return [
        make_feature("us2", ["us2"], updated=1792137700000),
        # The preferred ID changes to one that sorts first: the key stays the first ID.
        make_feature("ab2", ["us2", "ab2"], updated=1792137800000),
        make_feature("ak9", ["ak9"], updated=1792137700000),
        make_feature("us9", ["us9"], updated=1792137710000),
        make_feature("us9", ["us9", "ak9"], updated=1792137800000),  # merges into ak9's record
    ]


def test_a_record_keeps_the_id_it_was_created_with(db, make_envelope, make_feature):
    deliver(db, make_envelope, stream(make_feature))
    assert keys(db) == {"ab2": "us2", "us9": "ak9"}


def test_the_migration_gives_existing_rows_the_earliest_seen_of_their_ids(
    db, make_envelope, make_feature, make_alembic_config
):
    deliver(db, make_envelope, stream(make_feature))
    before = keys(db)
    config = make_alembic_config(db.url.render_as_string(hide_password=False))
    command.downgrade(config, "7f09df13c9d4")  # drops the column
    command.upgrade(config, "head")
    assert keys(db) == before
    with db.connect() as conn:
        assert (
            conn.execute(text("SELECT count(*) FROM earthquakes WHERE key IS NULL")).scalar() == 0
        )
