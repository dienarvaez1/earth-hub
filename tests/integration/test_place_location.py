"""City, region and country stored with each revision, and filled in for existing events."""

from datetime import UTC, datetime

import pytest
from alembic import command
from sqlalchemy import select, text

from earth_hub.database.models import Earthquake
from earth_hub.database.repository import IncomingMessage, process_batch, process_message

pytestmark = [pytest.mark.integration, pytest.mark.req("REQ-DQ-PLACE-001")]

NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)
UPDATED = 1792137700000


def store(db, make_envelope, features, *, batched: bool):
    messages = [
        IncomingMessage("earthquakes.raw", 0, offset, b"k", make_envelope(f))
        for offset, f in enumerate(features)
    ]
    with db.begin() as conn:
        if batched:
            process_batch(conn, messages, now=NOW)
        else:
            for m in messages:
                process_message(conn, m, now=NOW)


def locations(db):
    with db.connect() as conn:
        return conn.execute(
            select(
                Earthquake.preferred_event_id,
                Earthquake.place,
                Earthquake.city,
                Earthquake.region,
                Earthquake.country,
            ).order_by(Earthquake.preferred_event_id)
        ).all()


@pytest.mark.parametrize("batched", [False, True], ids=["one-by-one", "batched"])
def test_new_events_and_revisions_store_the_location(db, make_envelope, make_feature, batched):
    store(
        db,
        make_envelope,
        [
            make_feature("ak1", updated=UPDATED, place="86 km E of Ugashik, Alaska"),
            make_feature("ak1", updated=UPDATED + 1, place="80 km E of Pilot Point, Alaska"),
            make_feature("ci1", updated=UPDATED, place="10 km N of Cobb, CA"),
            make_feature("us1", updated=UPDATED, place="Fiji region"),
        ],
        batched=batched,
    )
    assert locations(db) == [
        ("ak1", "80 km E of Pilot Point, Alaska", "Pilot Point", "Alaska", "United States"),
        ("ci1", "10 km N of Cobb, CA", "Cobb", "California", "United States"),
        ("us1", "Fiji region", None, None, None),
    ]


def test_a_revision_without_a_place_clears_the_location(db, make_envelope, make_feature):
    store(
        db,
        make_envelope,
        [
            make_feature("ak1", updated=UPDATED, place="86 km E of Ugashik, Alaska"),
            make_feature("ak1", updated=UPDATED + 1, place=None),
        ],
        batched=False,
    )
    assert locations(db) == [("ak1", None, None, None, None)]


def test_migration_fills_existing_events(db, make_envelope, make_feature, make_alembic_config):
    store(
        db,
        make_envelope,
        [
            make_feature("ak1", updated=UPDATED, place="86 km E of Ugashik, Alaska"),
            make_feature("mx1", updated=UPDATED, place="10 km NE of Mexicali, B.C., MX"),
            make_feature("us1", updated=UPDATED, place="south of the Fiji Islands"),
        ],
        batched=False,
    )
    config = make_alembic_config(db.url.render_as_string(hide_password=False))
    command.downgrade(config, "1f84493aeaa9")
    command.upgrade(config, "head")
    assert [row[2:] for row in locations(db)] == [
        ("Ugashik", "Alaska", "United States"),
        ("Mexicali", "Baja California", "Mexico"),
        (None, "the Fiji Islands", None),
    ]
    with db.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM earthquakes")).scalar_one() == 3
