"""Deletions against real PostgreSQL, from the cases the 2026 backfill found (DEF-026, DEF-027).

Each scenario runs one message at a time and as one batch: both paths must agree.
"""

import json
from datetime import UTC, datetime

import pytest
from alembic import command
from sqlalchemy import select, text

from earth_hub.database.models import Earthquake, RawEvent
from earth_hub.database.progress import Progress
from earth_hub.database.repository import IncomingMessage, process_batch, process_message
from earth_hub.ingestion.events import parse_feed

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)
SURVIVOR_IDS = ["aka2026jntiye", "av94194953", "us6000sxun", "aka2026jntkwa"]
UPDATED = 1792137700000


@pytest.fixture
def survivor(make_feature):
    return make_feature("aka2026jntiye", SURVIVOR_IDS, updated=UPDATED, status="reviewed")


@pytest.fixture
def duplicate_deleted(make_feature):
    """The duplicate's deletion lists only its own ID, and is newer than the survivor."""
    return make_feature("aka2026jntkwa", updated=UPDATED + 2_912_000, status="deleted")


def store(db, make_envelope, features, *, batched: bool, first_offset: int = 0):
    messages = [
        IncomingMessage("earthquakes.backfill", 0, offset, b"k", make_envelope(f))
        for offset, f in enumerate(features, start=first_offset)
    ]
    with db.begin() as conn:
        if batched:
            return process_batch(conn, messages, now=NOW)
        return [process_message(conn, m, now=NOW) for m in messages]


def events(db):
    with db.connect() as conn:
        return conn.execute(select(Earthquake).order_by(Earthquake.id)).mappings().all()


def outcomes(db):
    with db.connect() as conn:
        return conn.execute(
            select(RawEvent.event_id, RawEvent.outcome, RawEvent.reason).order_by(
                RawEvent.kafka_offset
            )
        ).all()


@pytest.mark.req("REQ-DQ-DEL-002")
@pytest.mark.parametrize("batched", [False, True], ids=["one-by-one", "batched"])
def test_a_duplicates_deletion_does_not_hide_the_survivor(
    db, make_envelope, survivor, duplicate_deleted, batched
):
    store(db, make_envelope, [survivor, duplicate_deleted], batched=batched)

    (event,) = events(db)
    assert (event["preferred_event_id"], event["review_status"]) == ("aka2026jntiye", "reviewed")
    assert event["revision_ids"] == SURVIVOR_IDS
    assert outcomes(db)[1] == (
        "aka2026jntkwa",
        "stale",
        "deletion of aka2026jntkwa, a duplicate merged into aka2026jntiye; event kept",
    )


@pytest.mark.req("REQ-DQ-DEL-002")
@pytest.mark.parametrize("batched", [False, True], ids=["one-by-one", "batched"])
def test_the_survivor_replaces_its_duplicates_deletion_that_arrived_first(
    db, make_envelope, survivor, duplicate_deleted, batched
):
    """Backfill windows run in time order, so the duplicate's deletion can come first."""
    store(db, make_envelope, [duplicate_deleted, survivor, duplicate_deleted], batched=batched)

    (event,) = events(db)
    assert (event["preferred_event_id"], event["review_status"]) == ("aka2026jntiye", "reviewed")
    assert event["revision_count"] == 2
    assert [o[1] for o in outcomes(db)] == ["applied", "applied", "stale"]


@pytest.mark.req("REQ-DQ-DEL-002")
@pytest.mark.parametrize("batched", [False, True], ids=["one-by-one", "batched"])
def test_deleting_the_whole_merged_event_still_hides_it(
    db, make_envelope, make_feature, survivor, batched
):
    whole = make_feature("aka2026jntiye", SURVIVOR_IDS, updated=UPDATED + 1000, status="deleted")
    store(db, make_envelope, [survivor, whole], batched=batched)
    (event,) = events(db)
    assert event["review_status"] == "deleted"


@pytest.mark.req("REQ-DQ-DEL-001")
@pytest.mark.parametrize("batched", [False, True], ids=["one-by-one", "batched"])
def test_a_deletion_with_a_placeholder_magnitude_hides_the_stored_event(
    db, make_envelope, make_feature, batched
):
    live = make_feature("uw62228261", updated=UPDATED, mag=1.2)
    deletion = make_feature("uw62228261", updated=UPDATED + 1000, mag=-5, status="deleted")
    store(db, make_envelope, [live, deletion], batched=batched)
    (event,) = events(db)
    assert (event["review_status"], event["magnitude"]) == ("deleted", None)
    assert [o[1] for o in outcomes(db)] == ["applied", "applied"]


@pytest.mark.req("REQ-DQ-DEL-002", "REQ-BF-009")
def test_backfill_publishes_by_the_same_rule(db, make_envelope, survivor, duplicate_deleted):
    def needs(*features):
        doc = {"type": "FeatureCollection", "features": list(features)}
        parsed = parse_feed(json.dumps(doc).encode(), fetched_at=NOW).messages
        return {m.event_id for m in Progress(db).needs_publish(parsed)}

    store(db, make_envelope, [duplicate_deleted], batched=False)
    assert needs(survivor) == {"aka2026jntiye"}  # older, but it must replace the deletion
    store(db, make_envelope, [survivor], batched=False, first_offset=1)
    assert needs(survivor, duplicate_deleted) == set()  # nothing the consumer would apply


@pytest.mark.req("REQ-DQ-DEL-002", "REQ-PER-012")
def test_migration_fills_revision_ids_from_the_stored_revision(
    db, make_envelope, make_feature, survivor, make_alembic_config
):
    older = make_feature("aka2026jntiye", ["aka2026jntiye"], updated=UPDATED - 1000)
    store(db, make_envelope, [older, survivor], batched=False)
    config = make_alembic_config(db.url.render_as_string(hide_password=False))
    command.downgrade(config, "6f7955f3b0b2")
    command.upgrade(config, "head")
    with db.connect() as conn:
        filled = conn.execute(text("SELECT revision_ids FROM earthquakes")).scalar_one()
    assert filled == sorted(SURVIVOR_IDS)  # from the current revision, not the older one
