"""Full mode's side of the check against USGS: events, IDs and watermarks read from PostgreSQL."""

from datetime import UTC, datetime

import pytest

from earth_hub import usgs_reconcile as u
from earth_hub.database.progress import Progress
from earth_hub.database.repository import IncomingMessage, process_message

pytestmark = [pytest.mark.integration, pytest.mark.req("REQ-REC-001")]

NOW = datetime(2026, 10, 21, 4, 0, tzinfo=UTC)


def test_postgresql_gives_every_event_with_all_its_ids_and_the_watermarks(
    db, make_envelope, make_feature
):
    merged = make_feature("us9", ["us9", "ak9"], updated=1792540000000)
    gone = make_feature("nc1", updated=1792540000000, status="deleted")
    with db.begin() as conn:
        process_message(
            conn, IncomingMessage("earthquakes.raw", 0, 0, b"k", make_envelope(merged)), now=NOW
        )
        process_message(
            conn, IncomingMessage("earthquakes.backfill", 0, 0, b"k", make_envelope(gone)), now=NOW
        )
    progress = Progress(db)
    progress.set_checkpoint("catchup", NOW)
    progress.set_checkpoint("deleted", NOW.replace(hour=1))

    events, marks = u.from_postgres(db, "earthquakes.raw")
    by_key = {e.id: e for e in events}
    assert by_key["us9"].ids == {"us9", "ak9"} and not by_key["us9"].deleted
    assert by_key["us9"].updated_ms == 1792540000000
    assert by_key["nc1"].deleted
    # The day feed was last read when the live topic last delivered (the database's clock);
    # catch-up's checkpoints for the rest.
    assert marks.day_feed is not None
    assert (marks.month_feed, marks.deletions) == (NOW, NOW.replace(hour=1))


@pytest.mark.req("REQ-REC-004")
def test_full_mode_measures_each_feed_by_when_usgs_generated_it(db, make_envelope, make_feature):
    # DEF-054: the newest generation time among each feed's messages, not their delivery.
    feed, catchup = 1793272613000, 1793272662000
    messages = [
        ("earthquakes.raw", make_envelope(make_feature("ak1"), feed_generated_ms=feed - 3_600_000)),
        ("earthquakes.raw", make_envelope(make_feature("ak2"), feed_generated_ms=feed)),
        ("earthquakes.raw", make_envelope(make_feature("ak3"))),  # no generation time
        (
            "earthquakes.backfill",
            make_envelope(make_feature("ci4"), source="catchup", feed_generated_ms=catchup),
        ),
        ("earthquakes.backfill", b'{"source": "feed", "feed_generated_ms": "soon"}'),  # rejected
    ]
    with db.begin() as conn:
        for offset, (topic, value) in enumerate(messages):
            process_message(conn, IncomingMessage(topic, 0, offset, b"k", value), now=NOW)
    Progress(db).set_checkpoint("catchup", NOW)

    _, marks = u.from_postgres(db, "earthquakes.raw")
    assert marks.day_feed == datetime.fromtimestamp(feed / 1000, UTC)
    assert marks.month_feed == datetime.fromtimestamp(catchup / 1000, UTC)
