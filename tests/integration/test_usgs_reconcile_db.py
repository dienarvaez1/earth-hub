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
