"""The one rule that decides whether a revision replaces the stored one (DEF-026)."""

from datetime import UTC, datetime

import pytest

from earth_hub.database.revisions import Revision, compare, needed
from earth_hub.ingestion.events import build_message

pytestmark = pytest.mark.req("REQ-DQ-DEL-002")

# The live case: aka2026jntkwa was merged into aka2026jntiye, then deleted on its own.
SURVIVOR = Revision(
    "aka2026jntiye",
    ("aka2026jntiye", "av94194953", "us6000sxun", "aka2026jntkwa"),
    1788546486203,
    False,
)
DUPLICATE_DELETED = Revision("aka2026jntkwa", ("aka2026jntkwa",), 1788549398193, True)


def rev(event_id="a", ids=None, updated=1000, deleted=False) -> Revision:
    return Revision(event_id, tuple(ids or [event_id]), updated, deleted)


@pytest.mark.parametrize(
    ("updated", "outcome"), [(1001, "applied"), (1000, "duplicate"), (999, "stale")]
)
def test_otherwise_the_newer_revision_wins(updated, outcome):
    assert compare(rev(), rev(updated=updated)) == (outcome, None)


def test_a_duplicates_deletion_does_not_hide_the_event_it_was_merged_into():
    outcome, reason = compare(SURVIVOR, DUPLICATE_DELETED)
    assert outcome == "stale"
    assert reason == (
        "deletion of aka2026jntkwa, a duplicate merged into aka2026jntiye; event kept"
    )


def test_the_event_wins_over_its_duplicates_deletion_that_arrived_first():
    assert compare(DUPLICATE_DELETED, SURVIVOR) == ("applied", None)  # although older


def test_deleting_the_whole_event_still_applies():
    whole = rev("a", ["a", "b"], updated=2000, deleted=True)
    assert compare(rev("a", ["a", "b"]), whole) == ("applied", None)
    # Under a new preferred ID, as long as the deletion lists the stored one.
    assert compare(rev("a", ["a", "b"]), rev("b", ["b", "a"], 2000, True)) == ("applied", None)


def test_an_old_revision_does_not_bring_back_a_deleted_event():
    deleted = rev("a", ["a", "b"], updated=2000, deleted=True)
    assert compare(deleted, rev("b", ["b", "a"], updated=1500)) == ("stale", None)
    assert compare(deleted, rev("b", ["b"], updated=1500)) == ("stale", None)


@pytest.mark.req("REQ-DQ-PROP-002")
def test_an_upper_case_deletion_is_a_deletion_when_deciding_what_to_publish(feature_template):
    """A duplicate's deletion saying `DELETED` must not be published over its survivor (DEF-049)."""
    feature_template["id"] = "aka2026jntkwa"
    feature_template["properties"].update(
        ids=",aka2026jntkwa,", status="DELETED", updated=DUPLICATE_DELETED.source_updated_ms
    )
    message = build_message(
        feature_template, fetched_at=datetime(2026, 9, 5, tzinfo=UTC), feed_generated_ms=None
    )
    assert needed([message], {i: "survivor" for i in SURVIVOR.ids}, {"survivor": SURVIVOR}) == []
