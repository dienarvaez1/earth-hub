"""Earth-Hub against USGS (earth_hub/usgs_reconcile.py): every difference explained or not."""

from datetime import UTC, datetime, timedelta

import pytest
from support.export import BUCKET, start_direct
from support.fdsn import FdsnServer, at, event_at

from earth_hub import usgs_reconcile as u
from earth_hub.ingestion.fdsn import FdsnClient
from earth_hub.ingestion.usgs_client import make_http_client

START = datetime(2026, 10, 19, tzinfo=UTC)
END = START + 3 * u.DAY
NOW = END - timedelta(hours=20)  # 2026-10-21 04:00
MARKS = u.Marks(
    day_feed=NOW - timedelta(minutes=30),
    month_feed=NOW - timedelta(minutes=50),
    deletions=NOW - timedelta(hours=18),
)

pytestmark = pytest.mark.req("REQ-REC-001")


def event(event_id, when, updated, *, ids=(), deleted=False) -> u.Event:
    return u.Event(event_id, frozenset(ids or {event_id}), at(when), at(updated), deleted)


def run(ours, live=(), deleted=(), marks=MARKS, lookup=lambda i: None) -> u.Report:
    return u.compare(ours, live, deleted, marks, start=START, end=END, lookup=lookup)


def kinds(report) -> list[tuple[str, str, bool]]:
    return [(d.kind, d.event_id, d.explained) for d in report.differences]


def test_the_same_events_on_both_sides_are_no_difference():
    a = event("a1", NOW - timedelta(hours=3), NOW - timedelta(hours=2))
    report = run([a], [a])
    assert report.ok and report.differences == []
    assert report.days["2026-10-21"] == {"usgs": 1, "ours": 1}


def test_an_event_usgs_published_after_the_day_feed_was_read_is_pending():
    late = event("n1", NOW - timedelta(hours=5), MARKS.day_feed + timedelta(minutes=5))
    missed = event("n2", NOW - timedelta(hours=5), MARKS.day_feed - timedelta(minutes=5))
    report = run([], [late, missed])
    assert kinds(report) == [("missing", "n1", True), ("missing", "n2", False)]
    assert not report.ok and "before the day feed was last read" in report.unexplained[0].reason


def test_older_events_are_brought_by_the_month_feed():
    """An event older than the day feed's 24 hours comes only with catch-up's month feed."""
    old = START + timedelta(hours=2)  # 2 days before the day feed was read
    between = event("m1", old, MARKS.month_feed + timedelta(minutes=10))  # after the month feed
    report = run([], [between])
    assert kinds(report) == [("missing", "m1", True)]
    assert "after the month feed was last read" in report.differences[0].reason


def test_a_deletion_after_the_last_deleted_events_check_is_pending():
    t = NOW - timedelta(hours=30)
    recent = event("d1", t, MARKS.deletions + timedelta(hours=1), deleted=True)
    applied_late = event("d2", t, MARKS.deletions - timedelta(hours=1), deleted=True)
    ours = [event("d1", t, t), event("d2", t, t)]
    report = run(ours, deleted=[recent, applied_late])
    assert kinds(report) == [("deleted at USGS", "d1", True), ("deleted at USGS", "d2", False)]


def test_a_newer_revision_at_usgs_is_pending_only_after_its_feed_was_read():
    t = NOW - timedelta(hours=4)
    ours = [event("r1", t, t), event("r2", t, t)]
    live = [
        event("r1", t, MARKS.day_feed + timedelta(minutes=1)),
        event("r2", t, MARKS.day_feed - timedelta(minutes=1)),
    ]
    assert kinds(run(ours, live)) == [
        ("older revision", "r1", True),
        ("older revision", "r2", False),
    ]


def test_revision_times_a_few_seconds_apart_are_the_same_revision():
    """For `uw` events FDSN's `updated` is 5 to 9 s later than the summary feeds' (2026-10-21)."""
    t = NOW - timedelta(hours=12)
    mine = event("uw1", t, t)
    theirs = event("uw1", t, t + timedelta(seconds=9))
    assert run([mine], [theirs]).differences == []


def test_a_merged_event_matches_by_any_of_its_ids():
    t = NOW - timedelta(hours=2)
    ours = [event("us9", t, t, ids={"us9", "ak9"})]  # keyed by the ID it was created with
    live = [event("ak9", t, t, ids={"ak9", "us9"})]  # USGS prefers the other one now
    report = run(ours, live)
    assert report.ok and report.days["2026-10-21"] == {"usgs": 1, "ours": 1}


def test_an_event_usgs_split_after_earth_hub_merged_it_is_unexplained():
    """Seen live on 2026-10-20: USGS once listed us6000u3jl and pt26293000 as one event, then as
    two. Both match the one record Earth-Hub merged them into, which it never splits."""
    t = NOW - timedelta(hours=2)
    ours = [event("attn6uqd", t, t, ids={"attn6uqd", "pt26293000", "us6000u3jl"})]
    live = [
        event("pt26293000", t, t, ids={"attn6uqj", "pt26293000", "usauto6000u3jl"}),
        event("us6000u3jl", t, t, ids={"attn6uqd", "us6000u3jl"}),
    ]
    report = run(ours, live)
    assert kinds(report) == [("split at USGS", "us6000u3jl", False)]
    assert "merged both into attn6uqd" in report.differences[0].reason
    assert report.days["2026-10-21"] == {"usgs": 2, "ours": 1}


def test_an_event_whose_time_usgs_moved_out_of_the_window_is_looked_up():
    t = START + timedelta(minutes=5)
    moved = event("t1", START - timedelta(minutes=5), MARKS.month_feed + timedelta(minutes=1))
    report = run([event("t1", t, t)], lookup={"t1": moved}.get)
    assert kinds(report) == [("older revision", "t1", True)]
    assert "moved it to" in report.differences[0].reason


def test_an_event_usgs_has_never_heard_of_is_unexplained():
    t = NOW - timedelta(hours=8)
    report = run([event("x1", t, t)])
    assert kinds(report) == [("unknown at USGS", "x1", False)]


def test_deleted_here_but_live_at_usgs():
    t = NOW - timedelta(hours=2)
    revived = event("z1", t, MARKS.day_feed + timedelta(minutes=2))
    report = run([event("z1", t, t, deleted=True)], [revived])
    assert kinds(report) == [("deleted here", "z1", True)]
    assert report.days["2026-10-21"] == {"usgs": 1, "ours": 0}


def test_a_feature_that_names_no_event_is_ignored(feature_template):
    anonymous = event_at(feature_template, "", NOW, status="deleted")
    anonymous["properties"]["ids"] = None
    assert u.from_feature(anonymous) is None
    by_ids = event_at(feature_template, "", NOW, status="deleted")
    by_ids["properties"]["ids"] = ",tx2025wbgwbq,"
    assert u.from_feature(by_ids).id == "tx2025wbgwbq"


def fdsn_client(server: FdsnServer) -> FdsnClient:
    return FdsnClient(
        "https://earthquake.usgs.gov/fdsnws/event/1",
        make_http_client(transport=server.transport),
        pause_s=0,
        sleep=lambda s: None,
    )


def test_usgs_events_are_read_one_day_at_a_time_and_midnight_belongs_to_the_next(
    feature_template,
):
    server = FdsnServer()
    server.put(
        event_at(feature_template, "e1", START + timedelta(hours=1)),
        event_at(feature_template, "e2", START + u.DAY),  # exactly midnight
        event_at(feature_template, "gone", START + timedelta(hours=2), status="deleted"),
    )
    live = u.usgs_events(fdsn_client(server), START, END, deleted="false")
    assert sorted(e.id for e in live) == ["e1", "e2"]  # e2 once, on the 20th
    assert [u._day(e.time_ms) for e in live if e.id == "e2"] == ["2026-10-20"]
    assert len(server.requests_for("query")) == 3
    assert [e.id for e in u.usgs_events(fdsn_client(server), START, END, deleted="only")] == [
        "gone"
    ]


def test_an_event_is_looked_up_by_any_of_its_ids_deletions_included(feature_template):
    server = FdsnServer()
    merged = event_at(feature_template, "us1", NOW)
    merged["properties"]["ids"] = ",us1,ak1,"
    server.put(merged, event_at(feature_template, "gone", NOW, status="deleted"))
    lookup = u.usgs_lookup(fdsn_client(server))
    assert lookup("ak1").id == "us1"
    assert lookup("gone").deleted
    assert lookup("nope") is None


@pytest.mark.req("REQ-DQ-PROP-002")
def test_an_upper_case_deletion_at_usgs_is_a_deletion(feature_template):
    """DEF-049: the Pacific Tsunami Warning Center writes its statuses in upper case."""
    gone = event_at(feature_template, "pt1", NOW, status="DELETED")
    assert u.from_feature(gone).deleted


# --- the command, in direct mode -------------------------------------------------------------


@pytest.fixture
def check(s3, monkeypatch):
    """`python -m earth_hub.usgs_reconcile` with the bucket on moto and USGS mocked."""
    from earth_hub.config import Settings

    settings = Settings(
        _env_file=None, postgres_db="x", postgres_user="x", postgres_password="x", r2_bucket=BUCKET
    )
    monkeypatch.setattr("earth_hub.config.get_settings", lambda: settings)
    monkeypatch.setattr("earth_hub.export.bucket.make_s3_client", lambda settings: s3)
    state = {"ours": [], "live": [], "deleted": [], "last_round": datetime.now(UTC)}

    def from_bucket(bucket):
        marks = u.Marks(state["last_round"], state["last_round"], state["last_round"])
        return state["ours"], marks, state["last_round"]

    monkeypatch.setattr(u, "from_bucket", from_bucket)
    monkeypatch.setattr(
        u,
        "usgs_events",
        lambda fdsn, start, end, deleted: state[deleted == "only" and "deleted" or "live"],
    )
    return state


@pytest.mark.req("REQ-REC-003")
def test_the_command_skips_a_bucket_not_in_direct_mode_when_asked(check, capsys):
    assert u.main(["--skip-unless-direct"]) == 0
    assert "skipped: the bucket is in full mode" in capsys.readouterr().out


@pytest.mark.req("REQ-REC-003")
def test_the_command_fails_when_no_direct_round_has_written_for_too_long(check, s3, capsys):
    start_direct(s3)
    check["last_round"] = datetime.now(UTC) - timedelta(hours=3)
    assert u.main(["--skip-unless-direct", "--max-age-hours", "2"]) == 1
    assert "is the schedule running?" in capsys.readouterr().err


def test_the_command_exits_1_on_an_unexplained_difference_and_0_otherwise(check, s3, capsys):
    start_direct(s3)
    old = datetime.now(UTC) - timedelta(hours=5)
    check["live"] = [event("n1", old, old)]  # USGS has it since before the last round
    assert u.main(["--days", "1"]) == 1
    out = capsys.readouterr().out
    assert "UNEXPLAINED" in out and "RESULT: 1 unexplained" in out
    check["ours"] = [event("n1", old, old)]
    assert u.main(["--days", "1"]) == 0
    assert "RESULT: OK" in capsys.readouterr().out


def test_the_command_reports_in_json_free_lines(check, s3, capsys):
    start_direct(s3)
    u.main(["--days", "2"])
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("Earth-Hub (direct mode, the bucket) against USGS")
    assert all(not line.startswith("{") for line in lines)  # plain text, for the run page
