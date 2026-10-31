"""Earth-Hub against USGS (earth_hub/usgs_reconcile.py): every difference explained or not."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

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


def test_an_event_fdsn_lists_without_an_origin_at_our_revision_is_explained():
    # DEF-064: uw714132881 on 2026-10-31. The day feed listed it with its origin; FDSN, by ID,
    # only with its phase data, so its search by time left it out: "USGS has no such event".
    t = NOW - timedelta(hours=8)
    shell = u.Event("uw1", frozenset({"uw1"}), at(t), at(t) + 7_000, origin=False)
    report = run([event("uw1", t, t)], lookup={"uw1": shell}.get)
    assert kinds(report) == [("no origin at FDSN", "uw1", True)]
    assert report.ok and report.differences[0].settled
    assert report.days["2026-10-20"] == {"usgs": 0, "ours": 1}


@pytest.mark.parametrize(
    ("origin", "later", "says"),
    [
        (False, timedelta(minutes=5), "without an origin), but its search"),
        (True, timedelta(0), "but its search by time leaves it out"),
    ],
    ids=["origin dropped in a later revision", "with an origin"],
)
def test_an_event_usgs_has_but_its_search_leaves_out_is_otherwise_unexplained(origin, later, says):
    t = NOW - timedelta(hours=8)
    theirs = u.Event("uw1", frozenset({"uw1"}), at(t), at(t + later), origin=origin)
    report = run([event("uw1", t, t)], lookup={"uw1": theirs}.get)
    assert kinds(report) == [("unknown at USGS", "uw1", False)]
    assert says in report.differences[0].reason


@pytest.mark.parametrize(
    ("types", "origin"),
    [(",origin,phase-data,", True), (",phase-data,", False), (None, True), ("", True)],
)
def test_a_feature_says_whether_it_has_an_origin(types, origin):
    props = {"time": 1, "updated": 2, "ids": ",uw1,", "status": "automatic"}
    if types is not None:
        props["types"] = types
    assert u.from_feature({"id": "uw1", "properties": props}).origin is origin


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
    check["last_round"] = datetime.now(UTC) - timedelta(hours=6)
    assert u.main(["--skip-unless-direct", "--max-age-hours", "5"]) == 1
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


@pytest.mark.req("REQ-REC-004")
def test_direct_mode_measures_each_feed_by_when_usgs_generated_it(s3, tmp_path, make_envelope,
                                                                  make_feature):  # fmt: skip
    # DEF-054: the 11:17 round of 2026-10-29. USGS generated the day feed at 11:16:53, the round
    # fetched it at 11:17:38, its catch-up finished at 11:18:10; nc75459342 changed at 11:18:08.
    # The day feed was taken as read at 11:18:10, so the change looked missed.
    from earth_hub.direct.runner import DirectRunner
    from earth_hub.export.bucket import Bucket

    day = datetime(2026, 10, 29, tzinfo=UTC)
    feed_generated, catchup_generated = (
        day.replace(hour=11, minute=16, second=53),
        day.replace(hour=11, minute=17, second=42),
    )
    start_direct(s3)
    runner = DirectRunner(Bucket(s3, BUCKET), workdir=tmp_path, workers=2)

    def message(event_id, source, generated):
        ms = None if generated is None else int(generated.timestamp() * 1000)
        return make_envelope(make_feature(event_id), source=source, feed_generated_ms=ms)

    runner.process([message("ak1", "feed", feed_generated)], now=day.replace(hour=11, minute=17))
    runner.process(
        [message("ci2", "catchup", catchup_generated)], now=day.replace(hour=11, minute=18)
    )
    # A later round naming an older copy, or none, takes nothing back.
    runner.process(
        [message("ak3", "feed", feed_generated - timedelta(hours=1)), message("ak4", "feed", None)],
        now=day.replace(hour=11, minute=19),
    )

    _, marks, last_round = u.from_bucket(runner.bucket)
    assert (marks.day_feed, marks.month_feed) == (feed_generated, catchup_generated)
    assert last_round == day.replace(hour=11, minute=19)  # the freshness check keeps the end
    usgs = [
        u.Event("nc75459342", frozenset({"nc75459342"}), at(day.replace(hour=10)),
                at(day.replace(hour=11, minute=18, second=8))),  # after the feed was generated
        u.Event("nc1", frozenset({"nc1"}), at(day.replace(hour=10)),
                at(feed_generated - timedelta(seconds=1))),  # before it: a real miss
    ]  # fmt: skip
    report = u.compare([], usgs, [], marks, start=day, end=day + u.DAY)
    assert sorted(kinds(report)) == [("missing", "nc1", False), ("missing", "nc75459342", True)]


@pytest.mark.req("REQ-REC-004")
def test_without_a_generation_time_the_last_round_and_the_checkpoint_stand_in(
    s3, tmp_path, make_envelope, make_feature
):
    from earth_hub.direct.runner import DirectRunner
    from earth_hub.export.bucket import Bucket

    start_direct(s3)
    runner = DirectRunner(Bucket(s3, BUCKET), workdir=tmp_path, workers=2)
    runner.process([make_envelope(make_feature("ak1"))], now=NOW)  # feed_generated_ms: None
    runner.set_checkpoint("catchup", NOW - timedelta(minutes=5))
    _, marks, _ = u.from_bucket(runner.bucket)
    assert (marks.day_feed, marks.month_feed) == (NOW, NOW - timedelta(minutes=5))


# --- a range of days, counted by day, month or year ------------------------------------------


@pytest.mark.req("REQ-REC-005")
def test_the_window_is_the_last_days_or_a_range_of_whole_utc_days_cut_at_today():
    now = datetime(2026, 10, 31, 6, 30, tzinfo=UTC)
    midnight = datetime(2026, 11, 1, tzinfo=UTC)
    assert u.window(now, 3) == (midnight - 3 * u.DAY, midnight)
    assert u.window(now, 3, date(2026, 1, 1), date(2026, 3, 31)) == (
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 4, 1, tzinfo=UTC),
    )
    assert u.window(now, 3, date(2026, 10, 1)) == (datetime(2026, 10, 1, tzinfo=UTC), midnight)
    assert u.window(now, 3, date(2026, 10, 1), date(2027, 1, 1))[1] == midnight


@pytest.mark.req("REQ-REC-005")
def test_counts_and_differences_add_up_by_month_and_year():
    report = u.Report(
        days={
            "2025-12-31": {"usgs": 5, "ours": 5},
            "2026-01-01": {"usgs": 3, "ours": 2},
            "2026-01-31": {"usgs": 4, "ours": 4},
            "2026-02-01": {"usgs": 1, "ours": 2},
        },
        differences=[
            u.Difference("2025-12-31", "no origin at FDSN", "c", True, ""),  # settled
            u.Difference("2026-01-01", "missing", "a", True, ""),
            u.Difference("2026-02-01", "unknown at USGS", "b", False, ""),
        ],
    )
    assert u.by_period(report, "month") == {
        "2025-12": {"usgs": 5, "ours": 5, "pending": 0, "explained": 1, "unexplained": 0},
        "2026-01": {"usgs": 7, "ours": 6, "pending": 1, "explained": 0, "unexplained": 0},
        "2026-02": {"usgs": 1, "ours": 2, "pending": 0, "explained": 0, "unexplained": 1},
    }
    assert u.by_period(report, "year") == {
        "2025": {"usgs": 5, "ours": 5, "pending": 0, "explained": 1, "unexplained": 0},
        "2026": {"usgs": 8, "ours": 8, "pending": 1, "explained": 0, "unexplained": 1},
    }
    assert list(u.by_period(report)) == sorted(report.days)


@pytest.mark.req("REQ-REC-005")
def test_the_command_compares_the_range_asked_and_prints_a_line_per_period_and_a_total(
    check, s3, monkeypatch, capsys
):
    start_direct(s3)
    asked = []
    monkeypatch.setattr(
        u, "usgs_events", lambda fdsn, start, end, deleted: asked.append((start, end)) or []
    )
    assert u.main(["--start", "2026-01-01", "--end", "2026-02-28", "--by", "month"]) == 0
    assert set(asked) == {(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 3, 1, tzinfo=UTC))}
    lines = capsys.readouterr().out.splitlines()
    assert "by UTC month of the event" in lines[0]
    assert lines[2:5] == [
        "  2026-01: USGS 0, Earth-Hub 0; 0 pending, 0 unexplained",
        "  2026-02: USGS 0, Earth-Hub 0; 0 pending, 0 unexplained",
        "  total: USGS 0, Earth-Hub 0; 0 pending, 0 unexplained",
    ]


@pytest.mark.req("REQ-REC-005")
@pytest.mark.parametrize(
    "argv",
    [
        ["--end", "2026-01-31"],
        ["--start", "2026-02-01", "--end", "2026-01-31"],
        ["--days", "3", "--start", "2026-01-01"],
        ["--start", "2026-01-01", "--by", "week"],
    ],
)
def test_a_range_without_a_start_backwards_or_with_days_is_refused(check, argv):
    with pytest.raises(SystemExit) as exc:
        u.main(argv)
    assert exc.value.code == 2


# --- history older than the month feed, and deletions with no time (DEF-066, DEF-067) ----------

RECORD = json.loads(
    (
        Path(__file__).resolve().parents[1] / "fixtures/usgs/deleted_without_time_tx2026pfurqq.json"
    ).read_text()
)
AUG_4 = datetime(2026, 8, 4, tzinfo=UTC)


@pytest.mark.req("REQ-REC-006")
def test_a_deletion_fdsn_kept_no_time_for_is_read_by_its_products():
    found = u.from_feature(RECORD)
    assert (found.id, found.ids, found.deleted, found.timeless) == (
        "tx2026pfurqq",
        frozenset({"tx2026pfurqq"}),
        True,
        True,
    )
    assert u.from_feature({"properties": {"status": "deleted"}}) is None  # names nothing


@pytest.mark.req("REQ-REC-006")
@pytest.mark.parametrize(
    ("audited", "explained", "says"),
    [
        (None, True, "after the audit covered 2026-08"),  # not covered yet
        (datetime(2026, 10, 1, tzinfo=UTC), True, "after the audit last covered 2026-08"),
        (datetime(2026, 10, 31, tzinfo=UTC), False, "before the audit last covered 2026-08"),
    ],
)
def test_such_a_deletion_is_judged_by_when_the_audit_last_covered_its_month(
    audited, explained, says
):
    mine = event("tx2026pfurqq", AUG_4 + timedelta(hours=3), AUG_4 + timedelta(hours=4))
    marks = u.Marks(NOW, NOW, NOW, NOW, {"2026-08": audited} if audited else {})
    report = u.compare(
        [mine],
        [],
        [],
        marks,
        start=AUG_4,
        end=AUG_4 + u.DAY,
        lookup=lambda i: u.from_feature(RECORD),
    )
    [d] = report.differences
    assert (d.kind, d.event_id, d.explained) == ("deleted at USGS", "tx2026pfurqq", explained)
    assert says in d.reason and "keeping no time" in d.reason


@pytest.mark.req("REQ-REC-006")
def test_older_than_the_month_feed_a_revision_is_judged_by_the_history_pass():
    marks = u.Marks(day_feed=NOW, month_feed=NOW, deletions=NOW, history=NOW - timedelta(hours=10))
    revised_after = event("a", AUG_4, NOW - timedelta(hours=2))  # after the last history pass
    revised_before = event("b", AUG_4, NOW - timedelta(hours=12))  # before it: a real miss
    ours = [event("a", AUG_4, AUG_4), event("b", AUG_4, AUG_4)]
    report = u.compare(
        ours, [revised_after, revised_before], [], marks, start=AUG_4, end=AUG_4 + u.DAY
    )
    assert sorted(kinds(report)) == [("older revision", "a", True), ("older revision", "b", False)]
    assert all("history pass" in d.reason for d in report.differences)
    # Within the month feed's 30 days, the month feed still judges it.
    recent = NOW - timedelta(days=5)
    assert marks.feed_for(event("c", recent, recent)) == ("month feed", NOW)
