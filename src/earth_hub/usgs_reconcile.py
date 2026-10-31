"""Earth-Hub against USGS itself: `python -m earth_hub.usgs_reconcile` (make reconcile-usgs).

The other checks compare Earth-Hub with itself (make reconcile: Kafka with PostgreSQL;
make reconcile-direct: the bucket's parts; the handovers: one mode with the other). This one
compares what the mode writing the bucket holds with what USGS's FDSN service says now, for the
last few UTC days or any range of them (--start, --end), counted by UTC day, month or year
(--by), and explains every difference by where Earth-Hub is in reading USGS:

| Difference                         | Explained (pending) when                                  |
| ---------------------------------- | --------------------------------------------------------- |
| USGS has it, Earth-Hub doesn't     | USGS updated it after the feed that brings it was read    |
| Earth-Hub has it, USGS deleted it  | USGS deleted it after the last deleted-events check       |
| Both have it, USGS's is newer      | USGS updated it after the feed that brings it was read    |
| Both have it, FDSN without origin  | always, at the revision Earth-Hub holds: FDSN's search    |
|                                    | leaves out events with no origin, which the feeds list    |

The feed that brings an event is the day feed (`all_day`) for events of the 24 hours before its
last read, and catch-up's month feed otherwise. Anything else is unexplained, and the command
exits 1. Events are matched by any of their IDs, so a merged event counts once.

A feed's last read is the time USGS generated the newest copy of it Earth-Hub processed (its
`metadata.generated`, in every message's envelope): what its content was as of. The feed is
fetched up to a minute after that, and a round finishes a minute or two later; an update in
between is not in what was read (DEF-054). Full mode reads PostgreSQL (the newest generation
time among raw_events' feed and catch-up messages); direct mode reads the bucket's snapshot and
ID map, and the generation times each round keeps in its state (direct/runner.py, FEEDS).
Without one (no message carried it yet), the older, later marks stand in: the live topic's
last delivery or the last round's end for the day feed, catch-up's checkpoint for the month
feed. The deleted-events checkpoint says when the deletions were last checked.

Read-only; two FDSN requests a day, and one per event of ours that USGS no longer lists in the
window (its time may have moved), at most LOOKUPS.
"""

import argparse
import json
import logging
import sys
import tempfile
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock
from pathlib import Path

from earth_hub.ingestion import history
from earth_hub.validation.earthquake_rules import review_status

logger = logging.getLogger(__spec__.name)  # the module name, also under `python -m`

DAY = timedelta(days=1)
DAY_FEED = timedelta(hours=24)  # what all_day lists, before the moment it was read
MONTH_FEED = timedelta(days=30)  # what all_month lists, before the moment it was read
LOOKUPS = 25  # events looked up again by ID, at most, per run
# The same revision can carry different `updated` times: for `uw` events FDSN says 5 to 9 s later
# than the summary feeds the poller and catch-up read (measured 2026-10-21).
SAME_REVISION_MS = 60_000
SETTLED = {"no origin at FDSN"}  # explained, but not pending: USGS's services disagree (DEF-064)


@dataclass(frozen=True)
class Event:
    """One event, from either side: its IDs, time, revision time, and whether it is deleted."""

    id: str
    ids: frozenset[str]
    time_ms: int
    updated_ms: int
    deleted: bool = False
    # Whether USGS lists an `origin` product for it. FDSN's searches by time leave out an event
    # without one; its summary feeds may still list it with one (DEF-064).
    origin: bool = True
    # A deletion FDSN kept no time for: only a lookup by ID finds it, and only the audit
    # brings it (DEF-066).
    timeless: bool = False


@dataclass(frozen=True)
class Marks:
    """When Earth-Hub last read each USGS source (None: never)."""

    day_feed: datetime | None
    month_feed: datetime | None
    deletions: datetime | None
    history: datetime | None = None  # the last history pass (DEF-067)
    audits: dict[str, datetime] = field(default_factory=dict)  # YYYY-MM -> last audited

    def feed_for(self, event: Event) -> tuple[str, datetime | None]:
        if self.day_feed is not None and event.time_ms >= _ms(self.day_feed - DAY_FEED):
            return "day feed", self.day_feed
        if self.month_feed is not None and event.time_ms >= _ms(self.month_feed - MONTH_FEED):
            return "month feed", self.month_feed
        if self.month_feed is None and self.history is None:
            return "month feed", None
        return "history pass", self.history  # older than the month feed lists (DEF-067)


@dataclass
class Difference:
    day: str
    kind: str  # missing, deleted at USGS, older revision, deleted here, no origin at FDSN, ...
    event_id: str
    explained: bool
    reason: str

    @property
    def settled(self) -> bool:
        """Explained without waiting for anything: no later read of USGS will change it."""
        return self.explained and self.kind in SETTLED


@dataclass
class Report:
    days: dict[str, dict[str, int]] = field(default_factory=dict)  # day -> usgs, ours
    differences: list[Difference] = field(default_factory=list)

    @property
    def unexplained(self) -> list[Difference]:
        return [d for d in self.differences if not d.explained]

    @property
    def ok(self) -> bool:
        return not self.unexplained


def _ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


PERIODS = {"day": 10, "month": 7, "year": 4}  # how much of YYYY-MM-DD names a period
STATUSES = ("pending", "explained", "unexplained")


def _status(d: Difference) -> str:
    if not d.explained:
        return "unexplained"
    return "explained" if d.settled else "pending"


def by_period(report: Report, by: str = "day") -> dict[str, dict[str, int]]:
    """The report's counts per UTC day, month or year of the event, in order: USGS's events,
    Earth-Hub's, and its differences by status (STATUSES)."""
    width = PERIODS[by]
    rows: dict[str, dict[str, int]] = {}

    def row(day: str) -> dict[str, int]:
        return rows.setdefault(day[:width], dict.fromkeys(("usgs", "ours", *STATUSES), 0))

    for day, n in report.days.items():
        row(day)["usgs"] += n["usgs"]
        row(day)["ours"] += n["ours"]
    for d in report.differences:
        row(d.day)[_status(d)] += 1
    return dict(sorted(rows.items()))


def window(
    now: datetime, days: int, first: date | None = None, last: date | None = None
) -> tuple[datetime, datetime]:
    """The UTC days to compare, as [start, end): the last `days` days, today included, or
    from `first` to `last` (both included; `last` defaults to today, and is cut at today)."""
    tomorrow = datetime.combine(now.astimezone(UTC).date() + DAY, clock(), UTC)
    if first is None:
        return tomorrow - days * DAY, tomorrow
    end = min(tomorrow, datetime.combine(last + DAY, clock(), UTC)) if last else tomorrow
    return datetime.combine(first, clock(), UTC), end


def _day(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d")


def _when(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S")


def _after(event: Event, mark: datetime | None) -> bool:
    return mark is None or event.updated_ms > _ms(mark)


def compare(  # noqa: C901, PLR0912 (to refactor: docs/progress/plan.md)
    ours: Iterable[Event],
    live: Iterable[Event],
    deleted: Iterable[Event],
    marks: Marks,
    *,
    start: datetime,
    end: datetime,
    lookup: Callable[[str], Event | None] = lambda event_id: None,
) -> Report:
    """Every difference between Earth-Hub (`ours`, all its events) and USGS (`live` and
    `deleted`: its events in [start, end)), explained or not."""
    lo, hi = _ms(start), _ms(end)
    by_id: dict[str, Event] = {}
    for event in ours:
        for i in event.ids | {event.id}:
            by_id[i] = event
    report = Report()
    for day in range(lo, hi, _ms(start + DAY) - lo):
        report.days[_day(day)] = {"usgs": 0, "ours": 0}

    matched: dict[str, str] = {}  # our event's id -> the first USGS event matched to it
    for theirs in live:
        report.days.setdefault(_day(theirs.time_ms), {"usgs": 0, "ours": 0})["usgs"] += 1
        mine = next((by_id[i] for i in sorted(theirs.ids | {theirs.id}) if i in by_id), None)
        source, mark = marks.feed_for(theirs)
        pending = _after(theirs, mark)
        if mine is None:
            report.differences.append(
                Difference(
                    _day(theirs.time_ms),
                    "missing",
                    theirs.id,
                    pending,
                    f"USGS updated it at {_when(theirs.updated_ms)}, "
                    + (
                        f"after the {source} was last read"
                        if pending
                        else f"before the {source} was last read"
                    ),
                )
            )
            continue
        if mine.id in matched:
            # USGS split what it once listed as one event; Earth-Hub never splits a record.
            report.differences.append(
                Difference(
                    _day(theirs.time_ms),
                    "split at USGS",
                    theirs.id,
                    False,
                    f"USGS lists it apart from {matched[mine.id]}; Earth-Hub merged both "
                    f"into {mine.id}",
                )
            )
            continue
        matched[mine.id] = theirs.id
        if mine.deleted:
            report.differences.append(
                Difference(
                    _day(theirs.time_ms),
                    "deleted here",
                    theirs.id,
                    pending and theirs.updated_ms > mine.updated_ms,
                    "deleted in Earth-Hub, live at USGS"
                    + (
                        f" since {_when(theirs.updated_ms)}"
                        if theirs.updated_ms > mine.updated_ms
                        else ""
                    ),
                )
            )
        elif theirs.updated_ms - mine.updated_ms > SAME_REVISION_MS:
            report.differences.append(
                Difference(
                    _day(theirs.time_ms),
                    "older revision",
                    theirs.id,
                    pending,
                    f"USGS revised it at {_when(theirs.updated_ms)}, "
                    + (
                        f"after the {source} was last read"
                        if pending
                        else f"before the {source} was last read"
                    ),
                )
            )

    deletions = {i: e for e in deleted for i in e.ids | {e.id}}
    seen: set[str] = set()
    for mine in by_id.values():
        if mine.id in seen or mine.id in matched or mine.deleted:
            continue
        seen.add(mine.id)
        if not lo <= mine.time_ms < hi:
            continue
        gone = next((deletions[i] for i in sorted(mine.ids | {mine.id}) if i in deletions), None)
        if gone is None:
            gone = lookup(mine.id)  # its time may have moved, or it was deleted elsewhere
            if gone is not None and not gone.deleted:
                inside = lo <= gone.time_ms < hi
                source, mark = marks.feed_for(gone)
                pending = _after(gone, mark)
                if not inside:
                    report.differences.append(
                        Difference(
                            _day(mine.time_ms),
                            "older revision",
                            mine.id,
                            pending,
                            f"USGS moved it to {_when(gone.time_ms)} at {_when(gone.updated_ms)}",
                        )
                    )
                    continue
                report.differences.append(_left_out(mine, gone))
                continue
        if gone is not None and gone.deleted:
            report.differences.append(_deleted_at_usgs(mine, gone, marks))
            continue
        report.differences.append(
            Difference(
                _day(mine.time_ms), "unknown at USGS", mine.id, False, "USGS has no such event"
            )
        )

    for mine in {e.id: e for e in by_id.values()}.values():
        if lo <= mine.time_ms < hi and not mine.deleted:
            report.days.setdefault(_day(mine.time_ms), {"usgs": 0, "ours": 0})["ours"] += 1
    report.differences.sort(key=lambda d: (d.day, d.kind, d.event_id))
    return report


def _deleted_at_usgs(mine: Event, gone: Event, marks: Marks) -> Difference:
    """Earth-Hub has it, USGS deleted it: pending if the source that brings the deletion was
    last read before USGS deleted it."""
    if gone.timeless:  # only the audit, a month at a time, brings it (DEF-066)
        month = _day(mine.time_ms)[:7]
        mark, read = marks.audits.get(month), f"the audit last covered {month}"
        if mark is None:
            read = f"the audit covered {month}"  # not yet: pending
    else:
        mark, read = marks.deletions, "the last deleted-events check"
    pending = _after(gone, mark)
    return Difference(
        _day(mine.time_ms),
        "deleted at USGS",
        mine.id,
        pending,
        f"USGS deleted it at {_when(gone.updated_ms)}"
        + (", keeping no time for it," if gone.timeless else ",")
        + (f" after {read}" if pending else f" before {read}"),
    )


def _left_out(mine: Event, gone: Event) -> Difference:
    """USGS has the event, in the window, not deleted, yet its search by time left it out."""
    if not gone.origin and gone.updated_ms - mine.updated_ms <= SAME_REVISION_MS:
        # Seen on 2026-10-31 (uw714132881): the day feed listed it with its origin; FDSN, at the
        # same revision, only with its phase data (DEF-064). USGS's services disagree, not
        # Earth-Hub with USGS; a later revision without the origin is a change, unexplained.
        return Difference(
            _day(mine.time_ms),
            "no origin at FDSN",
            mine.id,
            True,
            "FDSN lists it without an origin, at the revision Earth-Hub holds, so its search by "
            "time leaves it out; the summary feeds listed it with one",
        )
    return Difference(
        _day(mine.time_ms),
        "unknown at USGS",
        mine.id,
        False,
        f"USGS has it (revised {_when(gone.updated_ms)}"
        + (", without an origin" if not gone.origin else "")
        + "), but its search by time leaves it out",
    )


# --- USGS -------------------------------------------------------------------------------------


def from_feature(feature: dict) -> Event | None:
    props = feature.get("properties") or {}
    event_id = feature.get("id") or ""
    ids = frozenset(i for i in (props.get("ids") or "").split(",") if i)
    if not event_id and not ids:
        found = history.without_time(feature)  # named only by its products (DEF-066)
        if found is not None:
            code, deleted_ms = found
            return Event(code, frozenset({code}), 0, deleted_ms, True, timeless=True)
        return None  # a deletion that names no event (REQ-BF-014)
    if not isinstance(props.get("time"), int) or not isinstance(props.get("updated"), int):
        return None
    return Event(
        event_id or sorted(ids)[0],
        ids,
        props["time"],
        props["updated"],
        review_status(props) == "deleted",
        _has_origin(props),
    )


def _has_origin(props: dict) -> bool:
    """False only when USGS lists the event's products and an origin is not among them."""
    types = props.get("types")
    if not isinstance(types, str) or not types.strip(","):
        return True  # not listed: nothing to go by
    return "origin" in types.split(",")


def usgs_events(fdsn, start: datetime, end: datetime, *, deleted: str) -> list[Event]:
    """USGS's events in [start, end), one request a day (`deleted`: false or only)."""
    events = []
    day = start
    while day < end:
        upper = min(day + DAY, end)
        body = json.loads(fdsn.query(day, upper, include_deleted=deleted))
        for feature in body.get("features", []):
            event = from_feature(feature)
            # FDSN's end is inclusive: an event at midnight belongs to the next day.
            if event is not None and event.time_ms < _ms(upper):
                events.append(event)
        day = upper
    return events


def usgs_lookup(fdsn) -> Callable[[str], Event | None]:
    def lookup(event_id: str) -> Event | None:
        body = fdsn.event(event_id)
        return None if body is None else from_feature(json.loads(body))

    return lookup


# --- Earth-Hub ---------------------------------------------------------------------------------


def _from_ms(ms) -> datetime | None:
    return None if ms is None else datetime.fromtimestamp(float(ms) / 1000, UTC)


def from_postgres(engine, live_topic: str) -> tuple[list[Event], Marks]:
    """The events and the marks from one snapshot. Read apart, a message stored in between was
    in the marks (its feed read) but not in the events, a false `missing` (DEF-065)."""
    from sqlalchemy import text

    with (
        engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn,
        conn.begin(),
    ):
        ids: dict[str, set[str]] = {}
        for key, event_id in conn.execute(
            text(
                "SELECT e.key, i.event_id FROM earthquake_ids i"
                " JOIN earthquakes e ON e.id = i.earthquake_id"
            )
        ):
            ids.setdefault(key, set()).add(event_id)
        events = [
            Event(key, frozenset(ids.get(key, ())), t, u, status == "deleted")
            for key, t, u, status in conn.execute(
                text("SELECT key, event_time_ms, source_updated_ms, review_status FROM earthquakes")
            )
        ]
        delivered = conn.execute(
            text("SELECT max(received_at) FROM raw_events WHERE kafka_topic = :t"),
            {"t": live_topic},
        ).scalar()
        generated = dict(
            conn.execute(
                text(
                    "SELECT envelope->>'source', max((envelope->>'feed_generated_ms')::numeric)"
                    " FROM raw_events"
                    " WHERE envelope->>'source' IN ('feed', 'catchup')"
                    " AND jsonb_typeof(envelope->'feed_generated_ms') = 'number'"
                    " GROUP BY 1"
                )
            ).all()
        )
        checkpoints = dict(
            conn.execute(text("SELECT name, value FROM ingestion_checkpoints")).all()
        )
    return events, Marks(
        _from_ms(generated.get("feed")) or delivered,
        _from_ms(generated.get("catchup")) or checkpoints.get("catchup"),
        checkpoints.get("deleted"),
        checkpoints.get("revised"),
        _audits(checkpoints),
    )


def _audits(checkpoints: dict) -> dict[str, datetime]:
    """When the audit last covered each month: its checkpoints `audit:YYYY-MM`."""
    return {
        name.removeprefix("audit:"): value
        for name, value in checkpoints.items()
        if name.startswith("audit:") and value is not None
    }


def from_bucket(bucket) -> tuple[list[Event], Marks, datetime | None]:
    import duckdb

    from earth_hub.direct.runner import DirectRunner

    with tempfile.TemporaryDirectory() as tmp:
        state = DirectRunner(bucket, workdir=Path(tmp)).state
        state.refresh()
        rows = duckdb.sql(
            "SELECT key, event_time_ms, source_updated_ms, review_status "  # noqa: S608 (our temp file)
            f"FROM '{state.snapshot_path}'"
        ).fetchall()
        ids = state.ids_by_key
        status = state.status
    events = [
        Event(key, frozenset(ids.get(key, ())), t, u, review == "deleted")
        for key, t, u, review in rows
    ]
    checkpoints = status.get("checkpoints", {})
    generated = status.get("generated", {})
    parse = lambda v: datetime.fromisoformat(v) if v else None  # noqa: E731
    last_round = parse(status.get("written_at"))
    return (
        events,
        Marks(
            parse(generated.get("feed")) or last_round,
            parse(generated.get("catchup")) or parse(checkpoints.get("catchup")),
            parse(checkpoints.get("deleted")),
            parse(checkpoints.get("revised")),
            _audits({name: parse(value) for name, value in checkpoints.items()}),
        ),
        last_round,
    )


# --- The command -------------------------------------------------------------------------------


def print_report(report: Report, marks: Marks, where: str, by: str = "day") -> None:
    def show(moment):
        return moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S") if moment else "never"

    print(f"Earth-Hub ({where}) against USGS, by UTC {by} of the event")
    print(
        f"  last read (as USGS generated it): day feed {show(marks.day_feed)}, "
        f"month feed {show(marks.month_feed)}; deletions checked {show(marks.deletions)}; "
        f"history pass {show(marks.history)}, audit {len(marks.audits)} months"
    )
    rows = by_period(report, by)
    total = {key: sum(n[key] for n in rows.values()) for key in ("usgs", "ours", *STATUSES)}
    for period, n in [*rows.items(), ("total", total)]:
        print(
            f"  {period}: USGS {n['usgs']}, Earth-Hub {n['ours']}; "
            + ", ".join(
                f"{n[status]} {status}"
                for status in STATUSES
                if n[status] or status != "explained"  # shown only when there are some
            )
        )
    for d in report.differences:
        tag = ("explained" if d.settled else "pending") if d.explained else "UNEXPLAINED"
        print(f"    {tag:11} {d.day} {d.kind:15} {d.event_id}: {d.reason}")
    print("RESULT: " + ("OK" if report.ok else f"{len(report.unexplained)} unexplained"))


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare Earth-Hub with USGS, explaining every difference."
    )
    span = parser.add_mutually_exclusive_group()
    span.add_argument(
        "--days", type=int, default=3, help="UTC days back from today, today included"
    )
    span.add_argument("--start", type=date.fromisoformat, help="first UTC day, YYYY-MM-DD")
    parser.add_argument(
        "--end", type=date.fromisoformat, help="with --start: last UTC day (default today)"
    )
    parser.add_argument(
        "--by", choices=PERIODS, default="day", help="count by UTC day, month or year"
    )
    parser.add_argument(
        "--skip-unless-direct", action="store_true", help="exit 0 unless in direct mode"
    )
    parser.add_argument(
        "--max-age-hours", type=float, help="direct mode: fail if the last round is older"
    )
    parser.add_argument(
        "--wait", type=float, default=0, help="full mode: seconds to wait for consumer lag 0"
    )
    args = parser.parse_args(argv)
    if args.end is not None and (args.start is None or args.end < args.start):
        parser.error("--end needs --start, and not before it")
    return args


def main(argv: list[str] | None = None) -> int:  # noqa: PLR0915 (to refactor: docs/progress/plan.md)
    from earth_hub.config import get_settings
    from earth_hub.export import mode
    from earth_hub.export.bucket import Bucket, MissingSettings, make_s3_client
    from earth_hub.ingestion.fdsn import FdsnClient
    from earth_hub.ingestion.usgs_client import make_http_client
    from earth_hub.logging_config import configure_logging

    args = _arguments(argv)
    configure_logging()
    settings = get_settings()
    try:
        bucket = Bucket(make_s3_client(settings), settings.r2_bucket)
        current = mode.read(bucket)
    except MissingSettings as exc:
        print(f"reconcile-usgs: {exc}", file=sys.stderr)
        return 2
    direct = (current.mode, current.state) == (mode.DIRECT, mode.ACTIVE)
    if args.skip_unless_direct and not direct:
        print(f"reconcile-usgs: skipped: the bucket is in {current.mode} mode ({current.state})")
        return 0
    if current.state != mode.ACTIVE:
        print("reconcile-usgs: a handover is running; try again after it", file=sys.stderr)
        return 2

    now = datetime.now(UTC)
    if direct:
        events, marks, last_round = from_bucket(bucket)
        where = "direct mode, the bucket"
        if args.max_age_hours is not None and (
            last_round is None or now - last_round > timedelta(hours=args.max_age_hours)
        ):
            print(
                f"reconcile-usgs: no direct round has written since {last_round} "
                f"(over {args.max_age_hours} h): is the schedule running?",
                file=sys.stderr,
            )
            return 1
    else:
        from earth_hub.database.engine import make_engine
        from earth_hub.reconcile import reconcile

        engine = make_engine(settings)
        deadline = time.monotonic() + args.wait
        for topic, group in (
            (settings.kafka_topic_raw, settings.kafka_consumer_group),
            (settings.kafka_topic_backfill, settings.kafka_backfill_consumer_group),
        ):
            while reconcile(engine, settings.kafka_bootstrap_servers, topic, group).pending:
                if time.monotonic() > deadline:
                    break
                time.sleep(2)
        events, marks = from_postgres(engine, settings.kafka_topic_raw)
        where = "full mode, PostgreSQL"

    start, end = window(now, args.days, args.start, args.end)
    with make_http_client(settings.usgs_contact, timeout_s=60.0) as http:
        fdsn = FdsnClient(str(settings.fdsn_url), http, pause_s=settings.fdsn_pause_seconds)
        live = usgs_events(fdsn, start, end, deleted="false")
        gone = usgs_events(fdsn, start, end, deleted="only")
        lookups = [0]

        def lookup(event_id: str) -> Event | None:
            lookups[0] += 1
            return usgs_lookup(fdsn)(event_id) if lookups[0] <= LOOKUPS else None

        report = compare(events, live, gone, marks, start=start, end=end, lookup=lookup)
    print_report(report, marks, where, args.by)
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
