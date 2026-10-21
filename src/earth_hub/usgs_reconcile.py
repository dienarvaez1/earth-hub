"""Earth-Hub against USGS itself: `python -m earth_hub.usgs_reconcile` (make reconcile-usgs).

The other checks compare Earth-Hub with itself (make reconcile: Kafka with PostgreSQL;
make reconcile-direct: the bucket's parts; the handovers: one mode with the other). This one
compares what the mode writing the bucket holds with what USGS's FDSN service says now, for the
last few UTC days, and explains every difference by where Earth-Hub is in reading USGS:

| Difference                         | Explained (pending) when                                  |
| ---------------------------------- | --------------------------------------------------------- |
| USGS has it, Earth-Hub doesn't     | USGS updated it after the feed that brings it was read    |
| Earth-Hub has it, USGS deleted it  | USGS deleted it after the last deleted-events check       |
| Both have it, USGS's is newer      | USGS updated it after the feed that brings it was read    |

The feed that brings an event is the day feed (`all_day`) for events of the 24 hours before its
last read, and catch-up's month feed otherwise. Anything else is unexplained, and the command
exits 1. Events are matched by any of their IDs, so a merged event counts once.

Full mode reads PostgreSQL (the day feed's last read: the live topic's last delivery); direct
mode reads the bucket's snapshot and ID map (the last direct round). Catch-up's checkpoints say
when the month feed and the deletions were last read. Read-only; two FDSN requests a day, and
one per event of ours that USGS no longer lists in the window (its time may have moved), at most
LOOKUPS.
"""

import argparse
import json
import logging
import sys
import tempfile
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__spec__.name)  # the module name, also under `python -m`

DAY = timedelta(days=1)
DAY_FEED = timedelta(hours=24)  # what all_day lists, before the moment it was read
LOOKUPS = 25  # events looked up again by ID, at most, per run
# The same revision can carry different `updated` times: for `uw` events FDSN says 5 to 9 s later
# than the summary feeds the poller and catch-up read (measured 2026-10-21).
SAME_REVISION_MS = 60_000


@dataclass(frozen=True)
class Event:
    """One event, from either side: its IDs, time, revision time, and whether it is deleted."""

    id: str
    ids: frozenset[str]
    time_ms: int
    updated_ms: int
    deleted: bool = False


@dataclass(frozen=True)
class Marks:
    """When Earth-Hub last read each USGS source (None: never)."""

    day_feed: datetime | None
    month_feed: datetime | None
    deletions: datetime | None

    def feed_for(self, event: Event) -> tuple[str, datetime | None]:
        if self.day_feed is not None and event.time_ms >= _ms(self.day_feed - DAY_FEED):
            return "day feed", self.day_feed
        return "month feed", self.month_feed


@dataclass
class Difference:
    day: str
    kind: str  # missing, deleted at USGS, older revision, deleted here
    event_id: str
    explained: bool
    reason: str


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


def _day(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d")


def _when(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S")


def _after(event: Event, mark: datetime | None) -> bool:
    return mark is None or event.updated_ms > _ms(mark)


def compare(
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
        if gone is not None and gone.deleted:
            pending = _after(gone, marks.deletions)
            report.differences.append(
                Difference(
                    _day(mine.time_ms),
                    "deleted at USGS",
                    mine.id,
                    pending,
                    f"USGS deleted it at {_when(gone.updated_ms)}, "
                    + (
                        "after the last deleted-events check"
                        if pending
                        else "before the last deleted-events check"
                    ),
                )
            )
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


# --- USGS -------------------------------------------------------------------------------------


def from_feature(feature: dict) -> Event | None:
    props = feature.get("properties") or {}
    event_id = feature.get("id") or ""
    ids = frozenset(i for i in (props.get("ids") or "").split(",") if i)
    if not event_id and not ids:
        return None  # a deletion that names no event (REQ-BF-014)
    if not isinstance(props.get("time"), int) or not isinstance(props.get("updated"), int):
        return None
    return Event(
        event_id or sorted(ids)[0],
        ids,
        props["time"],
        props["updated"],
        props.get("status") == "deleted",
    )


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


def from_postgres(engine, live_topic: str) -> tuple[list[Event], Marks]:
    from sqlalchemy import text

    with engine.connect() as conn:
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
        checkpoints = dict(
            conn.execute(text("SELECT name, value FROM ingestion_checkpoints")).all()
        )
    return events, Marks(delivered, checkpoints.get("catchup"), checkpoints.get("deleted"))


def from_bucket(bucket) -> tuple[list[Event], Marks, datetime | None]:
    import duckdb

    from earth_hub.direct.runner import DirectRunner

    with tempfile.TemporaryDirectory() as tmp:
        state = DirectRunner(bucket, workdir=Path(tmp)).state
        state.refresh()
        rows = duckdb.sql(
            "SELECT key, event_time_ms, source_updated_ms, review_status "
            f"FROM '{state.snapshot_path}'"
        ).fetchall()
        ids = state.ids_by_key
        status = state.status
    events = [
        Event(key, frozenset(ids.get(key, ())), t, u, review == "deleted")
        for key, t, u, review in rows
    ]
    checkpoints = status.get("checkpoints", {})
    parse = lambda v: datetime.fromisoformat(v) if v else None  # noqa: E731
    last_round = parse(status.get("written_at"))
    return (
        events,
        Marks(last_round, parse(checkpoints.get("catchup")), parse(checkpoints.get("deleted"))),
        last_round,
    )


# --- The command -------------------------------------------------------------------------------


def print_report(report: Report, marks: Marks, where: str) -> None:
    def show(moment):
        return moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S") if moment else "never"

    print(f"Earth-Hub ({where}) against USGS, by UTC day of the event")
    print(
        f"  last read: day feed {show(marks.day_feed)}, month feed {show(marks.month_feed)}, "
        f"deletions {show(marks.deletions)}"
    )
    for day, n in report.days.items():
        mine = [d for d in report.differences if d.day == day]
        pending = sum(d.explained for d in mine)
        print(
            f"  {day}: USGS {n['usgs']}, Earth-Hub {n['ours']}; "
            f"{pending} pending, {len(mine) - pending} unexplained"
        )
    for d in report.differences:
        tag = "pending" if d.explained else "UNEXPLAINED"
        print(f"    {tag:11} {d.day} {d.kind:15} {d.event_id}: {d.reason}")
    print("RESULT: " + ("OK" if report.ok else f"{len(report.unexplained)} unexplained"))


def main(argv: list[str] | None = None) -> int:
    from earth_hub.config import get_settings
    from earth_hub.export import mode
    from earth_hub.export.bucket import Bucket, MissingSettings, make_s3_client
    from earth_hub.ingestion.fdsn import FdsnClient
    from earth_hub.ingestion.usgs_client import make_http_client
    from earth_hub.logging_config import configure_logging

    parser = argparse.ArgumentParser(
        description="Compare Earth-Hub with USGS, explaining every difference."
    )
    parser.add_argument(
        "--days", type=int, default=3, help="UTC days back from today, today included"
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

    end = (now + DAY).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - args.days * DAY
    with make_http_client(settings.usgs_contact, timeout_s=60.0) as http:
        fdsn = FdsnClient(str(settings.fdsn_url), http, pause_s=settings.fdsn_pause_seconds)
        live = usgs_events(fdsn, start, end, deleted="false")
        gone = usgs_events(fdsn, start, end, deleted="only")
        lookups = [0]

        def lookup(event_id: str) -> Event | None:
            lookups[0] += 1
            return usgs_lookup(fdsn)(event_id) if lookups[0] <= LOOKUPS else None

        report = compare(events, live, gone, marks, start=start, end=end, lookup=lookup)
    print_report(report, marks, where)
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
