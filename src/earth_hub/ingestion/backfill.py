"""Backfill history from the USGS FDSN event service, one calendar week per request.

    python -m earth_hub.ingestion.backfill --start 2026-01-01 [--end 2026-06-01] [--no-sweep]

For each week (Sunday 00:00:00 UTC to the next Sunday, the first and last weeks cut to the
range), in order, as a single request where USGS can serve one:
1. ask /count how many events the week holds, and fetch them: once for non-deleted events and
   once for deleted ones (mixed in one query USGS is 20-100x slower and returns stub records);
2. fail the week if fewer arrive than were counted, for either kind;
3. publish only events that are new or newer than what PostgreSQL already stores;
4. record the week as done, so a re-run skips it. The current week stays `open`.

A week is split into days, on the same boundaries, when it holds more than 20,000 events (the
service's cap) or when USGS times out on it (504 after 60 s, which happens for some busy recent
weeks while single days take about a second). A day that is still too slow is halved, down to
an hour.
After the weeks, a final sweep fetches everything USGS added or revised *while the backfill
ran*, across the whole range: events that arrived late, or whose revised time moved them
into a week that had already been fetched.
"""

import argparse
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from earth_hub.database.progress import Progress, WindowCounts
from earth_hub.ingestion.events import parse_feed
from earth_hub.ingestion.fdsn import MAX_RESULTS, FdsnClient, FdsnTooSlow, FdsnUnavailable
from earth_hub.ingestion.producer import EventPublisher
from earth_hub.ingestion.windows import Window, floor_to_second, weekly_windows

logger = logging.getLogger(__spec__.name)  # the module name, also under `python -m`


WINDOW_ATTEMPTS = 3
# includedeleted values asked for separately in every window: non-deleted, then deleted.
KINDS = ("false", "only")
# Smallest window worth splitting further when USGS is too slow; below it, give up for now.
MIN_SPLIT = timedelta(hours=1)


class IncompleteWindow(Exception):
    """A window could not be fetched or published completely; it is not marked done."""


@dataclass
class Report:
    windows_done: int = 0
    windows_skipped: int = 0
    windows_split: int = 0
    fetched: int = 0
    published: int = 0
    skipped: int = 0
    rejected: int = 0
    sweep: dict = field(default_factory=dict)


class Backfill:
    def __init__(
        self,
        fdsn: FdsnClient,
        publisher: EventPublisher,
        progress: Progress,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self._fdsn = fdsn
        self._publisher = publisher
        self._progress = progress
        self._clock = clock

    def run(self, start: datetime, end: datetime | None = None, *, sweep: bool = True) -> Report:
        started = floor_to_second(self._clock())
        end = floor_to_second(end) if end else started
        report = Report()
        done = self._progress.finished("backfill")
        plan = weekly_windows(start, end)
        logger.info(
            "backfill started",
            extra={
                "start": plan[0].start,
                "end": end,
                "windows": len(plan),
                "already_done": len(done),
            },
        )
        for window in plan:
            self._window("backfill", window, report, done)

        if sweep:
            sweep_report = Report()
            self._window(
                "sweep", Window(plan[0].start, end), sweep_report, set(), updated_after=started
            )
            report.sweep = sweep_report.__dict__ | {"updated_after": started.isoformat()}
        logger.info("backfill finished", extra={"stage": "publish", **report.__dict__})
        return report

    def _window(
        self,
        job: str,
        window: Window,
        report: Report,
        done: set,
        *,
        updated_after: datetime | None = None,
    ) -> None:
        if (window.start, window.end) in done:
            report.windows_skipped += 1
            return
        if any(window.start <= start and end <= window.end for start, end in done):
            # Part of this window was finished in smaller pieces: carry on with those.
            self._split(job, window, report, done, updated_after, reason="partly done")
            return
        try:
            self._fetch(job, window, report, done, updated_after)
        except FdsnTooSlow as exc:
            if window.end - window.start <= MIN_SPLIT:
                raise
            logger.warning(
                "window too slow for USGS, splitting",
                extra={"start": window.start, "end": window.end, "error": str(exc)},
            )
            self._split(job, window, report, done, updated_after, reason="too slow")

    def _split(self, job, window, report, done, updated_after, *, reason: str) -> None:
        report.windows_split += 1
        for part in window.split():
            self._window(job, part, report, done, updated_after=updated_after)

    def _fetch(self, job, window, report, done, updated_after) -> None:
        """Both kinds of event for the window, each against its own count, then publish.

        Non-deleted and deleted events are asked for separately. Mixed in one query
        (includedeleted=true) USGS answers 20 to 100 times slower, timing out on busy weeks,
        and returns stub records with no time or location for some events; asked for
        separately, each kind answers a whole week in under a second (measured 2026-10-17).
        """
        messages, rejected, expected = [], [], 0
        for kind in KINDS:
            kind_messages, kind_rejected, kind_expected = self._fetch_kind(
                job, window, report, done, updated_after, kind
            )
            if kind_messages is None:  # split instead; the parts record themselves
                return
            messages += kind_messages
            rejected += kind_rejected
            expected += kind_expected
        fetched = len(messages) + len(rejected)
        to_send = self._progress.needs_publish(messages)
        outcome = self._publisher.publish(to_send)
        if outcome.failed:
            raise IncompleteWindow(
                f"{job} {window.start}..{window.end}: {len(outcome.failed)} events not delivered"
            )
        counts = WindowCounts(
            expected=expected,
            fetched=fetched,
            published=len(outcome.delivered),
            skipped=len(messages) - len(to_send),
            rejected=len(rejected),
        )
        self._progress.record(job, window, counts)
        report.windows_done += 1
        report.fetched += counts.fetched
        report.published += counts.published
        report.skipped += counts.skipped
        report.rejected += counts.rejected
        logger.info(
            "window done",
            extra={
                "stage": "publish",
                "job": job,
                "start": window.start,
                "end": window.end,
                "open": window.open,
                **counts.__dict__,
            },
        )

    def _fetch_kind(self, job, window, report, done, updated_after, kind):
        """(messages, rejected, expected) for one kind, or (None, None, None) if the window
        was too big and has been split instead."""
        for attempt in range(1, WINDOW_ATTEMPTS + 1):
            expected = self._fdsn.count(
                window.start, window.end, include_deleted=kind, updated_after=updated_after
            )
            if expected > MAX_RESULTS:
                logger.warning(
                    "window over the FDSN cap, splitting",
                    extra={"start": window.start, "end": window.end, "count": expected},
                )
                self._split(job, window, report, done, updated_after, reason="over the cap")
                return None, None, None
            body = self._fdsn.query(
                window.start, window.end, include_deleted=kind, updated_after=updated_after
            )
            parsed = parse_feed(body, fetched_at=self._clock(), source=job)
            fetched = len(parsed.messages) + len(parsed.rejected)
            if fetched >= expected:
                return parsed.messages, parsed.rejected, expected
            # USGS revises events all the time: one can move out of this window between the
            # count and the query (the final sweep finds it in its new window). Ask again.
            logger.warning(
                "fewer events than counted, fetching the window again",
                extra={
                    "start": window.start,
                    "end": window.end,
                    "kind": kind,
                    "expected": expected,
                    "fetched": fetched,
                    "attempt": attempt,
                },
            )
        # Never record a window as done with events missing; the next run tries again.
        raise IncompleteWindow(
            f"{job} {window.start}..{window.end} ({kind}): "
            f"/count said {expected}, query returned {fetched}, {WINDOW_ATTEMPTS} times"
        )


def parse_date(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def main(argv: list[str] | None = None) -> int:
    from earth_hub.config import get_settings
    from earth_hub.database.engine import make_engine
    from earth_hub.ingestion.producer import make_producer
    from earth_hub.ingestion.usgs_client import make_http_client
    from earth_hub.logging_config import configure_logging

    parser = argparse.ArgumentParser(description="Backfill USGS events, one week per request.")
    parser.add_argument("--start", required=True, type=parse_date, help="e.g. 2026-01-01 (UTC)")
    parser.add_argument("--end", type=parse_date, help="default: now")
    parser.add_argument(
        "--no-sweep", action="store_true", help="skip the final updated-since sweep"
    )
    args = parser.parse_args(argv)

    configure_logging()
    settings = get_settings()
    engine = make_engine(settings)
    publisher = EventPublisher(
        make_producer(settings.kafka_bootstrap_servers), settings.kafka_topic_backfill
    )
    try:
        with make_http_client(settings.usgs_contact, timeout_s=60.0) as http:
            fdsn = FdsnClient(str(settings.fdsn_url), http, pause_s=settings.fdsn_pause_seconds)
            report = Backfill(fdsn, publisher, Progress(engine)).run(
                args.start, args.end, sweep=not args.no_sweep
            )
    except (IncompleteWindow, FdsnUnavailable) as exc:
        # Finished windows are recorded; running the same command again resumes from here.
        logger.error("backfill stopped; run it again to resume", extra={"error": str(exc)})
        print(f"backfill stopped: {exc}\nRun the same command again to resume.", file=sys.stderr)
        return 1
    finally:
        publisher.flush(30)
        engine.dispose()
    print(
        f"backfill: {report.windows_done} windows fetched, {report.windows_skipped} already done, "
        f"{report.fetched} events, {report.published} published, {report.skipped} already stored, "
        f"{report.rejected} unroutable; sweep: {report.sweep.get('published', 0)} published"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
