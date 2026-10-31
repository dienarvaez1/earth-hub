"""Catch-up: what the live poller cannot see.

    python -m earth_hub.ingestion.catchup [--once]

- Hourly (and on startup): the CDN-cached `all_month` feed, which covers revisions to events
  up to 30 days old and outages up to 30 days. Only events newer than what is stored are sent.
- Daily: FDSN events marked deleted since the last check (`includedeleted=only`), for events
  from all stored history (at least the last year). The summary feeds never list deleted events.
- Daily, the history pass: FDSN events added or revised since the last pass, across all stored
  history, which the month feed's 30 days miss (DEF-067).
- Daily, the audit: one month of stored history against USGS, a month a day in turn, for
  deletions FDSN kept no time for, which no search by time returns (DEF-066; history.py).

Both publish to the backfill topic, so the live topic and its consumer are never slowed down.
"""

import argparse
import json
import logging
import sys
import threading
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from earth_hub.database.progress import Progress
from earth_hub.ingestion import history
from earth_hub.ingestion.backfill import MIN_SPLIT
from earth_hub.ingestion.events import InvalidFeed, log_rejected, parse_feed
from earth_hub.ingestion.fdsn import MAX_RESULTS, FdsnClient, FdsnTooSlow, FdsnUnavailable
from earth_hub.ingestion.producer import EventPublisher
from earth_hub.ingestion.usgs_client import FeedUnavailable, UsgsClient
from earth_hub.ingestion.windows import Window, floor_to_second

logger = logging.getLogger(__spec__.name)  # the module name, also under `python -m`

DELETED_LOOKBACK = timedelta(days=366)  # at least; all stored history when it is older
FIRST_DELETED_CHECK = timedelta(days=30)
FIRST_REVISED_CHECK = timedelta(days=30)
AUDIT_LOOKUPS = 50  # stored events looked up by ID, at most, per audit
LOOP_TICK_S = 60.0


class CatchUp:
    def __init__(
        self,
        month_feed: UsgsClient,
        fdsn: FdsnClient,
        publisher: EventPublisher,
        progress: Progress,
        *,
        catchup_interval_s: float = 3600,
        deleted_interval_s: float = 86400,
        heartbeat_path: Path | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self._month_feed = month_feed
        self._fdsn = fdsn
        self._publisher = publisher
        self._progress = progress
        self._catchup_interval = timedelta(seconds=catchup_interval_s)
        self._deleted_interval = timedelta(seconds=deleted_interval_s)
        self._heartbeat_path = heartbeat_path
        self._clock = clock
        self._last_modified: str | None = None
        self._last_catchup: datetime | None = None  # in memory: always run on startup
        self.stop_event = threading.Event()

    def catch_up(self) -> int:
        """One pass over the month feed. Returns how many events were published."""
        response = self._month_feed.fetch(self._last_modified)
        now = self._clock()
        if response.not_modified:
            self._done("catchup", now)
            return 0
        parsed = parse_feed(response.body or b"", fetched_at=response.fetched_at, source="catchup")
        log_rejected(logger, parsed.rejected, job="catchup")
        to_send = self._progress.needs_publish(parsed.messages)
        outcome = self._publisher.publish(to_send)
        if not outcome.failed:  # otherwise the next pass gets the full feed again
            self._last_modified = response.last_modified
            self._done("catchup", now)
        logger.info(
            "catch-up pass",
            extra={
                "stage": "publish",
                "in_feed": len(parsed.messages),
                "published": len(outcome.delivered),
                "failed": len(outcome.failed),
                "already_stored": len(parsed.messages) - len(to_send),
                "rejected": len(parsed.rejected),
            },
        )
        return len(outcome.delivered)

    def check_deleted(self) -> int:
        """Events USGS marked deleted since the last check. Returns how many were published."""
        return self._changed("deleted", "only", FIRST_DELETED_CHECK, "deleted-events check")

    def check_revised(self) -> int:
        """Events USGS added or revised since the last pass, across all stored history: the
        month feed covers 30 days only (DEF-067). Returns how many were published."""
        return self._changed("revised", "false", FIRST_REVISED_CHECK, "history pass")

    def _changed(self, name: str, include_deleted: str, first: timedelta, summary: str) -> int:
        started = floor_to_second(self._clock())
        # Rounded down to the second: FDSN ignores fractions, and down only widens the window.
        since = floor_to_second(self._progress.checkpoint(name) or started - first)
        published, rejected = self._updated(
            self._history(started), since, include_deleted, source=name
        )
        self._progress.set_checkpoint(name, started)
        logger.info(
            summary,
            extra={
                "stage": "publish",
                "since": since,
                "published": published,
                "rejected": rejected,
            },
        )
        return published

    def _history(self, now: datetime) -> Window:
        """All stored history, from its oldest event's day; at least the last year (DEF-067)."""
        start = now - DELETED_LOOKBACK
        oldest = self._progress.oldest()
        if oldest is not None:
            start = min(start, history.month_start(oldest))
        return Window(start, now)

    def _updated(
        self, window: Window, since: datetime, include_deleted: str, *, source: str
    ) -> tuple[int, int]:
        """(published, rejected) for the events updated in `window` since `since`."""
        try:
            expected = self._fdsn.count(
                window.start, window.end, include_deleted=include_deleted, updated_after=since
            )
            if expected > MAX_RESULTS:
                return self._updated_parts(window, since, include_deleted, source=source)
            body = self._fdsn.query(
                window.start, window.end, include_deleted=include_deleted, updated_after=since
            )
        except FdsnTooSlow:
            # USGS times out on large windows; smaller ones on the same boundaries are fine.
            if window.end - window.start <= MIN_SPLIT:
                raise
            return self._updated_parts(window, since, include_deleted, source=source)
        parsed = parse_feed(body, fetched_at=self._clock(), source=source)
        log_rejected(logger, parsed.rejected, job=source, start=window.start, end=window.end)
        outcome = self._publisher.publish(self._progress.needs_publish(parsed.messages))
        if outcome.failed:
            raise FdsnUnavailable(f"{len(outcome.failed)} {source} events not delivered")
        return len(outcome.delivered), len(parsed.rejected)

    def _updated_parts(
        self, window: Window, since: datetime, include_deleted: str, *, source: str
    ) -> tuple[int, int]:
        parts = [
            self._updated(part, since, include_deleted, source=source) for part in window.split()
        ]
        return sum(p for p, _ in parts), sum(r for _, r in parts)

    def audit(self) -> int:
        """One month of stored history against USGS, a month a day in turn (DEF-066): per
        UTC day the events USGS lists against those stored; on a day that differs, the stored
        events USGS no longer lists are looked up by ID, and a deletion USGS kept no time for
        is published. Returns how many were published."""
        started = floor_to_second(self._clock())
        oldest = self._progress.oldest()
        if oldest is None:  # nothing stored: nothing to compare
            self._progress.set_checkpoint("audit", started)
            return 0
        first = history.month_start(oldest)
        month = self._progress.checkpoint("audit_month") or first
        if month > started:
            month = first  # a full cycle is done: start again from the oldest month
        end = min(history.next_month(month), started)
        stored = self._progress.stored_in(Window(month, end))
        ours = Counter(history.day_of(e.event_time_ms) for e in stored)
        differing = [
            day
            for day in history.days(month, end)
            if self._fdsn.count(day, min(day + history.DAY, end), include_deleted="false")
            != ours[f"{day:%Y-%m-%d}"]
        ]
        candidates = []
        for day in differing:
            listed = self._listed(day, min(day + history.DAY, end))
            candidates += [
                e
                for e in stored
                if history.day_of(e.event_time_ms) == f"{day:%Y-%m-%d}" and not e.ids & listed
            ]
        messages = []
        for event in candidates[:AUDIT_LOOKUPS]:
            body = self._fdsn.event(event.event_id)
            message = history.deletion(
                json.loads(body) if body else None, event, fetched_at=self._clock()
            )
            if message is not None:
                messages.append(message)
        outcome = self._publisher.publish(self._progress.needs_publish(messages))
        if outcome.failed:
            raise FdsnUnavailable(f"{len(outcome.failed)} audit deletions not delivered")
        complete = len(candidates) <= AUDIT_LOOKUPS  # else the same month again next time
        if complete:
            self._progress.set_checkpoint(f"audit:{month:%Y-%m}", started)
            self._progress.set_checkpoint("audit_month", history.next_month(month))
        self._progress.set_checkpoint("audit", started)
        looked_up = min(len(candidates), AUDIT_LOOKUPS)
        logger.info(
            "history audit",
            extra={
                "stage": "publish",
                "month": f"{month:%Y-%m}",
                "differing_days": len(differing),
                "looked_up": looked_up,
                "published": len(outcome.delivered),
            },
        )
        return len(outcome.delivered)

    def _listed(self, start: datetime, end: datetime) -> set[str]:
        """Every ID of the events USGS lists in [start, end), deleted ones included."""
        parsed = parse_feed(
            self._fdsn.query(start, end, include_deleted="true"),
            fetched_at=self._clock(),
            source="audit",
        )
        return {i for m in parsed.messages for i in m.ids}

    def due(self, name: str, interval: timedelta) -> bool:
        last = self._last_catchup if name == "catchup" else self._progress.checkpoint(name)
        return last is None or self._clock() - last >= interval

    def run_once(self) -> None:
        for name, interval, job in (
            ("catchup", self._catchup_interval, self.catch_up),
            ("deleted", self._deleted_interval, self.check_deleted),
            ("revised", self._deleted_interval, self.check_revised),
            ("audit", self._deleted_interval, self.audit),
        ):
            if not self.due(name, interval):
                continue
            try:
                job()
            except (FeedUnavailable, FdsnUnavailable, InvalidFeed) as exc:
                logger.error(f"{name} failed, retrying next tick", extra={"error": str(exc)})
                return
        self._beat()

    def run_forever(self) -> None:
        logger.info("catch-up started")
        while not self.stop_event.is_set():
            self.run_once()
            self.stop_event.wait(LOOP_TICK_S)
        self._publisher.flush(10)
        logger.info("catch-up stopped")

    def _done(self, name: str, when: datetime) -> None:
        self._last_catchup = when
        self._progress.set_checkpoint(name, when)

    def _beat(self) -> None:
        if self._heartbeat_path is not None:
            self._heartbeat_path.write_text(self._clock().isoformat())


def main(argv: list[str] | None = None) -> int:
    import signal

    from earth_hub.config import get_settings
    from earth_hub.database.engine import make_engine
    from earth_hub.ingestion.producer import make_producer
    from earth_hub.ingestion.usgs_client import make_http_client
    from earth_hub.logging_config import configure_logging

    parser = argparse.ArgumentParser(description="Catch up on revisions and deleted events.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="one catch-up pass, deleted check, history pass and audit",
    )
    args = parser.parse_args(argv)

    configure_logging()
    settings = get_settings()
    engine = make_engine(settings)
    publisher = EventPublisher(
        make_producer(settings.kafka_bootstrap_servers), settings.kafka_topic_backfill
    )
    with (
        make_http_client(settings.usgs_contact, timeout_s=60.0) as feed_http,
        make_http_client(settings.usgs_contact, timeout_s=60.0) as fdsn_http,
    ):
        job = CatchUp(
            UsgsClient(str(settings.usgs_month_feed_url), feed_http),
            FdsnClient(str(settings.fdsn_url), fdsn_http, pause_s=settings.fdsn_pause_seconds),
            publisher,
            Progress(engine),
            catchup_interval_s=settings.catchup_interval_seconds,
            deleted_interval_s=settings.deleted_check_interval_seconds,
            heartbeat_path=settings.catchup_heartbeat_path,
        )
        if args.once:
            published = job.catch_up() + job.check_deleted() + job.check_revised() + job.audit()
            publisher.flush(30)
            print(f"catch-up: {published} events published")
            return 0
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: job.stop_event.set())
        job.run_forever()
    engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
