"""Catch-up: what the live poller cannot see.

    python -m earth_hub.ingestion.catchup [--once]

- Hourly (and on startup): the CDN-cached `all_month` feed, which covers revisions to events
  up to 30 days old and outages up to 30 days. Only events newer than what is stored are sent.
- Daily: FDSN events marked deleted since the last check (`includedeleted=only`), for events
  from the last year. The summary feeds never list deleted events.

Both publish to the backfill topic, so the live topic and its consumer are never slowed down.
"""

import argparse
import logging
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from earth_hub.database.progress import Progress
from earth_hub.ingestion.backfill import MIN_SPLIT
from earth_hub.ingestion.events import InvalidFeed, parse_feed
from earth_hub.ingestion.fdsn import MAX_RESULTS, FdsnClient, FdsnTooSlow, FdsnUnavailable
from earth_hub.ingestion.producer import EventPublisher
from earth_hub.ingestion.usgs_client import FeedUnavailable, UsgsClient
from earth_hub.ingestion.windows import Window, floor_to_second

logger = logging.getLogger(__name__)

DELETED_LOOKBACK = timedelta(days=366)
FIRST_DELETED_CHECK = timedelta(days=30)
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
            },
        )
        return len(outcome.delivered)

    def check_deleted(self) -> int:
        """Events USGS marked deleted since the last check. Returns how many were published."""
        started = floor_to_second(self._clock())
        # Rounded down to the second: FDSN ignores fractions, and down only widens the window.
        since = floor_to_second(
            self._progress.checkpoint("deleted") or started - FIRST_DELETED_CHECK
        )
        published = self._deleted(Window(started - DELETED_LOOKBACK, started), since)
        self._progress.set_checkpoint("deleted", started)
        logger.info(
            "deleted-events check",
            extra={"stage": "publish", "since": since, "published": published},
        )
        return published

    def _deleted(self, window: Window, since: datetime) -> int:
        try:
            expected = self._fdsn.count(
                window.start, window.end, include_deleted="only", updated_after=since
            )
            if expected > MAX_RESULTS:
                return sum(self._deleted(part, since) for part in window.split())
            body = self._fdsn.query(
                window.start, window.end, include_deleted="only", updated_after=since
            )
        except FdsnTooSlow:
            # USGS times out on large windows; smaller ones on the same boundaries are fine.
            if window.end - window.start <= MIN_SPLIT:
                raise
            return sum(self._deleted(part, since) for part in window.split())
        parsed = parse_feed(body, fetched_at=self._clock(), source="deleted")
        outcome = self._publisher.publish(self._progress.needs_publish(parsed.messages))
        if outcome.failed:
            raise FdsnUnavailable(f"{len(outcome.failed)} deleted events not delivered")
        return len(outcome.delivered)

    def due(self, name: str, interval: timedelta) -> bool:
        last = self._last_catchup if name == "catchup" else self._progress.checkpoint(name)
        return last is None or self._clock() - last >= interval

    def run_once(self) -> None:
        for name, interval, job in (
            ("catchup", self._catchup_interval, self.catch_up),
            ("deleted", self._deleted_interval, self.check_deleted),
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
        "--once", action="store_true", help="one catch-up pass and one deleted check"
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
            published = job.catch_up() + job.check_deleted()
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
