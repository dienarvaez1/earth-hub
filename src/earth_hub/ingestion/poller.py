"""The polling loop: fetch, publish what changed, wait for the next feed version."""

import logging
import random
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from earth_hub.ingestion.events import EventMessage, InvalidFeed, log_rejected, parse_feed
from earth_hub.ingestion.producer import EventPublisher
from earth_hub.ingestion.state import SentState
from earth_hub.ingestion.usgs_client import FeedUnavailable, UsgsClient

logger = logging.getLogger(__name__)

MIN_WAIT_S = 10.0
MAX_WAIT_S = 300.0
JITTER_MIN_S = 2.0
JITTER_SPAN_S = 3.0
SHUTDOWN_FLUSH_TIMEOUT_S = 10.0


def next_wait(
    expires: datetime | None, now: datetime, fallback_s: float, rng: Callable[[], float]
) -> float:
    """Seconds until the next fetch: just after the feed's cache expires, within bounds."""
    if expires is None:
        return fallback_s
    wait = (expires - now).total_seconds() + JITTER_MIN_S + JITTER_SPAN_S * rng()
    return min(max(wait, MIN_WAIT_S), MAX_WAIT_S)


@dataclass
class RoundResult:
    status: str  # "published", "not_modified" or "failed"
    published: int = 0
    unchanged: int = 0
    rejected: int = 0
    failed_delivery: int = 0
    expires: datetime | None = None


class Poller:
    def __init__(
        self,
        client: UsgsClient,
        publisher: EventPublisher,
        state: SentState,
        *,
        fallback_wait_s: float,
        heartbeat_path: Path | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        rng: Callable[[], float] = random.random,
    ):
        self._client = client
        self._publisher = publisher
        self._state = state
        self._fallback_wait_s = fallback_wait_s
        self._heartbeat_path = heartbeat_path
        self._clock = clock
        self._rng = rng
        self._last_modified: str | None = None
        # Since start, for the round summary (stages: earth_hub.observability).
        self.totals = {"published": 0, "rejected": 0, "failed_delivery": 0}
        self.stop_event = threading.Event()

    def run_round(self) -> RoundResult:
        try:
            response = self._client.fetch(self._last_modified)
        except FeedUnavailable as exc:
            logger.error(
                "feed unavailable, skipping round", extra={"stage": "fetch", "error": str(exc)}
            )
            return RoundResult("failed")

        if response.not_modified:
            self._beat()
            return RoundResult("not_modified", expires=response.expires)

        try:
            parsed = parse_feed(response.body or b"", fetched_at=response.fetched_at)
        except InvalidFeed as exc:
            logger.error(
                "feed body unusable, skipping round", extra={"stage": "fetch", "error": str(exc)}
            )
            return RoundResult("failed", expires=response.expires)

        log_rejected(logger, parsed.rejected)

        candidates = self._changed(parsed.messages)
        outcome = self._publisher.publish(candidates)
        for message in outcome.delivered:
            self._state.mark_sent(
                message.event_id, message.source_updated_ms, message.event_time_ms
            )
            logger.info(
                "event published",
                extra={
                    "stage": "publish",
                    "event_id": message.event_id,
                    "source_updated_ms": message.source_updated_ms,
                    "topic": self._publisher.topic,
                },
            )
        for message, error in outcome.failed:
            logger.error(
                "delivery failed, will retry next round",
                extra={"stage": "publish", "event_id": message.event_id, "error": error},
            )

        # Only remember Last-Modified when everything was delivered. Otherwise the next
        # request would get a 304 and the failed events would never be retried.
        if not outcome.failed:
            self._last_modified = response.last_modified

        self._state.prune(self._clock())
        self._beat()
        result = RoundResult(
            "published",
            published=len(outcome.delivered),
            unchanged=len(parsed.messages) - len(candidates),
            rejected=len(parsed.rejected),
            failed_delivery=len(outcome.failed),
            expires=response.expires,
        )
        for name in self.totals:
            self.totals[name] += getattr(result, name)
        logger.info(
            "round complete",
            extra={
                "stage": "publish",
                "published": result.published,
                "unchanged": result.unchanged,
                "rejected": result.rejected,
                "failed_delivery": result.failed_delivery,
                "tracked_events": len(self._state),
                "totals": dict(self.totals),
            },
        )
        return result

    def _changed(self, messages: list[EventMessage]) -> list[EventMessage]:
        """New or revised events, one message per event (the newest if listed twice)."""
        newest: dict[str, EventMessage] = {}
        for message in messages:
            if not self._state.needs_publish(message):
                continue
            current = newest.get(message.event_id)
            if current is None or message.source_updated_ms > current.source_updated_ms:
                newest[message.event_id] = message
        return list(newest.values())

    def run_forever(self) -> None:
        logger.info(
            "poller started",
            extra={"stage": "fetch", "url": self._client.url, "topic": self._publisher.topic},
        )
        while not self.stop_event.is_set():
            result = self.run_round()
            wait = (
                self._fallback_wait_s
                if result.status == "failed"
                else next_wait(result.expires, self._clock(), self._fallback_wait_s, self._rng)
            )
            self.stop_event.wait(wait)
        self.shutdown()

    def shutdown(self) -> None:
        pending = self._publisher.flush(SHUTDOWN_FLUSH_TIMEOUT_S)
        if pending:
            logger.error(
                "shutdown with undelivered messages", extra={"stage": "publish", "pending": pending}
            )
        else:
            logger.info("poller stopped cleanly", extra={"stage": "publish"})

    def _beat(self) -> None:
        if self._heartbeat_path is not None:
            self._heartbeat_path.write_text(self._clock().isoformat())
