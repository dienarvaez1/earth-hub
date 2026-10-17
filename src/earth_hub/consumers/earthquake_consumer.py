"""Consume earthquakes.raw and persist each message, committing offsets only after the DB.

Delivery is at-least-once: a crash between the database commit and the offset commit
redelivers the message, which the raw_events offset key then recognizes and skips.
"""

import logging
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy import Engine
from sqlalchemy.exc import DBAPIError, OperationalError

from earth_hub.database import repository
from earth_hub.database.repository import IncomingMessage, ProcessResult

logger = logging.getLogger(__name__)

MAX_PROCESSING_ATTEMPTS = 3
MAX_BACKOFF_S = 30.0
IDLE_PING_INTERVAL_S = 30.0


class KafkaConsumerLike(Protocol):
    def poll(self, timeout: float): ...
    def commit(self, *, message, asynchronous: bool): ...
    def close(self) -> None: ...


def make_consumer(bootstrap_servers: str, group_id: str, topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "client.id": "earth-hub-consumer",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
            # A long database outage blocks polling; stay in the group for up to 10 min.
            "max.poll.interval.ms": 600_000,
        }
    )
    consumer.subscribe([topic])
    return consumer


class Store:
    """Database access for the consumer, one transaction per call."""

    def __init__(self, engine: Engine):
        self._engine = engine

    def process(self, message: IncomingMessage) -> ProcessResult:
        with self._engine.begin() as conn:
            return repository.process_message(conn, message)

    def record_error(self, message: IncomingMessage, error: str) -> None:
        with self._engine.begin() as conn:
            repository.record_error(conn, message, error)

    def ping(self) -> None:
        with self._engine.connect() as conn:
            repository.ping(conn)


def is_infrastructure_error(exc: Exception) -> bool:
    """Errors that say nothing about the message: retry it until the database is back."""
    return isinstance(exc, OperationalError) or (
        isinstance(exc, DBAPIError) and exc.connection_invalidated
    )


class EarthquakeConsumer:
    def __init__(
        self,
        consumer: KafkaConsumerLike,
        store: Store,
        *,
        heartbeat_path: Path | None = None,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self._consumer = consumer
        self._store = store
        self._heartbeat_path = heartbeat_path
        self.stop_event = threading.Event()
        self._sleep = sleep or self.stop_event.wait
        self._monotonic = monotonic
        self._last_ping = float("-inf")
        self.outcomes: dict[str, int] = {}

    def run_forever(self) -> None:
        logger.info("consumer started")
        try:
            while not self.stop_event.is_set():
                self.run_once()
        finally:
            self._consumer.close()
            logger.info("consumer stopped", extra={"outcomes": self.outcomes})

    def run_once(self, timeout_s: float = 1.0) -> bool:
        """Handle at most one message. Returns True if a message was processed and committed."""
        message = self._consumer.poll(timeout_s)
        if message is None:
            self._idle()
            return False
        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                logger.error("kafka error", extra={"error": str(message.error())})
            return False

        incoming = IncomingMessage(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            key=message.key(),
            value=message.value(),
            timestamp_ms=message.timestamp()[1] if message.timestamp()[0] else None,
        )
        result = self._process(incoming)
        if result is None:  # stopping while the database is down: leave the offset alone
            return False

        try:
            self._consumer.commit(message=message, asynchronous=False)
        except KafkaException as exc:
            # The row is committed; a redelivery will be recognized and skipped.
            logger.warning("offset commit failed", extra={"error": str(exc), **_where(incoming)})
        self.outcomes[result.outcome] = self.outcomes.get(result.outcome, 0) + 1
        logger.info(
            "message processed",
            extra={
                **_where(incoming),
                "event_id": _key(incoming),
                "stage": "persist",
                "outcome": result.outcome,
                "earthquake_id": result.earthquake_id,
                "reason": result.reason,
            },
        )
        self._beat()
        return True

    def _process(self, incoming: IncomingMessage) -> ProcessResult | None:
        failures = 0
        outage_attempt = 0
        while not self.stop_event.is_set():
            try:
                return self._store.process(incoming)
            except Exception as exc:
                if is_infrastructure_error(exc):
                    outage_attempt += 1
                    delay = min(MAX_BACKOFF_S, 2 ** (outage_attempt - 1))
                    logger.error(
                        "database unavailable, retrying the same message",
                        extra={**_where(incoming), "attempt": outage_attempt, "retry_in_s": delay},
                    )
                    self._sleep(delay)
                    continue
                failures += 1
                logger.exception(
                    "unexpected error processing message",
                    extra={**_where(incoming), "attempt": failures},
                )
                if failures >= MAX_PROCESSING_ATTEMPTS:
                    return self._give_up(incoming, exc)
        return None

    def _give_up(self, incoming: IncomingMessage, exc: Exception) -> ProcessResult | None:
        error = f"{type(exc).__name__}: {exc}"
        while not self.stop_event.is_set():
            try:
                self._store.record_error(incoming, error)
                return ProcessResult("error", reason=error)
            except Exception as record_exc:
                if not is_infrastructure_error(record_exc):
                    raise
                self._sleep(MAX_BACKOFF_S)
        return None

    def _idle(self) -> None:
        # With no messages there is nothing to prove the database works, so check it now
        # and then; the heartbeat only advances while the database answers.
        now = self._monotonic()
        if now - self._last_ping < IDLE_PING_INTERVAL_S:
            self._beat()
            return
        try:
            self._store.ping()
        except Exception as exc:
            logger.error("database ping failed while idle", extra={"error": str(exc)})
            return
        self._last_ping = now
        self._beat()

    def _beat(self) -> None:
        if self._heartbeat_path is not None:
            self._heartbeat_path.write_text(str(time.time()))


def _where(incoming: IncomingMessage) -> dict:
    return {"partition": incoming.partition, "offset": incoming.offset}


def _key(incoming: IncomingMessage) -> str | None:
    return incoming.key.decode("utf-8", errors="replace") if incoming.key else None
