"""Publishing event messages to Kafka with confirmed delivery."""

import logging
from dataclasses import dataclass, field
from typing import Protocol

from confluent_kafka import KafkaError, Producer

from earth_hub.ingestion.events import EventMessage

logger = logging.getLogger(__name__)


class KafkaProducerLike(Protocol):
    def produce(self, topic: str, *, key: bytes, value: bytes, on_delivery) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


def make_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "earth-hub-poller",
            # Idempotence implies acks=all and safe retries: no duplicates or reordering
            # caused by the producer itself retrying.
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )


@dataclass
class PublishResult:
    delivered: list[EventMessage] = field(default_factory=list)
    failed: list[tuple[EventMessage, str]] = field(default_factory=list)


class EventPublisher:
    def __init__(self, producer: KafkaProducerLike, topic: str, flush_timeout_s: float = 30.0):
        self._producer = producer
        self.topic = topic
        self._flush_timeout_s = flush_timeout_s

    def publish(self, messages: list[EventMessage]) -> PublishResult:
        """Send `messages` and wait for the broker to confirm each one."""
        result = PublishResult()
        outcomes: dict[int, str | None] = {}

        def on_delivery(index: int):
            def callback(err: KafkaError | None, _msg) -> None:
                outcomes[index] = str(err) if err else None

            return callback

        for index, message in enumerate(messages):
            try:
                self._send(message, on_delivery(index))
            except Exception as exc:  # recorded as this message's failure, retried next round
                outcomes[index] = f"produce failed: {exc}"

        if messages:
            self._producer.flush(self._flush_timeout_s)

        for index, message in enumerate(messages):
            error = outcomes.get(index, "no delivery report before flush timeout")
            if error is None:
                result.delivered.append(message)
            else:
                result.failed.append((message, error))
        return result

    def _send(self, message: EventMessage, callback) -> None:
        try:
            self._producer.produce(
                self.topic, key=message.key, value=message.to_json(), on_delivery=callback
            )
        except BufferError:
            # Local queue full: serve delivery reports to make room, then try once more.
            self._producer.poll(1.0)
            self._producer.produce(
                self.topic, key=message.key, value=message.to_json(), on_delivery=callback
            )

    def flush(self, timeout_s: float) -> int:
        """Wait for pending messages; returns how many are still undelivered."""
        return self._producer.flush(timeout_s)
