"""Kafka metadata lookups that survive the moments when a partition has no leader:
right after a topic is created, or while a broker restarts."""

import time
from collections.abc import Callable

from confluent_kafka import KafkaError, KafkaException

RETRIABLE = frozenset(
    {
        KafkaError.NOT_LEADER_FOR_PARTITION,
        KafkaError.LEADER_NOT_AVAILABLE,
        KafkaError.UNKNOWN_TOPIC_OR_PART,
        KafkaError._TRANSPORT,
        KafkaError._TIMED_OUT,
        KafkaError._ALL_BROKERS_DOWN,
    }
)


def with_retries[T](
    lookup: Callable[[], T],
    *,
    timeout_s: float = 30.0,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> T:
    """Call `lookup`, retrying temporary leadership and connection errors until `timeout_s`."""
    deadline = monotonic() + timeout_s
    delay = 0.1
    while True:
        try:
            return lookup()
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            code = error.code() if isinstance(error, KafkaError) else None
            if code not in RETRIABLE or monotonic() + delay > deadline:
                raise
        sleep(delay)
        delay = min(delay * 2, 2.0)
