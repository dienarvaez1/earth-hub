"""What the poller has already published, so unchanged events are not sent again.

The state is rebuilt on startup from earthquakes.raw itself: the topic is the record of
what was published, so no separate state store is needed.
"""

import json
import logging
from collections.abc import Iterable
from datetime import datetime, timedelta

from confluent_kafka import Consumer, TopicPartition

from earth_hub.ingestion.events import EventMessage
from earth_hub.kafka_metadata import with_retries

logger = logging.getLogger(__name__)

# all_day covers events up to 24 h old; the margin keeps an event's first publish in range.
STATE_WINDOW = timedelta(hours=26)


class SentState:
    def __init__(self) -> None:
        # event_id -> (source_updated_ms, event_time_ms) of the newest version published
        self._sent: dict[str, tuple[int, int]] = {}

    def __len__(self) -> int:
        return len(self._sent)

    def needs_publish(self, message: EventMessage) -> bool:
        sent = self._sent.get(message.event_id)
        return sent is None or message.source_updated_ms > sent[0]

    def mark_sent(self, event_id: str, source_updated_ms: int, event_time_ms: int) -> None:
        sent = self._sent.get(event_id)
        if sent is None or source_updated_ms > sent[0]:
            self._sent[event_id] = (source_updated_ms, event_time_ms)

    def updated_ms(self, event_id: str) -> int | None:
        sent = self._sent.get(event_id)
        return sent[0] if sent else None

    def prune(self, now: datetime) -> int:
        """Forget events too old to appear in the feed again. Returns how many were dropped."""
        cutoff_ms = int((now - STATE_WINDOW).timestamp() * 1000)
        stale = [eid for eid, (_, event_time) in self._sent.items() if event_time < cutoff_ms]
        for event_id in stale:
            del self._sent[event_id]
        return len(stale)


def state_from_messages(values: Iterable[bytes]) -> SentState:
    """Rebuild state from raw message values; unreadable messages are skipped."""
    state = SentState()
    skipped = 0
    for value in values:
        try:
            envelope = json.loads(value)
            state.mark_sent(
                envelope["event_id"],
                int(envelope["source_updated_ms"]),
                int(envelope["event_time_ms"]),
            )
        except (ValueError, TypeError, KeyError):
            skipped += 1
    if skipped:
        logger.warning(
            "skipped unreadable messages while rebuilding state", extra={"skipped": skipped}
        )
    return state


def read_topic_since(
    bootstrap_servers: str, topic: str, since: datetime, timeout_s: float = 30.0
) -> list[bytes]:
    """Every message value in `topic` produced at or after `since`, across all partitions."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "earth-hub-poller-state",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        partitions = consumer.list_topics(topic, timeout=timeout_s).topics[topic].partitions
        since_ms = int(since.timestamp() * 1000)
        starts = with_retries(
            lambda: consumer.offsets_for_times(
                [TopicPartition(topic, p, since_ms) for p in partitions], timeout=timeout_s
            )
        )
        assignments, remaining = [], {}
        for tp in starts:
            _, high = with_retries(
                lambda tp=tp: consumer.get_watermark_offsets(tp, timeout=timeout_s)
            )
            # offset -1: no message at or after `since` in this partition.
            if tp.offset >= 0 and tp.offset < high:
                assignments.append(tp)
                remaining[tp.partition] = high
        if not assignments:
            return []

        consumer.assign(assignments)
        values = []
        while remaining:
            message = consumer.poll(timeout_s)
            if message is None:
                raise TimeoutError(f"timed out reading {topic} to rebuild poller state")
            if message.error():
                raise RuntimeError(f"error reading {topic}: {message.error()}")
            values.append(message.value())
            if message.offset() + 1 >= remaining.get(message.partition(), 0):
                remaining.pop(message.partition(), None)
        return values
    finally:
        consumer.close()
