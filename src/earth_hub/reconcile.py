"""Source-to-database reconciliation: `python -m earth_hub.reconcile`.

Every message in earthquakes.raw must have a raw_events row. A missing row below the
consumer group's committed offset means a message was acknowledged without being stored
(data loss); a missing row at or above it is just consumer lag. Exits 1 if anything is
lost or an invariant is broken.
"""

import sys
from collections import Counter
from dataclasses import dataclass, field

from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import Engine, text

from earth_hub.config import get_settings
from earth_hub.database.engine import make_engine
from earth_hub.kafka_metadata import with_retries

# Each query counts rows that break an invariant; all must be zero.
INVARIANTS = {
    "preferred id not mapped to its own event": """
        SELECT count(*) FROM earthquakes e
        LEFT JOIN earthquake_ids i ON i.event_id = e.preferred_event_id
        WHERE i.earthquake_id IS DISTINCT FROM e.id""",
    "event without an applied message": """
        SELECT count(*) FROM earthquakes e
        WHERE NOT EXISTS (SELECT 1 FROM raw_events r
                          WHERE r.earthquake_id = e.id AND r.outcome = 'applied')""",
    "applied/stale/duplicate message without an event": """
        SELECT count(*) FROM raw_events
        WHERE outcome IN ('applied', 'stale', 'duplicate') AND earthquake_id IS NULL""",
    "rejected/error message without a reason": """
        SELECT count(*) FROM raw_events
        WHERE outcome IN ('rejected', 'error') AND reason IS NULL""",
    "event IDs with no event": """
        SELECT count(*) FROM earthquake_ids i
        WHERE NOT EXISTS (SELECT 1 FROM earthquakes e WHERE e.id = i.earthquake_id)""",
}


@dataclass
class Report:
    topic: str
    in_topic: int = 0
    recorded: int = 0
    lost: list[tuple[int, int]] = field(default_factory=list)
    pending: int = 0
    not_in_topic: int = 0
    outcomes: dict[str, int] = field(default_factory=dict)
    broken_invariants: dict[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.lost and not self.broken_invariants


def topic_offsets(
    bootstrap_servers: str, topic: str, timeout_s: float = 30.0
) -> set[tuple[int, int]]:
    """(partition, offset) of every message currently in the topic."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "earth-hub-reconcile",
            "enable.auto.commit": False,
        }
    )
    try:
        partitions = consumer.list_topics(topic, timeout=timeout_s).topics[topic].partitions
        assignments, remaining = [], {}
        for partition in partitions:
            low, high = with_retries(
                lambda partition=partition: consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=timeout_s
                )
            )
            if high > low:
                assignments.append(TopicPartition(topic, partition, low))
                remaining[partition] = high
        found: set[tuple[int, int]] = set()
        if assignments:
            consumer.assign(assignments)
        while remaining:
            message = consumer.poll(timeout_s)
            if message is None:
                raise TimeoutError(f"timed out scanning {topic}")
            if message.error():
                raise RuntimeError(str(message.error()))
            found.add((message.partition(), message.offset()))
            if message.offset() + 1 >= remaining.get(message.partition(), 0):
                remaining.pop(message.partition(), None)
        return found
    finally:
        consumer.close()


def committed_offsets(bootstrap_servers: str, group: str, topic: str, partitions) -> dict[int, int]:
    consumer = Consumer({"bootstrap.servers": bootstrap_servers, "group.id": group})
    try:
        committed = consumer.committed([TopicPartition(topic, p) for p in partitions], timeout=30)
        # A negative offset means nothing committed yet.
        return {tp.partition: max(tp.offset, 0) for tp in committed}
    finally:
        consumer.close()


def reconcile(engine: Engine, bootstrap_servers: str, topic: str, group: str) -> Report:
    report = Report(topic)
    in_topic = topic_offsets(bootstrap_servers, topic)
    with engine.connect() as conn:
        recorded = {
            (row.kafka_partition, row.kafka_offset)
            for row in conn.execute(
                text("SELECT kafka_partition, kafka_offset FROM raw_events WHERE kafka_topic = :t"),
                {"t": topic},
            )
        }
        report.outcomes = dict(
            conn.execute(
                text("SELECT outcome, count(*) FROM raw_events WHERE kafka_topic = :t GROUP BY 1"),
                {"t": topic},
            ).all()
        )
        for name, query in INVARIANTS.items():
            count = conn.execute(text(query)).scalar_one()
            if count:
                report.broken_invariants[name] = count

    committed = committed_offsets(
        bootstrap_servers, group, topic, {partition for partition, _ in in_topic}
    )
    report.in_topic, report.recorded = len(in_topic), len(recorded & in_topic)
    report.not_in_topic = len(recorded - in_topic)  # e.g. removed by topic retention
    for partition, offset in sorted(in_topic - recorded):
        if offset < committed.get(partition, 0):
            report.lost.append((partition, offset))
        else:
            report.pending += 1
    return report


def format_report(report: Report) -> str:
    lines = [
        f"topic {report.topic}: {report.in_topic} messages, "
        f"{report.recorded} recorded in raw_events",
        f"  pending (consumer lag): {report.pending}",
        f"  lost (committed but not stored): {len(report.lost)}"
        + (f" first: {report.lost[:5]}" if report.lost else ""),
        f"  recorded but no longer in topic: {report.not_in_topic}",
        "  outcomes: "
        + (", ".join(f"{k}={v}" for k, v in sorted(Counter(report.outcomes).items())) or "none"),
    ]
    if report.broken_invariants:
        lines.append("  BROKEN INVARIANTS:")
        lines += [f"    {name}: {count}" for name, count in report.broken_invariants.items()]
    else:
        lines.append(f"  invariants: all {len(INVARIANTS)} hold")
    lines.append("RESULT: " + ("OK" if report.ok else "FAILED"))
    return "\n".join(lines)


def main() -> int:
    settings = get_settings()
    report = reconcile(
        make_engine(settings),
        settings.kafka_bootstrap_servers,
        settings.kafka_topic_raw,
        settings.kafka_consumer_group,
    )
    print(format_report(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
