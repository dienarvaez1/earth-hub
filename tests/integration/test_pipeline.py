"""Kafka → consumer → PostgreSQL, on a temporary topic and database, with fixture events."""

import json
import uuid

import pytest
from confluent_kafka import Producer
from sqlalchemy import func, select

from earth_hub.consumers.earthquake_consumer import EarthquakeConsumer, Store, make_consumer
from earth_hub.database.models import Earthquake, RawEvent
from earth_hub.database.repository import IncomingMessage
from earth_hub.reconcile import reconcile

pytestmark = pytest.mark.integration


@pytest.fixture
def topic(make_temp_topic):
    return make_temp_topic(partitions=3)


@pytest.fixture
def publish(settings, topic):
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})

    def publish(values: list[tuple[bytes, bytes]]):
        for key, value in values:
            producer.produce(topic, key=key, value=value)
        assert producer.flush(30) == 0

    return publish


def consume(settings, topic, db, group, expected, max_polls=200):
    """Run a real consumer until `expected` messages are processed and committed."""
    consumer = EarthquakeConsumer(
        make_consumer(settings.kafka_bootstrap_servers, group, topic), Store(db)
    )
    processed = 0
    try:
        for _ in range(max_polls):
            if processed >= expected:
                break
            processed += consumer.run_once(timeout_s=1.0)
    finally:
        consumer._consumer.close()
    assert processed == expected, f"processed {processed} of {expected}"
    return consumer.outcomes


def count(db, table, *where):
    with db.connect() as conn:
        return conn.execute(select(func.count()).select_from(table).where(*where)).scalar_one()


def keyed(make_envelope, features):
    return [(f["id"].encode(), make_envelope(f)) for f in features]


@pytest.mark.req("REQ-PER-001", "REQ-PER-013")
def test_fixture_feed_flows_into_postgres(settings, topic, db, publish, make_envelope, feed_doc):
    publish(keyed(make_envelope, feed_doc["features"]))
    outcomes = consume(settings, topic, db, f"g-{uuid.uuid4().hex[:8]}", len(feed_doc["features"]))
    assert outcomes == {"applied": len(feed_doc["features"])}
    assert count(db, Earthquake.__table__) == len(feed_doc["features"])

    report = reconcile(db, settings.kafka_bootstrap_servers, topic, "unused-group")
    assert report.in_topic == report.recorded == len(feed_doc["features"])
    assert report.lost == [] and report.broken_invariants == {}


@pytest.mark.req("REQ-PER-007", "REQ-PER-002")
def test_crash_after_db_commit_before_offset_commit_creates_no_duplicates(
    settings, topic, db, publish, make_envelope, feed_doc
):
    group = f"g-{uuid.uuid4().hex[:8]}"
    features = feed_doc["features"]
    publish(keyed(make_envelope, features))

    # Process everything but "crash" before committing offsets: store directly, no commit.
    crashed = make_consumer(settings.kafka_bootstrap_servers, group, topic)
    store, stored = Store(db), 0
    try:
        for _ in range(200):
            message = crashed.poll(1.0)
            if message is None or message.error():
                if stored == len(features):
                    break
                continue
            store.process(
                IncomingMessage(
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    message.key(),
                    message.value(),
                )
            )
            stored += 1
    finally:
        crashed.close()
    assert stored == len(features)

    # After the "restart" every message is redelivered and recognized.
    outcomes = consume(settings, topic, db, group, len(features))
    assert outcomes == {"already_processed": len(features)}
    assert count(db, Earthquake.__table__) == len(features)
    assert count(db, RawEvent.__table__) == len(features)


@pytest.mark.req("REQ-PER-013", "REQ-PER-004")
def test_mixed_batch_is_fully_accounted_for(
    settings, topic, db, publish, make_envelope, make_feature
):
    good = make_feature("us0001", ["us0001"], updated=1792137700000)
    revised = make_feature("us0001", ["us0001"], updated=1792137760000, mag=5.0)
    bad = make_feature("us0002", ["us0002"])
    bad["geometry"]["coordinates"][0] = 999.0
    values = [
        (b"us0001", make_envelope(good)),
        (b"us0001", make_envelope(revised)),
        (b"us0001", make_envelope(good)),  # older again: stale
        (b"us0002", make_envelope(bad)),
        (b"junk", b"not json"),
    ]
    publish(values)
    group = f"g-{uuid.uuid4().hex[:8]}"
    outcomes = consume(settings, topic, db, group, len(values))
    assert outcomes == {"applied": 2, "stale": 1, "rejected": 2}

    with db.connect() as conn:
        event = conn.execute(select(Earthquake)).mappings().one()
    assert event["magnitude"] == 5.0

    report = reconcile(db, settings.kafka_bootstrap_servers, topic, group)
    assert report.ok and report.pending == 0
    assert report.outcomes == {"applied": 2, "stale": 1, "rejected": 2}


@pytest.mark.req("REQ-PER-013")
def test_reconcile_reports_lost_and_pending_messages(
    settings, topic, db, publish, make_envelope, feed_doc
):
    features = feed_doc["features"][:4]
    publish(keyed(make_envelope, features))
    group = f"g-{uuid.uuid4().hex[:8]}"
    consume(settings, topic, db, group, len(features))

    # Simulate data loss: a committed message whose row disappeared.
    with db.begin() as conn:
        lost_row = conn.execute(
            select(RawEvent.kafka_partition, RawEvent.kafka_offset).limit(1)
        ).one()
        conn.execute(
            RawEvent.__table__.delete().where(
                RawEvent.kafka_partition == lost_row.kafka_partition,
                RawEvent.kafka_offset == lost_row.kafka_offset,
            )
        )
    publish(keyed(make_envelope, [feed_doc["features"][4]]))  # not consumed yet: pending

    report = reconcile(db, settings.kafka_bootstrap_servers, topic, group)
    assert report.lost == [(lost_row.kafka_partition, lost_row.kafka_offset)]
    assert report.pending == 1
    # Its event is now missing the applied message that created it.
    assert report.broken_invariants == {"event without an applied message": 1}
    assert not report.ok


@pytest.mark.req("REQ-PER-001")
def test_envelope_from_kafka_matches_the_stored_payload(
    settings, topic, db, publish, make_envelope, feed_doc
):
    feature = feed_doc["features"][1]  # the event with two IDs
    publish(keyed(make_envelope, [feature]))
    consume(settings, topic, db, f"g-{uuid.uuid4().hex[:8]}", 1)
    with db.connect() as conn:
        raw = conn.execute(select(RawEvent)).mappings().one()
    assert raw["envelope"] == json.loads(make_envelope(feature))
    assert raw["message_key"] == feature["id"]
