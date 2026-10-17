"""Real outages: these tests stop and start the shared PostgreSQL and Kafka containers.

They run only with --run-disruptive (make test-disruptive, or as the last layer of
make regression). Anything else using the services, the live pipeline included, sees the
outage too and has to recover from it like these tests do.
"""

import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer
from sqlalchemy import text

from .conftest import BackgroundConsumer, compose, wait_for, wait_healthy

pytestmark = [pytest.mark.disruptive, pytest.mark.req("REQ-QA-002")]


@pytest.mark.req("REQ-E2E-005", "REQ-PER-008", "REQ-API-001")
def test_database_outage_loses_and_duplicates_nothing(pipeline, feed, event, caplog):
    feed.show(*[event(f"a{i:03d}", minutes_ago=i) for i in range(10)])
    pipeline.poll()
    with BackgroundConsumer(pipeline):
        wait_for(lambda: pipeline.stored() == 10, 60, "first batch not stored")

        compose("stop", "postgres")
        assert pipeline.get("/health", expect=503)["database"] == "unavailable"
        # The poller only needs Kafka, so new events keep arriving during the outage.
        feed.show(*[event(f"b{i:03d}", minutes_ago=i) for i in range(10)])
        assert pipeline.poll().published == 10
        wait_for(
            lambda: "database unavailable, retrying the same message" in caplog.text,
            30,
            "consumer did not notice the outage",
        )

        compose("start", "postgres")
        wait_healthy("postgres")
        wait_for(lambda: pipeline.stored() == 20, 120, "second batch not stored after recovery")

    assert len(pipeline.events()) == 20
    report = pipeline.reconcile()
    assert report.ok and report.lost == [] and report.pending == 0
    assert report.outcomes == {"applied": 20}  # nothing rejected because of the outage


@pytest.mark.req("REQ-E2E-013", "REQ-ING-006")
def test_kafka_restart_loses_and_duplicates_nothing(pipeline, feed, event):
    feed.show(*[event(f"a{i:03d}", minutes_ago=i) for i in range(10)])
    pipeline.poll()
    with BackgroundConsumer(pipeline):
        wait_for(lambda: pipeline.stored() == 10, 60, "first batch not stored")

        compose("stop", "kafka")
        feed.show(*[event(f"b{i:03d}", minutes_ago=i) for i in range(10)])
        during = pipeline.poll()
        # Nothing confirmed while the broker is down: no event may count as sent.
        assert during.published == 0 and during.failed_delivery == 10

        compose("start", "kafka")
        wait_healthy("kafka")
        after = pipeline.poll()  # the unconfirmed events are retried
        assert after.published == 10
        wait_for(
            lambda: pipeline.stored() == pipeline.messages_in_topic(),
            120,
            "messages not all stored after the broker came back",
        )

    # The client library may still deliver the first attempt once Kafka returns, so an
    # event can reach the topic twice; it must never be stored twice.
    assert len(pipeline.events()) == 20
    report = pipeline.reconcile()
    assert report.ok and report.lost == [] and report.pending == 0
    assert report.outcomes["applied"] == 20
    assert set(report.outcomes) <= {"applied", "duplicate"}


@pytest.mark.req("REQ-OPS-002")
def test_data_survives_restart_of_kafka_and_postgres(settings, db, make_temp_topic):
    token = f"restart-{uuid.uuid4().hex[:8]}"
    topic = make_temp_topic()
    with db.begin() as conn:
        conn.execute(text("CREATE TABLE restart_probe (token text PRIMARY KEY)"))
        conn.execute(text("INSERT INTO restart_probe VALUES (:t)"), {"t": token})
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})
    producer.produce(topic, key=b"probe", value=token.encode())
    assert producer.flush(30) == 0

    compose("restart", "kafka", "postgres")
    wait_healthy("kafka")
    wait_healthy("postgres")

    with db.connect() as conn:
        assert conn.execute(text("SELECT token FROM restart_probe")).scalar_one() == token
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"probe-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    try:
        consumer.subscribe([topic])
        deadline, message = time.monotonic() + 60, None
        while message is None and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is not None and message.error():
                message = None
        assert message is not None and message.value() == token.encode()
    finally:
        consumer.close()
