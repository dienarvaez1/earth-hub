import time
import uuid

import pytest
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

pytestmark = pytest.mark.integration

TIMEOUT_SECONDS = 30


@pytest.fixture(scope="module")
def admin(settings):
    client = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    try:
        client.list_topics(timeout=10)
    except KafkaException as exc:
        pytest.fail(
            f"Kafka is not reachable at {settings.kafka_bootstrap_servers}; run `make up`.\n{exc}"
        )
    return client


@pytest.fixture
def temp_topic(admin):
    """A throwaway topic, so smoke tests never write to earthquakes.raw."""
    name = f"smoke.{uuid.uuid4().hex[:12]}"
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result(
        timeout=TIMEOUT_SECONDS
    )
    yield name
    admin.delete_topics([name])[name].result(timeout=TIMEOUT_SECONDS)


def test_raw_topic_exists_with_three_partitions(admin, settings):
    topic = admin.list_topics(topic=settings.kafka_topic_raw, timeout=10).topics[
        settings.kafka_topic_raw
    ]
    assert topic.error is None, f"{settings.kafka_topic_raw} is missing: {topic.error}"
    assert len(topic.partitions) == 3


def test_message_round_trip(settings, temp_topic):
    key, value = b"us7000smoke", f'{{"probe": "{uuid.uuid4()}"}}'.encode()

    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all"})
    delivered = []
    producer.produce(temp_topic, key=key, value=value, on_delivery=lambda e, m: delivered.append(e))
    assert producer.flush(TIMEOUT_SECONDS) == 0, "message was not delivered in time"
    assert delivered == [None], f"delivery failed: {delivered}"

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"smoke-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.subscribe([temp_topic])
        deadline = time.monotonic() + TIMEOUT_SECONDS
        message = None
        while message is None and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is not None and message.error():
                pytest.fail(f"consumer error: {message.error()}")
        assert message is not None, "message was not consumed in time"
        assert (message.key(), message.value()) == (key, value)
    finally:
        consumer.close()
