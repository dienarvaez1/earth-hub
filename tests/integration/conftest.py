import uuid

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# PostgreSQL fixtures (settings, engine, db, make_database, ...) live in tests/conftest.py.

KAFKA_TIMEOUT_S = 30


@pytest.fixture(scope="session")
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
def make_temp_topic(admin):
    """Create throwaway topics, so tests never write to earthquakes.raw. Deleted afterwards."""
    created = []

    def make(partitions: int = 1) -> str:
        name = f"test.{uuid.uuid4().hex[:12]}"
        admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])[
            name
        ].result(timeout=KAFKA_TIMEOUT_S)
        created.append(name)
        return name

    yield make
    if created:
        for future in admin.delete_topics(created).values():
            future.result(timeout=KAFKA_TIMEOUT_S)


@pytest.fixture
def temp_topic(make_temp_topic) -> str:
    return make_temp_topic()
