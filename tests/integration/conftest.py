import uuid

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from earth_hub.config import Settings, get_settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return get_settings()


@pytest.fixture(scope="session")
def engine(settings):
    engine = create_engine(settings.database_url(), connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as exc:
        # pytrace=False: pytest's full traceback would print the connection
        # arguments, password included. str(URL) masks it.
        pytest.fail(
            f"PostgreSQL is not reachable at {settings.database_url()}; run `make up`.\n{exc.orig}",
            pytrace=False,
        )
    yield engine
    engine.dispose()


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
