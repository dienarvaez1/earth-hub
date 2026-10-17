import uuid
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from earth_hub.config import Settings, get_settings
from earth_hub.database.engine import make_engine

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


def alembic_config(database_url) -> Config:
    config = Config(str(ALEMBIC_INI))
    config.attributes["database_url"] = database_url
    return config


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


@pytest.fixture
def make_alembic_config():
    """Alembic config pointed at a given database URL."""
    return alembic_config


@pytest.fixture
def make_database(engine, settings):
    """Create brand-new databases, dropped after the test. Returns the database name."""
    created = []

    def make() -> str:
        name = f"earth_hub_test_{uuid.uuid4().hex[:8]}"
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f'CREATE DATABASE "{name}"'))
        created.append(name)
        return name

    yield make
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for name in created:
            conn.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))


@pytest.fixture
def empty_database(make_database, settings):
    """URL of a brand-new, empty database."""
    return settings.database_url(make_database())


@pytest.fixture
def db(make_database, settings):
    """Engine for a brand-new database migrated to the latest schema."""
    name = make_database()
    command.upgrade(alembic_config(settings.database_url(name)), "head")
    engine = make_engine(settings, name)
    yield engine
    engine.dispose()
