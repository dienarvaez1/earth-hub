import copy
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import OperationalError

from earth_hub.config import Settings, get_settings
from earth_hub.database.engine import make_engine
from earth_hub.ingestion.events import build_message

FIXTURES = Path(__file__).resolve().parent / "fixtures"
_FEED_SAMPLE = json.loads((FIXTURES / "usgs" / "all_day_sample.geojson").read_text())


@pytest.fixture(scope="session")
def feature_template() -> dict:
    """The first fixture feature, for building synthetic events. Copy before changing."""
    return copy.deepcopy(_FEED_SAMPLE["features"][0])


@pytest.fixture
def feed_doc() -> dict:
    """A trimmed real all_day feed with 6 events covering known edge cases. Safe to mutate."""
    return copy.deepcopy(_FEED_SAMPLE)


FETCHED_AT = datetime(2026, 10, 16, 8, 10, 31, tzinfo=UTC)
# A "now" just after the fixture events, for validating them as the consumer would.
FIXTURE_NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)


@pytest.fixture
def make_envelope():
    """Build an earthquakes.raw message from a feature with the poller's own code, so
    consumer tests always use the real producer contract. Returns bytes."""

    def make(feature: dict, **envelope_overrides) -> bytes:
        message = build_message(feature, fetched_at=FETCHED_AT, feed_generated_ms=None)
        if not envelope_overrides:
            return message.to_json()
        envelope = json.loads(message.to_json())
        envelope.update(envelope_overrides)
        return json.dumps(envelope).encode()

    return make


@pytest.fixture
def make_feature(feed_doc):
    """A copy of the first fixture feature with a new id and optional property changes."""

    def make(event_id: str, ids: list[str] | None = None, **properties) -> dict:
        feature = copy.deepcopy(feed_doc["features"][0])
        feature["id"] = event_id
        feature["properties"]["ids"] = "," + ",".join(ids or [event_id]) + ","
        feature["properties"].update(properties)
        return feature

    return make


# --- PostgreSQL (used by integration and API tests; nothing connects until a test asks) ---

ALEMBIC_INI = Path(__file__).resolve().parents[1] / "alembic.ini"


def alembic_config(database_url) -> Config:
    config = Config(str(ALEMBIC_INI))
    config.attributes["database_url"] = database_url
    return config


@pytest.fixture(scope="session")
def settings() -> Settings:
    return get_settings()


@pytest.fixture(scope="session")
def engine(settings):
    """Engine for the main database, used to create and drop throwaway ones."""
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


class Databases:
    """Creates throwaway databases and drops them all at the end."""

    def __init__(self, admin: Engine, settings: Settings):
        self._admin, self._settings = admin, settings
        self._names: list[str] = []
        self._engines: list[Engine] = []

    def create(self, *, migrate: bool = False) -> str:
        name = f"earth_hub_test_{uuid.uuid4().hex[:8]}"
        with self._admin.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f'CREATE DATABASE "{name}"'))
        self._names.append(name)
        if migrate:
            command.upgrade(alembic_config(self._settings.database_url(name)), "head")
        return name

    def engine(self, name: str) -> Engine:
        engine = make_engine(self._settings, name)
        self._engines.append(engine)
        return engine

    def drop_all(self) -> None:
        for engine in self._engines:
            engine.dispose()
        with self._admin.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            for name in self._names:
                conn.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))


@pytest.fixture
def make_alembic_config():
    """Alembic config pointed at a given database URL."""
    return alembic_config


@pytest.fixture
def make_database(engine, settings):
    """Create brand-new databases, dropped after the test. Returns the database name."""
    databases = Databases(engine, settings)
    yield databases.create
    databases.drop_all()


@pytest.fixture
def empty_database(make_database, settings):
    """URL of a brand-new, empty database."""
    return settings.database_url(make_database())


@pytest.fixture
def db(engine, settings):
    """Engine for a brand-new database migrated to the latest schema."""
    databases = Databases(engine, settings)
    yield databases.engine(databases.create(migrate=True))
    databases.drop_all()


@pytest.fixture(scope="module")
def module_databases(engine, settings):
    """Like make_database, but shared by every test in a module (for read-only tests)."""
    databases = Databases(engine, settings)
    yield databases
    databases.drop_all()
