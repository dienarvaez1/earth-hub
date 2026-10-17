import copy
import json
import re
import socket
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import OperationalError

from earth_hub.config import Settings, get_settings
from earth_hub.database.engine import make_engine
from earth_hub.ingestion.events import build_message

REPO = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
REQUIREMENTS_DOC = REPO / "docs" / "requirements.md"
LOOPBACK = {"127.0.0.1", "::1", "localhost"}
NETWORKED_MARKERS = {"integration", "e2e", "disruptive", "performance"}


# --- QA plumbing: disruptive opt-in, requirement markers, traceability, network guard ---


def pytest_addoption(parser):
    parser.addoption(
        "--run-disruptive",
        action="store_true",
        help="run tests that stop and start the shared Docker containers",
    )
    parser.addoption(
        "--run-performance",
        action="store_true",
        help="run the performance tests (a full year of synthetic backfill, a few minutes)",
    )
    parser.addoption(
        "--trace-out",
        metavar="PATH",
        help="write requirement -> test outcomes as JSON, for scripts/traceability.py",
    )


def known_requirements() -> set[str]:
    return set(re.findall(r"^\| (REQ-[A-Z0-9-]+) \|", REQUIREMENTS_DOC.read_text(), re.M))


def pytest_collection_modifyitems(config, items):
    known = known_requirements()
    unknown = {
        (item.nodeid, req)
        for item in items
        for marker in item.iter_markers("req")
        for req in marker.args
        if req not in known
    }
    if unknown:
        raise pytest.UsageError(
            "tests reference requirements missing from docs/requirements.md: "
            + ", ".join(f"{req} ({nodeid})" for nodeid, req in sorted(unknown))
        )
    for marker, option, reason in (
        ("disruptive", "--run-disruptive", "stops shared containers"),
        ("performance", "--run-performance", "takes minutes"),
    ):
        if not config.getoption(option):
            skip = pytest.mark.skip(reason=f"{marker}: {reason}; use {option}")
            for item in items:
                if marker in item.keywords:
                    item.add_marker(skip)


_trace: dict[str, dict[str, str]] = {}


def pytest_runtest_logreport(report):
    # An item's outcome is its worst phase: a failed setup or teardown fails the test.
    if report.when == "call" or report.outcome != "passed":
        reqs = getattr(report, "reqs", ())
        for req in reqs:
            current = _trace.setdefault(req, {}).get(report.nodeid)
            if current != "failed":
                _trace[req][report.nodeid] = report.outcome


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    report = yield
    report.reqs = tuple(req for marker in item.iter_markers("req") for req in marker.args)
    return report


def pytest_sessionfinish(session):
    path = session.config.getoption("--trace-out")
    if path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(_trace, indent=2, sort_keys=True))


@pytest.fixture(autouse=True)
def _no_network_in_unit_tests(request, monkeypatch):
    """Unit tests must never reach the network, the live USGS feed included (REQ-QA-001).
    Loopback stays allowed, for tests that point at a deliberately closed local port."""
    if NETWORKED_MARKERS & set(request.keywords):
        return
    real_getaddrinfo = socket.getaddrinfo
    real_connect = socket.socket.connect

    def guarded_getaddrinfo(host, *args, **kwargs):
        if host not in LOOPBACK and host is not None:
            raise RuntimeError(f"unit tests must not use the network (tried to resolve {host!r})")
        return real_getaddrinfo(host, *args, **kwargs)

    def guarded_connect(sock, address):
        host = address[0] if isinstance(address, tuple) else address
        if isinstance(host, str) and host not in LOOPBACK and not host.startswith("/"):
            raise RuntimeError(f"unit tests must not use the network (tried {address!r})")
        return real_connect(sock, address)

    monkeypatch.setattr(socket, "getaddrinfo", guarded_getaddrinfo)
    monkeypatch.setattr(socket.socket, "connect", guarded_connect)


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
    # pre_ping: disruptive tests restart PostgreSQL, which kills pooled connections.
    engine = create_engine(
        settings.database_url(), pool_pre_ping=True, connect_args={"connect_timeout": 5}
    )
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


# --- Kafka (used by integration, e2e and disruptive tests) ---------------------------

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
        # A new topic's partitions have no leader for a moment; wait so tests start clean.
        deadline = time.monotonic() + KAFKA_TIMEOUT_S
        while time.monotonic() < deadline:
            meta = admin.list_topics(name, timeout=10).topics.get(name)
            if (
                meta
                and meta.error is None
                and len(meta.partitions) == partitions
                and all(p.leader >= 0 for p in meta.partitions.values())
            ):
                return name
            time.sleep(0.1)
        raise TimeoutError(f"topic {name} has no partition leaders after {KAFKA_TIMEOUT_S}s")

    yield make
    if created:
        for future in admin.delete_topics(created).values():
            future.result(timeout=KAFKA_TIMEOUT_S)


@pytest.fixture
def temp_topic(make_temp_topic) -> str:
    return make_temp_topic()
