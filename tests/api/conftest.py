import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from earth_hub.api.main import create_app
from earth_hub.database.repository import IncomingMessage, process_message
from earth_hub.ingestion.events import build_message

from .dataset import API_NOW, NOT_APPLIED, PROCESSED_AT, SPECS, feature, seed


@pytest.fixture(scope="module")
def seeded_engine(module_databases, feature_template):
    engine = module_databases.engine(module_databases.create(migrate=True))
    seed(engine, feature_template)
    return engine


@pytest.fixture(scope="module")
def client(seeded_engine):
    with TestClient(create_app(seeded_engine, clock=lambda: API_NOW)) as client:
        yield client


@pytest.fixture(scope="module")
def empty_client(module_databases):
    engine = module_databases.engine(module_databases.create(migrate=True))
    with TestClient(create_app(engine, clock=lambda: API_NOW)) as client:
        yield client


@pytest.fixture(scope="module")
def documented_engine(module_databases, feature_template):
    """The dataset of the documented examples (docs/api.md, docs/sql-examples.md): the API
    dataset, then three messages that are not applied. first_seen_at is set by the database
    clock, so it is fixed here to when the dataset was processed, for responses that repeat."""
    engine = module_databases.engine(module_databases.create(migrate=True))
    seed(engine, feature_template)
    with engine.begin() as conn:
        for offset, (spec, updated_offset_s, outcome) in enumerate(NOT_APPLIED, len(SPECS) + 1):
            f = feature(feature_template, spec, [spec.event_id], updated_offset_s)
            message = build_message(f, fetched_at=PROCESSED_AT, feed_generated_ms=None)
            incoming = IncomingMessage("earthquakes.raw", 0, offset, message.key, message.to_json())
            assert process_message(conn, incoming, now=PROCESSED_AT).outcome == outcome
        conn.execute(text("UPDATE earthquakes SET first_seen_at = :at"), {"at": PROCESSED_AT})
    return engine


@pytest.fixture(scope="module")
def documented_client(documented_engine):
    with TestClient(create_app(documented_engine, clock=lambda: API_NOW)) as client:
        yield client
