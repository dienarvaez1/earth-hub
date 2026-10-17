import pytest
from fastapi.testclient import TestClient

from earth_hub.api.main import create_app

from .dataset import API_NOW, seed


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
