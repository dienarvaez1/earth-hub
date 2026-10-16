import pytest
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
