import uuid
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text

pytestmark = pytest.mark.integration

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


def test_create_insert_query(engine):
    with engine.connect() as conn, conn.begin() as tx:
        conn.execute(
            text("CREATE TEMP TABLE smoke_probe (id int PRIMARY KEY, label text NOT NULL)")
        )
        conn.execute(text("INSERT INTO smoke_probe VALUES (1, 'probe')"))
        rows = conn.execute(text("SELECT id, label FROM smoke_probe")).all()
        tx.rollback()
    assert rows == [(1, "probe")]


def test_server_is_postgresql_18(engine):
    with engine.connect() as conn:
        version = conn.execute(text("SHOW server_version_num")).scalar_one()
    assert int(version) // 10000 == 18


@pytest.fixture
def empty_database(engine, settings):
    """A brand-new database, dropped after the test."""
    name = f"earth_hub_smoke_{uuid.uuid4().hex[:8]}"
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    yield settings.database_url(name)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f'DROP DATABASE "{name}" WITH (FORCE)'))


def test_migrations_upgrade_empty_database_to_head(empty_database):
    config = Config(str(ALEMBIC_INI))
    config.attributes["database_url"] = empty_database
    command.upgrade(config, "head")

    head = ScriptDirectory.from_config(config).get_current_head()
    db = create_engine(empty_database)
    try:
        with db.connect() as conn:
            current = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    finally:
        db.dispose()
    assert current == head
