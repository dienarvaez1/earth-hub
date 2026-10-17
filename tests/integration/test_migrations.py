import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, inspect

from earth_hub.database.models import Base

pytestmark = pytest.mark.integration

TABLES = {"earthquakes", "earthquake_ids", "raw_events"}


@pytest.mark.req("REQ-PER-012")
def test_migrations_match_the_models(db):
    """A model change without a migration (or the reverse) fails here."""
    with db.connect() as conn:
        diff = compare_metadata(MigrationContext.configure(conn), Base.metadata)
    assert diff == []


@pytest.mark.req("REQ-PER-012")
def test_upgrade_then_downgrade_to_base(empty_database, make_alembic_config):
    config = make_alembic_config(empty_database)
    engine = create_engine(empty_database)
    try:
        command.upgrade(config, "head")
        assert TABLES <= set(inspect(engine).get_table_names())
        command.downgrade(config, "base")
        assert not TABLES & set(inspect(engine).get_table_names())
        command.upgrade(config, "head")  # and back up again cleanly
        assert TABLES <= set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
