from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from earth_hub.config import get_settings

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# No models yet; set this to the models' MetaData once tables exist (Milestone 3).
target_metadata = None


def _database_url():
    # Tests pass a URL for a throwaway database; otherwise use the app settings.
    # The URL never goes through alembic.ini, so the password stays out of config files.
    return config.attributes.get("database_url") or get_settings().database_url()


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_database_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
