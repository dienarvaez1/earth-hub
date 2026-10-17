from sqlalchemy import Engine, create_engine

from earth_hub.config import Settings


def make_engine(settings: Settings, database: str | None = None) -> Engine:
    # pre_ping replaces connections broken by a PostgreSQL restart instead of failing once.
    return create_engine(
        settings.database_url(database),
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )
