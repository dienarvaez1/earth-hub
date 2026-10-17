"""The FastAPI app: `uvicorn earth_hub.api.main:app`, or create_app() in tests."""

from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI
from sqlalchemy import Engine

from earth_hub import __version__
from earth_hub.api.errors import register_error_handlers
from earth_hub.api.routes import router
from earth_hub.config import get_settings
from earth_hub.database.engine import make_engine
from earth_hub.logging_config import configure_logging

DESCRIPTION = """Read-only access to USGS earthquake events collected and validated by
Earth-Hub. Data comes from PostgreSQL only, never live from USGS or Kafka.

All times are UTC. Every error has the shape `{"error": {"code", "message", "details"}}`."""


def create_app(
    engine: Engine | None = None, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        owned = engine is None
        if owned:  # running for real, not under a test that brings its own engine
            configure_logging()
        app.state.engine = engine or make_engine(get_settings())
        app.state.clock = clock
        yield
        if owned:
            app.state.engine.dispose()

    app = FastAPI(
        title="Earth-Hub API",
        version=__version__,
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    # Also set now, so test clients that skip the lifespan still work.
    if engine is not None:
        app.state.engine = engine
    app.state.clock = clock
    register_error_handlers(app)
    app.include_router(router)
    return app


app = create_app()
