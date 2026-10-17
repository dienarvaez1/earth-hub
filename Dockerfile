FROM python:3.13.16-slim-trixie

COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Dependencies first, so code changes don't reinstall them.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY README.md alembic.ini ./
COPY src ./src
COPY migrations ./migrations
RUN uv sync --locked --no-dev

RUN useradd --system --uid 10001 earthhub
USER earthhub

CMD ["python", "-m", "earth_hub.ingestion"]
