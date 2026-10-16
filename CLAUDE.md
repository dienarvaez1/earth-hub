# Earth-Hub

Local-first earthquake data platform: USGS GeoJSON feed → Python poller → Kafka (`earthquakes.raw`) → consumer + validation → PostgreSQL → read-only FastAPI REST API. The full plan is in `docs/IMPLEMENTATION.md`.

## Stack

- Python 3.13 (pinned in `.python-version`), dependencies managed with uv
- Apache Kafka in KRaft mode (official `apache/kafka` image), `confluent-kafka` client
- PostgreSQL, SQLAlchemy, Alembic
- FastAPI, Pydantic, Uvicorn
- pytest, HTTPX, pytest-cov, Ruff
- Docker Compose for local orchestration; GitHub Actions for CI

## Rules

- **Never call the live USGS feed in unit tests.** Use fixtures in `tests/fixtures/` and mocked HTTP responses.
- Unit tests must not need Docker. Infrastructure-dependent tests go in `tests/integration/` or `tests/e2e/`.
- Commit the database transaction before committing the Kafka offset. Don't claim exactly-once processing; rely on idempotent upserts.
- Apply a revision only if its `updated` timestamp is newer than the stored one.
- GeoJSON coordinates are `[longitude, latitude, depth]`.
- Store all timestamps as UTC `timestamptz`.
- The API reads from PostgreSQL only, never from Kafka or USGS.
- Never commit `.env` or credentials. Add new settings to `.env.example`.
- Work one milestone at a time. Propose the plan and test strategy before implementing, and wait for approval before broad changes.

## Commands

Fill these in as Milestone 1 adds them (install, lint, test, start infrastructure, smoke test).
