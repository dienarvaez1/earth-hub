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

- `make install`: install dependencies from `uv.lock`
- `make up` / `make down` / `make reset`: start, stop, or stop and wipe Kafka and PostgreSQL
- `make lint` / `make format`: Ruff checks and fixes
- `make test`: unit tests (no Docker)
- `make smoke`: integration smoke tests (needs `make up`)
- `make smoke-restart`: restart services and check data survives
- `make migrate`: `alembic upgrade head`

Add dependencies with `uv add <pkg>` (or `uv add --dev <pkg>`); never edit `uv.lock` by hand.

## Layout notes

- `.env` holds host-side values (`localhost:5433`, `localhost:29092`). Containers get in-network names (`postgres:5432`, `kafka:9092`) from `docker-compose.yml`.
- Alembic gets its database URL from `earth_hub.config`, never from `alembic.ini`. Tests pass a URL through `config.attributes["database_url"]`.
- Smoke tests use temporary topics and databases; never write test data to `earthquakes.raw` or the main database.
