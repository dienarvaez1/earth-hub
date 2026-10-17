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
- `make ingest` / `make ingest-once`: run the pipeline (migrate, poller, consumer) on the live feed / one poller round on the host. Never part of the test suite.
- `make regression`: every layer in order, with reports in `reports/` and the traceability matrix. Run it before proposing a milestone as done.
- `make backfill START=2026-01-01 [END=...]`, `make catchup-once`, `make test-performance` (a year of synthetic backfill, timed).
- `make test-e2e`, `make test-disruptive` (stops shared containers), `make flake-check` (end-to-end suite ×5), `make traceability`.
- `make reconcile`: topic-versus-database check.
- `make api` / `make api-dev`: serve the API (container / host with reload) at http://127.0.0.1:8000/docs.

Add dependencies with `uv add <pkg>` (or `uv add --dev <pkg>`); never edit `uv.lock` by hand.

## Layout notes

- `.env` holds host-side values (`localhost:5433`, `127.0.0.1:29092`). Use `127.0.0.1` for Kafka, never `localhost`: the port is IPv4-only and `localhost` resolving to `::1` caused flaky offset lookups. Containers get in-network names (`postgres:5432`, `kafka:9092`) from `docker-compose.yml`.
- Alembic gets its database URL from `earth_hub.config`, never from `alembic.ini`. Tests pass a URL through `config.attributes["database_url"]`.
- Ingestion code lives in `src/earth_hub/ingestion/`. The poller publishes the unchanged USGS feature inside an envelope; it rejects only features without `id`, `updated` or `time`. Data-quality rules belong in the consumer.
- Unit tests get feed data from the `feed_doc` fixture and fake Kafka through `fake_producer` / `fake_producer_cls`; integration tests use `make_temp_topic`.
- Persistence lives in `src/earth_hub/database/` (models, repository), `validation/` and `consumers/`. Each message is handled in one transaction by `repository.process_message`; the consumer commits the Kafka offset only after it.
- Validation rules have stable IDs documented in `docs/data-quality-rules.md`; add a rule there and a test for it together.
- Schema changes: edit `database/models.py`, then `uv run alembic revision --autogenerate`, then review the file. `tests/integration/test_migrations.py` fails if models and migrations drift.
- DB integration tests use the `db` fixture (a fresh migrated database per test); build messages with `make_envelope` / `make_feature` so tests use the poller's real contract.
- In shell scripts, never export settings like `POSTGRES_DB`: docker compose picks them up and recreates containers. Pass them per command.
- The API lives in `src/earth_hub/api/`: `routes.py` (parameters and validation), `services.py` (queries), `schemas.py` (response models = the OpenAPI contract), `errors.py` (the one error format). Routes take the engine and connect inside their body, never in a dependency, so invalid requests return 422 even when the database is down.
- New fixed API paths go before `/earthquakes/{event_id}` in `routes.py`.
- String parameters that reach SQL use the `Text` type (rejects NUL); times use `DateTime` (ISO 8601 only, years 1900–2099). Both came from Schemathesis findings.
- API tests use the `client` / `empty_client` fixtures over the fixed dataset in `tests/api/dataset.py`.
- Markers: `integration`, `e2e`, `disruptive` (skipped unless `--run-disruptive`), and `req("REQ-...")`. Every requirement in `docs/requirements.md` needs at least one passing test; new behavior gets a requirement and a marked test. Unknown IDs fail at collection.
- Unit tests cannot use the network: a guard in `tests/conftest.py` blocks anything but loopback.
- End-to-end and disruptive tests use `tests/support/` (`FeedServer`, `EventFactory`, `Pipeline`) and assert through the API, ending with a reconciliation.
- Record every defect found in `docs/defects/README.md`, with a regression test.
- Backfill (`ingestion/backfill.py`, `fdsn.py`, `windows.py`): FDSN ignores milliseconds in query times. Windows must end exactly where the next begins: never `- 1 ms`, never `limit`. Always compare with `/count` using the same `includedeleted`. A `504` or timeout is not retried; the window is split instead (the gateway caches the `504`). Requests are sequential, with `FDSN_PAUSE_SECONDS` between them.
- Backfill and catch-up publish to `earthquakes.backfill`, which the live poller never reads back. It is consumed by `consumer-backfill`, the same code with `KAFKA_CONSUMER_TOPIC` set.
- `repository.process_batch` processes a batch as a set and must give exactly the results of `process_message` one by one: `tests/integration/test_batch.py` checks this on random streams. Merges fall back to the one-by-one path. Every lock goes through `_lock`, which uses sorted order (deadlock safety).
- The mock FDSN service (`tests/support/fdsn.py`) reproduces the real one's boundary semantics; keep it faithful when changing either.
- Smoke tests use temporary topics and databases; never write test data to `earthquakes.raw` or the main database.
