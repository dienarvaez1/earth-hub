# Earth-Hub

Local-first earthquake data platform: USGS GeoJSON feed → Python poller → Kafka (`earthquakes.raw`) → consumer + validation → PostgreSQL → read-only FastAPI REST API. The full plan is in `docs/progress/plan.md`.

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
- `make backfill START=2026-01-01 [END=...]`, `make catchup-once`, `make test-performance` (a year of synthetic backfill, timed; the API under load).
- `make load [USERS=] [RUN_TIME=]` / `make load-ui`: Locust against the API over a temporary, seeded database (`scripts/load_test.py`, `tests/load/locustfile.py`); report in `reports/load-report.html`. Locust runs as a subprocess and is never imported by pytest (gevent patches networking on import).
- `make test-e2e`, `make test-disruptive` (stops shared containers), `make flake-check` (end-to-end suite ×5), `make traceability`.
- `make reconcile`: topic-versus-database check.
- `make reconcile-direct [DEEP=1]`: direct mode's bucket invariants (read-only).
- `make reconcile-usgs [DAYS=3]`: the mode writing the bucket against USGS, every difference explained (read-only; `earth_hub/usgs_reconcile.py`). `make catchup-now`: catch up with USGS now, in either mode. Both run at the end of `make mode-direct` and `make mode-full`.
- `make mode` / `make mode-direct` / `make mode-full`: show / hand over the bucket between modes. `make direct-once` / `make direct-run`: run direct mode. All but `make mode` write to the real bucket or stop services: only when the user asks.
- `make dashboard-install` / `make dashboard-build` / `make dashboard-dev` / `make test-dashboard`: the dashboard's Node setup, build, local server over the real bucket, browser tests.
- `make dashboard-deploy`: publish the dashboard to Cloudflare Workers. Only when the user asks; never part of the tests.
- `make export-r2 [DRY_RUN=1] [VERIFY=1]` / `make export-r2-runs`: on-demand copy of `earthquakes` to R2 through the API / list runs. Never part of the test suite.
- `make r2-ls [PREFIX=] [LIMIT=]` / `make r2-count [PREFIX=]` / `make r2-get KEY= [OUT=]`: read-only look at the bucket (`export/browse.py`; it must never write).
- Skills: `.claude/skills/eh*` (`/eh-status`, `/eh-kafka`, `/eh-db`, …), indexed in `docs/overview/cheatsheet.md`. A new skill or make target used in them goes in the cheat sheet; `tests/unit/test_cheatsheet.py` checks both exist.
- `make release [DRY_RUN=1]`: the public release (`scripts/release.py`), interactive: run by the user in a terminal, never by Claude or the tests.
- `make api` / `make api-dev`: serve the API (container / host with reload) at http://127.0.0.1:8000/docs.

Add dependencies with `uv add <pkg>` (or `uv add --dev <pkg>`); never edit `uv.lock` by hand.

## Layout notes

- `.env` holds host-side values (`localhost:5433`, `127.0.0.1:29092`). Use `127.0.0.1` for Kafka, never `localhost`: the port is IPv4-only and `localhost` resolving to `::1` caused flaky offset lookups. Containers get in-network names (`postgres:5432`, `kafka:9092`) from `docker-compose.yml`.
- Alembic gets its database URL from `earth_hub.config`, never from `alembic.ini`. Tests pass a URL through `config.attributes["database_url"]`.
- Ingestion code lives in `src/earth_hub/ingestion/`. The poller publishes the unchanged USGS feature inside an envelope; it rejects only features without `id`, `updated` or `time`. Data-quality rules belong in the consumer.
- Unit tests get feed data from the `feed_doc` fixture and fake Kafka through `fake_producer` / `fake_producer_cls`; integration tests use `make_temp_topic`.
- Persistence lives in `src/earth_hub/database/` (models, repository), `validation/` and `consumers/`. Each message is handled in one transaction by `repository.process_message`; the consumer commits the Kafka offset only after it.
- Validation rules have stable IDs documented in `docs/technical/data-quality-rules.md`; add a rule there and a test for it together.
- Schema changes: edit `database/models.py`, then `uv run alembic revision --autogenerate`, then review the file. `tests/integration/test_migrations.py` fails if models and migrations drift.
- DB integration tests use the `db` fixture (a fresh migrated database per test); build messages with `make_envelope` / `make_feature` so tests use the poller's real contract.
- In shell scripts, never export settings like `POSTGRES_DB`: docker compose picks them up and recreates containers. Pass them per command.
- The API lives in `src/earth_hub/api/`: `routes.py` (parameters and validation), `services.py` (queries), `schemas.py` (response models = the OpenAPI contract), `errors.py` (the one error format). Routes take the engine and connect inside their body, never in a dependency, so invalid requests return 422 even when the database is down.
- New fixed API paths go before `/earthquakes/{event_id}` in `routes.py`.
- String parameters that reach SQL use the `Text` type (rejects NUL); times use `DateTime` (ISO 8601 only, years 1900–2099). Both came from Schemathesis findings.
- API tests use the `client` / `empty_client` fixtures over the fixed dataset in `tests/api/dataset.py`.
- Markers: `integration`, `e2e`, `disruptive` (skipped unless `--run-disruptive`), and `req("REQ-...")`. Every requirement in `docs/progress/requirements.md` needs at least one passing test; new behavior gets a requirement and a marked test. Unknown IDs fail at collection.
- Unit tests cannot use the network: a guard in `tests/conftest.py` blocks anything but loopback.
- End-to-end and disruptive tests use `tests/support/` (`FeedServer`, `EventFactory`, `Pipeline`) and assert through the API, ending with a reconciliation.
- Record every defect found in `docs/progress/defects.md`, with a regression test.
- Backfill (`ingestion/backfill.py`, `fdsn.py`, `windows.py`): FDSN ignores milliseconds in query times. Windows must end exactly where the next begins: never `- 1 ms`, never `limit`. Always compare with `/count` using the same `includedeleted`. A `504` or timeout is not retried; the window is split instead (the gateway caches the `504`). Requests are sequential, with `FDSN_PAUSE_SECONDS` between them.
- Backfill and catch-up publish to `earthquakes.backfill`, which the live poller never reads back. It is consumed by `consumer-backfill`, the same code with `KAFKA_CONSUMER_TOPIC` set, in batches of `CONSUMER_BATCH_SIZE`. The live `consumer` has a batch size of 1 in `docker-compose.yml`, so every live event's outcome is logged (DEF-036; `tests/unit/test_compose.py`).
- The decisions of persistence (which record an event belongs to, merges, whether a revision applies, which IDs are recorded) are made in memory by `processing/ledger.py`, never in SQL; `repository.py` only loads the records a batch touches and writes the ledger's decisions (`PostgresStore`, an `EventStore`), in the order one-by-one processing would. `process_message` is a batch of one; `process_batch` must give exactly the results of one by one, merges included: `tests/integration/test_batch.py` checks this on random streams. Processing times (`first_seen_at`, `last_applied_at`) come from the `now` passed in, never the database clock. Every lock goes through `_lock`, which uses sorted order (deadlock safety). `direct/store.py:BucketStore` is the second `EventStore`, over R2 (direct mode); new persistence rules go in the ledger, so both stores get them.
- Direct mode (`src/earth_hub/direct/`, docs/technical/architecture.md#direct-mode) must give the bucket full mode's export would, byte for byte: `tests/integration/test_direct_parity.py` (random streams and defect scenarios through both stores) and `tests/e2e/test_direct_end_to_end.py` check it. Rows are encoded through `EarthquakeRow`, as the API does; records are loaded from the snapshot, so a value must survive Parquet exactly (REQ-MODE-008). A round's writes go through the redo record (`BucketState.begin`/`apply`), in a fixed order, each repeatable; a new write goes in `apply`, and `tests/unit/test_direct.py` interrupts every step. Catch-up's "is this stored" uses `revisions.needed` in both modes. Gzipped state uses `mtime=0` (DEF-041). `_state/mode.json` (`export/mode.py`) says which mode writes the bucket: every writer checks it (the exporter after taking the lock, a direct round in `_prepare`). The handovers are in `direct/handover.py`; the direct → full replay puts entries on partition 0 of the backfill topic with `earth-hub-processed-at` and `earth-hub-seq` headers, and `process_batch` processes each run of messages with the same replayed time as one batch. `tests/e2e/test_mode_handover.py` checks round trips against a database that stayed in full mode.
- Whether a revision replaces the stored one is decided only by `database/revisions.py:compare` (newer `updated` wins, except for duplicates' deletions: DEF-026). The consumer's two paths and `Progress.needs_publish` all use it; don't compare `updated` anywhere else.
- The mock FDSN service (`tests/support/fdsn.py`) reproduces the real one's boundary semantics; keep it faithful when changing either.
- The R2 exporter (`export/r2.py`, `export/bucket.py`) reads only `GET /api/v1/export/earthquakes`, never PostgreSQL. `EarthquakeRow` in `api/schemas.py` must list every `earthquakes` column, in order (a test checks it). Tests use moto, never real R2. Shared test pieces are in `tests/support/export.py`, fixtures in `tests/unit/conftest.py`.
- The analytics snapshot (`export/snapshot.py`) is written only after a run that succeeded. `COLUMNS` there must match `EarthquakeRow` less `id` (a test checks it); a new column needs both. R2 rows and the snapshot are named and ordered by `key` (the USGS ID a record was created with) and never carry the database's `id`, which differs between stores (REQ-EXP-012).
- The dashboard (`dashboard/`): `src/worker.ts` exposes only the two files in `OBJECTS`; never widen it to other bucket keys. Dashboard tests (`tests/dashboard/`, marker `dashboard`) run `wrangler dev --local` over a simulated R2 and block every request off the machine (`support/dashboard.route_offline`); they fail on any browser console error. `dashboard/wrangler.jsonc` is generated (`scripts/dashboard_config.py`), never committed.
- DuckDB-WASM is pinned in `dashboard/package.json`; its engine version must match `ENGINE` in `scripts/fetch_duckdb_extensions.py` (the browser tests fail otherwise). Add Node packages with `npm install --save-exact` in `dashboard/`.
- Dashboard reports are the SQL files in `dashboard/queries/`, run unchanged by the page and by `tests/support/analytics.py`. Bucket times from `event_time_ms` as a plain UTC `TIMESTAMP`, never through time zones. A new report needs a test with hand-worked answers in `tests/unit/test_dashboard_queries.py`.
- Log lines (docs/technical/architecture.md#observability): every pipeline line has a `stage` (`fetch`, `parse`, `publish`, `persist`; `earth_hub/observability.py`), and every line about one event its `event_id`. A new log call in the pipeline names its stage, or inherits its module's from `DEFAULT_STAGES` in `logging_config.py`. The consumer's summary line (counts and lag) is `consumer summary`. Loggers are `getLogger(__name__)`, except in modules run with `python -m`, which use `getLogger(__spec__.name)` (`__name__` is `__main__` there; DEF-034).
- Smoke tests use temporary topics and databases; never write test data to `earthquakes.raw` or the main database.
- CI (`.github/workflows/ci.yml`, docs/technical/ci.md) runs `make lint`, `make test`, then `scripts/regression.py --skip performance` from `.env.example`. It must never need secrets; actions are pinned to commit SHAs (a release at least two weeks old) and `tests/unit/test_ci.py` checks the workflow. A layer left out with `--skip` is reported, not failed, and `traceability.py --not-run` lists its requirements as not run.
- The dashboard build writes `dist/third-party-licenses.txt` from Vite's list of bundled packages (`vite.config.ts`); a package that ships no license text needs one in `dashboard/licenses/`, or the build fails (DEF-035). The page is shared by link, not promoted: keep it out of search engines with `X-Robots-Tag: noindex` (`public/_headers`, `worker.ts`), the robots meta tag and `robots.txt`.
- Docs are tested (REQ-DOC-*): the requests in `docs/technical/api.md` and the queries in `docs/technical/sql-examples.md` are replayed (`tests/api/test_api_examples.py`, `test_sql_examples.py`, `tests/unit/test_report_examples.py`, parser in `tests/support/doc_examples.py`) on fixed datasets, so changing the API, a report or the datasets means updating those pages. `docs/technical/schema.md` must list every column (`test_schema_doc.py`); relative links and anchors, and the README's requirement and defect counts, are checked (`test_doc_links.py`). Docs are in three groups, indexed in `docs/README.md`: `docs/overview/` (demo, cheat sheet, limitations), `docs/technical/` (architecture with observability, schema, API, SQL, data-quality rules, CI) and `docs/progress/` (plan and status, requirements, traceability matrix, test strategy, defect log). The README is the reviewer's entry point; new pages go in one of the three groups and in the index.
