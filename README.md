# Earth Hub

Local-first earthquake data platform: the USGS earthquake feed flows through Apache Kafka into PostgreSQL and out through a REST API, with data quality and testing treated as first-class concerns. See [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for the full plan.

**Status:** Milestone 5 (end-to-end QA). Live USGS events flow through Kafka into validated PostgreSQL tables and are served by a read-only REST API. The whole path is covered by automated end-to-end and outage tests, run by one regression command. Analytics, observability and performance come next, in Milestone 6.

## Requirements

| Tool | Version used | Notes |
| --- | --- | --- |
| Docker Desktop with Compose | Docker 29.8, Compose 5.5 | Give Docker at least 4 GB of memory |
| Python | 3.13 | Pinned in `.python-version` |
| uv | 0.13 | `brew install uv` |
| GNU Make | any | Preinstalled on macOS and Linux |

Services run in Docker, so Kafka and PostgreSQL don't need to be installed locally:

| Service | Image | Reachable from your machine at |
| --- | --- | --- |
| Kafka (KRaft, single node) | `apache/kafka:4.2.2` | `127.0.0.1:29092` (Kafka protocol), `127.0.0.1:9101` (JMX) |
| PostgreSQL | `postgres:18.6` | `localhost:5433` |

PostgreSQL uses host port 5433 because 5432 is often taken by a local PostgreSQL. All ports are bound to `127.0.0.1` only. Kafka uses `PLAINTEXT` with no authentication, and JMX has no authentication either, which is acceptable only because nothing outside this machine can reach them.

To inspect the broker with JConsole: `jconsole 127.0.0.1:9101`, then choose *Insecure connection*. Kafka's metrics are under the `kafka.server`, `kafka.network` and `kafka.log` MBean domains.

To browse messages, run `make kafka-ui` and open `http://127.0.0.1:8080`. It uses [Kafbat UI](https://github.com/kafbat/kafka-ui) (`ghcr.io/kafbat/kafka-ui:v1.5.0`, Apache 2.0), which is not started by `make up`. In it you can:
- read messages in `earthquakes.raw`, filtered by key, partition or offset
- see how messages are spread across partitions
- see the `earth-hub-consumer` group's lag

It runs **read-only**, so producing, deleting or changing topics is refused (`405`). It doesn't show broker JMX metrics: Kafka's JMX advertises `127.0.0.1` for JConsole on your machine, and that address isn't reachable from another container.

## Getting started

```bash
cp .env.example .env     # then change POSTGRES_PASSWORD
make install             # create .venv from uv.lock
make up                  # start Kafka and PostgreSQL, create the earthquakes.raw topic
make migrate             # apply database migrations
make smoke               # check Kafka and PostgreSQL work end to end
make ingest              # run the pipeline (poller + consumer) on the live USGS feed
make reconcile           # check every Kafka message reached PostgreSQL
make api                 # serve the REST API at http://127.0.0.1:8000/docs
```

## Commands

| Command | What it does |
| --- | --- |
| `make install` | Install dependencies into `.venv` from `uv.lock` |
| `make up` | Start Kafka and PostgreSQL, wait until healthy, create topics |
| `make down` | Stop the services, keeping their data |
| `make reset` | Stop the services and delete their data |
| `make lint` | Check style and formatting with Ruff |
| `make format` | Fix style and formatting |
| `make regression` | **Every test layer in order, with reports in `reports/`** |
| `make test` | Unit tests; no Docker or network needed |
| `make test-integration` / `make smoke` | Integration, API and contract tests against the running services |
| `make test-e2e` | End-to-end tests, from the feed to the API |
| `make test-disruptive` | Tests that stop and start the shared Kafka and PostgreSQL containers |
| `make test-all` | Every non-disruptive test in one process, which also catches tests that affect each other |
| `make flake-check` | The end-to-end suite 5 times in a row |
| `make traceability` | Rebuild the traceability matrix from the last regression run |
| `make smoke-restart` | Restart Kafka and PostgreSQL and check data survives (disruptive) |
| `make migrate` | Apply database migrations with Alembic |
| `make ingest` | Build and start the pipeline containers: migrate, poller, consumer (live USGS feed) |
| `make ingest-once` | Poll the live feed once from your machine and exit |
| `make ingest-logs` / `make ingest-stop` | Follow or stop the poller and consumer |
| `make api` / `make api-stop` | Start or stop the API container (`http://127.0.0.1:8000`) |
| `make api-dev` | Run the API on your machine with auto-reload |
| `make kafka-ui` / `make kafka-ui-stop` | Start or stop the read-only Kafka web viewer (`http://127.0.0.1:8080`) |
| `make consume` | Run the consumer from your machine |
| `make reconcile` | Check every Kafka message has a database row, and the tables' invariants |
| `make check-db-outage` | Stop PostgreSQL mid-stream and check nothing is lost or duplicated (disruptive) |
| `make coverage` | Unit tests with a coverage report |

## Ingestion

The poller (`src/earth_hub/ingestion/`) fetches the USGS `all_day` feed and publishes each new or revised event to `earthquakes.raw`, keyed by the USGS event ID.

- **Timing:** the feed is regenerated every minute. The poller sends `If-Modified-Since` and waits until just after the feed's `Expires` time (plus 2–5 s of jitter, between 10 s and 5 min), falling back to `POLL_INTERVAL_SECONDS` when the header is missing. Expect events in Kafka 1–2 minutes after USGS publishes them.
- **Only changes are sent:** an event is published when it is new or its `updated` timestamp has moved forward, and counts as sent only once Kafka confirms delivery. Failed deliveries are retried next round.
- **Restarts:** on startup the poller rebuilds what it has sent from the last 26 hours of `earthquakes.raw`, so a restart doesn't resend the feed.
- **Errors:** timeouts, connection errors, `429` and `5xx` are retried up to 3 times with backoff; other `4xx` are not. A failed round sends nothing and is retried at the fallback interval.
- **Message format:** each value is a JSON envelope (`schema_version`, `event_id`, `ids`, `source_updated_ms`, `event_time_ms`, `source`, `feed_generated_ms`, `fetched_at`) around the unchanged USGS feature in `payload`. Only features without a usable `id`, `updated` or `time` are rejected; data-quality rules are applied by the consumer (Milestone 3).
- **Not yet covered:** revisions to events older than 24 hours, and outages longer than the feed window. A catch-up query against the USGS FDSN API is planned for a later milestone.

`make up` never starts the poller, so tests don't depend on live data. Its container is healthy while successful rounds keep happening (heartbeat within 3 poll intervals).

## Persistence

The consumer (`src/earth_hub/consumers/`) reads `earthquakes.raw` and stores each message in PostgreSQL:

| Table | Holds |
| --- | --- |
| `earthquakes` | One row per physical event: normalized time, location, magnitude, status and source fields, plus `revision_count` |
| `earthquake_ids` | Every USGS ID ever seen, mapped to its event. When USGS changes an event's preferred ID, or two reports turn out to be one quake, the IDs stay linked |
| `raw_events` | Every Kafka message with its topic, partition and offset, the full message, and its outcome: `applied`, `duplicate`, `stale`, `rejected` (with the rule broken) or `error` |

Each message is handled in one transaction:
1. Record it in `raw_events`. A redelivered offset is recognized and skipped.
2. Validate it against the [data-quality rules](docs/data-quality-rules.md).
3. Find its event through any of its IDs, merging records that turn out to be the same quake.
4. Update the event only if the message's `updated` time is newer.

The Kafka offset is committed only after the database commit, so delivery is at-least-once and redelivery is harmless.

- **Database outage:** the consumer retries the same message with backoff (up to 30 s) until PostgreSQL is back, and never rejects a message because of it. Its health check fails meanwhile.
- **Unexpected errors:** a message that fails 3 times for a non-database reason is stored with `outcome = 'error'` and the exception, so one bad message can't stall a partition.
- **Several consumers:** PostgreSQL advisory locks on each event ID stop two consumers from creating or merging the same event at once.

`make reconcile` compares the topic with `raw_events`. A missing row below the consumer group's committed offset counts as **lost**; at or above it, as **pending** (lag). It also checks five invariants, such as every preferred ID mapping to its own event.

## REST API

A read-only FastAPI app (`src/earth_hub/api/`) over PostgreSQL. It never reads from Kafka or USGS. Interactive docs are at `/docs` and the OpenAPI schema at `/openapi.json`.

| Endpoint | Returns |
| --- | --- |
| `GET /api/v1/health` | Database status, newest event time, version. `503` when the database is down |
| `GET /api/v1/earthquakes` | A page of events: `{items, total, limit, offset}` |
| `GET /api/v1/earthquakes/{event_id}` | One event. Former USGS IDs work too, and return the event under its current ID |
| `GET /api/v1/earthquakes/stats` | Count, min/max/average magnitude, counts by event type and by magnitude type |
| `GET /api/v1/earthquakes/summary/hourly` | Counts per UTC hour, empty hours included (24 h by default, 31 days at most) |
| `GET /api/v1/earthquakes/summary/magnitude` | Counts for `<0`, `0-1`, … `6-7`, `>=7` and `unknown`, empty ranges included |

**Filters** (list, stats and summaries):
- `min_magnitude` and `max_magnitude` (−2 to 10.5)
- `start_time` (inclusive) and `end_time` (exclusive): ISO 8601 date-times, UTC assumed without a timezone, years 1900–2099
- `place`: a case-insensitive substring, where `%` and `_` match literally
- `event_type`, `review_status`, and `include_deleted` (deleted events are hidden by default)

**List only:**
- `limit`: 1–500, default 20
- `offset`: 0–10,000,000
- `sort`: `event_time`, `magnitude` or `depth`
- `order`: `asc` or `desc` (default `desc`)

Missing magnitudes sort last, and ties break by storage order, so pages are stable.

**Errors** always look like `{"error": {"code", "message", "details"}}`:

| Code | Status |
| --- | --- |
| `invalid_parameters` | `422`, listing every problem |
| `not_found` | `404` |
| `method_not_allowed` | `405`, with an `Allow` header |
| `database_unavailable` | `503` |
| `internal_error` | `500`, with no internals |

```bash
curl -s 'http://127.0.0.1:8000/api/v1/earthquakes?min_magnitude=4&sort=magnitude&limit=5'
curl -s 'http://127.0.0.1:8000/api/v1/earthquakes/summary/hourly?start_time=2026-10-16T00:00:00Z'
```

## Tests and QA

Run everything with **`make regression`**: lint, unit, integration, API, contract, end-to-end and disruptive tests, in that order, stopping at the first failing layer. It takes about 2 minutes and writes:
- `reports/regression-summary.md`
- JUnit XML for each layer
- a coverage report covering all layers (`reports/coverage-html/index.html`)
- the [traceability matrix](docs/traceability-matrix.md)

| Layer | Location | What it covers |
| --- | --- | --- |
| Unit | `tests/unit/` | Parsing, the 16 validation rules at their boundaries, retries and timing, poller state, consumer control flow, API validation and errors. **No Docker, no network:** a guard blocks it and the live USGS feed |
| Integration | `tests/integration/` | Real Kafka and PostgreSQL on temporary topics and databases: smoke tests, migrations, redelivery, revisions, ID changes and merges, concurrency, constraints, reconciliation |
| API | `tests/api/` | Every endpoint over a fixed set of 20 events stored through the real consumer code; statistics and summaries checked against separate SQL |
| Contract | `tests/api/test_contract.py` | Schemathesis generates requests from the OpenAPI schema (`CONTRACT_EXAMPLES=1000` for a deeper run) |
| End-to-end | `tests/e2e/` | Mock USGS feed → real poller → Kafka → real consumer → PostgreSQL → API, checked only through the API. These are the plan's scenarios: new, duplicate, revised, invalid, merged and deleted events, consumer and poller restarts, an empty database, invalid requests, and a reconciled batch of 55 messages |
| Disruptive | `tests/disruptive/` | **Stops and starts the shared containers:** PostgreSQL stopped while events arrive, a Kafka restart, data surviving a restart of both. Runs only with `--run-disruptive` |

Other test commands:
- `make test-e2e`, `make test-disruptive`, and `make test-all` (every non-disruptive test in one process)
- `make flake-check` runs the end-to-end suite 5 times in a row
- `make smoke-restart` and `make check-db-outage` run single disruptive tests

**QA documents** in `docs/`:
- [test strategy](docs/test-strategy.md) and [test plan](docs/test-plan.md)
- [requirements](docs/requirements.md) (74, each with an ID) and the generated [traceability matrix](docs/traceability-matrix.md)
- the [defect log](docs/defects/README.md): 20 defects found and fixed, 6 with full reports
- the [data-quality rules](docs/data-quality-rules.md)

Tests declare what they verify with `@pytest.mark.req("REQ-...")`. `make traceability` fails if a requirement has no passing test, and a marker naming an unknown requirement fails at collection.

## Configuration

Settings come from environment variables or `.env` (see `.env.example`) and are validated at startup by `src/earth_hub/config.py`. Values in `.env` are for code running on your machine; containers get their in-network host names from `docker-compose.yml`. `.env` is git-ignored and must never be committed.

## Troubleshooting

- **Port already in use:** change `POSTGRES_PORT` in `.env`, or stop whatever is using 29092.
- **Kafka connection errors mentioning `::1`:** use `127.0.0.1:29092`, not `localhost:29092`; the port is published on IPv4 only.
- **Kafka slow to become healthy:** the first start takes 15–30 seconds; `make up` waits for it.
- **Start over with empty data:** `make reset && make up && make migrate`.
- **`make reconcile` reports lost messages:** a message was acknowledged without a `raw_events` row. Check the consumer logs for that partition and offset; resetting the consumer group's offset replays it safely, because redelivery is idempotent.

## Data source

Earthquake data comes from the [USGS Earthquake Hazards Program](https://earthquake.usgs.gov/), which is in the public domain.
