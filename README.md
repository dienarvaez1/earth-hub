# Earth Hub

Local-first earthquake data platform: the USGS earthquake feed flows through Apache Kafka into PostgreSQL and out through a REST API, with data quality and testing treated as first-class concerns. See [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for the full plan.

**Status:** Milestone 2 (USGS ingestion). The poller publishes new and revised USGS events to Kafka; the consumer and database tables arrive in Milestone 3.

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

## Getting started

```bash
cp .env.example .env     # then change POSTGRES_PASSWORD
make install             # create .venv from uv.lock
make up                  # start Kafka and PostgreSQL, create the earthquakes.raw topic
make migrate             # apply database migrations
make smoke               # check Kafka and PostgreSQL work end to end
make ingest              # start the poller against the live USGS feed
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
| `make test` | Unit tests; no Docker needed |
| `make test-integration` / `make smoke` | Integration tests against the running services |
| `make smoke-restart` | Restart Kafka and PostgreSQL and check data survives |
| `make migrate` | Apply database migrations with Alembic |
| `make ingest` | Build and start the poller container (live USGS feed) |
| `make ingest-once` | Poll the live feed once from your machine and exit |
| `make ingest-logs` / `make ingest-stop` | Follow or stop the poller |
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

## Tests

- **Unit tests** (`tests/unit/`) run without Docker or network access, and never call the live USGS feed. Feed data comes from `tests/fixtures/usgs/all_day_sample.geojson`, six real events chosen for edge cases (several IDs, negative magnitude and depth, an explosion), and HTTP is mocked with httpx's `MockTransport`.
- **Integration tests** (`tests/integration/`, marked `integration`) need `make up`. The smoke tests:
  - send a message to a temporary Kafka topic and read it back,
  - check that `earthquakes.raw` exists with 3 partitions,
  - create, insert and query a PostgreSQL table inside a rolled-back transaction,
  - run all migrations against a brand-new, throwaway database.
- **Ingestion integration tests** run the poller against real Kafka on temporary topics with a mocked feed: keyed envelopes, stable partitioning per event, and a restart that rebuilds state from the topic and resends only real revisions.
- **Restart check** (`make smoke-restart`) writes a token to PostgreSQL and Kafka, restarts both, confirms the token is still there, then reruns the smoke tests.

## Configuration

Settings come from environment variables or `.env` (see `.env.example`) and are validated at startup by `src/earth_hub/config.py`. Values in `.env` are for code running on your machine; containers get their in-network host names from `docker-compose.yml`. `.env` is git-ignored and must never be committed.

## Troubleshooting

- **Port already in use:** change `POSTGRES_PORT` in `.env`, or stop whatever is using 29092.
- **Kafka connection errors mentioning `::1`:** use `127.0.0.1:29092`, not `localhost:29092`; the port is published on IPv4 only.
- **Kafka slow to become healthy:** the first start takes 15–30 seconds; `make up` waits for it.
- **Start over with empty data:** `make reset && make up && make migrate`.

## Data source

Earthquake data comes from the [USGS Earthquake Hazards Program](https://earthquake.usgs.gov/), which is in the public domain.
