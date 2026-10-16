# Earth Hub

Local-first earthquake data platform: the USGS earthquake feed flows through Apache Kafka into PostgreSQL and out through a REST API, with data quality and testing treated as first-class concerns. See [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for the full plan.

**Status:** Milestone 1 (foundation and infrastructure). Kafka, PostgreSQL, configuration, migrations and smoke tests are in place; ingestion starts in Milestone 2.

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
| Kafka (KRaft, single node) | `apache/kafka:4.2.2` | `localhost:29092` |
| PostgreSQL | `postgres:18.6` | `localhost:5433` |

PostgreSQL uses host port 5433 because 5432 is often taken by a local PostgreSQL. Both ports are bound to `127.0.0.1` only.

## Getting started

```bash
cp .env.example .env     # then change POSTGRES_PASSWORD
make install             # create .venv from uv.lock
make up                  # start Kafka and PostgreSQL, create the earthquakes.raw topic
make migrate             # apply database migrations
make smoke               # check Kafka and PostgreSQL work end to end
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
| `make coverage` | Unit tests with a coverage report |

## Tests

- **Unit tests** (`tests/unit/`) run without Docker or network access, and never call the live USGS feed.
- **Integration tests** (`tests/integration/`, marked `integration`) need `make up`. The smoke tests:
  - send a message to a temporary Kafka topic and read it back,
  - check that `earthquakes.raw` exists with 3 partitions,
  - create, insert and query a PostgreSQL table inside a rolled-back transaction,
  - run all migrations against a brand-new, throwaway database.
- **Restart check** (`make smoke-restart`) writes a token to PostgreSQL and Kafka, restarts both, confirms the token is still there, then reruns the smoke tests.

## Configuration

Settings come from environment variables or `.env` (see `.env.example`) and are validated at startup by `src/earth_hub/config.py`. Values in `.env` are for code running on your machine; containers get their in-network host names from `docker-compose.yml`. `.env` is git-ignored and must never be committed.

## Troubleshooting

- **Port already in use:** change `POSTGRES_PORT` in `.env`, or stop whatever is using 29092.
- **Kafka slow to become healthy:** the first start takes 15–30 seconds; `make up` waits for it.
- **Start over with empty data:** `make reset && make up && make migrate`.

## Data source

Earthquake data comes from the [USGS Earthquake Hazards Program](https://earthquake.usgs.gov/), which is in the public domain.
