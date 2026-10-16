# Earth-Hub — Complete Project Implementation Plan

Project identity · Architecture · Milestones · QA strategy · Environment setup

Earth-Hub is a local-first, open-source data platform that ingests earthquake data from the USGS, processes events through Apache Kafka, validates and stores them in PostgreSQL, and exposes the resulting data through its own REST API.

The project is designed to showcase your experience in software quality assurance, test automation, database testing, ETL validation, API testing, and AI-assisted engineering with Claude Code.

## 1. Project vision

**Earth-Hub**
Earthquake Data & Quality Intelligence Platform
*Ingest. Validate. Persist. Analyze. Expose.*

### Mission

Build a reliable, testable data platform that transforms public earthquake data into validated, queryable information through a documented API, while making data quality and software testing first-class engineering concerns.

### Core objectives

- Ingest public earthquake data from the USGS GeoJSON feed.
- Use Kafka to transport events between ingestion and processing services.
- Validate event quality and preserve source traceability.
- Store normalized data and processing records in PostgreSQL.
- Develop a versioned REST API backed by PostgreSQL.
- Automate unit, integration, API, regression, and end-to-end tests.
- Demonstrate observability, analytics, and performance testing.
- Use Claude Code to assist development and QA while reviewing its work critically.
- Run the complete system locally without paid cloud infrastructure.

## 2. Target architecture

```
USGS Earthquake Feed
  External GeoJSON API · Periodic polling
        │
        ▼
Python Ingestion Service
  Fetch · Parse · Identify revisions
        │
        ▼
Apache Kafka
  Event topic: earthquakes.raw
        │
        ▼
Consumer + Data Quality Engine
  Validation · Deduplication · Error handling
        │
        ▼
PostgreSQL
  Earthquake records · Raw payloads · Audit history · Analytics
        │
        ▼
Earth-Hub REST API
  FastAPI · Filtering · Pagination · Statistics · OpenAPI
```

Cross-cutting capabilities: automated QA · logging · metrics · CI/CD · documentation

The source is a periodically polled feed, not a push-based stream. Earth-Hub must handle repeated events, source revisions, failures, and recovery. Kafka provides event transport; PostgreSQL is the source of truth for API responses.

### 2.1 Source data risks to resolve before coding

These three behaviors of the USGS data shape the data model and the Kafka message key, which are expensive to change later. Design for them from Milestone 2 onward.

**Risk 1: The feed window is too short to catch revisions or survive downtime.**
USGS revises events hours or days after they first appear (magnitude, location, review status). An event leaves the `all_hour` feed after 60 minutes, so later revisions would never be seen, and a poller outage longer than an hour would lose events permanently.

- Poll the `all_day` feed for normal operation.
- Add a catch-up job that queries the USGS FDSN event API with `updatedafter=<last_seen>` after downtime or on a schedule, to pick up older revisions. *Deferred from Milestone 2 to a later milestone; until then, revisions to events older than 24 hours and outages longer than the feed window are not covered.*
- Record the last successful poll time so the catch-up window is known.

**Risk 2: An event's ID can change when networks merge their reports.**
When several seismic networks report the same quake, USGS associates the reports and may change the preferred event ID (for example from `ak0123…` to `us7000…`). The previous IDs are listed in the feed's `ids` property. Keying Kafka messages and database upserts on the current ID alone would create duplicate records for one physical event.

- Store the full `ids` list for every event, for example in an `earthquake_ids` alias table mapping every known ID to one internal event.
- On each message, look up all of its `ids`. If any is already known, update that event and record the new preferred ID; otherwise create a new event.
- Keep the Kafka message key as the preferred ID at publish time, and do the merge in the consumer.

**Risk 3: Revisions can arrive out of order.**
Kafka redelivery, consumer restarts, and catch-up queries can all deliver an older revision after a newer one. An unconditional upsert would let the older data overwrite the newer.

- Apply a revision only if it is newer: `INSERT ... ON CONFLICT ... DO UPDATE ... WHERE excluded.updated > earthquakes.updated`.
- Record skipped stale revisions in the ingestion audit table rather than discarding them silently.

**Tests required by these risks (Milestones 2, 3 and 5):**

| Scenario | Expected result |
| --- | --- |
| Event revised after leaving the hourly window | Revision is captured from `all_day` or the catch-up query |
| Poller down longer than the feed window | Catch-up publishes the missed events and revisions |
| Preferred ID changes between revisions | One event record; the old ID still resolves to it |
| Two reports later merged under one ID | Existing records are merged, with no duplicate in API results |
| Older revision delivered after a newer one | Stored record keeps the newer data; skip is audited |
| Same revision delivered twice | No change and no duplicate |

## 3. Prerequisites: tools and environment setup

Set up the development environment before implementing the application. The objective is to keep installation repeatable and avoid dependencies on paid services.

### 3.1 Required tools

- **Docker Desktop and Docker Compose** — Run Kafka, PostgreSQL, and Earth-Hub services in containers. This avoids needing separate native installations of Kafka and PostgreSQL. *(Reference: Docker Desktop documentation)*
- **Python 3.12+** — Implement ingestion, Kafka integration, validation, database access, API endpoints, and automated tests. Pin the actual version and dependencies used by the project. *(Reference: Python downloads)*
- **Apache Kafka in KRaft mode** — Use a local Kafka setup without requiring ZooKeeper. Configure topics and health checks in Docker Compose. *(Reference: Kafka quickstart)*
- **PostgreSQL and a database client** — Run PostgreSQL in Docker. Use psql or a GUI client to inspect schemas, query data, and validate persistence. *(Reference: PostgreSQL downloads)*
- **Git and GitHub** — Manage incremental changes, issues, pull requests, test evidence, and CI workflows. *(Reference: Git downloads)*
- **Claude Code and a code editor** — Use Claude Code to inspect the repository, plan implementation, generate code and tests, and review defects. Inspect diffs and run the tests yourself before accepting changes. *(Reference: Claude Code documentation)*

### 3.2 Recommended software stack

| Concern | Technology |
| --- | --- |
| Ingestion and processing | Python, HTTPX |
| Event transport | Apache Kafka |
| Data storage | PostgreSQL |
| Database migrations | SQLAlchemy, Alembic |
| REST API | FastAPI, Pydantic, Uvicorn |
| Unit and integration testing | pytest, HTTPX |
| Code quality | Ruff, optional mypy |
| API contract testing | OpenAPI, optional Schemathesis |
| Performance testing | Locust or k6 |
| Test coverage | pytest-cov |
| CI automation | GitHub Actions |
| Local orchestration | Docker Compose |

Start with Python validation rules and SQL assertions. Add a dedicated data-quality framework later if it provides value beyond the custom rules.

### 3.3 Verify the local environment

Run the following commands in your terminal:

```bash
git --version
python3 --version
docker --version
docker compose version
claude --version
```

Before starting development:

- [ ] Docker Desktop is installed and running.
- [ ] Git, Python, and Claude Code are accessible from the terminal.
- [ ] A GitHub repository named earth-hub is created or ready to be created.
- [ ] The repository contains a `.gitignore` and `.env.example`.
- [ ] Local configuration does not expose credentials or secrets.
- [ ] Docker can run a test container.
- [ ] The USGS feed is reachable from your development environment.
- [ ] Tool and dependency versions are documented.

### 3.4 Configure the environment

Create `.env.example` with the required settings and use an untracked `.env` for local values.

```env
APP_NAME=Earth-Hub
APP_ENV=development
API_PORT=8000

POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=earth_hub
POSTGRES_USER=earth_hub_app
POSTGRES_PASSWORD=change_me_locally

KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:29092
KAFKA_TOPIC_RAW=earthquakes.raw

USGS_FEED_URL=https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson
POLL_INTERVAL_SECONDS=60
```

`config.py` builds the database URL from the `POSTGRES_*` parts so the password is defined only once. The `all_day` feed is used instead of `all_hour` to catch revisions (see section 2.1).

These values are a template for code running on your machine, which connects through the localhost ports published by Docker Compose (5433 for PostgreSQL, since 5432 is often taken by a local install, and 29092 for Kafka). Containers use the in-network service names (`postgres:5432`, `kafka:9092`), which `docker-compose.yml` sets for them. Use appropriate credentials and avoid exposing Kafka or PostgreSQL to public networks.

## 4. Implementation roadmap: seven milestones

| # | Milestone | Scope | Goal |
| --- | --- | --- | --- |
| 01 | Foundation and infrastructure | Repository · Docker Compose · Kafka · PostgreSQL · configuration | Establish a clean, repeatable local environment and smoke-test the infrastructure. |
| 02 | USGS ingestion and Kafka producer | Feed retrieval · parsing · event publishing · retry logic | Turn source data into well-formed, traceable Kafka messages. |
| 03 | Consumer, validation, and persistence | Data rules · PostgreSQL · idempotency · reconciliation | Ensure data is stored correctly and recoverably. |
| 04 | Earth-Hub REST API | FastAPI · database access · filters · analytics endpoints | Expose validated, persisted data through a documented API. |
| 05 | End-to-end QA and regression | Unit · integration · API contract · negative · E2E | Prove that data moves correctly from the source through Kafka and PostgreSQL to the API. |
| 06 | Analytics, observability, and performance | SQL reporting · structured logs · metrics · load tests | Measure system correctness and performance with reproducible evidence. |
| 07 | CI/CD and portfolio release | GitHub Actions · documentation · test reports · demo | Make Earth-Hub easy to run, test, understand, and evaluate. |

### Milestone 1 — Foundation and infrastructure

**Implementation**

- Create the earth-hub repository and Python package.
- Configure dependency management and linting.
- Set up Kafka in KRaft mode and PostgreSQL through Docker Compose.
- Add service health checks, persistent local storage, and a migration strategy.
- Create the `earthquakes.raw` topic.
- Add logging, environment configuration, and a smoke-test command.

**QA work**

- Verify service readiness and connectivity.
- Publish and consume a sample Kafka message.
- Create, insert, and query a PostgreSQL test record.
- Test startup and recovery after a service restart.
- Validate configuration and secret handling.

**Exit gate:** A clean checkout can start the infrastructure, perform basic Kafka and database operations, and pass documented smoke tests.

### Milestone 2 — USGS ingestion and Kafka producer

**Implementation**

- Fetch the USGS feed at a configurable interval.
- Parse GeoJSON features into a defined event contract.
- Preserve the USGS event ID, event time, update timestamp, and original payload.
- Publish events to Kafka using the event ID as the message key.
- Implement HTTP timeouts, bounded retries, backoff, and structured logging.
- Handle repeated records and revised source events.

**QA work**

- Test valid and malformed GeoJSON.
- Test missing fields, optional values, invalid coordinates, and timestamps.
- Simulate HTTP timeouts, server errors, and empty feeds.
- Verify Kafka message keys and payload serialization.
- Keep unit tests independent of the live USGS service.

**Exit gate:** Valid fixture data can be reliably published to Kafka, and source or network errors are handled predictably.

#### Poller service design

The USGS GeoJSON feed is pull-only: there is no webhook or stream to subscribe to. (USGS does offer a push system, the Product Distribution Layer (PDL), but it requires a Java client and its own product formats, which is out of scope for this project.) The feed itself is regenerated about once a minute, as its response headers show:

```
cache-control: public, max-age=60
last-modified: <time of the last regeneration>
```

A well-built poller is therefore effectively real-time; polling more often would not deliver data sooner. The poller runs as a long-lived Docker Compose service rather than a script started by hand:

1. **Loop forever.** On each round, fetch the feed with an `If-Modified-Since` header. If USGS answers `304 Not Modified`, skip the round. The feed sends no ETag, but `Last-Modified` works for this.
2. **Time requests to the cache.** Wait until the `Expires` time from the last response, plus a few seconds of jitter, then fetch again. This picks up each new version almost as soon as USGS publishes it, without wasted requests.
3. **Publish only what changed.** Keep each event's last-seen `updated` timestamp. Send a Kafka message only when an event is new or its `updated` timestamp has moved forward. This also keeps raw-payload and audit storage from filling with near-duplicates.
4. **Remember state across restarts.** After a restart the service must neither resend everything nor miss events. *Implemented in Milestone 2:* on startup the poller reads the last 26 hours of `earthquakes.raw` and rebuilds the newest `updated` timestamp it published for each event, so the topic itself is the state and no separate store is needed. A catch-up query against the USGS FDSN event API (`fdsnws/event/1/query?updatedafter=<last_seen>`) after downtime is deferred to a later milestone.
5. **Run reliably.** Use `restart: unless-stopped` in Docker Compose and a health check based on the time of the last successful poll. On `SIGTERM`, flush pending Kafka messages before exiting so nothing is lost on shutdown.

Sketch of the main loop:

```python
while not shutdown:
    resp = client.get(FEED_URL, headers={"If-Modified-Since": last_modified})
    if resp.status_code == 200:
        last_modified = resp.headers["Last-Modified"]
        for feature in resp.json()["features"]:
            eid, updated = feature["id"], feature["properties"]["updated"]
            if seen.get(eid, -1) < updated:
                producer.produce(TOPIC, key=eid, value=serialize(feature))
                seen[eid] = updated
        producer.flush()
    sleep_until(expires_from(resp) + jitter)
```

Implications for this milestone:

- `POLL_INTERVAL_SECONDS` becomes a fallback. The actual wait comes from the feed's cache headers.
- Additional QA scenarios, all using mocked HTTP responses so unit tests never touch the live feed:
  - `304 Not Modified` responses are skipped without publishing.
  - Unchanged events are not republished.
  - A restart neither resends already-published events nor skips new ones.
  - Graceful shutdown flushes pending messages without loss.
  - Catch-up after downtime publishes the events that were missed *(deferred with the catch-up job)*.
- Expected latency is about 1–2 minutes from USGS publication to Kafka. Document it as near-real-time, not real-time.

### Milestone 3 — Consumer, validation, and PostgreSQL

**Implementation**

- Define normalized earthquake tables and ingestion audit records.
- Create schema migrations and indexes.
- Implement consumer-side validation rules.
- Use PostgreSQL upserts to prevent duplicate normalized records.
- Update existing events only when a newer source revision arrives, and resolve merged event IDs through the alias table (see section 2.1).
- Record invalid messages and implement a documented retry or quarantine strategy.
- Commit database transactions before acknowledging Kafka offsets.

**QA work**

- Test database inserts, updates, constraints, and migrations.
- Test duplicate message delivery and source revisions.
- Simulate database outages and consumer restarts.
- Reconcile controlled input batches with persisted records.
- Verify that optional source fields are handled according to the data contract.

**Exit gate:** Repeated deliveries do not create duplicate normalized records, revisions update correctly, and processing failures are observable and recoverable.

### Milestone 4 — Earth-Hub REST API

**Implementation**

Create a versioned FastAPI application with the following initial endpoints:

| Endpoint | Function |
| --- | --- |
| `GET /api/v1/health` | API and database health |
| `GET /api/v1/earthquakes` | Paginated, filterable event listing |
| `GET /api/v1/earthquakes/{event_id}` | Retrieve one event |
| `GET /api/v1/earthquakes/stats` | Aggregate earthquake statistics |
| `GET /api/v1/earthquakes/summary/hourly` | Event counts by hour |
| `GET /api/v1/earthquakes/summary/magnitude` | Event counts by magnitude range |

Support parameters such as `min_magnitude`, `start_time`, `end_time`, `place`, `limit`, `offset`, `sort`, and `order`. Validate them explicitly.

Keep the API read-only in the initial release. It should query PostgreSQL, not the USGS feed or Kafka directly.

**QA work**

- Verify response schemas, status codes, and error formats.
- Test pagination, sorting, filtering, and time boundaries.
- Test unknown IDs, invalid parameters, empty results, and database failures.
- Verify aggregates against SQL queries over known data.
- Check parameterized queries and document the API using OpenAPI.

**Exit gate:** All endpoints return predictable, documented results backed by PostgreSQL, and automated tests cover normal and error behavior.

### Milestone 5 — End-to-end QA and regression automation

**Implementation**

- Build deterministic synthetic earthquake fixtures.
- Separate unit tests from infrastructure-dependent integration tests.
- Create reusable database and API test fixtures.
- Add complete-path tests from Kafka message publication to API retrieval.
- Produce test and coverage reports.
- Create a documented command to run the regression suite.

**Required scenarios**

| Scenario | Expected result |
| --- | --- |
| New valid event | Persisted and retrievable through the API |
| Duplicate delivery | No duplicate normalized record |
| Revised event | Existing record reflects the latest revision |
| Invalid event | Rejected or quarantined with an auditable reason |
| Database outage | Safe retry/recovery without silent loss |
| Consumer restart | Redelivered events handled safely |
| Empty database | Valid empty results and zero-count summaries |
| Invalid API request | Consistent client error |
| Batch processing | Input records reconciled with processing outcomes |

**QA deliverables:** Test strategy, test plan, traceability matrix, regression suite, coverage report, and sample defect reports.

**Exit gate:** Critical end-to-end paths pass reproducibly, and test results do not depend on live earthquakes occurring during the run.

### Milestone 6 — Analytics, observability, and performance

**Implementation**

- Add SQL reports for hourly counts, magnitude distribution, geographic summaries, and recent activity.
- Record ingestion counts, validation failures, consumer lag, and persistence errors.
- Add structured logs linked by event ID and processing stage.
- Load-test common API endpoints using Locust or k6.
- Test query behavior at increasing synthetic data volumes.
- Tune indexes based on observed query plans and measurements.

**QA work**

- Verify analytics against deterministic datasets.
- Measure response latency, throughput, and error rates.
- Test pagination and filters against larger datasets.
- Introduce a controlled failure and demonstrate diagnosis using logs and metrics.

**Exit gate:** Analytics are correct, performance results are repeatable, and bottlenecks can be investigated with evidence.

### Milestone 7 — CI/CD, documentation, and portfolio release

**Implementation**

- Add GitHub Actions for linting, unit tests, and selected integration tests.
- Publish test and coverage reports as workflow artifacts.
- Document the architecture, schema, API contracts, setup, and recovery behavior.
- Add example requests, responses, and SQL queries.
- Document test execution, known limitations, and troubleshooting.
- Prepare a short end-to-end demo.

**QA deliverables:** CI workflow, README, architecture diagrams, test documentation, traceability matrix, quality reports, and demo instructions.

**Exit gate:** A reviewer can clone Earth-Hub, start it, run tests, inspect persisted records, query the API, and understand the quality controls.

## 5. Recommended repository structure

```
earth-hub/
├── src/
│   └── earth_hub/
│       ├── ingestion/
│       │   ├── usgs_client.py
│       │   └── producer.py
│       ├── consumers/
│       │   └── earthquake_consumer.py
│       ├── validation/
│       │   └── earthquake_rules.py
│       ├── database/
│       │   ├── models.py
│       │   ├── repository.py
│       │   └── migrations/
│       ├── api/
│       │   ├── main.py
│       │   ├── routes.py
│       │   ├── schemas.py
│       │   └── services.py
│       └── config.py
├── sql/
│   └── analytics.sql
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── api/
│   ├── e2e/
│   ├── regression/
│   ├── performance/
│   └── fixtures/
├── docs/
│   ├── architecture.md
│   ├── test-strategy.md
│   ├── test-plan.md
│   ├── traceability-matrix.md
│   └── data-quality-rules.md
├── scripts/
│   ├── seed_test_events.py
│   └── reconcile_events.py
├── .github/
│   └── workflows/
│       └── ci.yml
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── .env.example
├── .gitignore
├── CLAUDE.md
└── README.md
```

## 6. Definition of done for each milestone

Use this checklist before moving to the next phase.

**Earth-Hub milestone gate**

- [ ] Implementation is reviewed against acceptance criteria.
- [ ] Relevant automated tests pass, including negative cases.
- [ ] Regression checks show no unintended changes.
- [ ] Data integrity and recovery behavior are verified where applicable.
- [ ] Linting and configured static checks pass.
- [ ] Documentation and known limitations are updated.
- [ ] Changes are reviewed and committed in Git.

## 7. Claude Code development workflow

Use Claude Code as an engineering assistant with explicit quality gates, not as an unrestricted code generator.

For every milestone:

1. **Plan:** Inspect the repository and propose a small, reviewable implementation.
2. **Define tests:** Specify acceptance criteria and negative scenarios before coding.
3. **Implement:** Make focused changes without unnecessarily refactoring unrelated modules.
4. **Verify:** Run the actual tests and review their output.
5. **Inspect:** Review the diff, error handling, security, data integrity, and test quality.
6. **Document:** Record decisions, evidence, and known limitations.
7. **Commit:** Save the reviewed result before proceeding.

Add a `CLAUDE.md` that defines project conventions, commands, architecture, and a strict rule against using the live USGS feed in unit tests.

### Initial prompt for Claude Code

> **Earth-Hub — Project Initialization**
>
> Act as a senior software engineer and QA automation architect helping me implement Earth-Hub, a local-first, open-source earthquake data platform.
>
> **Mission**
>
> Build a data platform that polls the USGS earthquake GeoJSON feed, publishes events to Apache Kafka, validates and persists them in PostgreSQL, and exposes the stored data through a versioned FastAPI REST API.
>
> The main purpose is to demonstrate software QA engineering, ETL and data-quality testing, event-driven processing, database validation, API automation, and AI-assisted development practices.
>
> **Technology**
>
> - Python
> - Apache Kafka in KRaft mode
> - PostgreSQL
> - FastAPI and Pydantic
> - SQLAlchemy and Alembic
> - pytest and HTTPX
> - Docker Compose
> - Ruff and optional mypy
> - GitHub Actions
> - Locust or k6 for performance testing
>
> Avoid paid cloud services. Pin compatible dependencies and document the development environment.
>
> **Required capabilities**
>
> 1. Poll the USGS GeoJSON feed at a configurable interval.
> 2. Publish traceable events to Kafka using stable event IDs and source update timestamps.
> 3. Validate and consume events before database persistence.
> 4. Persist events idempotently in PostgreSQL and correctly process source revisions.
> 5. Preserve raw payloads or equivalent source traceability and provide reconciliation.
> 6. Expose API endpoints for health, earthquake listings, individual events, statistics, hourly summaries, and magnitude summaries.
> 7. Support pagination, filters, sorting, request validation, consistent error responses, and OpenAPI documentation.
> 8. Provide deterministic synthetic fixtures and repeatable local test environments.
> 9. Automate unit, integration, API contract, negative, regression, and end-to-end testing.
> 10. Include SQL analytics, structured logs, performance measurements, CI, and comprehensive documentation.
>
> **QA requirements**
>
> Prioritize data integrity, repeatability, failure recovery, and API correctness.
>
> Test malformed payloads, missing or optional fields, invalid timestamps and coordinates, duplicate delivery, revised events, HTTP failures, database outages, consumer restarts, pagination boundaries, invalid API parameters, and analytics accuracy.
>
> Never make unit tests depend on live USGS availability. Do not claim end-to-end exactly-once processing without evidence. Use idempotency, transactions, and a documented offset and recovery strategy.
>
> **Implementation milestones**
>
> 1. Foundation and infrastructure.
> 2. USGS ingestion and Kafka producer.
> 3. Consumer, validation, and PostgreSQL persistence.
> 4. Earth-Hub REST API.
> 5. End-to-end QA and regression automation.
> 6. Analytics, observability, and performance.
> 7. CI/CD, documentation, and portfolio release.
>
> **Working method**
>
> Begin by inspecting the current repository and environment. Do not overwrite existing work or make broad changes without approval.
>
> First propose the architecture, dependencies, Docker Compose services, data model, repository structure, risks, and Milestone 1 acceptance criteria.
>
> For each milestone, explain the planned changes and test strategy before implementation. Work incrementally and wait for approval before broad changes or starting a new milestone.
>
> At each milestone's completion, report:
>
> - Files changed.
> - Commands executed and actual test results.
> - Known defects and limitations.
> - Data quality and recovery evidence.
> - Documentation updated.
> - Recommended Git commit message.
>
> Start with the initial inspection and Milestone 1 proposal only. Do not implement the entire project in one pass.

## 8. Final success criteria

Earth-Hub is portfolio-ready when it has:

- A working USGS → Kafka → PostgreSQL → REST API flow.
- Reliable data-quality rules, duplicate handling, and revision management.
- A documented API with automated contract and integration tests.
- Reproducible end-to-end tests and source-to-database reconciliation.
- Performance and observability evidence based on actual runs.
- A CI workflow and clear setup instructions.
- Documentation showing how Claude Code assisted implementation while you retained responsibility for QA decisions and validation.

**Recommended first action:** Create the earth-hub repository, complete the prerequisite checks, and give Claude Code the initialization prompt above. Start with infrastructure and a working smoke test; expand only after that milestone is verified.
