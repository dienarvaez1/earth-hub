# Earth-Hub — Complete Project Implementation Plan

Project identity · Architecture · Milestones · QA strategy · Environment setup

Earth-Hub is a local-first, open-source data platform that ingests earthquake data from the USGS, processes events through Apache Kafka, validates and stores them in PostgreSQL, and exposes the resulting data through its own REST API.

The project is designed to showcase your experience in software quality assurance, test automation, database testing, ETL validation, API testing, and AI-assisted engineering with Claude Code.

## Status

As of 2026-10-30. The plan below is the original, with Milestone 7 revised; the work as built is described in the [architecture](../technical/architecture.md), and what each step found in the [defect log](defects.md).

| Milestone | Status | Merged in |
| --- | --- | --- |
| Setup: plan and project rules | Done | #1 |
| 1. Foundation and infrastructure | Done | #2 |
| 2. USGS ingestion and Kafka producer | Done | #3 |
| 3. Consumer, validation and PostgreSQL | Done | #4 |
| 4. REST API (and the read-only Kafka UI) | Done | `4d2748f`, `620b8c7` |
| 5. End-to-end QA and regression automation | Done | #6 |
| Backfill and catch-up, between 5 and 6 | Done | #7, #8; place-derived location #9; an end after now cut to now (DEF-053) #45 |
| R2 export, before 6 | Done | #10, #11 |
| 6. Analytics dashboard from R2, observability, performance | Done | #12, #13, #14, #15 |
| 7.1 CI, third-party licenses, preview marking | Done | #16 |
| 7.2 Documentation and demo | Done | #18; skills and cheat sheet #19; defect reports to history #20; docs reorganized #21 |
| 7.3 Release, prepared: audit, MIT license, `USGS_CONTACT` setting, preview label removed, the release script | Done | #37, #38 |
| 7.3 Release, the rest: public repository, v1.0.0, branch protection on `main`, `USGS_CONTACT`, R2 secrets in the `r2` environment only (the script's merge and deploy steps removed: DEF-055) | Done, 2026-10-29 (`make release`, run by the author) | #51 |
| 8.1 Persistence decisions in memory (`processing/ledger.py`), `EventStore`, `PostgresStore`; the record key | Done | #22, #23 |
| 8.2 `BucketStore`, the direct runner, parity tests | Done | #24 |
| 8.3 The mode switch, handovers with log replay | Done | #25 |
| Direct mode on GitHub Actions' hourly schedule, after 8.3 | Done | #27; skills and docs for the modes #28 |
| Rounds started by the dashboard Worker's Cloudflare cron trigger (GitHub's schedule skipped runs, DEF-052); `make trigger-token`; an e-mailed reminder before the token expires | Done | #40, #41 |
| Direct mode's audit trail: a record per execution in the bucket, `make direct-runs` | Done | #42 |
| Dashboard: opens on This week per hour; menus that recount; top countries and regions as shares by magnitude, with a note on network density; US- places; events over time as columns, areas, lines or trends | Done | #39 |
| Load test of the API with Locust, after 8 | Done | #26 |
| Reconciliation with USGS, catching up after each switch | Done | #32 |
| One command each to switch modes and to backfill in full mode (`make to-full`, `make to-direct`, `make backfill-full`) | Done | #44 |
| Dashboard map above 25,000 events: cells counted in SQL, the visible area's events once zoomed in (history back to 2024 loaded) | Done | #46 |
| Dashboard: popups with the time to the second and the UTC offset, tsunami flag, closing on a click or resize; significant events under the map, 10, 25 or 50, each opening on the map; zoom 1 to 15; the Countries card naming the chosen country; a Tsunami filter; ranking notes on what a bar is | Done | #47 |
| CI's regression in parallel parts (the dashboard layer in two shards, merged into the required check); the snapshot compressed smaller (zstd level 19: 24.8 MB to 18.1 MB); the popup's depth to one decimal | Done | #48 |
| The check against USGS measures each feed by when USGS generated it, not when a round read it or ended (DEF-054) | Done | #49 |
| Dashboard: a link to the project on GitHub in the footer | Done | #50 |
| No schedule in `direct.yml`: the Worker starts the daily reconcile too, and a watchdog workflow holds the only schedule (DEF-056) | Done | #52 |
| `make trigger-check`: whether the Worker's deployed token works, asked on Cloudflare, starting nothing | Done | #54 |
| The USGS credit and a disclaimer (research purposes, not affiliated with or endorsed by USGS, data that may be late or inaccurate when a part fails, no warranty, not for emergencies) in the dashboard's footer and the README | Done | #57 |
| A privacy notice (`/privacy`, linked from the footer): no cookies, storage, tracking or location; the Worker's invocation logs, which recorded visitors' IP addresses and IP-derived locations, switched off | Done | #58 |
| Code quality: Ruff's complexity (`C90`), security (`S`), simplification (`SIM`) and Pylint (`PL`) rules, with their first findings fixed or marked; CodeQL code scanning | Done | #59 |
| [9. Standby: direct mode takes over from one full-mode machine](#milestone-9--standby-direct-mode-takes-over-from-one-full-mode-machine) | Done, 2026-10-30: steps 1 to 4 (lock fencing, DEF-058; owner and lease; the exporting service and its standby state), steps 5 to 8 (automatic takeover, rebuild from the bucket, `/eh-host`, failover end to end) | #61, #62 |

Current numbers: 199 requirements, all covered by a passing test; 62 defects, all fixed; 1,321 tests in the full regression (2026-10-30), with 91.3% line coverage.

**To refactor** (#59): functions over Ruff's complexity limits when the rules were added, each marked `# noqa: … (to refactor: docs/progress/plan.md)`. New code is held to the limits. Each one is a separate change, with the full regression as its check.

| Function | Over |
| --- | --- |
| `validation/earthquake_rules.py`: `validate`, `_validate_envelope` | complexity, branches, statements |
| `usgs_reconcile.py`: `compare`, `main` | complexity, branches, statements |
| `direct/__main__.py`: `_execute` | complexity, returns |
| `direct/handover.py`: `main`; `direct/store.py`: `BucketStore.write` | complexity |
| `export/r2.py`: `_upload_changes`; `ingestion/fdsn.py`: `_get`; `export/browse.py`: `main` | complexity or returns |
| `scripts/load_test.py`: `main`; `tests/support/streams.py`: `random_stream`; `tests/unit/test_release.py`: `Fake.read` | statements, complexity or returns |

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
- Add a catch-up job that queries the USGS FDSN event API with `updatedafter=<last_seen>` after downtime or on a schedule, to pick up older revisions. *Built in the backfill and catch-up milestone: hourly passes over the CDN-cached `all_month` feed cover 30 days, a daily FDSN query picks up deleted events, and `make backfill` loads any older range (see "Backfill and catch-up" below).*
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
4. **Remember state across restarts.** After a restart the service must neither resend everything nor miss events. *Implemented in Milestone 2:* on startup the poller reads the last 26 hours of `earthquakes.raw` and rebuilds the newest `updated` timestamp it published for each event, so the topic itself is the state and no separate store is needed. Recovery beyond the feed window is handled by the catch-up job (see "Backfill and catch-up").
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
  - Catch-up after downtime publishes the events that were missed *(built in the backfill and catch-up milestone)*.
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

**Decisions made in Milestone 3:**

- **Invalid messages go to a quarantine table, not a dead-letter topic.** `raw_events` records every message with its outcome, so rejected messages sit next to the accepted ones and can be queried and reconciled in one place. The rules are in `docs/data-quality-rules.md`.
- **Unexpected processing errors are retried 3 times**, then stored with `outcome = 'error'` so the partition keeps moving. Database outages are never treated this way: the same message is retried until PostgreSQL is back.
- **Merges keep the oldest internal record**, take the newest data among the merged records, and move every ID and raw row to it.
- **Redelivery is detected by the Kafka offset** (`raw_events` is unique on topic, partition and offset), which makes at-least-once delivery safe without claiming exactly-once.

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

**Decisions made in Milestone 4:**

- **Former IDs return the event (`200`) instead of redirecting.** The body shows the current `event_id` and every former ID in `ids`.
- **Deleted events are hidden by default.** `include_deleted=true` or `review_status=deleted` shows them. A lookup by ID always finds them.
- **Invalid parameters return `422`** in the shared error format, listing every problem.
- **Contract testing uses Schemathesis.** It found 6 defects, each now fixed and covered by its own test:
  - numbers such as `0.5` accepted as times
  - a missing `Allow` header on `405`
  - an hourly default window that went past 31 days
  - NUL characters reaching PostgreSQL
  - dates near year 1 or 9999 overflowing
  - offsets beyond bigint
- **Schemathesis's "valid requests must be accepted" check is turned off.** Rules that compare two parameters can't be expressed in OpenAPI; they're covered by their own tests.

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

**Decisions made in Milestone 5:**

- **Traceability is generated, not written by hand.** Tests carry `@pytest.mark.req` markers that refer to `docs/requirements.md`. `make traceability` builds the matrix from real test outcomes and fails on any requirement without a passing test.
- **Outage and restart checks are pytest tests** in `tests/disruptive/`, replacing the shell scripts. They stop the shared containers for real, so they only run on request (`--run-disruptive`) and as the last layer of `make regression`.
- **The defect log records all 20 defects found so far**, 6 of them as full reports.
- **Reports are JUnit XML plus a Markdown summary**, with no new dependency.

### Backfill and catch-up (between Milestones 5 and 6)

Loads history from the USGS FDSN event service, and recovers what the live poller cannot see.

**Backfill** (`make backfill START=2026-01-01 [END=...]`):

- **One request per calendar week**, Sunday 00:00:00 UTC to the next Sunday. The first window starts at the requested date (2026-01-01, a Thursday), and the current week stays open until it is over. For 2026 up to 17 October that is 42 windows.
- **No events lost at boundaries.** FDSN ignores milliseconds in `starttime` and `endtime` and treats both edges as inclusive at whole seconds (measured against the live service). So every window ends at exactly the instant the next begins. An event exactly on a boundary may be returned twice, which the consumer stores once; "ending at 23:59:59.999" would lose the events in the last second of the week.
- **Completeness:**
  - each window is compared with `/count` using the same parameters, and is never recorded as done if fewer events arrive
  - `limit` is never used
  - a window that comes back short is asked for again, up to 3 times, because an event can move out of a window between the count and the query
- **Split when needed:**
  - a window over the 20,000-event cap is split into days
  - so is a window USGS times out on: some busy recent weeks get a `504` after USGS's 60-second gateway limit, while single days answer in about a second
  - splitting keeps the same boundary rule, so it never loses events
- **Resumable:** finished windows are recorded in `backfill_windows`, and a re-run skips them, continuing inside a week that was partly done in days.
- **Final sweep:** everything USGS added or revised while the backfill ran (`updatedafter=<start of the run>`) across the whole range, for events that arrived late or whose revised time moved them into a week already fetched.
- **Considerate to USGS:**
  - one request at a time, with a 1-second pause between them
  - backoff on `429` and `5xx`, honouring `Retry-After`
  - no retries on a cached `504`
  - an optional contact in the `User-Agent` (`USGS_CONTACT`)

**Catch-up** (the `catchup` service):

- hourly, and on startup: the CDN-cached `all_month` feed, which covers revisions and outages up to 30 days
- daily: FDSN events marked deleted since the previous check

**Both** publish only events that are new or newer than PostgreSQL holds, to their own topic, `earthquakes.backfill`. A separate, batched consumer reads that topic, so live events never queue behind historical ones.

**Batched consumer:**
- **How a batch is stored:** messages are processed 500 at a time, as a set. Three reads, outcomes decided in memory in message order, then a few bulk writes. That is about 7 statements per batch instead of 6 per message.
- **Fallbacks:** a batch that would merge records falls back to message-by-message processing, which is already proven. A batch that fails for another reason is redone one message at a time, so only the bad message becomes an `error`.
- **Locking:** all of a batch's advisory locks are taken up front, in one sorted order.

**Measured:**

| | Result |
| --- | --- |
| Consumer throughput | 166 messages/s one by one, 6,121 messages/s batched (37×) |
| A full year of synthetic history (139,579 events) through the whole pipeline | About 42 s (budget: 3 min) |
| A live event during a 20,000-event backfill | Reached the API in 0.7 s |

### Milestone 6 — Analytics dashboard from R2, observability, and performance

Revised on 2026-10-19: reports are built from the R2 bucket, not PostgreSQL, and shown on a public dashboard hosted on Cloudflare Workers (free tier). API load testing was dropped; observability was kept small.

**Implementation**

- After each export run that succeeded, write the whole table as one Parquet file (`analytics/earthquakes.parquet`) with `analytics/meta.json`, from the rows the run uploaded.
- Write the reports as SQL files (`dashboard/queries/`): events over time, magnitude bins, top countries and regions, significant events, summary numbers, map points, all over one set of filters.
- Serve a static dashboard from a Cloudflare Worker at `*.workers.dev`, public. The Worker reads the snapshot from R2 through a binding (no credentials in the page, bucket stays private) with caching and HTTP Range support. The browser runs the SQL with DuckDB-WASM and shows an interactive map (MapLibre GL) and charts.
- Observability, small: event ID and processing stage on every pipeline log line; periodic count summaries from the poller and consumer; one controlled failure diagnosed from the logs.

**QA work**

- Verify every report against fixed datasets with answers worked by hand, running the same SQL files the page runs.
- Check the snapshot equals the table, every row, column and type.
- Test the Worker locally (`wrangler dev`) and the page in a browser (Playwright through pytest): filters, totals, map points, no console errors, phone width.
- Measure snapshot size, time to first map and time per filter change at 115k and 1M rows, three runs each.

**Exit gate:** The dashboard is live and shows the latest export; its totals match the database; reports pass on fixed datasets; performance results are repeatable within ±15%; the month's Cloudflare bill is $0.

### Milestone 7 — CI/CD, documentation, and portfolio release

Revised on 2026-10-19: the release (license, public repository, branch protection, `USGS_CONTACT`, removing the dashboard's preview marking) and the pre-release audit are deferred to a later step. Until then the repository stays private and the dashboard public, marked as a preview and kept out of search engines. The demo is a written walkthrough. Released on 2026-10-29 as v1.0.0, with `make release` (#37, #51): the repository public, the email in old commits accepted (new commits use the noreply address; [limitations](../overview/limitations.md#testing-and-ci)), the MIT license, `main` protected, direct mode's R2 credentials in an `r2` environment only `main` can use, and the dashboard's preview label removed (#38). Direct mode stays hourly, started from Cloudflare (DEF-052, DEF-056). The repository and the dashboard are not promoted: the dashboard stays out of search engines, and the repository has no description, topics or website.

**Implementation**

1. **CI** (`.github/workflows/ci.yml`, [ci.md](../technical/ci.md)): on every pull request and push to `main`, a quick job (lint, unit tests) and a regression job running `scripts/regression.py --skip performance` from the example settings, with no secrets; reports kept as artifacts and the summary on the run page; actions pinned to commit SHAs, read-only token. Performance stays local: shared runners cannot repeat its timings. Also in this step: the third-party licenses the public page must carry (DEF-035), and the page's preview marking.
2. **Documentation and demo:** architecture diagrams (pipeline, topics and consumer groups, export and dashboard, recovery), the schema, API examples replayed by a test, SQL report examples, known limitations, a README written as the reviewer's entry point (including how Claude Code was used and which QA decisions stayed with the author), and `docs/demo.md`, a walkthrough of about 10 minutes.
3. **Release** (done on the release day, with `make release`): the audit (a secret scan of the whole history, the author emails of three early commits), a LICENSE, the repository made public, branch protection requiring CI, `USGS_CONTACT`, and the preview marking removed.

**QA work**

- Test the workflow file itself: triggers, the commands each job runs, no secrets, pinned actions, read-only token, reports kept even when a run fails.
- Prove the gate: a deliberately failing pull request shows a red check (blocking the merge needs branch protection, at the release).
- The regression job runs the README's quick start on a clean runner.
- Relative links in `docs/` are checked, and documented API examples are replayed against the fixed dataset.

**QA deliverables:** CI workflow, README, architecture diagrams, test documentation, traceability matrix, quality reports, and demo instructions.

**Exit gate:** A reviewer can clone Earth-Hub, start it, run tests, inspect persisted records, query the API, and understand the quality controls; every pull request is checked by CI.

### Milestone 8 — Direct mode, with parity

Approved on 2026-10-20, with the recommended decisions. Earth-Hub runs in either of two modes, chosen by one setting, and both produce **the same R2 bucket and dashboard snapshot** from the same USGS input:

| Mode | Path | Needs |
| --- | --- | --- |
| `full` (the default) | poller → Kafka → consumer → PostgreSQL → API → export → R2 | Docker, Kafka, PostgreSQL |
| `direct` | poller → validate → apply → R2, state kept in the bucket | Python and R2 credentials |

Full mode stays the reference implementation; none of its behavior, tests or evidence changes.

**Implementation**

1. **Decisions apart from storage.** The persistence decisions (record identity, merges, revisions, recorded IDs) move from SQL into a pure module, `processing/ledger.py`, behind an `EventStore` interface; `PostgresStore` loads and writes for it. `process_message` becomes a batch of one, and merges go through the set-based path. Processing times come from the `now` passed in, never the database clock (REQ-PER-018). No other behavior change: the whole suite, `test_batch.py`'s random streams included, must pass unchanged.
2. **`BucketStore` and the direct runner.** Rows `earthquakes/<key>.json`, an ID map (as built: one object, `_state/ids.json.gz`, instead of one per ID, because catch-up asks about every event of the month feed each hour, which would be about 11 million reads a month), an append-only log `log/<date>/<run>.ndjson` (each applied envelope, its sequence, time and outcome: the replacement for `raw_events`), and the poller's 26-hour state; conditional writes, the existing lock. The snapshot is updated, not rebuilt. Catch-up and the deleted-events check in direct mode; a full-year backfill stays a full-mode job. `make reconcile-direct`.
3. **The mode switch.** `EARTH_HUB_MODE=full|direct`; `make direct-once` / `make direct-run`; a mode marker and checkpoint in `_state/mode.json`, so the modes take turns and never write at once. (As built, the marker is the setting: a per-machine `EARTH_HUB_MODE` could disagree with it, which is what the marker exists to prevent. `make mode` shows it; `make mode-direct` and `make mode-full` change it.)
   - **full → direct:** export, then build direct mode's ID map and 26-hour state, then write the checkpoint.
   - **direct → full:** freeze the bucket; replay every log entry after the checkpoint, in order, through `earthquakes.backfill`, so Kafka, `raw_events` and PostgreSQL get the deltas full mode never saw, with their original processing times; wait for lag 0; then `make reconcile`, and an export dry run that must find no differences from the bucket. Only then switch; otherwise abort and stay in direct mode. Fallback without a log: an FDSN backfill of the period, reporting any differences.

**Decisions:** a mode-independent row key (a `key` column, the first USGS ID of the surviving record; a migration and a one-time rename in the bucket); processing times carried by the log, so parity covers every column; direct mode runs on the developer machine first, a scheduled GitHub Actions job only after the release audit decides on R2 secrets in GitHub; the work starts after the documentation reorganization (#21).

**QA work**

- Parity at the store level, in CI: the same random event streams (revisions, duplicates, ID changes, two- and three-way merges, deletions, invalid events) and every defect scenario through `PostgresStore` and `BucketStore` (moto), exported with the same writer: identical buckets and snapshots.
- Parity end to end: the same mock feed and FDSN service through the full pipeline and the direct runner.
- Round trips: full → direct (revisions, merges, deletions) → full must leave PostgreSQL as a run that stayed in full mode would; a gap in the log, or a broken rule in one mode, must fail the check.
- A replay repeated after a crash changes nothing; a budget test keeps direct mode inside R2's free operations.

**Exit gate:** the same input gives the same bucket and snapshot in both modes; a handover in either direction keeps parity; direct mode runs with Docker stopped; full mode's suite passes unchanged; CI enforces it on every pull request.

**Direct mode on a schedule** (approved on 2026-10-20). Direct mode needs a machine running it; to keep the bucket current with none of ours on, `.github/workflows/direct.yml` runs `direct once` every hour and `reconcile-direct` every day, at no cost ([how](../technical/ci.md#direct-mode-on-a-schedule)). This takes the R2-secrets decision early, for this workflow only: a dedicated, revocable R2 token as repository secrets (GitHub Free has no environments for private repositories; one limited to `main` comes with going public); the rest of the release stays deferred. Both commands get `--skip-unless-direct`, so the mode marker pauses and resumes the schedule (REQ-MODE-014); the workflow's catch-up intervals are shorter than its schedule, so GitHub's late starts never skip a pass. Tests: the workflow file (REQ-CI-005), the skip in each mode and during a handover, and the R2 budget at the schedule's rate. Cloudflare was considered: Workers' free plan allows 10 ms of CPU a run (a round takes tens of seconds), and Containers need the $5 paid plan.

### Milestone 9 — Standby: direct mode takes over from one full-mode machine

**Approved and done on 2026-10-30 (#61, #62), with the decisions below.** One machine runs full mode as the bucket's writer, polling USGS every minute; direct mode's rounds on GitHub Actions stay as they are and become its standby. While the machine is alive, they write nothing; when it stops, the next round takes the bucket over and keeps the dashboard current; when the machine returns, it hands back with `make to-full`. Only one machine can run full mode, so there is never a second full-mode writer to coordinate.

The guarantees are those of Milestone 8, kept across an unplanned failure: **one writer at a time** (no conflicts), **one ledger over one state** (no duplicate records), **catch-up from the last exported checkpoints** (no lost event or revision). Idempotent writes alone would not give them: two writers deciding from different states make different decisions (record keys, merges, processing times), and the exporter would delete what the other created.

| | Today | With this milestone |
| --- | --- | --- |
| Who writes the bucket | The mode in `_state/mode.json`, switched by hand | The same, plus an owner and a lease: full mode's machine renews it; a direct round takes over when it has expired |
| Full mode's exports | On demand (`make export-r2`) | After every consumer batch that changed something, by a compose service, renewing the lease |
| Direct mode's state in full mode | Written once, at `make mode-direct` | Written with every export: the standby is always ready |
| A machine that stops | The bucket stays in full mode; rounds skip; the dashboard goes stale | The next round after the lease expires takes over; catch-up fills the gap from USGS |
| The machine's return | `make to-full` (replay, then the dry run must agree) | The same; when its database holds events stored after its last export, it is rebuilt from the bucket first |
| Moving to another host | By hand: `make to-direct` on one, copying Docker volumes, `make to-full` on the other | /eh-host release on the old host, /eh-host claim on the new one: no file transfer, the new host rebuilt from the bucket |

**Implementation**, in this order (each is useful alone), in two pull requests to keep CI runs few (decided on 2026-10-30): steps 1 to 4, which change nothing until a takeover exists (#61), then steps 5 to 8:

1. **Fencing the lock (DEF-058).** Row writes are unconditional: in `BucketState.apply` and between the exporter's lock renewals, a process paused past its lock's expiry (a laptop asleep, a stalled runner) can overwrite rows another writer has since written, before its next conditional write fails. A standby makes that likely. As built: every write confirms the lock first (`lock.Held.confirm`). A lock is never taken over before it expires, so while it is young that is a clock check, with no request; once a third of its time has passed it is renewed (`If-Match`), which fails if another run took it; a lost lock stops the writer at once (`LockLost`). What is left (the writes already in flight when it paused) can leave a row object older than the manifest, never the manifest, snapshot or ID map (their writes are conditional): the scheduled reconcile reads every row object on Sundays (`--deep`; daily would be about 3.6 million reads a month, over REQ-MODE-007's budget) to find it, and `make repair-rows` rewrites mismatched row objects from the snapshot, under the lock. Recorded in the defect log, with a test that takes the lock over between every pair of writes.
2. **Owner and lease in the marker.** `_state/mode.json` gains `owner` (a machine ID, `EARTH_HUB_MACHINE_ID`, new in `.env.example`) and `lease_expires_at`. In full mode, the exporter writes only if the owner is its machine, and renews the lease with `If-Match` (at most every 5 minutes, 20 minutes' TTL). A direct round finding full mode with a live lease is skipped (audit status `skipped`, reason `full mode active`). `make mode` shows the owner and the lease. `make to-full` makes the machine running it the owner. As built: an export finding no owner takes the bucket (decided on 2026-10-30), and `EARTH_HUB_MACHINE_ID` defaults to the hostname.
3. **The standby kept warm.** Every successful export also writes direct mode's state from PostgreSQL and Kafka (the ID map, the poller's 26-hour state, catch-up's checkpoints), the code `make mode-direct` uses today, moved into one function both call. The marker's `checkpoint_seq` follows. **The state is never ahead of the export:** the poller's state and the checkpoints are read at the consumers' committed position before the export reads its rows, so nothing the state calls sent or caught up is missing from the bucket (otherwise, after a takeover, the poller would skip it as unchanged). As built (decided on 2026-10-30): the exporter reads only the API, page by page, so the state is written by the exporting service (step 4), inside a cut: an exclusive advisory lock that every consumer batch takes shared, so no batch commits while the export and the state are written. The poller's state is read below the live consumer's committed offsets; catch-up's checkpoints, which it saves when it publishes, are copied only when the backfill consumer has no lag. The daily reconcile runs in either mode (it skips in full mode today), so the bucket's invariants are checked every day whoever writes it.
4. **The exporting service.** An `exporter` service in `docker-compose.yml`, started by `make ingest`: after each consumer batch that applied something (read from `raw_events`), and at least every 5 minutes to renew the lease, it runs the export. It writes nothing when nothing changed but the lease. When the machine is no longer the owner (a takeover while it was only slow), the service logs an error and stops. As built: at most every 5 minutes (decided on 2026-10-30: each export with changes uploads about 23 MB); it exits 0, which `restart: on-failure` leaves stopped, and it cannot stop the poller and catch-up without the Docker socket, so they keep filling the host's database, which never reaches the bucket and is rebuilt on a claim (step 6).
5. **Automatic takeover.** A direct round finding full mode with an expired lease takes the bucket: it takes the lock, then rewrites the marker with `If-Match` to `direct`, owner `direct`, then runs a normal round on the state of the last export, catch-up included. Two rounds racing: one marker write wins, the other round is skipped. The round's audit record has the status `took-over` and the previous owner; `/eh-status` and `make direct-runs` show it; the run fails after its round (so GitHub e-mails) only if the takeover found an unfinished export (see the decisions). As built: the audit status stays `succeeded`, with a `took_over` field (previous owner, lease, rows rolled back) that `make direct-runs` shows; a bucket an export left unfinished is rolled back to its last complete export first (decided on 2026-10-30: rows and manifest rewritten from the snapshot, catch-up refetching what that removes), and refused only when the rollback cannot make it consistent; a bucket in full mode with no owner (from before this milestone) is never taken over, as no standby state was kept for it.
6. **Rebuilding from the bucket.** `make host-rebuild` (behind /eh-host claim when the dry run disagrees). **It refuses to wipe anything the bucket lacks:** every event in the old database is compared with the bucket by USGS ID and `updated`, and one missing from the bucket or newer in PostgreSQL stops it with the list. It then saves a backup to `reports/backup/<time>/` (`pg_dump`, and every topic's messages as NDJSON with their keys, headers and offsets), checks the dump restores (into a temporary database), shows what will be wiped and where the backup is, and asks for the machine's ID to be typed (`--yes` exists for the tests only; run by the user in a terminal, as `make release` is). Then it wipes Kafka and PostgreSQL, migrates, and loads `earthquakes` from the snapshot and `earthquake_ids` from the ID map, with one `raw_events` row per record under a reserved topic (`bootstrap`) so `make reconcile`'s invariants hold; then `to-full` replays the direct period as now. The dry run must then find nothing to change. As built: the load runs inside the handover to full mode (`make mode-full REBUILD=1`) once the bucket is frozen, so nothing moves while it loads and nothing is left to replay; a load that fails puts the bucket back in direct mode.
7. **Host transitions, packaged.** `make host-status`, `host-release`, `host-claim`, `host-rebuild` and `host-takeover` in `scripts/modes.py`, each with its checks, printed steps, a stop at the first failure and `DRY_RUN=1`, around the commands above; one skill, /eh-host, in the cheat sheet:

   | Command | On | Does |
   | --- | --- | --- |
   | /eh-host status | any host | The bucket's mode, owner and lease; this host's machine ID; whether its database is behind, equal to or ahead of the bucket; the latest takeovers. Read-only |
   | /eh-host release | the owner | Stops the poller and catch-up, waits for the consumers, `make reconcile`, a last export with the warm state, hands the bucket to direct mode (`make mode-direct`), `make down`. Direct rounds keep the dashboard current from then on |
   | /eh-host claim | the new host | Checks Docker, the R2 settings and a unique machine ID; `make up` and `make migrate`; rebuilds from the bucket when the database is empty or differs (the typed confirmation, below); `make to-full`, which makes this host the owner; starts the pipeline with the exporting service; ends with `make reconcile` and `make reconcile-usgs` |
   | /eh-host rebuild | a host | Step 6 on its own: backup, check, typed confirmation, wipe, reload from the bucket |
   | /eh-host takeover | any host | Has direct mode take over now (`make direct-once` with the takeover), without waiting for the lease: for a host known to be gone |

   A planned move is `release` on the old host, then `claim` on the new one, whenever convenient: in between, direct mode writes. An unplanned one is `claim` on the new host after the automatic takeover. The steps that write the bucket or wipe a host stay the user's to start (CLAUDE.md); the skill runs the read-only parts, and gives the commands that need the typed confirmation to run in a terminal.
8. **Failover end to end, docs and skills.** The scenarios below, the architecture's direct-mode section, `/eh-mode`, `/eh-status`, the cheat sheet, the R2 budget at the new rates. As built: `tests/e2e/test_failover.py` (a host that stops, direct mode taking over and catching up, the returning host refused, a new host rebuilt and owning the bucket, with every event at its latest version once and only once; a planned move after which the new host holds exactly the one writer's data). Not done: the disruptive test killing the `exporter` container mid-export (the rollback it would exercise is covered by unit tests on moto), which needs compose services pointed at a test bucket.

**Decisions** (taken on 2026-10-30, each the recommended choice):

- **Takeover:** automatic, by the next direct round once the lease has expired; /eh-host takeover for doing it at once. Safe only after step 1, which comes first.
- **Lease TTL and round frequency:** 20 minutes and hourly rounds, unchanged: the dashboard is at most about 80 minutes behind after a failure, nothing lost (catch-up fills the gap from USGS).
- **The fencing fix:** the lock confirmed before every write, the scheduled reconcile's deep check weekly (decided while building step 1: daily would cost about 3.6 million Class B reads a month), and `make repair-rows`. The alternative, the manifest as the only commit point, would close the gap fully but change the bucket's layout, parity and both stores: a milestone of its own if ever needed.
- **A planned move:** through the bucket (release, then claim with a rebuild), never by copying Docker volumes. The old host's message history (`raw_events`, the topics) stays in its backup, not on the new host.
- **Backups:** in `reports/backup/` on the host wiped, checked to restore; nothing extra in R2.
- **Notification of a takeover:** the audit trail, `/eh-status`, /eh-host status and the daily reconcile's output; a GitHub e-mail (the round failing after it succeeds) only for a takeover that found an unfinished export.
- **Destructive steps:** a typed confirmation (the machine's ID), after the bucket is checked to hold everything and the backup to restore.

**What can and cannot be lost.** No event and no revision, by the safeguards in steps 3 (state never ahead of the export) and 6 (nothing wiped that the bucket lacks), and catch-up after a takeover. Lost by design: the processing times (`first_seen_at`, `last_applied_at`) of events stored after the last export before a failure (direct mode stores them again, with its own), at most one batch with step 4; and on a rebuilt host, its message history, kept in its backup.

**New requirements** (to `requirements.md` with their tests):

| ID | Requirement |
| --- | --- |
| REQ-MODE-017 | A writer whose lock was taken over writes nothing after it: every write of a direct round, an export, a handover or a repair confirms the lock first, renewing it (`If-Match`) once a third of its time has passed, and a lost lock stops the writer (DEF-058); `make repair-rows` writes again, under the lock and in either mode, every row object missing or differing from its manifest fingerprint, from the snapshot, and refuses during a handover or when the snapshot does not describe the manifest; the scheduled reconcile reads every row object once a week |
| REQ-MODE-018 | In full mode the marker names the bucket's owner, a machine ID (`EARTH_HUB_MACHINE_ID`, the hostname when empty), and its lease of 20 minutes, which the owner's exports renew at most every 5 minutes; only the owner exports: an export finding another owner writes nothing, one finding no owner (a bucket from before) takes it; `make mode-full` makes its host the owner and `make mode-direct` clears it; a direct round while the bucket is in full mode writes nothing, and its skip names the owner and the lease; `make mode` shows the owner, the lease and this host |
| REQ-MODE-019 | Each export of the exporting service is followed, under the same lock, by direct mode's state as `make mode-direct` would write it (one function): the ID map, the poller's 26-hour state and catch-up's checkpoints, from the same cut as the rows exported, during which no consumer batch commits; the poller's state covers only messages below the live consumer's committed offsets; catch-up's checkpoints are copied only when the backfill consumer has stored everything, otherwise the bucket keeps its older ones; when the rows exported and the cut's records differ, nothing of the state is written |
| REQ-MODE-020 | A direct round finding full mode's lease expired takes over with a conditional write; of two racing, exactly one takes over and the other writes nothing; the audit record says `took-over` and names the previous owner |
| REQ-MODE-021 | After a takeover, no event or revision is lost: the bucket ends with every event at its latest version, those the machine stored after its last export included, and with no record twice |
| REQ-MODE-022 | A rebuild refuses, wiping nothing, when the database holds an event the bucket lacks or holds at an older `updated`, when the backup (PostgreSQL and every topic) does not restore, or when the typed machine ID is wrong; otherwise it leaves PostgreSQL holding exactly the bucket (`make reconcile` passes, an export dry run finds nothing to change), and the host then hands back to full mode; a machine that is not the owner never exports |
| REQ-MODE-023 | The exporting service (`exporter`, started with the pipeline) exports when something was stored since its last export, at most every `EXPORT_INTERVAL_SECONDS` (300); otherwise it renews the lease every 5 minutes and writes nothing else; once the bucket is not in full mode, or another host owns it, it stops (exit 0, not restarted), and without R2 settings it exports nothing; R2's operations stay within REQ-MODE-007's budget with the service and the hourly rounds |
| REQ-MODE-024 | `make host-status`, `host-release`, `host-claim`, `host-rebuild` and `host-takeover` (`scripts/modes.py`) each check their starting state and refuse a wrong one without changing anything, print their steps, stop at the first failure and list their steps without running them with `DRY_RUN=1`; `host-status` writes nothing; `host-claim` refuses a machine ID already the owner's on another host; a release followed by a claim on another host, with USGS publishing throughout, loses no event or revision |

**QA work**

- **Unit (moto, no Docker):** the lease: renewal, expiry, owner check, a takeover racing another and an export; the fencing: the lock taken over between every pair of writes of a round and of an export, both stores, the bucket then as one writer would leave it (REQ-MODE-017); the warm state equal to the handover's, and an attempt to write it ahead of the rows refused (REQ-MODE-019); `repair-rows` over a bucket with stale row objects; the audit's `took-over`; the budget at the new rates (`test_direct_budget.py`); `scripts/modes.py`'s host commands against a fake shell, as `test_modes_script.py` does today: each refusal, the dry run, the stop at the first failure (REQ-MODE-024); the typed confirmation, wrong and right.
- **Integration (PostgreSQL, Kafka):** the rebuild: a random stream (`tests/support/streams.py`) stored in full mode, exported, rebuilt from the bucket: `make reconcile` passes and the export dry run finds nothing to change; the refusal: an event stored after the export, or a backup that does not restore, wipes nothing; the backup restored into a temporary database equals the original; the exporting service's trigger after a batch, and its stop once not the owner.
- **End to end (`tests/support/`: `FeedServer`, mock FDSN, `Pipeline`):** the failover scenario: full mode stores and exports; the machine stops in the middle of a stream, with events stored after its last export; the lease expires (controlled clock); a direct round takes over and catches up; USGS keeps publishing; the machine returns, is rebuilt and takes over again. The bucket and the API must hold exactly what one uninterrupted writer would (REQ-MODE-021, -022), ending with `make reconcile-direct DEEP=1` and `make reconcile`. The same with the machine stopping during an export, and during a handover. A planned move: `host-release`, USGS publishing, `host-claim` with a rebuild on a second database and Kafka standing in for the new host (temporary topics and databases, as the smoke tests use), the bucket then as one uninterrupted writer's (REQ-MODE-024).
- **Disruptive:** the exporting service's container killed mid-export, and Docker stopped with the lease live, on the shared containers.
- **Flake check** over the new end-to-end scenarios (the lease is time-driven; every clock in them is injected).

**Exit gate:** a full-mode machine stopped at any point is replaced by direct mode within one round after its lease, and takes over again when it returns, with the bucket and snapshot then the same as one uninterrupted writer's; a move to another host is /eh-host release and /eh-host claim, losing no event; nothing is wiped that the bucket lacks; a non-owner never writes; the regression passes, CI enforces it.

## 5. Recommended repository structure

The structure recommended before any code; the layout as built is in the [README](../../README.md#what-is-in-it) and the [architecture](../technical/architecture.md).

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
