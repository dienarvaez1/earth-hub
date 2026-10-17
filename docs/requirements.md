# Requirements

The requirements Earth-Hub is tested against, collected from the [implementation plan](IMPLEMENTATION.md), the [data-quality rules](data-quality-rules.md) and the API contract. Each has a stable ID.

Tests declare which requirements they verify with `@pytest.mark.req("REQ-...")`. `make traceability` builds the [traceability matrix](traceability-matrix.md) from those markers and the latest test results, and **fails if any requirement has no passing test**. A test that names an ID missing from this file fails at collection, so the two cannot drift apart.

Every table row that starts with `| REQ-` is read by the tooling: keep one requirement per row.

## Ingestion

| ID | Requirement |
| --- | --- |
| REQ-ING-001 | The poller fetches the USGS `all_day` feed with `If-Modified-Since`; a `304` publishes nothing |
| REQ-ING-002 | The next fetch is timed from the feed's `Expires` header plus 2–5 s of jitter, between 10 s and 5 min, falling back to `POLL_INTERVAL_SECONDS` without the header |
| REQ-ING-003 | Timeouts, connection errors, `429` and `5xx` are retried up to 3 times with exponential backoff; other `4xx` are not retried; a failed round publishes nothing |
| REQ-ING-004 | Only new events, or events whose `updated` time moved forward, are published |
| REQ-ING-005 | Messages are keyed by USGS event ID and carry a versioned envelope around the unchanged USGS feature |
| REQ-ING-006 | An event counts as sent only after Kafka confirms delivery; failed deliveries are retried next round |
| REQ-ING-007 | Only features without a usable `id`, `updated` or `time` are dropped by the poller; everything else is published unchanged |
| REQ-ING-008 | On restart the poller rebuilds what it has sent from `earthquakes.raw` and does not resend unchanged events |
| REQ-ING-009 | The poller shuts down cleanly on `SIGTERM`, flushing pending messages |
| REQ-ING-010 | Messages with the same key always go to the same partition |

## Persistence

| ID | Requirement |
| --- | --- |
| REQ-PER-001 | Every Kafka message gets one `raw_events` row with its topic, partition, offset, full message and outcome |
| REQ-PER-002 | A redelivered offset is recognized and skipped without a new row |
| REQ-PER-003 | A message with the same `updated` time as the stored event is a `duplicate` and changes nothing |
| REQ-PER-004 | A newer revision updates the event; an older one is `stale` and changes nothing |
| REQ-PER-005 | An event stays one record when USGS changes its preferred ID, and its former IDs still find it |
| REQ-PER-006 | Records that turn out to be one event are merged into the oldest, keeping the newest data and every ID |
| REQ-PER-007 | The Kafka offset is committed only after the database commit; a crash between the two causes no duplicates |
| REQ-PER-008 | During a database outage the consumer retries the same message until PostgreSQL is back, and never rejects it for that reason |
| REQ-PER-009 | A message that fails 3 times for an unexpected reason is stored as `error` with the exception, and the partition moves on |
| REQ-PER-010 | Concurrent processing of one event cannot create duplicate records |
| REQ-PER-011 | Database `CHECK` constraints enforce the range rules even when the application is bypassed |
| REQ-PER-012 | Migrations match the models exactly and upgrade and downgrade cleanly |
| REQ-PER-013 | Reconciliation accounts for every message in the topic, separating lost from pending, and checks the tables' invariants |
| REQ-PER-014 | Messages that `jsonb` cannot hold (not JSON, `NaN`, NUL, `null`) are kept as text and rejected, never crash the consumer |

## Backfill and catch-up

| ID | Requirement |
| --- | --- |
| REQ-BF-001 | Backfill requests one calendar week at a time, Sunday 00:00:00 UTC to the next Sunday, the first week starting at the requested date (2026-01-01) and the current week left open until it is over |
| REQ-BF-002 | Consecutive windows meet at exactly the same whole second, so no event is lost at a boundary even though FDSN ignores milliseconds in query times |
| REQ-BF-003 | Every window is checked against the FDSN `/count` with the same parameters, never uses `limit`, and is not recorded as done if fewer events arrive than were counted |
| REQ-BF-004 | A window over the FDSN cap of 20,000 events is split into smaller windows covering exactly the same range |
| REQ-BF-005 | An interrupted backfill resumes without fetching finished windows again |
| REQ-BF-006 | A final sweep publishes events added or revised during the backfill, including events whose revised time moved them into a week already fetched |
| REQ-BF-007 | Requests to FDSN go one at a time with a pause between them, back off on `429` and `5xx`, and honour `Retry-After` |
| REQ-BF-008 | Backfill includes deleted events, and counts them the same way it queries them |
| REQ-BF-009 | Backfill and catch-up publish only events that are new or newer than what PostgreSQL stores |
| REQ-BF-010 | Backfill and catch-up publish to `earthquakes.backfill`, consumed separately from the live topic |
| REQ-BF-011 | A full year of history backfills through the whole pipeline in 3 minutes or less, with exact totals in the API |
| REQ-BF-012 | Live events keep their normal latency while a backfill is being consumed |
| REQ-BF-013 | A window USGS times out on (504 or no answer in time) is not retried as is, but split into days, and smaller if needed, on the same boundaries; a resumed week continues with its remaining days |
| REQ-CU-001 | Catch-up reads the `all_month` feed hourly and on startup, recovering revisions and outages up to 30 days old |
| REQ-CU-002 | A daily check publishes events USGS marked deleted since the previous check |
| REQ-PER-015 | Processing messages in a batch gives exactly the same stored results as processing them one by one |
| REQ-PER-016 | A batch that fails for a non-database reason is redone one message at a time, so only the bad message is recorded as an error |
| REQ-PER-017 | Consumers processing overlapping batches never deadlock |

## Data quality

One requirement per rule in [data-quality-rules.md](data-quality-rules.md).

| ID | Requirement |
| --- | --- |
| REQ-DQ-ENV-001 | Reject messages that are not a JSON object |
| REQ-DQ-ENV-002 | Reject messages whose `schema_version` is not 1 |
| REQ-DQ-ENV-003 | Reject envelopes with missing or mistyped fields |
| REQ-DQ-ENV-004 | Reject messages whose payload `id` differs from `event_id` |
| REQ-DQ-ENV-005 | Reject messages whose envelope timestamps disagree with the payload |
| REQ-DQ-GEO-001 | Reject geometry that is not a Point with 3 finite numbers |
| REQ-DQ-GEO-002 | Reject latitudes outside −90 to 90 |
| REQ-DQ-GEO-003 | Reject longitudes outside −180 to 180 |
| REQ-DQ-GEO-004 | Reject depths outside −10 to 1000 km |
| REQ-DQ-TIME-001 | Reject event times before 1900 or not representable |
| REQ-DQ-TIME-002 | Reject event times more than 1 hour in the future |
| REQ-DQ-TIME-003 | Reject `updated` times earlier than the event time or not representable |
| REQ-DQ-MAG-001 | Reject magnitudes outside −2 to 10.5 or not numbers; allow missing magnitudes |
| REQ-DQ-PROP-001 | Reject a missing or empty `type` |
| REQ-DQ-PROP-002 | Reject a `status` other than `automatic`, `reviewed` or `deleted` |
| REQ-DQ-PROP-003 | Reject optional fields with the wrong type |
| REQ-DQ-ACC-001 | Accept values real USGS data contains: negative magnitude and depth, non-earthquake types, `deleted` status, missing optional fields |
| REQ-DQ-DEL-001 | Accept a deletion whose magnitude or optional fields hold placeholder values, storing those values as unknown (DEF-027) |
| REQ-DQ-DEL-002 | The deletion of a duplicate merged into another event never hides that event, whichever arrives first; backfill and catch-up decide what to publish by the same rule (DEF-026) |

## API

| ID | Requirement |
| --- | --- |
| REQ-API-001 | `GET /api/v1/health` reports database status, newest event and version, and returns `503` when the database is down |
| REQ-API-002 | The event list is paginated with `limit` (1–500) and `offset`, reporting the total across all pages |
| REQ-API-003 | The list, statistics and summaries filter by magnitude range, time window (start inclusive, end exclusive), place substring (matched literally), event type and review status |
| REQ-API-004 | Sorting by event time, magnitude or depth puts missing values last and breaks ties stably |
| REQ-API-005 | An event can be fetched by its current or any former USGS ID; unknown IDs return `404` |
| REQ-API-006 | Statistics (count, magnitude min/max/average, counts by type) match the stored data |
| REQ-API-007 | The hourly summary covers every hour in the window, empty hours included, for at most 31 days |
| REQ-API-008 | The magnitude summary returns every range, empty ranges included |
| REQ-API-009 | Deleted events are hidden unless asked for, and always found by ID |
| REQ-API-010 | Invalid parameters return `422` listing every problem, without touching the database |
| REQ-API-011 | Every error uses `{"error": {"code", "message", "details"}}`, including `404`, `405` (with `Allow`), `503` and `500`, and never exposes internals |
| REQ-API-012 | The OpenAPI schema documents every endpoint, parameter and error, and responses conform to it |
| REQ-API-013 | The API is read-only and serves data from PostgreSQL only |

## End-to-end scenarios

The scenarios from Milestone 5 of the plan, verified through the API on the whole pipeline.

| ID | Requirement |
| --- | --- |
| REQ-E2E-001 | A new valid event in the feed becomes retrievable through the API |
| REQ-E2E-002 | Duplicate delivery of a message creates no duplicate record |
| REQ-E2E-003 | A revised event is shown with its latest revision |
| REQ-E2E-004 | An invalid event is never served, and is recorded as rejected with an auditable reason |
| REQ-E2E-005 | A database outage is recovered from without losing or duplicating events |
| REQ-E2E-006 | A consumer restart, clean or between the database commit and the offset commit, handles redelivered events safely |
| REQ-E2E-007 | An empty database gives valid empty results and zero-count summaries |
| REQ-E2E-008 | An invalid API request gets a consistent client error |
| REQ-E2E-009 | A batch of input records reconciles exactly with the processing outcomes and the API |
| REQ-E2E-010 | Two reports merged into one event are served as one event under either ID |
| REQ-E2E-011 | An event revised to `deleted` is hidden by default |
| REQ-E2E-012 | A poller restart resends nothing and changes nothing in the API |
| REQ-E2E-013 | A Kafka broker restart is recovered from without losing or duplicating events |

## Operations and quality

| ID | Requirement |
| --- | --- |
| REQ-OPS-001 | The infrastructure starts healthy with one command: Kafka (KRaft), the `earthquakes.raw` topic with 3 partitions, and PostgreSQL 18 |
| REQ-OPS-002 | Data in Kafka and PostgreSQL survives a restart of both |
| REQ-OPS-003 | Configuration is validated at startup, and passwords never appear in logs, test output or API responses |
| REQ-OPS-004 | Logs are one JSON object per line, with extra fields such as event ID and stage |
| REQ-OPS-005 | The poller's, consumers' and catch-up's health checks follow their heartbeats |
| REQ-OPS-006 | Requests to USGS identify Earth-Hub in the `User-Agent`, with an optional operator contact |
| REQ-QA-001 | Unit tests never use the network, the live USGS feed included |
| REQ-QA-002 | Test data is deterministic: results do not depend on live earthquakes |
