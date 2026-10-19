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
| REQ-DQ-PLACE-001 | Derive city, region and country from `place` (split at the first " of ", then at commas; US states give country United States) for new revisions and for events already stored, keeping `place` unchanged |
| REQ-DQ-PLACE-002 | Events stored by backfill and catch-up get city, region and country by the same rule as the live feed, including when a revision changes `place` |

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

## Export to R2

| ID | Requirement |
| --- | --- |
| REQ-EXP-001 | `GET /api/v1/export/earthquakes` returns every column of every `earthquakes` row exactly as stored, deleted events included, in `id` order, paged by `after_id` (1–5000 rows) |
| REQ-EXP-002 | `make export-r2` reads only through the API and uploads rows that are not in the bucket or changed since their last upload, skips unchanged rows, and deletes objects for rows no longer in the table |
| REQ-EXP-003 | Each row is one object named by its primary key, so reruns and interrupted runs never create duplicates; uploads that failed are retried by the next run, and progress is saved every few pages so the next run skips rows an interrupted one already uploaded |
| REQ-EXP-004 | After a run the bucket holds exactly the rows read, with the same value in every column, as checked against the bucket listing on a verified run and against the manifest otherwise; if not, the run is marked failed |
| REQ-EXP-005 | Every run is recorded in the bucket (`runs/`) and in `reports/exports/`, with status, counts, timings and errors; a run that never finished is listed as interrupted |
| REQ-EXP-006 | Only one export runs at a time; the lock is taken atomically, and a lock left by a crashed run expires |
| REQ-EXP-007 | A dry run reports what would change without writing to the bucket; missing R2 settings stop the command with a clear message |
| REQ-EXP-008 | By default a run decides uploads and deletions from the manifest without listing the bucket; `make export-r2 VERIFY=1`, and any run without a readable manifest, lists the bucket once after the uploads, deletes objects the manifest does not know and re-uploads rows whose object is missing |
| REQ-EXP-009 | After a run that succeeded, the export writes `analytics/earthquakes.parquet`: every row it read and every column, with the same values and types (timestamps with time zone, lists as lists), in `id` order; it is rewritten only when the rows changed or the snapshot is missing; dry runs and failed runs never write it |
| REQ-EXP-010 | `analytics/meta.json` describes the snapshot (run, time, row count, size, SHA-256 of the file, the rows' fingerprint, column types) and is written only after the file; a missing or unreadable `meta.json` makes the next run rewrite both |

## Dashboard

The dashboard's SQL lives in `dashboard/queries/`; the page runs it in the browser over the R2 snapshot, and the tests run the same files.

| ID | Requirement |
| --- | --- |
| REQ-DASH-001 | Filters select events by time (start inclusive, end exclusive, UTC), magnitude (bounds inclusive; a bound leaves out events without a magnitude), country and region (missing ones shown and selected as `Unknown`) and event type; deleted events are left out unless included; invalid filter values are rejected |
| REQ-DASH-002 | Events are counted per day, ISO week (from Monday) or month in UTC, with the highest magnitude per period, from the first period with an event to the last, empty periods listed with 0 |
| REQ-DASH-003 | Magnitudes are counted in bins of 0.5 holding `bin_start <= magnitude < bin_end`, empty bins between the lowest and highest listed with 0, events without a magnitude left out |
| REQ-DASH-004 | The top countries and regions are ranked by number of events, ties in alphabetical order, limited to the requested number |
| REQ-DASH-005 | The summary (counts, highest magnitude and its place, first and last event, countries, deletions), the significant events (the latest, or the largest with equal magnitudes newest first) and the map points are computed from the filtered events; no events give empty results and a zero summary |
| REQ-DASH-006 | The Worker serves only `analytics/earthquakes.parquet` and `analytics/meta.json` (as `/data/...`), read-only, with `ETag`, `304 Not Modified`, byte ranges and a 5-minute browser cache; nothing else in the bucket is reachable; without a snapshot it answers 404 saying so |
| REQ-DASH-007 | The page shows the summary, map, charts and significant events for the filters, with DuckDB-WASM giving the same answers as the tests' DuckDB on the same snapshot; it works at phone width, labels every control, and reports a missing snapshot; nothing it loads comes from outside its own origin except the DuckDB-WASM engine and map tiles |
| REQ-DASH-008 | Load and filter times are measured at 115k and 1M rows, five runs each, and kept in `reports/`: the first map within the limits in `tests/performance/test_dashboard_load.py`, each filter change within its limit, and at least four of the five runs within ±15% (at least 50 ms) of their median |
| REQ-DASH-009 | Up to 5,000 filtered events, the map shows every event as its own point; above that it clusters them when zoomed out, except events of magnitude 6 or more, which always stay single points drawn above the clusters; each cluster is colored by its strongest event |
| REQ-DASH-010 | The time filter offers Today (the default), This week (from Monday), This month, Last 3 months, This year, Last 3 years and Custom: each preset is whole UTC days from its first day through today, month and year steps landing on the last day of a shorter month; Custom shows From and To calendars (To inclusive), starting from the last preset's days; each preset picks a fitting chart grouping |
| REQ-DASH-011 | The event type filter starts on `earthquake`, and Reset returns to it; other types (quarry blasts, explosions and so on) appear only when chosen, or with All; when the data has no earthquakes it starts on All |

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
