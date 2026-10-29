# Requirements

The requirements Earth-Hub is tested against, collected from the [implementation plan](plan.md), the [data-quality rules](../technical/data-quality-rules.md) and the API contract. Each has a stable ID.

Tests declare which requirements they verify with `@pytest.mark.req("REQ-...")`. `make traceability` builds the [traceability matrix](traceability-matrix.md) from those markers and the latest test results, and **fails if any requirement has no passing test**. A test that names an ID missing from this file fails at collection, so the two cannot drift apart.

Every table row that starts with `| REQ-` is read by the tooling: keep one requirement per row.

## Ingestion

| ID | Requirement |
| --- | --- |
| REQ-ING-001 | The poller fetches the USGS `all_day` feed with `If-Modified-Since`; a `304` publishes nothing |
| REQ-ING-002 | The next fetch is timed from the feed's `Expires` header plus 2–5 s of jitter, between 10 s and 5 min, falling back to `POLL_INTERVAL_SECONDS` without the header |
| REQ-ING-003 | Timeouts, connection errors, `429`, `500`, `502`, `503` and `504` are tried up to 3 times in all (2 retries) with exponential backoff; other `4xx` are not retried; a failed round publishes nothing |
| REQ-ING-004 | Only new events, or events whose `updated` time moved forward, are published |
| REQ-ING-005 | Messages are keyed by USGS event ID and carry a versioned envelope around the unchanged USGS feature |
| REQ-ING-006 | An event counts as sent only after Kafka confirms delivery; failed deliveries are retried next round |
| REQ-ING-007 | Only features without a usable `id`, `updated` or `time` are dropped by the poller; everything else is published unchanged |
| REQ-ING-008 | On restart the poller rebuilds what it has sent from `earthquakes.raw` and does not resend unchanged events |
| REQ-ING-009 | The poller shuts down cleanly on `SIGTERM`, flushing pending messages |
| REQ-ING-010 | Messages with the same key always go to the same partition |

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
| REQ-BF-014 | A feature dropped before Kafka by backfill or catch-up (no usable `id`, `updated` or `time`, as USGS sends some deletions) is logged once with its reason (`feature rejected`) and counted: per window in `backfill_windows.rejected`, and in catch-up's `catch-up pass` and `deleted-events check` lines |
| REQ-CU-001 | Catch-up reads the `all_month` feed hourly and on startup, recovering revisions and outages up to 30 days old |
| REQ-CU-002 | A daily check publishes events USGS marked deleted since the previous check |
| REQ-PER-015 | Processing messages in a batch gives exactly the same stored results as processing them one by one |
| REQ-PER-016 | A batch that fails for a non-database reason is redone one message at a time, so only the bad message is recorded as an error |
| REQ-PER-017 | Consumers processing overlapping batches never deadlock |
| REQ-PER-018 | An event's `first_seen_at` and `last_applied_at`, and an ID's `first_seen_at`, are the processing time the consumer passes in, never the database's clock |

## Data quality

One requirement per rule in [data-quality-rules.md](../technical/data-quality-rules.md).

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
| REQ-DQ-PROP-002 | Reject a `status` other than `automatic`, `reviewed` or `deleted`, accepting any letter case and storing it in lower case; an upper-case `DELETED` is a deletion wherever the status is read (DEF-049) |
| REQ-DQ-PROP-003 | Reject optional fields with the wrong type |
| REQ-DQ-ACC-001 | Accept values real USGS data contains: negative magnitude and depth, non-earthquake types, `deleted` status, missing optional fields |
| REQ-DQ-DEL-001 | Accept a deletion whose magnitude or optional fields hold placeholder values, storing those values as unknown (DEF-027) |
| REQ-DQ-DEL-002 | The deletion of a duplicate merged into another event never hides that event, whichever arrives first; backfill and catch-up decide what to publish by the same rule (DEF-026) |
| REQ-DQ-PLACE-001 | Derive city, region and country from `place` (split at the first " of ", then at commas; US states give country United States) for new revisions and for events already stored, keeping `place` unchanged |
| REQ-DQ-PLACE-002 | Events stored by backfill and catch-up get city, region and country by the same rule as the live feed, including when a revision changes `place` |

## Persistence

| ID | Requirement |
| --- | --- |
| REQ-PER-001 | Every Kafka message gets one `raw_events` row with its topic, partition, offset, full message and outcome |
| REQ-PER-002 | A redelivered offset is recognized and skipped without a new row |
| REQ-PER-003 | A message with the same `updated` time as the stored event is a `duplicate` and changes nothing |
| REQ-PER-004 | A newer revision updates the event; an older one is `stale` and changes nothing |
| REQ-PER-005 | An event stays one record when USGS changes its preferred ID, and its former IDs still find it |
| REQ-PER-006 | Records that turn out to be one event are merged into the one with the smallest key, whatever order they were created in, keeping the newest data and every ID (DEF-050) |
| REQ-PER-007 | The Kafka offset is committed only after the database commit; a crash between the two causes no duplicates |
| REQ-PER-008 | During a database outage the consumer retries the same message until PostgreSQL is back, and never rejects it for that reason |
| REQ-PER-009 | A message that fails 3 times for an unexpected reason is stored as `error` with the exception, and the partition moves on |
| REQ-PER-010 | Concurrent processing of one event cannot create duplicate records |
| REQ-PER-011 | Database `CHECK` constraints enforce the range rules even when the application is bypassed |
| REQ-PER-012 | Migrations match the models exactly and upgrade and downgrade cleanly |
| REQ-PER-013 | Reconciliation accounts for every message in the topic, separating lost from pending, and checks the tables' invariants |
| REQ-PER-014 | Messages that `jsonb` cannot hold (not JSON, `NaN`, NUL, `null`) are kept as text and rejected, never crash the consumer |

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
| REQ-API-014 | Under load, 50 users browsing and analyzing at once over 140,000 events, no request fails, the API serves at least 15 requests a second, and each endpoint's 95th-percentile response time stays within its budget; `make load` writes the Locust report (`reports/load-report.html`, CSV) |

## Export to R2

| ID | Requirement |
| --- | --- |
| REQ-EXP-001 | `GET /api/v1/export/earthquakes` returns every column of every `earthquakes` row exactly as stored, deleted events included, in `id` order, paged by `after_id` (1–5000 rows) |
| REQ-EXP-002 | `make export-r2` reads only through the API and uploads rows that are not in the bucket or changed since their last upload, skips unchanged rows, and deletes objects for rows no longer in the table |
| REQ-EXP-003 | Each row is one object named by its key (REQ-EXP-012), so reruns and interrupted runs never create duplicates; uploads that failed are retried by the next run, and progress is saved every few pages so the next run skips rows an interrupted one already uploaded |
| REQ-EXP-004 | After a run the bucket holds exactly the rows read, with the same value in every column, as checked against the bucket listing on a verified run and against the manifest otherwise; if not, the run is marked failed |
| REQ-EXP-005 | Every run is recorded in the bucket (`runs/`) and in `reports/exports/`, with status, counts, timings and errors; a run that never finished is listed as interrupted |
| REQ-EXP-006 | Only one export runs at a time, however long it lasts: the lock is taken atomically and renewed while the run lasts, a run whose lock was taken over stops writing, and a lock left by a crashed run expires |
| REQ-EXP-007 | A dry run reports what would change without writing to the bucket; missing R2 settings stop the command with a clear message |
| REQ-EXP-008 | By default a run decides uploads and deletions from the manifest without listing the bucket; `make export-r2 VERIFY=1`, and any run without a readable manifest, lists the bucket once after the uploads, deletes objects the manifest does not know and re-uploads rows whose object is missing |
| REQ-EXP-009 | After a run that succeeded, the export writes `analytics/earthquakes.parquet`: every row it read and every published column (all but `id`), with the same values and types (timestamps with time zone, lists as lists), in `key` order; it is rewritten only when the rows changed or the snapshot is missing; dry runs and failed runs never write it |
| REQ-EXP-010 | `analytics/meta.json` describes the snapshot (run, time, row count, size, SHA-256 of the file, the rows' fingerprint, column types) and is written only after the file; a missing or unreadable `meta.json` makes the next run rewrite both |
| REQ-EXP-011 | `make r2-ls`, `make r2-count` and `make r2-get` list, count and read the bucket's objects (gunzipping `.gz`, saving binary objects only to a file) and never write to it |
| REQ-EXP-012 | Each record has a `key`, the USGS ID it was created with, kept by the surviving record of a merge (existing rows: the earliest-seen of their IDs); R2 rows and the snapshot are named and ordered by `key` and never carry the database's `id`, so they are the same whatever store processed the events |
| REQ-EXP-013 | The snapshot is compressed with zstd at level 19, smaller than zstd's default level for the same rows, since the dashboard downloads the whole file before it shows anything |

## Direct mode

Milestone 8: the pipeline without Docker, keeping its state in the bucket ([architecture](../technical/architecture.md#direct-mode)).

| ID | Requirement |
| --- | --- |
| REQ-MODE-001 | The same messages, in the same batches with the same processing times, through full mode (PostgreSQL, the API and the exporter) or direct mode give the same bucket: every row object byte for byte, the manifest, the snapshot's rows, types and fingerprint, the ID map (each USGS ID's record and first-seen time, as `earthquake_ids`) and each message's outcome, rule, reason and record (the log, as `raw_events`) |
| REQ-MODE-002 | A direct round saves its writes in a redo record before making them; a round interrupted at any write is finished by the next one, and the bucket ends as if it had not been interrupted. No round starts on top of another's unfinished writes |
| REQ-MODE-003 | Every message direct mode processes is logged under `log/`, in order, with its sequence number, processing time, source, outcome, rule, reason, record key and envelope; sequence numbers have no gaps, and each log object is named by its first and last |
| REQ-MODE-004 | Each direct round that writes holds the bucket's lock, the exporter's, so a round and an export never write at once; direct mode refuses a bucket whose snapshot does not match its manifest, or whose ID map and manifest list different records, before writing anything |
| REQ-MODE-005 | The poller and catch-up run unchanged in direct mode: the poller's 26-hour state and catch-up's checkpoints are saved in the bucket and survive a restart, and catch-up skips exactly the messages full mode would (one rule, `revisions.needed`) |
| REQ-MODE-006 | `make reconcile-direct` checks the bucket's invariants (row objects, manifest, snapshot and ID map agree; every record's IDs point to it; the log has no gap; no round is unfinished) and exits 1 if any is broken; `DEEP=1` also reads every row object |
| REQ-MODE-007 | Direct mode keeps inside R2's free operations at three times the measured live load (437 live rounds and 24 catch-up passes a day): under half of the monthly Class A allowance (writes, listings) and a tenth of Class B (reads) |
| REQ-MODE-008 | A record's values survive the snapshot exactly: a row read back from the snapshot gives the same row object, byte for byte, so direct mode can load records from it |
| REQ-MODE-009 | One marker in the bucket, `_state/mode.json`, says which mode writes it: the exporter writes only in full mode and direct rounds only in direct mode, so the modes take turns. `make mode-direct` hands over only from an idle, reconciled full mode, after an export that succeeded, and writes direct mode's ID map, poller state and checkpoints from PostgreSQL and Kafka; `make mode-full` hands back. A round trip full → direct → full leaves PostgreSQL as it would be had it stayed in full mode: every column, processing times included, every ID and every message's outcome |
| REQ-MODE-010 | The handover to full mode replays every log entry since the last handover, in order, through `earthquakes.backfill` with its original processing time, and switches only when reconciliation passes and an export dry run finds PostgreSQL and the bucket the same; a gap in the log, or a difference, stops it and leaves the bucket in direct mode |
| REQ-MODE-011 | A handover to full mode interrupted while replaying keeps the bucket frozen (no direct round writes), and run again sends only what it had not sent, recognized by sequence number in the topic |
| REQ-MODE-012 | `make direct-once` and `make direct-run` run the real poller and catch-up in direct mode, needing only R2 credentials, and refuse to start unless the bucket is in direct mode |
| REQ-MODE-013 | Switching modes back and forth while USGS keeps publishing, during the handovers too, loses no event and no revision: after each switch, the first round of the mode taking over picks up what changed meanwhile, and the API (full mode) or the bucket (direct mode) holds every event at its latest version |
| REQ-MODE-014 | Run on a schedule (`--skip-unless-direct`), `direct once` and `make reconcile-direct` exit 0 without writing or checking while the bucket is not in direct mode, including when a handover freezes it during a round (catch-up then does not run); missing R2 settings still fail |
| REQ-MODE-015 | Each execution of direct mode (`python -m earth_hub.direct`) leaves an audit record in the bucket, `audit/direct/<day>/<run_id>.json`, written when it starts and when it ends (and every 10 minutes for `run`): the command, who started it (`cloudflare-trigger`, `github-schedule`, `github-manual`, `direct-run` or `local`), the bucket's mode at the start, the status (running, succeeded, failed or skipped; a `running` record not saved for 30 minutes is read as interrupted), start and end times and the elapsed seconds, the messages processed by part (feed, catchup, deleted) and outcome, the rows written and removed, the log's sequence numbers, the errors logged and the rejected messages; failing to write it never fails a round, and `make direct-runs` lists the latest |
| REQ-MODE-016 | One command each switches modes or backfills in full mode, with the checks around it (`scripts/modes.py`): `make to-full` (Docker running, the bucket in direct mode, no local `direct-run`, then `make mode-full`, the consumers caught up and `make reconcile`); `make backfill-full START=… [END=…]` (full mode only, END after START; the pipeline running, `make backfill`, `consumer-backfill` caught up, `make reconcile`, then `make export-r2`); `make to-direct` (the bucket in full mode, the consumers caught up, `make reconcile` clean, `make mode-direct`, then `make down`). Each prints its steps, refuses a wrong starting state without changing anything, stops at the first failure, gives up waiting for the consumers after a set time, and lists its steps without running them with `--dry-run` |

## Reconciliation with USGS

Earth-Hub against USGS itself, in the mode that writes the bucket ([architecture](../technical/architecture.md#reconciliation-with-usgs)).

| ID | Requirement |
| --- | --- |
| REQ-REC-001 | `make reconcile-usgs` compares the last `DAYS` UTC days of the mode writing the bucket (PostgreSQL in full mode, the bucket's snapshot and ID map in direct mode) with USGS's FDSN service, matching events by any of their IDs, and explains each difference by when Earth-Hub last read the source that brings it: an event or revision USGS published after that read, or a deletion after the last deleted-events check, is pending; any other is unexplained and the command exits 1. Revision times within 60 s are the same revision |
| REQ-REC-002 | After a successful switch, the mode taking over catches up at once and is checked against USGS: `make mode-direct` runs a direct round with catch-up and the deleted-events check whether due or not, `make mode-full` a catch-up pass and a deleted-events check, each then `make reconcile-usgs`; `make catchup-now` does the same catch-up in either mode |
| REQ-REC-003 | The daily scheduled reconcile also runs the check against USGS, and fails (so GitHub e-mails) if no direct round has written for 2 hours, or a difference is unexplained |
| REQ-REC-004 | The check against USGS takes each feed as read when USGS generated the newest copy of it Earth-Hub processed (the feed's `metadata.generated`), not when it was fetched or the round ended: direct rounds keep that time per feed (day feed, catch-up's month feed) in their state, and full mode takes it from `raw_events`. An update after it is pending, one before it unexplained. Without one, the live topic's last delivery or the last round's end stands in for the day feed, and catch-up's checkpoint for the month feed (DEF-054) |

## Dashboard

The dashboard's SQL lives in `dashboard/queries/`; the page runs it in the browser over the R2 snapshot, and the tests run the same files.

| ID | Requirement |
| --- | --- |
| REQ-DASH-001 | Filters select events by time (start inclusive, end exclusive, UTC), magnitude (minimum inclusive, maximum exclusive, like the legend's bands; a bound leaves out events without a magnitude), country and region (missing ones shown and selected as `Unknown`) and event type; deleted events are left out unless included; invalid filter values are rejected |
| REQ-DASH-002 | Events are counted per hour, day, ISO week (from Monday) or month in UTC, with the highest magnitude per period, from the first period with an event to the last, empty periods listed with 0 |
| REQ-DASH-003 | Magnitudes are counted in bins of 0.5 holding `bin_start <= magnitude < bin_end`, empty bins between the lowest and highest listed with 0, events without a magnitude left out |
| REQ-DASH-004 | The top countries and regions are ranked by number of events, ties in alphabetical order, limited to the requested number |
| REQ-DASH-005 | The summary (counts, highest magnitude and its place, first and last event, countries, deletions), the significant events (the latest, or the largest with equal magnitudes newest first) and the map points are computed from the filtered events; no events give empty results and a zero summary |
| REQ-DASH-006 | The Worker serves only `analytics/earthquakes.parquet` and `analytics/meta.json` (as `/data/...`), read-only, with `ETag`, `304 Not Modified`, byte ranges and a 5-minute browser cache; nothing else in the bucket is reachable; without a snapshot it answers 404 saying so |
| REQ-DASH-007 | The page shows the summary, map, charts and significant events for the filters, with DuckDB-WASM giving the same answers as the tests' DuckDB on the same snapshot; it works at phone width, labels every control, and reports a missing snapshot; nothing it loads comes from outside its own origin except the DuckDB-WASM engine and map tiles |
| REQ-DASH-008 | Load and filter times are measured at 115k and 1M rows, five runs each, and kept in `reports/`: the first map within the limits in `tests/performance/test_dashboard_load.py`, each filter change within its limit, and at least four of the five runs within ±15% (at least 50 ms) of their median |
| REQ-DASH-009 | Up to 5,000 filtered events, the map shows every event as its own point; above that it clusters them when zoomed out (above 25,000, cells: REQ-DASH-025), except events of magnitude 6 or more, which always stay single points drawn above the clusters; each cluster is colored by its strongest event |
| REQ-DASH-010 | The time filter offers Today, Yesterday, This week (from Monday; the default), Last week (Monday to Sunday), This month, Last month, Last 3 months, This year, Last year, Last 3 years and Custom: each preset is whole UTC days from its first day through today, or through the last day of the previous day, week, month or year, month and year steps landing on the last day of a shorter month; Custom shows From and To calendars (To inclusive), starting from the last preset's days; each preset picks a fitting chart grouping |
| REQ-DASH-011 | The event type filter starts on `earthquake`, and Reset returns to it; other types (quarry blasts, explosions and so on) appear only when chosen, or with All; when the data has no earthquakes it starts on All |
| REQ-DASH-012 | A legend beside the map's note shows how dot color follows magnitude: one bar blending the map's own color stops, labelled ≤0, 2.5, 4.5 and 6+, readable by screen readers; it wraps under the note on narrow screens |
| REQ-DASH-013 | Each bar of the magnitude chart takes the map's color for the middle of its bin, from the same color stops as the map and its legend |
| REQ-DASH-014 | Each bar of the events-over-time chart is split by magnitude level, the bands of the map's legend: below 2.5, 2.5 to 4.5, 4.5 to 6, 6 and above (each including its lower edge), plus no magnitude; the levels add up to the period's events and use the legend's colors, with grey for no magnitude |
| REQ-DASH-015 | The custom calendar allows only days from the first day with data to today (others greyed out; a typed day outside is moved to the nearest allowed one), and the magnitude filter is one list of the legend's bands (Below 2.5, 2.5 to 4.5, 4.5 to 6, 6 and above, each including its lower edge) after All, the default, which also keeps events without a magnitude |
| REQ-DASH-016 | The page links to `third-party-licenses.txt`, generated at build time with the license of every package bundled into the page and of the DuckDB extension it serves; the build fails if a package's license text is missing (DEF-035) |
| REQ-DASH-017 | The page is shared by link, not promoted: kept out of search engines with `X-Robots-Tag: noindex, nofollow` on every response (page, files and `/data/*`), a `robots` meta tag, and a `robots.txt` that disallows everything; it carries no preview label |
| REQ-DASH-018 | The significant-events table sorts by any of its Time, Magnitude, Place and Depth columns from a button in the column's header: ascending, then descending, then back to the order the table was chosen in; ties keep that order, events without a place come last, and the header says which sort is on (`aria-sort`). The sort reorders the events shown, stays when the filters change, and is cleared when Latest or Largest picks other events |
| REQ-DASH-019 | A map point's popup gives the event's time to the second in UTC and, on the next line, at the place it happened: in the time zone its coordinates fall in (a nautical zone at sea), with the zone's summer time, its abbreviation where the browser has one and its offset from UTC in parentheses (`AKDT (UTC-8)`, `(UTC+9)`, `(UTC+5:30)`), the zone's name on hover |
| REQ-DASH-020 | The country and region rankings each carry a note saying what a bar is (its share of the events in the bars shown, split by magnitude level, with its number of events at its end), that the ranking follows the filters and events without a country count as Unknown; and what a region is (the area USGS names in an event's place), events whose place names only a country counting under Unknown, beside that country |
| REQ-DASH-021 | The place menus show, without changing their values or the reports: in the country menu Unknown right after All, then the countries alphabetically, the US territories USGS names as the country grouped right after United States as `US-<territory>`, alphabetically (`US-American Samoa`, `US-Guam`, `US-Northern Mariana Islands`, `US-Puerto Rico`, `US-Virgin Islands`; the independent Samoa unchanged); in the region menu Unknown right after All, then the other regions alphabetically, then the US states as `US-<state>` alphabetically (`US-Alaska`), followed by any US territory |
| REQ-DASH-022 | The events-over-time chart has a Style menu: Stacked columns (REQ-DASH-014; the default, and what Reset returns to), Stacked areas, Lines (one line per magnitude level that has events in the range, zero periods included, with a crosshair and tooltip) and Trendlines (each level's periods as faded dots with its least-squares trend line); both stacks run from the baseline up 6 and above, 4.5 to 6, 2.5 to 4.5, Below 2.5, then No magnitude; all four use the legend's level colors and keep the chart's own legend |
| REQ-DASH-023 | Each bar of the top countries and top regions is its share of all the events in the bars shown (the bars add up to 100%), split by magnitude level in the legend's colors and stacking order, with its number of events at its end and the chart's own legend; the reports count each country's and region's events by level, the levels adding up to its events |
| REQ-DASH-024 | The country, region and event type menus count each option's events under the rest of the filter (time, magnitude, the other place and type choices, deletions, the tsunami box), leaving out the menu's own choice, and recount whenever a filter changes; every option stays listed, at 0 when nothing matches, the type menu in its order over all events |
| REQ-DASH-025 | Above 25,000 filtered events the map is not given every event: it gets cells of 0.25 degrees counted in SQL (`map_grid.sql`: each at its events' mean position, with their number and strongest magnitude), clustered as events are, and the events of magnitude 6 or more as their own points (`map_strong.sql`), so cells and strong events count every filtered event exactly once; the depth chart gets counts per 0.1 magnitude by 5 km (`depth_bins.sql`). Zoomed in to level 4 or more, the visible area and half its size again around it (`map_area.sql`, edges included, wrapping across the antimeridian) shows its events one by one, with their popups, in place of the cells, when it holds at most 25,000 of them |
| REQ-DASH-026 | A popup gives the depth to one decimal, as the table does, and says `tsunami flagged` after it when USGS flags the event, and nothing when it does not; one popup is open at a time, and it closes on a click elsewhere on the map and when the page's width changes |
| REQ-DASH-027 | The significant events come right after the map; a click on an event, or Enter or Space on it, flies the map to it at zoom 5 and opens its popup, whatever the map shows there (a point, a cluster or a cell), and scrolls the map into view when it is off the screen; the row's USGS link still opens the USGS page |
| REQ-DASH-028 | The map opens at zoom 1 and zooms in no closer than 15 |
| REQ-DASH-029 | The significant events show 10 rows by default, or 25 or 50 from a Show menu in their heading (`significant_n`), which says how many it lists and returns to 10 on Reset; it changes neither the top countries' and regions' bars (`top_n`) nor which events the Latest and Largest orders pick before the limit |
| REQ-DASH-030 | The summary's Countries card names the country when one is chosen in the country menu, as the menu shows it (`US-` for US territories), and says `Unknown country` for Unknown; with All it says how many countries the events are in (`1 country`, `4 countries`), Unknown counting as one |
| REQ-DASH-031 | A Tsunami box after Events in the filter bar, the same size as the Magnitude box, has a Flagged only checkbox (`tsunami_only`, off by default and after Reset): checked, every report keeps only the events USGS flags for tsunami, the menus' counts included |
| REQ-DASH-032 | The page's footer shows the build it was made from (`git describe --tags --always --dirty`: `v1.0.0`, or `v1.0.0-4-g790aa4b` after it) as a link to it on GitHub: a tag's release, any other build's commit; without git, `Earth-Hub on GitHub` and the repository |
| REQ-DASH-033 | The page's footer credits the U.S. Geological Survey as the data's source, with a link, and says Earth-Hub is an independent research project, not affiliated with or endorsed by USGS and not a complete or exact copy of its service; that events may be revised, late, or missing or inaccurate if any part of the system fails; that city, region and country are derived by Earth-Hub; that it comes without warranty; and that it is not for emergency or safety decisions, linking to USGS's own map. The README says the same |

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

## Observability

| ID | Requirement |
| --- | --- |
| REQ-OBS-001 | Every pipeline log line has a `stage` (`fetch`, `parse`, `publish`, `persist`), and every line about one event has its `event_id`: the poller logs each event it publishes, and the consumer each rejection and error (in batches; every message when one at a time), with the event's Kafka position |
| REQ-OBS-002 | The consumer logs a summary every minute (configurable): outcome counts since the last summary and since start, and its lag per partition; the poller's round line keeps totals since start |
| REQ-OBS-003 | An event the pipeline rejects can be diagnosed from the logs alone: filtering on its event ID shows it published and then rejected, with the rule and reason, and the consumer's summary counts it and shows the lag back at 0 |
| REQ-OBS-004 | The live consumer handles `earthquakes.raw` one message at a time, so the outcome of every live event is logged; only the backfill consumer takes batches (DEF-036) |

## Operations and quality

| ID | Requirement |
| --- | --- |
| REQ-OPS-001 | The infrastructure starts healthy with one command: Kafka (KRaft), the `earthquakes.raw` topic with 3 partitions, and PostgreSQL 18 |
| REQ-OPS-002 | Data in Kafka and PostgreSQL survives a restart of both |
| REQ-OPS-003 | Configuration is validated at startup, and passwords never appear in logs, test output or API responses |
| REQ-OPS-004 | Logs are one JSON object per line, with extra fields such as event ID and stage |
| REQ-OPS-005 | The poller's, consumers' and catch-up's health checks follow their heartbeats |
| REQ-OPS-006 | Requests to USGS identify Earth-Hub in the `User-Agent`, with an optional operator contact |
| REQ-OPS-007 | The release script (`make release`) asks before every step that changes the repository, GitHub or its secrets, needs `public` typed in full to change the visibility, stops at a no having changed nothing, resumes at the step it stopped at, and passes secrets only on standard input |
| REQ-QA-001 | Unit tests never use the network, the live USGS feed included |
| REQ-QA-002 | Test data is deterministic: results do not depend on live earthquakes |
| REQ-QA-003 | `make test` and `make coverage` run exactly the regression's unit layer, which needs no Docker, network or browser (DEF-039) |

## Continuous integration

`.github/workflows/ci.yml`; how it works is in [ci.md](../technical/ci.md).

| ID | Requirement |
| --- | --- |
| REQ-CI-001 | Every pull request and push to `main` runs lint and the unit tests, and the regression of every layer but performance followed by the traceability check, on a clean runner from the example settings, with no secrets |
| REQ-CI-002 | Each run keeps its reports (JUnit, coverage, traceability matrix, regression summary) as artifacts for 30 days and shows the regression summary on the run's page |
| REQ-CI-003 | A layer left out with `regression.py --skip` is reported as skipped and does not fail the run; requirements covered only by its tests are listed as not run; any layer that runs and fails, or a requirement left without a passing test otherwise, still fails it |
| REQ-CI-004 | The workflow's actions are pinned to commit SHAs and its token can only read the repository |
| REQ-CI-005 | A second workflow runs direct mode's rounds and its daily reconciliation only when asked (by the dashboard Worker's triggers, REQ-CI-006, or by hand), with no schedule of its own, so GitHub's 60-day inactivity rule never disables it (DEF-056): one run at a time, never cancelled, catch-up intervals shorter than the schedule, actions pinned to commit SHAs, a read-only token, and R2 credentials from an `r2` environment, given only to the step that runs the command; no other workflow uses secrets |
| REQ-CI-006 | The dashboard Worker's cron triggers start one direct round every hour (at :17) and the daily reconcile (at 05:41 UTC) by asking GitHub to run `direct.yml`'s job on `main`, with a token kept as a Worker secret, never in the configuration; without a token it asks nothing, and a refusal fails the trigger (DEF-052) |
| REQ-CI-007 | Once a day (the 08:17 UTC trigger) the Worker reads the token's expiry from GitHub's reply and passes the days left to the workflow; its token job, with no secrets, fails within 14 days, so GitHub e-mails the token's owner to run `make trigger-token`, while the round still runs; the Worker logs a warning too, and `make trigger-token` prints the new token's expiry |
| REQ-CI-008 | CI runs the regression in parallel parts: the layers that need Docker in one job, the dashboard layer split in two shards that together run each of its tests once; the check a merge requires merges the parts' results, traces and coverage and checks traceability over all of them, runs unless the run was cancelled, and fails when a part, or a shard of one, failed or never reported |
| REQ-CI-009 | A watchdog workflow, the repository's only schedule (every 3 hours), fails when no `direct.yml` run was started in the last 2 hours, saying to check the trigger and its token; it holds no secrets and its token can only read the repository and its Actions runs |
| REQ-CI-010 | `make trigger-check` says whether the Worker's deployed GitHub token works, without starting anything: it runs the Worker on Cloudflare with its deployed secrets and fires the handler's `check`, which reads `direct.yml` with the token and logs the days left; a refused or missing token fails it. The token never leaves Cloudflare, and no deployed trigger and no HTTP path reaches the check |

## Documentation

| ID | Requirement |
| --- | --- |
| REQ-DOC-001 | Every relative link in the README and in `docs/` points to a file that exists, and every `#anchor` to a heading in it |
| REQ-DOC-002 | Every request in `docs/technical/api.md` returns exactly the documented status, headers and body on the documented dataset |
| REQ-DOC-003 | Every query in `docs/technical/sql-examples.md`, the dashboard's reports and the PostgreSQL ones, gives exactly the documented result on its fixed dataset |
| REQ-DOC-004 | `docs/technical/schema.md` lists every column of every table, in order, with the type and nullability the models define |
| REQ-DOC-005 | The README's counts of requirements and defects match `docs/progress/requirements.md` and the defect log |
| REQ-DOC-006 | The cheat sheet (`docs/overview/cheatsheet.md`) lists every Claude Code skill in `.claude/skills/`; the cheat sheet and the skills name only skills and make targets that exist; the README and `docs/` name only skills that exist, with arguments those skills take |
