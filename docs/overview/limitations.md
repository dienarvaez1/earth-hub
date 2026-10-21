# Known limitations

What Earth-Hub doesn't do, or does only within limits. These are deliberate trade-offs for a local-first, single-machine project, not defects; [the defect log](../progress/defects.md) lists what went wrong and was fixed.

## Data

- **City, region and country are derived from USGS's `place` text** (`validation/places.py`). Text that doesn't follow "<distance> of <city>, <area>", such as `Fiji region`, gives none, and the dashboard groups those events as `Unknown`. Only the codes seen so far are translated (`MX`, `B.C.`, and the US state codes). `place` itself is always kept unchanged.
- **Magnitude statistics mix scales** (`ml`, `mb`, `mww`, …), as USGS reports them. `by_magnitude_type` says which ones went into a figure.
- **FDSN ignores milliseconds** in query times. Backfill windows are whole seconds, and events are fetched by the window they fall in, not by an exact time.
- **Deletions without an `id` are dropped before Kafka**, logged and counted, but never stored (REQ-BF-014). Almost all name no event; a rare one names it only in `ids`, and would leave that event visible if it had been stored. None so far has been.
- **USGS deletes duplicates** after merging them. Earth-Hub keeps the event that absorbed a duplicate visible (DEF-026), but the rules for this come from cases seen live, not from a USGS specification.

## Freshness

- **Live events arrive 1–2 minutes** after USGS publishes them: the feed is regenerated every minute, and the poller waits for its `Expires` time.
- **Revisions and outages up to 30 days** are caught up hourly, from the `all_month` feed. Anything older needs `make backfill` (`/eh-backfill`).
- **Deletions arrive within a day,** with catch-up's daily deleted-events check. Until then, a deleted event is still served.
- **The dashboard shows the last export,** not the database: `make export-r2` (`/eh-export run`) runs on demand only, with no schedule.

## Processing guarantees

- **At-least-once, not exactly-once.** A message can be delivered more than once, for example after a crash between the database commit and the Kafka offset commit. Its effect is idempotent: the offset, or the revision's `updated` time, is recognized. Earth-Hub doesn't claim exactly-once processing, and nothing in it depends on it.
- **USGS's `updated` time is trusted** to order revisions. Two different revisions with the same `updated` time would count as duplicates, and the first one stored would stay.
- **The envelope has one version** (`schema_version` 1). A message of any other version is rejected (`ENV-002`) and kept, rather than read with rules it wasn't written for.

## Direct mode

Milestone 8 ([architecture](../technical/architecture.md#direct-mode)).
- **One direct process at a time.** The marker and the lock keep direct mode and full mode's exports apart, but nothing stops two `make direct-run` processes on two machines, or one and the [scheduled workflow](../technical/ci.md#direct-mode-on-a-schedule): both would poll the feed, and take turns writing. Run one.
- **On the schedule, events are up to an hour late**, and GitHub may delay or skip a scheduled run when busy. Nothing is lost while the gap stays under a day (the day feed); catch-up covers revisions of older events.
- **Events are late during a handover**, by its length: neither mode polls while it runs (the next round catches up, from the day feed). A handover to direct mode is mostly its export, about 35 s with few changes; one back to full mode, the replay and the checks.
- **Full mode's pipeline is not stopped by the marker.** `make mode-direct` stops the poller and catch-up; started again by hand in direct mode, they would store events in PostgreSQL that the replay then repeats. The handover back would find the processing times differ and stay in direct mode.
- **After a return to full mode, the poller sends the day's events again.** Its state is rebuilt from `earthquakes.raw`, which did not see the direct period. The consumer finds them already stored (duplicates): nothing changes, but `raw_events` gets the rows.
- **The handover to full mode needs the log.** Without it (a gap), the fallback is a backfill of the period by hand, and the processing times of that period are then the backfill's.
- **The log grows** by about 3 MB a day at today's load, about 1 GB a year, inside R2's 10 GB of free storage for years; nothing prunes it yet.
- **About 10 R2 writes per live round**, 16% of the free monthly Class A operations at today's load. Merging the state and the ID map into one object would save one write a round.
- **Processing times differ between the modes when each runs on its own clock.** The parity tests compare them exactly only with the same `now` given to both; the handover carries direct mode's into PostgreSQL through the log.

## Dashboard

- **It's a preview.** It's marked "Preview — in development" and asks search engines not to index it, until the release.
- **Two third parties:** the DuckDB-WASM engine comes from jsDelivr, since its 35 MB file is over a Worker's limit, and the map tiles from OpenFreeMap. If either is unavailable, the page or its map is too. Both see visitors' IP addresses.
- **DuckDB-WASM is a development release** (`1.33.1-dev65.0`), because the newest stable one fails in current Chromium (DEF-032). The browser tests compare its answers with Python's DuckDB on every report.
- **Tested in Chromium only.** Firefox and Safari aren't covered by the automated tests.
- **The first map takes about 4 seconds** on a cold visit, mostly the browser compiling DuckDB's engine. At 1,000,000 rows, a filter change takes about 0.9 s.

## Scale and deployment

- **One machine.**
  - **Kafka:** a single broker with replication factor 1.
  - **PostgreSQL:** a single instance with no replica.
  - **Backups:** none, apart from what Docker volumes keep.

  Losing the machine's disk loses the data. It can be backfilled again from USGS, but rows' `first_seen_at` times can't be recovered.
- **No authentication on Kafka, JMX or the API.** All three are bound to `127.0.0.1`, and the API is read-only. Exposing any of them beyond the machine would need authentication, TLS and rate limiting first.
- **The API pages with `offset`** (up to 10,000,000), and counts `total` on every request. Deep pages get slower, so the export endpoint pages by `id` instead. Under load (`make load`: 50 users, 140,000 events), the unfiltered list's p95 is about 230 ms and a deep page's about 730 ms, against 33 ms for a lookup by ID.
- **The load test runs on one machine:** Locust, the API and PostgreSQL share it, so its figures are relative, a baseline to catch regressions, not a capacity for a deployment.
- **The hourly summary covers at most 31 days** (744 buckets).

## Testing and CI

- **The performance layer runs only on a developer machine.** Shared CI runners can't repeat its timings within ±15%, so CI reports it as skipped, and its three requirements as not run (REQ-BF-011, REQ-DASH-008, REQ-API-014).
- **CI shows a failing check, but can't block a merge yet.** On GitHub's free plan, a branch can be protected only in a public repository; making it public and protecting `main` are part of the release.
- **The live feed is never part of the automated tests.** `make ingest` and `make reconcile` check the real feed by hand.
- **Contract testing samples the inputs.** Schemathesis generates 60 requests per endpoint by default (`CONTRACT_EXAMPLES=1000` for a deeper run), which isn't an exhaustive search.
- **No dependency or secret scanning in CI.** A secret scan of the whole history is part of the release audit.

## Not yet done

The release is deferred. These are planned, but not in place:
- a license;
- the public repository;
- branch protection requiring CI;
- `USGS_CONTACT` set to the repository's address;
- removing the dashboard's preview marking.

See [the implementation plan](../progress/plan.md#milestone-7--cicd-documentation-and-portfolio-release).
