# Known limitations

What Earth-Hub doesn't do, or does only within limits. These are deliberate trade-offs for a local-first, single-machine project, not defects; [the defect log](../progress/defects.md) lists what went wrong and was fixed.

## Processing guarantees

- **At-least-once, not exactly-once.** A message can be delivered more than once, for example after a crash between the database commit and the Kafka offset commit. Its effect is idempotent: the offset, or the revision's `updated` time, is recognized. Earth-Hub doesn't claim exactly-once processing, and nothing in it depends on it.
- **USGS's `updated` time is trusted** to order revisions. Two different revisions with the same `updated` time would count as duplicates, and the first one stored would stay.
- **The envelope has one version** (`schema_version` 1). A message of any other version is rejected (`ENV-002`) and kept, rather than read with rules it wasn't written for.

## Freshness

- **Live events arrive 1–2 minutes** after USGS publishes them: the feed is regenerated every minute, and the poller waits for its `Expires` time.
- **Revisions and outages up to 30 days** are caught up hourly, from the `all_month` feed. Anything older needs `make backfill` (`/eh-backfill`).
- **Deletions arrive within a day,** with catch-up's daily deleted-events check. Until then, a deleted event is still served.
- **The dashboard shows the last export,** not the database: `make export-r2` (`/eh-export run`) runs on demand only, with no schedule.

## Scale and deployment

- **One machine.**
  - **Kafka:** a single broker with replication factor 1.
  - **PostgreSQL:** a single instance with no replica.
  - **Backups:** none, apart from what Docker volumes keep.

  Losing the machine's disk loses the data. It can be backfilled again from USGS, but rows' `first_seen_at` times can't be recovered.
- **No authentication on Kafka, JMX or the API.** All three are bound to `127.0.0.1`, and the API is read-only. Exposing any of them beyond the machine would need authentication, TLS and rate limiting first.
- **The API pages with `offset`** (up to 10,000,000), and counts `total` on every request. Deep pages get slower, so the export endpoint pages by `id` instead.
- **The hourly summary covers at most 31 days** (744 buckets).

## Data

- **City, region and country are derived from USGS's `place` text** (`validation/places.py`). Text that doesn't follow "<distance> of <city>, <area>", such as `Fiji region`, gives none, and the dashboard groups those events as `Unknown`. Only the codes seen so far are translated (`MX`, `B.C.`, and the US state codes). `place` itself is always kept unchanged.
- **Magnitude statistics mix scales** (`ml`, `mb`, `mww`, …), as USGS reports them. `by_magnitude_type` says which ones went into a figure.
- **FDSN ignores milliseconds** in query times. Backfill windows are whole seconds, and events are fetched by the window they fall in, not by an exact time.
- **USGS deletes duplicates** after merging them. Earth-Hub keeps the event that absorbed a duplicate visible (DEF-026), but the rules for this come from cases seen live, not from a USGS specification.

## Dashboard

- **It's a preview.** It's marked "Preview — in development" and asks search engines not to index it, until the release.
- **Two third parties:** the DuckDB-WASM engine comes from jsDelivr, since its 35 MB file is over a Worker's limit, and the map tiles from OpenFreeMap. If either is unavailable, the page or its map is too. Both see visitors' IP addresses.
- **DuckDB-WASM is a development release** (`1.33.1-dev65.0`), because the newest stable one fails in current Chromium (DEF-032). The browser tests compare its answers with Python's DuckDB on every report.
- **Tested in Chromium only.** Firefox and Safari aren't covered by the automated tests.
- **The first map takes about 4 seconds** on a cold visit, mostly the browser compiling DuckDB's engine. At 1,000,000 rows, a filter change takes about 0.9 s.

## Testing and CI

- **The performance layer runs only on a developer machine.** Shared CI runners can't repeat its timings within ±15%, so CI reports it as skipped, and its two requirements as not run (REQ-BF-011, REQ-DASH-008).
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
