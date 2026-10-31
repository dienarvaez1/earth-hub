# Known limitations

What Earth-Hub doesn't do, or does only within limits. These are deliberate trade-offs for a local-first, single-machine project, not defects; [the defect log](../progress/defects.md) lists what went wrong and was fixed.

## Data

- **City, region and country are derived from USGS's `place` text** (`validation/places.py`). Text that doesn't follow "<distance> of <city>, <area>", such as `Fiji region`, gives none, and the dashboard groups those events as `Unknown`. Only the codes seen so far are translated (`MX`, `B.C.`, and the US state codes). `place` itself is always kept unchanged.
- **Magnitudes below −2 are rejected** (`MAG-001`, range −2 to 10.5), kept at −2 on 2026-10-31: the events below it are microquakes at noise level, 72 from January 2024 to October 2026, nearly all from the Oklahoma network (`ok`, −2.01 to −2.31). They are recorded as rejected, never stored, and `make reconcile-usgs` explains them as `rejected here`, with the rule.
- **Magnitude statistics mix scales** (`ml`, `mb`, `mww`, …), as USGS reports them. `by_magnitude_type` says which ones went into a figure.
- **FDSN ignores milliseconds** in query times. Backfill windows are whole seconds, and events are fetched by the window they fall in, not by an exact time.
- **Deletions without an `id` are dropped before Kafka**, logged and counted, but never stored (REQ-BF-014). Almost all name no event; a rare one names it only in `ids`, and would leave that event visible if it had been stored. None so far has been.
- **USGS can split what it once merged; Earth-Hub doesn't.** A record keeps every ID it was ever given, so when USGS later lists them as two events, Earth-Hub still holds one: on 2026-10-20 the M6.6 `us6000u3jl` and `pt26293000`. `make reconcile-usgs` reports it as `split at USGS`, explained: a known limitation, accepted on 2026-10-31 (5 cases from January 2024 to October 2026 in about 414,000 events), since undoing a merge would change the ledger and both stores' persistence rules; nothing undoes the merge.
- **The same revision can carry different times.** For `uw` events FDSN's `updated` is 5 to 9 s later than the summary feeds'. `make reconcile-usgs` treats times within 60 s as one revision; a backfill over events the live feed stored can apply such a revision again (no data changes, `revision_count` goes up by one).
- **A record's key is the first of its IDs Earth-Hub sees.** When two stored records merge, the smallest key survives, whatever order they arrived in (DEF-050). But if a report already lists another ID before that ID has a record of its own, no merge happens, and the key depends on which arrived first: full mode reads Kafka's partitions in no fixed order between them, direct mode the feed's order. Lookups by any ID are unaffected.
- **USGS deletes duplicates** after merging them. Earth-Hub keeps the event that absorbed a duplicate visible (DEF-026), but the rules for this come from cases seen live, not from a USGS specification.

## Freshness

- **Live events arrive 1–2 minutes** after USGS publishes them: the feed is regenerated every minute, and the poller waits for its `Expires` time.
- **Revisions and outages up to 30 days** are caught up hourly, from the `all_month` feed. Older revisions and additions arrive within a day, with catch-up's daily history pass over all stored history (DEF-067); an outage longer than the time since its last pass needs `make backfill` (`/eh-backfill`).
- **Deletions arrive within a day,** with catch-up's daily deleted-events check, over all stored history. Until then, a deleted event is still served. A deletion FDSN keeps no time for (rare: 3 in 2026's 117,000 events) arrives only when the daily audit reaches its month, within about 5 weeks (DEF-066).
- **The dashboard shows the last export,** not the database: `make export-r2` (`/eh-export run`) runs on demand only, with no schedule.

## Processing guarantees

- **At-least-once, not exactly-once.** A message can be delivered more than once, for example after a crash between the database commit and the Kafka offset commit. Its effect is idempotent: the offset, or the revision's `updated` time, is recognized. Earth-Hub doesn't claim exactly-once processing, and nothing in it depends on it.
- **USGS's `updated` time is trusted** to order revisions. Two different revisions with the same `updated` time would count as duplicates, and the first one stored would stay.
- **The envelope has one version** (`schema_version` 1). A message of any other version is rejected (`ENV-002`) and kept, rather than read with rules it wasn't written for.

## Direct mode

Milestone 8 ([architecture](../technical/architecture.md#direct-mode)).
- **One direct process at a time.** The marker and the lock keep direct mode and full mode's exports apart, but nothing stops two `make direct-run` processes on two machines, or one and the [scheduled workflow](../technical/ci.md#direct-mode-on-a-schedule): both would poll the feed, and take turns writing. Run one.
- **In direct mode, events are up to four hours late**: a Cloudflare cron trigger starts a round every 4 hours (00:05, 04:05, … UTC), since GitHub's own schedule skipped runs (DEF-052); it could run as often as every 15 minutes. The only GitHub schedule left is the watchdog's, which GitHub switches off after 60 days without commits; the rounds and the daily reconcile go on, as Cloudflare starts them (DEF-056). Nothing is lost while the gap stays under a day (the day feed); catch-up covers revisions of older events.
- **Events are late during a handover**, by its length: neither mode polls while it runs (the next round catches up, from the day feed). A handover to direct mode is mostly its export, about 35 s with few changes; one back to full mode, the replay and the checks.
- **Full mode's pipeline is not stopped by the marker.** `make mode-direct` stops the poller and catch-up; started again by hand in direct mode, they would store events in PostgreSQL that the replay then repeats. The handover back would find the processing times differ and stay in direct mode.
- **After a return to full mode, the poller sends the day's events again.** Its state is rebuilt from `earthquakes.raw`, which did not see the direct period. The consumer finds them already stored (duplicates): nothing changes, but `raw_events` gets the rows.
- **The handover to full mode needs the log.** Without it (a gap), the fallback is a backfill of the period by hand, and the processing times of that period are then the backfill's.
- **The log grows** by about 3 MB a day at today's load, about 1 GB a year, inside R2's 10 GB of free storage for years; nothing prunes it yet.
- **About 10 R2 writes per live round**, 16% of the free monthly Class A operations at today's load. Merging the state and the ID map into one object would save one write a round.
- **Processing times differ between the modes when each runs on its own clock.** The parity tests compare them exactly only with the same `now` given to both; the handover carries direct mode's into PostgreSQL through the log.

## Dashboard

- **Not in search engines.** The page asks search engines not to index it: it's shared by link, not promoted.
- **Two third parties:** the DuckDB-WASM engine comes from jsDelivr, since its 35 MB file is over a Worker's limit, and the map tiles from OpenFreeMap. If either is unavailable, the page or its map is too. Both see visitors' IP addresses.
- **DuckDB-WASM is a development release** (`1.33.1-dev65.0`), because the newest stable one fails in current Chromium (DEF-032). The browser tests compare its answers with Python's DuckDB on every report.
- **Tested in Chromium only.** Firefox and Safari aren't covered by the automated tests.
- **Counts by place follow seismic networks.** The United States records earthquakes too small to feel, most of the world only from about magnitude 4.5, so the top countries and regions say more about network density than about how often the ground shakes; a magnitude band from 2.5 up compares places more fairly. The dashboard's notes explain what the bars are, not this.
- **The first map takes about 4 seconds** on a cold visit, mostly the browser compiling DuckDB's engine, and every visitor downloads the whole snapshot first (18.1 MB for the 424,000 events since 2024, with zstd at level 19; 24.8 MB at its default level). Reading only the 14 columns the page uses would halve that, but over about 56 range requests to R2, one after another, so the page still reads the whole file. At 1,000,000 rows, a filter change takes about 1 s.
- **Above 25,000 events the map shows counts, not events,** until zoomed in: cells of 0.25 degrees, with only the events of magnitude 6 or more as their own points. From zoom level 4 the visible area's events come one by one, with their popups, once it holds at most 25,000; a wider view stays in cells.

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

- **The performance layer runs only on a developer machine.** Shared CI runners can't repeat its timings within ±15%, so CI reports it as skipped, and its requirements as not run (REQ-BF-011, REQ-DASH-008, REQ-API-014).
- **The live feed is never part of the automated tests.** `make ingest` and `make reconcile` check the real feed by hand.
- **Contract testing samples the inputs.** Schemathesis generates 60 requests per endpoint by default (`CONTRACT_EXAMPLES=1000` for a deeper run), which isn't an exhaustive search.
- **Dependency and secret scanning are GitHub's, not CI's.** Since 2026-10-29 GitHub scans every push for secrets and refuses one that carries a recognizable secret (secret scanning, push protection), and Dependabot alerts on known vulnerabilities in `uv.lock` and `dashboard/package-lock.json`; CI itself runs neither. A full-history scan (gitleaks) found no secrets on 2026-10-29. Dependabot's first alerts were fixed in #56. One was `undici`, in Wrangler's local server, pinned with an npm `overrides` entry until Wrangler ships a fixed version.
- **Three early commits carry the author's personal email** (`cc1a788`, `4d2748f`, `620b8c7`, 2026-10-16); later ones use GitHub's noreply address. Kept on purpose: every pull request's head on GitHub contains them, and only GitHub Support can remove those, so rewriting `main` would have changed every commit's SHA without taking the address back.
