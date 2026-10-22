# Demo walkthrough

A guided tour of about 10 minutes, for a reviewer with a fresh clone. It covers starting Earth-Hub, running the tests, ingesting the live feed, inspecting what was stored and why, querying the API, then breaking something on purpose and checking that nothing is lost. Each step says what to look for, and the [Claude Code skill](cheatsheet.md) that does the same: type it in Claude Code instead of the commands.

You need what [the quick start](../../README.md#quick-start) lists: Docker, Python 3.13 through uv, GNU Make, and `jq` for the log recipes (`brew install jq`). Step 3 onwards needs the internet, to reach USGS.

## 1. Start the infrastructure (2 minutes)

```bash
cp .env.example .env     # the defaults work; change POSTGRES_PASSWORD if you like
make install             # .venv from uv.lock
make up                  # Kafka (KRaft) and PostgreSQL, healthy, with both topics
make migrate             # the schema
make smoke               # Kafka and PostgreSQL work end to end
```

**With Claude Code:** `/eh-start` does the same, and `/eh-status` shows what's running.

**Look for:** `make up` returns only once both services are healthy and both topics exist; `make smoke` passes. Nothing has called USGS yet: `make up` never starts the pipeline.

## 2. Run the tests (1 minute, or 15)

```bash
make test                # the unit tests: about 600 of them in under a minute, with no Docker or network
```

**With Claude Code:** `/eh-test` (unit), or `/eh-test regression`.

**Look for:** the unit tests can't reach the network at all. A guard blocks everything but loopback, and a test proves it (`tests/unit/test_network_guard.py`).

To run everything, use `make regression`. That's every layer in order, including the end-to-end, browser and outage tests, and takes about 15 minutes (the dashboard layer needs `make dashboard-install` first). Afterwards, open:
- `reports/regression-summary.md`: each layer, its counts and coverage;
- `docs/progress/traceability-matrix.md`: every requirement, with the tests that verify it;
- `reports/coverage-html/index.html`: coverage across all layers.

CI runs the same script on every pull request ([docs/technical/ci.md](../technical/ci.md)).

## 3. Ingest the live feed (2 minutes)

```bash
make ingest              # migrate, poller, both consumers and catch-up, in containers
make ingest-logs         # Ctrl-C to stop following; the services keep running
```

**With Claude Code:** `/eh-start pipeline`, then `/eh-logs`.

**Look for:**
- **The poller's first round** publishes the past day's events, around 200 to 400 of them, at `stage` `publish`.
- **Later rounds** publish only what changed: `"published": 0, "unchanged": 209` is normal.
- **The consumer's summary every minute** shows its `outcomes` and its `lag` behind Kafka.
- **Every line is JSON** ([observability](../technical/architecture.md#observability)):

```bash
docker compose logs --no-log-prefix poller | jq -cR 'fromjson? | select(.message == "round complete") | {ts, published, unchanged}'
docker compose logs --no-log-prefix consumer | jq -cR 'fromjson? | select(.message == "consumer summary") | {ts, lag, outcomes}'
```

**With Claude Code:** `/eh-poller` for the rounds, `/eh-logs summary` for the consumer.

To follow one event, take an ID from a poller line and select on it:

```bash
ID=$(docker compose logs --no-log-prefix poller | jq -rR 'fromjson? | select(.message == "event published") | .event_id' | tail -1)
docker compose logs --no-log-prefix poller consumer | jq -cR --arg id "$ID" 'fromjson? | select(.event_id == $id)'
```

**With Claude Code:** `/eh-logs <event ID>`.

**Look for:** the poller's `event published` (`stage` `publish`), then the consumer's line with its `outcome` and Kafka position (`stage` `persist`). The backfill consumer works in batches and logs only rejections and errors per event, plus a `batch processed` line per batch.

## 4. Inspect what was stored, and why (2 minutes)

```bash
docker compose exec postgres psql -U earth_hub_app earth_hub
```

Try a few of the [SQL examples](../technical/sql-examples.md#inspecting-postgresql). On live data the numbers are yours; the page shows the results on a fixed dataset.

```sql
SELECT outcome, count(*) AS messages FROM raw_events GROUP BY outcome ORDER BY messages DESC;
SELECT kafka_offset, event_id, rule, reason FROM raw_events WHERE outcome = 'rejected' LIMIT 5;
SELECT preferred_event_id, magnitude, place, revision_count FROM earthquakes
ORDER BY last_applied_at DESC LIMIT 5;
```

**With Claude Code:** `/eh-db outcomes`, `/eh-db rejected`, `/eh-db latest`, or `/eh-db` with your own SELECT.

**Look for:**
- **Every Kafka message has a `raw_events` row** with its outcome, rejected ones included, kept whole in `envelope`.
- **`duplicate` rows are expected:** the poller re-reads the feed, and catch-up overlaps it.
- **`revision_count` above 1** means USGS revised the event and Earth-Hub applied it. It applies a revision only if its `updated` time is newer.

The tables are explained in [the schema](../technical/schema.md). Then check the whole topic against the database:

```bash
make reconcile
```

**With Claude Code:** `/eh-status reconcile`.

**Look for:** `lost (committed but not stored): 0`, `invariants: all 5 hold` and `RESULT: OK`, for each topic. A message the consumer hasn't reached yet counts as *pending*, not lost.

## 5. Query the API (1 minute)

```bash
make api                 # http://127.0.0.1:8000/docs for the interactive docs
curl -s 'http://127.0.0.1:8000/api/v1/health' | jq
curl -s 'http://127.0.0.1:8000/api/v1/earthquakes?min_magnitude=4&sort=magnitude&limit=3' | jq '.items[] | {event_id, magnitude, place, event_time}'
curl -s 'http://127.0.0.1:8000/api/v1/earthquakes/summary/magnitude' | jq -c '.buckets[] | [.label, .count]'
curl -s 'http://127.0.0.1:8000/api/v1/earthquakes?limit=501&start_time=yesterday' | jq
```

**With Claude Code:** `/eh-start api`, then `/eh-api health`, `/eh-api list min_magnitude=4&sort=magnitude&limit=3`, `/eh-api magnitude`.

**Look for:**
- **One error format,** listing every invalid parameter at once.
- **Empty hours and magnitude ranges are listed** in the summaries.
- **A former USGS ID still finds its event.** Every endpoint, with examples replayed by a test, is in [the API reference](../technical/api.md).

## 6. Break something (2 minutes)

Stop PostgreSQL while the pipeline runs, then bring it back:

```bash
docker compose stop postgres
curl -s -w ' %{http_code}\n' http://127.0.0.1:8000/api/v1/health      # 503: the database is unavailable
curl -s -w ' %{http_code}\n' 'http://127.0.0.1:8000/api/v1/earthquakes?limit=0'   # still 422: validated first
docker compose logs --no-log-prefix --since 2m consumer | jq -cR 'fromjson? | select(.level != "INFO") | {ts, level, message}'
docker compose start postgres
make reconcile
```

**With Claude Code:** `/eh-status` shows the unhealthy consumer during the outage; `/eh-test disruptive` runs the automated version.

**Look for:**
- **During the outage,** the health endpoint answers `503`, and an invalid request is still answered `422` (DEF-010).
- **The consumer** logs `database unavailable` with a growing retry delay, up to 30 s. It never rejects a message because of the outage, and its health check turns unhealthy (`docker compose ps`).
- **After PostgreSQL is back,** it carries on where it stopped, and `make reconcile` reports nothing lost and nothing duplicated.

The automated version of this, with events arriving during the outage, is `tests/disruptive/test_outages.py` (`make check-db-outage`).

## 7. The dashboard (optional)

The dashboard is built only from an export of the table to Cloudflare R2. It needs R2 credentials in `.env` and a Cloudflare account, so it's optional here:

```bash
make export-r2 DRY_RUN=1 # what would change, writing nothing
make export-r2           # upload new and changed rows, then the Parquet snapshot
make dashboard-install && make dashboard-dev     # http://127.0.0.1:8787, over the real bucket
```

**With Claude Code:** `/eh-export` (a dry run), `/eh-export run`, `/eh-dashboard dev`, `/eh-dashboard test`; `/eh-dashboard` checks the live page.

Without an account, `make test-dashboard` runs the page in Chromium against a simulated R2, offline. It compares the browser's answers with Python's on every report. The page's queries, with results, are in [the SQL examples](../technical/sql-examples.md#dashboard-reports).

## 8. Direct mode (optional)

The same bucket can be written without Docker: [direct mode](../technical/architecture.md#direct-mode) runs the poller and the consumer's rules straight to R2, and GitHub Actions runs it every hour. The handovers write the real bucket, so try them only on your own:

```bash
make mode                # full (active): this machine's export writes the bucket
make mode-direct         # hand over: stops the poller and catch-up, exports, switches, catches up, checks against USGS
make direct-once         # one round from this machine (or let the 15-minute schedule run)
make reconcile-direct    # the bucket's state agrees with itself
make reconcile-usgs      # and with USGS: every difference pending, or flagged
make catchup-now         # catch up now, without waiting for the schedule
make mode-full           # replay direct mode's log into Kafka and PostgreSQL, check, switch back
make reconcile           # and full mode has every event direct mode processed
```

**With Claude Code:** `/eh-mode`, `/eh-mode direct`, `/eh-mode once`, `/eh-mode usgs`, `/eh-mode catchup`, `/eh-mode full`.

## 9. Clean up

```bash
make api-stop
make ingest-stop         # stop the pipeline, keep the data
make down                # stop Kafka and PostgreSQL, keep the data
make reset               # or: stop them and delete the data
```

**With Claude Code:** `/eh-stop api`, `/eh-stop`, `/eh-stop all`, or `/eh-stop reset` (which asks first).

## Where to go next

- **[The README](../../README.md):** what was built, and how the quality work was organized.
- **[The test strategy](../progress/test-strategy.md) and [the defect log](../progress/defects.md):** how it was tested, and what the testing found.
- **[The architecture](../technical/architecture.md) and [the known limitations](limitations.md):** how it fits together, and what it doesn't do.
