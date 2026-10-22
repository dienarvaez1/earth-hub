# Earth-Hub cheat sheet

The everyday commands, each next to the Claude Code skill that runs it for you. Type a skill in Claude Code (for example `/eh-kafka lag`), or copy the command into a terminal at the repository's root. The skills live in `.claude/skills/`; `/eh <area>` prints a section of this page.

The sections follow the data, as in the [architecture](../technical/architecture.md#overview): start the services, then USGS → poller → Kafka → consumer → PostgreSQL → API → export → R2 → dashboard, with direct mode's shortcut from USGS to R2 in section 9; then watching it run, the tests, and stopping.

Commands that **call live services or change state** are marked ⚠. The skills ask before the destructive ones.

**Which mode first.** `make mode` says who writes the R2 bucket. In **full** mode your machine runs the pipeline (Docker, Kafka, PostgreSQL) and `make export-r2` updates the dashboard. In **direct** mode GitHub Actions updates the bucket every hour with no machine of yours running, and full mode's poller, catch-up, backfill and export stay stopped. See [Modes](#9-modes-full-and-direct).

## Skills

| Skill | Does | Main commands behind it |
| --- | --- | --- |
| `/eh [area]` | Shows this cheat sheet, or one section of it | |
| `/eh-start [infra\|pipeline\|api\|ui\|dashboard\|all]` | Starts services and waits until healthy | `make up`, `make ingest`, `make api` |
| `/eh-usgs [feed\|event\|count\|query\|headers]` | Asks the live USGS services directly | `curl` on the feeds and FDSN |
| `/eh-poller [rounds\|once\|restart\|health]` | The poller's rounds, one manual poll, a restart | `make ingest-once` |
| `/eh-backfill [START [END]\|catchup\|progress]` | Loads history from FDSN in full mode, then reconciles and exports; catches up | `make backfill-full`, `make catchup-once` (or `make catchup-now`, in either mode) |
| `/eh-kafka [topics\|offsets\|lag\|read\|tail\|find]` | Topics, offsets, lag, and messages by offset or event ID | `kafka-*.sh` in the broker container |
| `/eh-db [counts\|outcomes\|rejected\|event ID\|merged\|latest\|strongest\|SQL]` | Read-only SQL on PostgreSQL | `psql` in the database container |
| `/eh-api [health\|list\|event\|stats\|hourly\|magnitude\|export]` | Calls the REST API (served from PostgreSQL) | `curl` on `/api/v1/…` |
| `/eh-export [dry\|run\|verify\|runs]` | Copies PostgreSQL's table to R2 through the API, and the dashboard's snapshot | `make export-r2` |
| `/eh-r2 [ls\|count\|get\|snapshot\|runs\|audit]` | Looks inside the R2 bucket, read-only, direct mode's audit trail included | `make r2-ls`, `make r2-get`, `make direct-runs` |
| `/eh-mode [status\|direct\|full\|usgs\|catchup\|once\|run\|reconcile\|schedule\|runs]` | The mode, switching modes in one command, the check against USGS and catching up, direct mode by hand or on GitHub Actions, and its executions (the audit trail of the hourly rounds the Cloudflare trigger starts) | `make mode`, `make to-direct`, `make to-full`, `gh workflow run direct.yml`, `make direct-runs`, `make reconcile-usgs`, `make catchup-now` |
| `/eh-dashboard [check\|dev\|build\|test\|deployments\|deploy\|trigger]` | The live page and its data, a local server, tests, deploys, and the Worker's hourly trigger | `make dashboard-dev`, `make dashboard-deploy`, `wrangler secret list` |
| `/eh-status [reconcile]` | Health of every service, consumer lag, newest event, API, last export, the mode and direct mode's latest executions | `docker compose ps`, `make reconcile`, `make reconcile-usgs`, `make mode`, `make direct-runs` |
| `/eh-logs [service\|ID\|errors\|rejected\|summary]` | Reads the JSON logs with `jq`, follows one event | `docker compose logs … \| jq` |
| `/eh-test [layer\|regression\|flake\|REQ-…\|keyword]` | Runs the tests by layer, or everything | `make test`, `make regression` |
| `/eh-stop [pipeline\|api\|ui\|all\|reset]` | Stops services, keeping data (or wipes it, after asking) | `make ingest-stop`, `make down`, `make reset` |

## 1. Start the services

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-start` | `make up && make migrate` | Kafka (KRaft) and PostgreSQL, both topics, the schema |
| `/eh-start pipeline` | `make ingest` ⚠ | migrate, poller, consumer, consumer-backfill, catch-up, on the live feed (full mode only) |
| `/eh-start api` | `make api` | The REST API: http://127.0.0.1:8000/docs |
| `/eh-start ui` | `make kafka-ui` | A read-only Kafka viewer: http://127.0.0.1:8080 |
| `/eh-start dashboard` | `make dashboard-dev` | The page, http://127.0.0.1:8787, over the real bucket |
| `/eh-start all` | `make ingest` ⚠, then `make api` | Everything but the UI and the dashboard (full mode only) |
| `/eh` | `make help` | Lists every make target |

## 2. USGS (live): the source

Always send a User-Agent: `UA="earth-hub (manual check)"`.

| Skill | Command | What it answers |
| --- | --- | --- |
| `/eh-usgs feed all_day` | `curl -s -A "$UA" https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson \| jq -c '.metadata \| {generated, count}'` | The feed the poller reads (also `all_hour`, `all_week`, `all_month`, `significant_week`, `4.5_day`) |
| `/eh-usgs feed significant_week` | `curl -s -A "$UA" https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_week.geojson \| jq -c '.features[] \| {id, mag: .properties.mag, place: .properties.place}'` | This week's significant events |
| `/eh-usgs event us7000abcd` | `curl -s -A "$UA" "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventid=us7000abcd" \| jq -c '{id, mag: .properties.mag, place: .properties.place, ids: .properties.ids}'` | One event as USGS has it now (`&includedeleted=true` for deleted ones) |
| `/eh-usgs count …` | `curl -s -A "$UA" "https://earthquake.usgs.gov/fdsnws/event/1/count?starttime=2026-10-19&endtime=2026-10-20&minmagnitude=4"` | How many events match, as backfill checks each week |
| `/eh-usgs query …` | `curl -s -A "$UA" "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2026-10-19&minmagnitude=5&orderby=magnitude&limit=10"` | A search by time, magnitude, place (`latitude`, `longitude`, `maxradiuskm`) |
| `/eh-usgs headers` | `curl -sI -A "$UA" https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson \| grep -iE '^(last-modified\|expires)'` | What the poller times its next round from |

## 3. Poller, backfill and catch-up: into Kafka

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-poller` | `docker compose logs --no-log-prefix --since 30m poller \| jq -cR 'fromjson? \| select(.message == "round complete") \| {ts, published, unchanged}'` | The last rounds: published, unchanged, rejected |
| `/eh-poller once` | `make ingest-once` ⚠ | One round from your machine, then exit |
| `/eh-poller health` | `docker compose exec -T poller python -m earth_hub.healthcheck poller` | 0 if a round succeeded within 3 poll intervals |
| `/eh-backfill 2026-01-01` | `make backfill-full START=2026-01-01 [END=2026-06-01]` ⚠ | History from FDSN in full mode, in one command: starts the pipeline, backfills (one week per request; re-runs skip finished weeks), waits for the consumer, reconciles, exports to R2 for the dashboard. Refused in direct mode |
| | `make backfill START=… [END=…]` ⚠ | The backfill alone, without the waiting, the reconcile and the export |
| `/eh-backfill catchup` | `make catchup-once` ⚠ | One `all_month` pass and one deleted-events check (full mode) |
| `/eh-mode catchup` | `make catchup-now` ⚠ | The same in whichever mode writes the bucket; in direct mode a round with the day feed too |
| `/eh-backfill progress` | `docker compose exec -T postgres psql -U earth_hub_app earth_hub -c "SELECT job, status, count(*), sum(fetched), sum(rejected) FROM backfill_windows GROUP BY 1, 2"` | Which weeks are done, and the features dropped before Kafka |
| `/eh-status reconcile` | `make reconcile` | Every Kafka message has a database row; 5 invariants hold |
| `/eh-mode usgs` | `make reconcile-usgs [DAYS=3]` | Earth-Hub against USGS itself: every difference pending (explained) or not ([how](../technical/architecture.md#reconciliation-with-usgs)) |

## 4. Kafka

Run the broker's own tools inside its container. With `k() { local tool; for tool; do break; done; shift; docker compose exec -T kafka "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@"; }`:

| Skill | Command | What it shows |
| --- | --- | --- |
| `/eh-kafka topics` | `k kafka-topics.sh --describe --topic earthquakes.raw` | Partitions, leader, replicas (also `--list`) |
| `/eh-kafka offsets` | `k kafka-get-offsets.sh --topic earthquakes.raw` | End offset per partition: messages ever written |
| `/eh-kafka lag` | `k kafka-consumer-groups.sh --describe --group earth-hub-consumer` | Each partition's committed offset and lag (`earth-hub-consumer-backfill` for the other topic) |
| `/eh-kafka read earthquakes.raw 0 42` | `k kafka-console-consumer.sh --topic earthquakes.raw --partition 0 --offset 42 --max-messages 1 --timeout-ms 10000 \| jq` | One message, by partition and offset |
| `/eh-kafka tail earthquakes.raw 5` | `k kafka-console-consumer.sh --topic earthquakes.raw --partition 0 --offset <end−5> --max-messages 5 --timeout-ms 10000 \| jq -c '{event_id, mag: .payload.properties.mag}'` | The latest messages of a partition |
| `/eh-kafka find us7000abcd` | `k kafka-console-consumer.sh --topic earthquakes.raw --partition 0 --offset earliest --timeout-ms 8000 --formatter-property print.offset=true \| grep -F '"event_id":"us7000abcd"'` (and partitions 1, 2) | Every message for one event |
| `/eh-start ui` | `make kafka-ui` | The same in a browser, read-only: http://127.0.0.1:8080 |

Read with `--partition`: it joins no consumer group, so it can't move the pipeline's offsets.

## 5. PostgreSQL: what the consumers stored

With `q() { docker compose exec -T postgres psql -U earth_hub_app earth_hub -c "$1"; }`, or interactively with `docker compose exec postgres psql -U earth_hub_app earth_hub`. More queries, with their results explained: [SQL examples](../technical/sql-examples.md#inspecting-postgresql).

| Skill | Command | What it shows |
| --- | --- | --- |
| `/eh-db` | `q "SELECT count(*), max(event_time) FROM earthquakes"` | Stored events and the newest |
| `/eh-db outcomes` | `q "SELECT kafka_topic, outcome, count(*) FROM raw_events GROUP BY 1, 2 ORDER BY 1, 3 DESC"` | What happened to every message |
| `/eh-db rejected` | `q "SELECT kafka_offset, event_id, rule, reason FROM raw_events WHERE outcome = 'rejected' ORDER BY received_at DESC LIMIT 20"` | Rejected messages and the rule they broke |
| `/eh-db event us7000abcd` | `q "SELECT e.* FROM earthquakes e JOIN earthquake_ids i ON i.earthquake_id = e.id WHERE i.event_id = 'us7000abcd'"` | One event, by any of its IDs (add `-x` for one field per line) |
| `/eh-db event us7000abcd` | `q "SELECT r.kafka_offset, r.event_id, r.outcome FROM raw_events r JOIN earthquake_ids i ON i.earthquake_id = r.earthquake_id WHERE i.event_id = 'us7000abcd' ORDER BY r.received_at"` | Every message that touched it |
| `/eh-db merged` | `q "SELECT e.preferred_event_id, array_agg(i.event_id) FROM earthquakes e JOIN earthquake_ids i ON i.earthquake_id = e.id GROUP BY e.id HAVING count(*) > 1"` | Events USGS merged |
| `/eh-db latest` | `q "SELECT preferred_event_id, event_time, magnitude, place, review_status FROM earthquakes ORDER BY event_time DESC LIMIT 10"` | The newest stored events |
| `/eh-db strongest` | `q "SELECT preferred_event_id, event_time, magnitude, place FROM earthquakes WHERE review_status <> 'deleted' ORDER BY magnitude DESC NULLS LAST LIMIT 10"` | The strongest stored events |
| | `make migrate` | Apply the schema's migrations |

## 6. REST API (from PostgreSQL)

`BASE=http://127.0.0.1:8000/api/v1`. All endpoints, with examples: [API reference](../technical/api.md). Interactive docs: http://127.0.0.1:8000/docs.

| Skill | Command | What it returns |
| --- | --- | --- |
| `/eh-api` | `curl -s $BASE/health` | Database status, newest event, version |
| `/eh-api list …` | `curl -s "$BASE/earthquakes?min_magnitude=5&sort=magnitude&limit=10" \| jq '.items[] \| {event_id, magnitude, place}'` | A page of events, filtered and sorted |
| `/eh-api event us7000abcd` | `curl -s $BASE/earthquakes/us7000abcd` | One event, by its current or any former ID |
| `/eh-api stats …` | `curl -s "$BASE/earthquakes/stats?start_time=2026-10-19T00:00:00Z"` | Count, magnitudes, counts by type |
| `/eh-api hourly` | `curl -s $BASE/earthquakes/summary/hourly \| jq -c '.buckets[] \| [.hour, .count]'` | Events per hour, the last 24 h |
| `/eh-api magnitude` | `curl -s $BASE/earthquakes/summary/magnitude \| jq -c '.buckets[] \| [.label, .count]'` | Events per magnitude range |
| `/eh-api export` | `curl -s "$BASE/export/earthquakes?after_id=0&limit=2"` | Rows exactly as stored, as the exporter reads them |
| | `make api-dev` | Runs the API on your machine with reload |

## 7. Export from PostgreSQL to R2

Full mode only: in direct mode the exporter refuses, and direct rounds write the rows and snapshot. The exporter reads through the API, so start it first (`make api`).

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-export` | `make export-r2 DRY_RUN=1` | What a run would change, writing nothing |
| `/eh-export run` | `make export-r2` ⚠ | Uploads new and changed rows, deletes merged ones, rewrites the dashboard's snapshot |
| `/eh-export verify` | `make export-r2 VERIFY=1` ⚠ | The same, and checks the whole bucket (~100 s more) |
| `/eh-export runs` | `make export-r2-runs` | Past runs and their counts |

## 8. R2 bucket (read-only)

| Skill | Command | What it shows |
| --- | --- | --- |
| `/eh-r2 ls runs/` | `make r2-ls PREFIX=runs/ LIMIT=10` | Keys, sizes and times under a prefix |
| `/eh-r2 count` | `make r2-get KEY=analytics/meta.json \| jq '{run_id, written_at, rows}'` and `make export-r2-runs \| tail -1` | Rows in the bucket now, and in the latest export, in seconds |
| `/eh-r2 count earthquakes/ full` | `make r2-count PREFIX=earthquakes/` | Lists every row object and counts them, with bytes (~100 s) |
| `/eh-r2 get analytics/meta.json` | `make r2-get KEY=analytics/meta.json` | One object; `.gz` objects are printed gunzipped |
| `/eh-r2 get earthquakes/us7000abcd.json` | `make r2-get KEY=earthquakes/us7000abcd.json` | One exported row, named by its record's key |
| `/eh-r2 runs` | `make export-r2-runs \| tail -3` | The latest export runs, oldest first, with their counts |
| `/eh-r2 snapshot` | `make r2-get KEY=analytics/earthquakes.parquet OUT=/tmp/earthquakes.parquet` | Saves the dashboard's Parquet snapshot |
| `/eh-r2 ls log/` | `make r2-ls PREFIX=log/$(date -u +%F)/ LIMIT=0 \| tail -3` | Direct mode's newest log files, today's (UTC): every message it processed |
| `/eh-r2 get _state/mode.json` | `make r2-get KEY=_state/mode.json` | The mode marker itself (`make mode` says it in one line) |
| `/eh-r2 audit` | `make direct-runs` and `make r2-get KEY=audit/direct/<day>/<run_id>.json` | Direct mode's executions, one line each, and one record in full (`audit/direct/`) |

## 9. Modes: full and direct

One marker in the bucket, `_state/mode.json`, says which mode writes it ([how it works](../technical/architecture.md#switching-modes)). Full mode is the whole pipeline on your machine; direct mode needs only Python and the R2 credentials, and runs [every hour on GitHub Actions](../technical/ci.md#direct-mode-on-a-schedule), so the dashboard stays current with your computer closed.

### Which mode, and switching between them

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-mode` | `make mode` | Which mode writes the bucket: `full` or `direct`, and since when |
| `/eh-mode direct` | `make to-direct` ⚠ | **Full → direct, in one command:** waits for the consumers, `make reconcile`, then `make mode-direct` (stops the poller and catch-up, exports, writes direct mode's state, catches up, checks against USGS), then `make down`. The hourly rounds take over; `DRY_RUN=1` lists the steps |
| `/eh-mode full` | `make to-full` ⚠ | **Direct → full, in one command:** checks Docker and the mode, `make up`, `make mode-full` (replays direct mode's log into Kafka and PostgreSQL, checks, switches back, restarts the poller), waits for the consumers, `make reconcile`. The hourly rounds then skip |
| | `make mode-direct` / `make mode-full` ⚠ | The handovers alone, without the checks and waiting around them |
| `/eh-mode usgs` | `make reconcile-usgs [DAYS=3]` | The mode writing the bucket against USGS itself, every difference explained or flagged; runs by itself after each switch ([how](../technical/architecture.md#reconciliation-with-usgs)) |
| `/eh-mode catchup` | `make catchup-now` ⚠ | Catch up with USGS now in either mode: day feed, month feed, deletions; runs by itself after each switch |

### Full mode: the pipeline on your machine

Only while `make mode` says `full`. These write PostgreSQL, or the bucket through the export; in direct mode the marker doesn't stop them, and what they store makes the handover back fail.

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-start pipeline` | `make ingest` ⚠ | Poller, consumers and catch-up on the live feed |
| `/eh-poller once` | `make ingest-once` ⚠ | One poller round from your machine |
| `/eh-backfill catchup` | `make catchup-once` ⚠ | One catch-up pass (`/eh-backfill 2026-01-01` for history) |
| `/eh-status reconcile` | `make reconcile` | Every Kafka message has a database row |
| `/eh-mode catchup` | `make catchup-now` ⚠ | Catch up now: `make catchup-once` (the poller reads the day feed every minute) |
| `/eh-export run` | `make export-r2` ⚠ | Updates the bucket and the dashboard's snapshot from PostgreSQL; refused in direct mode |

### Direct mode: no machine needed

Only while `make mode` says `direct`. The hourly rounds (a Cloudflare cron trigger starting `direct.yml`) do this by itself; the rest is for running or checking it by hand.

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-mode schedule` | `gh workflow run direct.yml -f job=round` ⚠ | A round on GitHub Actions now (also `-f job=reconcile`); `gh run list --workflow direct.yml` lists the GitHub runs, `gh run view <id> --log-failed` shows a failure |
| `/eh-mode once` | `make direct-once` ⚠ | One direct round from your machine (and catch-up if due) |
| `/eh-mode run` | `make direct-run` ⚠ | Direct mode from your machine until Ctrl-C: a round a minute instead of an hour. Not while the schedule is on |
| `/eh-mode runs` | `make direct-runs [LIMIT=20]` | Direct mode's audit trail: each execution's status, who started it (Cloudflare trigger, GitHub schedule or by hand, `direct-run`, local), the mode, messages by part and outcome, elapsed time (read-only) |
| `/eh-mode catchup` | `make catchup-now` ⚠ | Catch up now: a direct round with the day feed, the month feed and the deletions, whether due or not |
| `/eh-mode reconcile` | `make reconcile-direct [DEEP=1]` | Rows, manifest, snapshot, ID map and log agree, read-only ([direct mode](../technical/architecture.md#direct-mode)) |
| `/eh-r2 count` | `make r2-get KEY=analytics/meta.json \| jq '{run_id, written_at, rows}'` | Rows in the bucket now (PostgreSQL falls behind until `make mode-full`) |
| | `gh secret set -f <file>` ⚠ | The workflow's four `R2_*` secrets, from a file of `NAME=value` lines: repository secrets while the repository is private, `--env r2` once it is public (empty values make every run fail) |

**Before closing the computer:** `make mode` says `direct (active)`, and no `make direct-run` is left running. **Back at it:** `make to-full` brings PostgreSQL and the API up to date.

## 10. Dashboard: from the bucket

`URL=https://earth-hub.diego-narvaez.workers.dev`

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-dashboard` | `curl -s $URL/data/meta.json \| jq '{run_id, written_at, rows}'` | Which snapshot the live page shows (an export's, or a direct round's) |
| `/eh-dashboard` | `curl -sI $URL/data/earthquakes.parquet \| grep -iE '^(etag\|content-length\|x-robots-tag)'` | The snapshot's headers |
| `/eh-dashboard dev` | `make dashboard-dev` | Builds, then serves http://127.0.0.1:8787 over the real bucket |
| `/eh-dashboard build` | `make dashboard-build` | Builds `dashboard/dist`, with the license notices |
| `/eh-dashboard test` | `make test-dashboard` | The page in Chromium, against a simulated R2 |
| `/eh-dashboard deploy` | `make dashboard-deploy` ⚠ | Publishes to Cloudflare Workers |
| `/eh-dashboard deployments` | `cd dashboard && npx --no-install wrangler deployments list 2>&1 \| grep -v '@'` | Past deploys and their versions |
| `/eh-dashboard trigger` | `cd dashboard && npx --no-install wrangler secret list` and `make direct-runs LIMIT=3` | The hourly trigger: its token stored (never shown), and the latest rounds it started |
| | `make dashboard-install` | Once: Node dependencies and the test browser |
| | `make trigger-token` ⚠ | The hourly trigger's GitHub token: opens GitHub's form, checks the token you paste (never shown), stores it on the Worker, prints its expiry; run it in a terminal, again before the token expires: GitHub e-mails you from 14 days before ([CI](../technical/ci.md#the-trigger)) |

## 11. Containers and logs: watching it run

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-status` | `docker compose --profile ingest --profile api --profile tools ps` | Every container and its health |
| `/eh-logs poller` | `docker compose logs --no-log-prefix --since 1h poller` | One service's recent logs (`-f` to follow) |
| `/eh-logs` | `make ingest-logs` | Follows the four pipeline services |
| `/eh-logs us7000abcd` | `docker compose logs --no-log-prefix poller consumer consumer-backfill catchup \| jq -cR --arg id us7000abcd 'fromjson? \| select(.event_id == $id)'` | Everything about one event, across services, in order |
| `/eh-logs errors` | `docker compose logs --no-log-prefix --since 1h poller consumer consumer-backfill catchup api \| jq -cR 'fromjson? \| select(.level == "WARNING" or .level == "ERROR")'` | Warnings and errors only |
| `/eh-logs rejected` | `docker compose logs --no-log-prefix consumer consumer-backfill \| jq -cR 'fromjson? \| select(.message == "message rejected") \| {ts, event_id, reason}'` | Rejections and the rule behind each |
| `/eh-logs summary` | `docker compose logs --no-log-prefix --since 1h consumer consumer-backfill \| jq -cR 'fromjson? \| select(.message == "consumer summary") \| {ts, logger, lag, outcomes}' \| tail -10` | The consumers' minute summaries: counts and lag |
| `/eh-poller restart` | `docker compose --profile ingest restart poller` | Restarts one service (any of `poller`, `consumer`, `consumer-backfill`, `catchup`) |
| | `docker compose --profile api restart api` | Restarts the API |
| | `docker compose stats --no-stream` | CPU and memory per container |
| | `docker compose exec kafka bash` / `docker compose exec postgres bash` | A shell inside a container |
| | `docker volume ls \| grep earth-hub` | The data volumes `make reset` deletes |

## 12. Tests

| Skill | Command | What it runs |
| --- | --- | --- |
| `/eh-test lint` | `make lint` | Ruff checks and formatting (`make format` fixes) |
| `/eh-test` | `make test` | Unit tests: no Docker, no network |
| `/eh-test integration` | `uv run pytest tests/integration -m integration` | Real Kafka and PostgreSQL (`make up`) |
| `/eh-test api` | `uv run pytest tests/api -m integration --deselect tests/api/test_contract.py` | Every endpoint on a fixed dataset |
| `/eh-test contract` | `uv run pytest tests/api/test_contract.py -m integration` | Schemathesis (`CONTRACT_EXAMPLES=1000` for more) |
| `/eh-test e2e` | `make test-e2e` | Mock feed → poller → Kafka → consumer → PostgreSQL → API |
| `/eh-test dashboard` | `make test-dashboard` | The page in Chromium |
| `/eh-test disruptive` | `make test-disruptive` ⚠ | Stops and starts the shared containers |
| `/eh-test performance` | `make test-performance` | A year of synthetic history, timed; the API under load |
| `/eh-test load` | `make load` | Locust: 50 users over 140,000 events; `reports/load-report.html` |
| `/eh-test load-ui` | `make load-ui` | The same in Locust's web UI, http://127.0.0.1:8089 |
| `/eh-test regression` | `make regression` | Every layer in order, with reports in `reports/` |
| `/eh-test flake` | `make flake-check` | The end-to-end suite 5 times |
| `/eh-test traceability` | `make traceability` | The traceability matrix, from the last run |
| `/eh-test coverage` | `make coverage` | Unit tests with coverage |
| `/eh-test merge` | `uv run pytest -k merge -m "not disruptive and not performance"` | The tests matching a keyword |
| | `make smoke-restart` / `make check-db-outage` ⚠ | One disruptive check each |

## 13. Stop and clean up

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-stop` | `make ingest-stop` | Stops the pipeline; Kafka and PostgreSQL keep running |
| `/eh-stop api` / `/eh-stop ui` | `make api-stop` / `make kafka-ui-stop` | Stops the API / the viewer |
| `/eh-stop all` | `make down` | Stops everything, keeping the data (in direct mode the dashboard keeps updating) |
| `/eh-stop reset` | `make reset` ⚠ | Stops everything and **deletes all data** |
