# Earth-Hub cheat sheet

The everyday commands, each next to the Claude Code skill that runs it. Type a skill in Claude Code (for example `/eh-kafka lag`), or copy the command into a terminal at the repository's root; `/eh <area>` prints a section of this page, and `make help` lists every make target. How each part works is in the [architecture](../technical/architecture.md); this page only says what to type.

Sections follow the data: start, USGS → poller → Kafka → PostgreSQL → API → export → R2 → dashboard, then modes and hosts, watching it run, tests, stopping. ⚠ marks commands that call live services or change state; the skills ask before the destructive ones.

## Skills

| Skill | Does | Main commands behind it |
| --- | --- | --- |
| `/eh [area]` | Shows this cheat sheet, or one section of it | `make help` |
| `/eh-start [infra\|pipeline\|api\|ui\|dashboard\|all]` | Starts services and waits until healthy | `make up`, `make ingest`, `make api` |
| `/eh-usgs [feed\|event\|count\|query\|headers]` | Asks the live USGS services directly | `curl` on the feeds and FDSN |
| `/eh-poller [rounds\|once\|restart\|health]` | The poller's rounds, one manual poll, a restart | `make ingest-once` |
| `/eh-backfill [START [END]\|catchup\|progress]` | History from FDSN in full mode; one catch-up pass | `make backfill-full`, `make catchup-once` |
| `/eh-kafka [topics\|offsets\|lag\|read\|tail\|find]` | Topics, offsets, lag, messages by offset or event ID | `kafka-*.sh` in the broker container |
| `/eh-db [counts\|outcomes\|rejected\|event ID\|merged\|latest\|strongest\|SQL]` | Read-only SQL on PostgreSQL | `psql` in the database container |
| `/eh-api [health\|list\|event\|stats\|hourly\|magnitude\|export]` | Calls the REST API | `curl` on `/api/v1/…` |
| `/eh-export [dry\|run\|verify\|runs]` | Copies PostgreSQL's table and the snapshot to R2 | `make export-r2` |
| `/eh-r2 [ls\|count\|get\|snapshot\|runs\|audit]` | Looks inside the R2 bucket, read-only | `make r2-ls`, `make r2-get`, `make direct-runs` |
| `/eh-mode [status\|direct\|full\|usgs\|catchup\|once\|run\|reconcile\|schedule\|runs]` | The mode, switching modes, the USGS check, catching up, direct mode's rounds | `make mode`, `make to-direct`, `make to-full`, `make reconcile-usgs`, `make catchup-now` |
| `/eh-host [status\|cutover-a\|cutover-b\|release\|claim\|rebuild\|takeover]` | Which host owns the bucket; moving full mode between hosts | `make host-status`, `make cutover-a`, `make cutover-b` |
| `/eh-dashboard [check\|dev\|build\|test\|deployments\|deploy\|trigger]` | The live page, a local server, tests, deploys, the cron trigger | `make dashboard-dev`, `make dashboard-deploy`, `make trigger-check` |
| `/eh-reconcile START [END] [day\|month\|year]` | Earth-Hub against USGS for any range, as a side-by-side table | `make reconcile-usgs START=… END=… BY=…` |
| `/eh-status [reconcile]` | Health of every service, lag, newest event, last export, the mode | `docker compose ps`, `make reconcile`, `make mode` |
| `/eh-logs [service\|ID\|errors\|rejected\|summary]` | Reads the JSON logs with `jq`, follows one event | `docker compose logs … \| jq` |
| `/eh-test [layer\|security\|regression\|flake\|REQ-…\|keyword]` | Runs the tests by layer, or everything; open security alerts | `make test`, `make regression` |
| `/eh-stop [pipeline\|api\|ui\|all\|reset]` | Stops services, keeping data (or wipes it, after asking) | `make ingest-stop`, `make down`, `make reset` |

## 1. Start the services

| Skill | Command | What it does |
| --- | --- | --- |
| | `make install` | `.venv` from `uv.lock`, once |
| `/eh-start` | `make up && make migrate` | Kafka and PostgreSQL, the topics, the schema |
| `/eh-start pipeline` | `make ingest` ⚠ | Poller, consumers, catch-up and the exporting service on the live feed (full mode only) |
| `/eh-start api` | `make api` | The REST API: http://127.0.0.1:8000/docs (`make api-dev`: on your machine, with reload) |
| `/eh-start ui` | `make kafka-ui` | A read-only Kafka viewer: http://127.0.0.1:8080 |
| `/eh-start dashboard` | `make dashboard-dev` | The page, http://127.0.0.1:8787, over the real bucket |
| `/eh-start all` | `make ingest` ⚠, then `make api` | Everything but the viewer and the dashboard |

## 2. USGS (live): the source

Always send a User-Agent: `UA="earth-hub (manual check)"`.

| Skill | Command | What it answers |
| --- | --- | --- |
| `/eh-usgs feed all_day` | `curl -s -A "$UA" https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson \| jq -c '.metadata \| {generated, count}'` | The feed the poller reads (also `all_hour`, `all_week`, `all_month`, `significant_week`) |
| `/eh-usgs event us7000abcd` | `curl -s -A "$UA" "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventid=us7000abcd" \| jq -c '{id, mag: .properties.mag, ids: .properties.ids}'` | One event as USGS has it now (`&includedeleted=true` for deleted ones) |
| `/eh-usgs count …` | `curl -s -A "$UA" "https://earthquake.usgs.gov/fdsnws/event/1/count?starttime=2026-10-19&endtime=2026-10-20&minmagnitude=4"` | How many events match |
| `/eh-usgs query …` | `curl -s -A "$UA" "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=2026-10-19&minmagnitude=5&orderby=magnitude&limit=10"` | A search by time, magnitude or place |
| `/eh-usgs headers` | `curl -sI -A "$UA" https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson \| grep -iE '^(last-modified\|expires)'` | What the poller times its next round from |

## 3. Into Kafka: poller, backfill, catch-up, and checks

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-poller` | `docker compose logs --no-log-prefix --since 30m poller \| jq -cR 'fromjson? \| select(.message == "round complete") \| {ts, published, unchanged}'` | The last rounds |
| `/eh-poller once` | `make ingest-once` ⚠ | One round from your machine (`make consume`: the consumer on your machine) |
| `/eh-poller health` | `docker compose exec -T poller python -m earth_hub.healthcheck poller` | 0 if a recent round succeeded |
| `/eh-backfill 2026-01-01` | `make backfill-full START=2026-01-01 [END=…]` ⚠ | History from FDSN in one command: backfill, wait, reconcile, export; refused in direct mode (`make backfill` alone skips the rest) |
| `/eh-backfill catchup` | `make catchup-once` ⚠ | One catch-up pass and the daily checks, full mode ([what they read](../technical/architecture.md#backfill-and-catch-up)) |
| `/eh-mode catchup` | `make catchup-now` ⚠ | The same, in whichever mode writes the bucket, whether due or not |
| `/eh-backfill progress` | `docker compose exec -T postgres psql -U earth_hub_app earth_hub -c "SELECT job, status, count(*), sum(fetched), sum(rejected) FROM backfill_windows GROUP BY 1, 2"` | Which weeks are done |
| `/eh-status reconcile` | `make reconcile` | Every Kafka message has a database row, and the invariants hold |
| `/eh-mode usgs` | `make reconcile-usgs [DAYS=3]` | Earth-Hub against USGS ([what it reports](../technical/architecture.md#reconciliation-with-usgs)) |
| `/eh-reconcile 2026-01-01 2026-10-31 month` | `make reconcile-usgs START=2026-01-01 [END=…] [BY=day\|month\|year]` | The same over any range, a line per period and a total |

## 4. Kafka

With `k() { local tool; for tool; do break; done; shift; docker compose exec -T kafka "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@"; }`. Reading with `--partition` joins no consumer group, so it can't move the pipeline's offsets.

| Skill | Command | What it shows |
| --- | --- | --- |
| `/eh-kafka topics` | `k kafka-topics.sh --describe --topic earthquakes.raw` | Partitions, leader, replicas (also `--list`) |
| `/eh-kafka offsets` | `k kafka-get-offsets.sh --topic earthquakes.raw` | End offset per partition |
| `/eh-kafka lag` | `k kafka-consumer-groups.sh --describe --group earth-hub-consumer` | Committed offsets and lag (`earth-hub-consumer-backfill` for the other topic) |
| `/eh-kafka read earthquakes.raw 0 42` | `k kafka-console-consumer.sh --topic earthquakes.raw --partition 0 --offset 42 --max-messages 1 --timeout-ms 10000 \| jq` | One message |
| `/eh-kafka tail earthquakes.raw 5` | `k kafka-console-consumer.sh --topic earthquakes.raw --partition 0 --offset <end−5> --max-messages 5 --timeout-ms 10000 \| jq -c '{event_id, mag: .payload.properties.mag}'` | The latest messages of a partition |
| `/eh-kafka find us7000abcd` | `k kafka-console-consumer.sh --topic earthquakes.raw --partition 0 --offset earliest --timeout-ms 8000 --formatter-property print.offset=true \| grep -F '"event_id":"us7000abcd"'` (and partitions 1, 2) | Every message for one event |

## 5. PostgreSQL: what the consumers stored

With `q() { docker compose exec -T postgres psql -U earth_hub_app earth_hub -c "$1"; }`. More queries, with their results explained: [SQL examples](../technical/sql-examples.md#inspecting-postgresql).

| Skill | Command | What it shows |
| --- | --- | --- |
| `/eh-db` | `q "SELECT count(*), max(event_time) FROM earthquakes"` | Stored events and the newest |
| `/eh-db outcomes` | `q "SELECT kafka_topic, outcome, count(*) FROM raw_events GROUP BY 1, 2 ORDER BY 1, 3 DESC"` | What happened to every message |
| `/eh-db rejected` | `q "SELECT kafka_offset, event_id, rule, reason FROM raw_events WHERE outcome = 'rejected' ORDER BY received_at DESC LIMIT 20"` | Rejected messages and their rule |
| `/eh-db event us7000abcd` | `q "SELECT e.* FROM earthquakes e JOIN earthquake_ids i ON i.earthquake_id = e.id WHERE i.event_id = 'us7000abcd'"` | One event, by any of its IDs |
| `/eh-db merged` | `q "SELECT e.preferred_event_id, array_agg(i.event_id) FROM earthquakes e JOIN earthquake_ids i ON i.earthquake_id = e.id GROUP BY e.id HAVING count(*) > 1"` | Events USGS merged |
| `/eh-db latest` | `q "SELECT preferred_event_id, event_time, magnitude, place FROM earthquakes ORDER BY event_time DESC LIMIT 10"` | The newest stored events |

## 6. REST API

`BASE=http://127.0.0.1:8000/api/v1`. Every endpoint, with examples: [API reference](../technical/api.md).

| Skill | Command | What it returns |
| --- | --- | --- |
| `/eh-api` | `curl -s $BASE/health` | Database status, newest event, version |
| `/eh-api list …` | `curl -s "$BASE/earthquakes?min_magnitude=5&sort=magnitude&limit=10" \| jq '.items[] \| {event_id, magnitude, place}'` | A page of events, filtered and sorted |
| `/eh-api event us7000abcd` | `curl -s $BASE/earthquakes/us7000abcd` | One event, by any of its IDs |
| `/eh-api stats …` | `curl -s "$BASE/earthquakes/stats?start_time=2026-10-19T00:00:00Z"` | Counts and magnitudes |
| `/eh-api hourly` | `curl -s $BASE/earthquakes/summary/hourly \| jq -c '.buckets[] \| [.hour, .count]'` | Events per hour |

## 7. Export to R2 (full mode)

The exporter reads through the API, so start it first (`make api`); in direct mode it refuses, as the rounds write the bucket.

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-export` | `make export-r2 DRY_RUN=1` | What a run would change, writing nothing |
| `/eh-export run` | `make export-r2` ⚠ | Uploads new and changed rows and the dashboard's snapshot |
| `/eh-export verify` | `make export-r2 VERIFY=1` ⚠ | The same, then checks the whole bucket |
| `/eh-export runs` | `make export-r2-runs` | Past runs and their counts |

## 8. R2 bucket (read-only)

| Skill | Command | What it shows |
| --- | --- | --- |
| `/eh-r2 ls runs/` | `make r2-ls PREFIX=runs/ LIMIT=10` | Keys, sizes and times under a prefix |
| `/eh-r2 count` | `make r2-get KEY=analytics/meta.json \| jq '{run_id, written_at, rows}'` | Rows in the bucket now (`make r2-count PREFIX=earthquakes/` lists them all) |
| `/eh-r2 get earthquakes/us7000abcd.json` | `make r2-get KEY=earthquakes/us7000abcd.json` | One object; `.gz` objects printed gunzipped |
| `/eh-r2 snapshot` | `make r2-get KEY=analytics/earthquakes.parquet OUT=/tmp/earthquakes.parquet` | Saves the dashboard's snapshot |
| `/eh-r2 audit` | `make direct-runs` | Direct mode's executions, one line each |

## 9. Modes and hosts

`make mode` says which mode writes the bucket; how the modes, the standby and the lease work is in the [architecture](../technical/architecture.md#switching-modes), and moving full mode to another machine, step by step, in [deployment](../technical/deployment.md#cutover-from-a-to-b).

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-mode` | `make mode` | Which mode writes the bucket, and since when |
| `/eh-mode direct` | `make to-direct` ⚠ | Full → direct with every check, then `make down` (`DRY_RUN=1` lists the steps) |
| `/eh-mode full` | `make to-full` ⚠ | Direct → full with every check |
| | `make mode-direct` / `make mode-full` ⚠ | The handovers alone |
| `/eh-host` | `make host-status` | The owner, its lease, this host (read-only) |
| `/eh-host cutover-a` | `make cutover-a [DRY_RUN=1]` ⚠ | On the machine giving up full mode; run it in a terminal |
| `/eh-host cutover-b` | `make cutover-b [DRY_RUN=1]` ⚠ | On the machine taking it; run it in a terminal |
| `/eh-host release` / `claim` | `make host-release` / `make host-claim` ⚠ | The two halves of a cutover, without the wrappers |
| `/eh-host rebuild` | `make host-rebuild` ⚠ | This host's database rebuilt from the bucket; run it in a terminal |
| `/eh-host takeover` | `make host-takeover` ⚠ | Direct mode takes over now from a host that is gone |
| `/eh-mode schedule` | `gh workflow run direct.yml -f job=round` ⚠ | A direct round on GitHub Actions now (`-f job=reconcile`: the daily reconcile) |
| `/eh-mode once` | `make direct-once` ⚠ | One direct round from your machine |
| `/eh-mode run` | `make direct-run` ⚠ | Direct mode from your machine until Ctrl-C; not while the schedule is on |
| `/eh-mode runs` | `make direct-runs [LIMIT=20]` | Direct mode's audit trail (read-only) |
| | `gh run list --workflow watchdog.yml --limit 3` | The watchdog's runs ([what it checks](../technical/ci.md#the-watchdog)) |
| `/eh-mode reconcile` | `make reconcile-direct [DEEP=1]` | Direct mode's bucket invariants, read-only |
| `/eh-mode repair` | `make repair-rows [DRY_RUN=1]` ⚠ | Row objects that differ from the snapshot written again (DEF-058) |

**Before closing the computer:** `make mode` says `direct (active)`, and no `make direct-run` is left running.

## 10. Dashboard

`URL=https://earth-hub.diego-narvaez.workers.dev`

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-dashboard` | `curl -s $URL/data/meta.json \| jq '{run_id, written_at, rows}'` | Which snapshot the live page shows |
| `/eh-dashboard dev` | `make dashboard-dev` | Builds, then serves the page over the real bucket |
| `/eh-dashboard build` | `make dashboard-build` | Builds `dashboard/dist` |
| `/eh-dashboard test` | `make test-dashboard` | The page in Chromium, against a simulated R2 |
| `/eh-dashboard deploy` | `make dashboard-deploy` ⚠ | Publishes the page and the cron triggers |
| `/eh-dashboard deployments` | `cd dashboard && npx --no-install wrangler deployments list 2>&1 \| grep -v '@'` | Past deploys |
| `/eh-dashboard trigger` | `make trigger-check` | Whether the trigger's deployed token works; starts nothing |
| | `make dashboard-install` | Once: Node dependencies and the test browser |
| | `make trigger-token` ⚠ | Creates and stores the trigger's GitHub token; run it in a terminal ([CI](../technical/ci.md#the-trigger)) |

## 11. Containers and logs

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-status` | `docker compose --profile ingest --profile api --profile tools ps` | Every container and its health |
| `/eh-logs poller` | `docker compose logs --no-log-prefix --since 1h poller` | One service's recent logs (`-f` to follow; `make ingest-logs` follows the pipeline) |
| `/eh-logs us7000abcd` | `docker compose logs --no-log-prefix poller consumer consumer-backfill catchup \| jq -cR --arg id us7000abcd 'fromjson? \| select(.event_id == $id)'` | Everything about one event, in order |
| `/eh-logs errors` | `docker compose logs --no-log-prefix --since 1h poller consumer consumer-backfill catchup exporter api \| jq -cR 'fromjson? \| select(.level == "WARNING" or .level == "ERROR")'` | Warnings and errors only |
| `/eh-logs rejected` | `docker compose logs --no-log-prefix consumer consumer-backfill \| jq -cR 'fromjson? \| select(.message == "message rejected") \| {ts, event_id, reason}'` | Rejections and their rule |
| `/eh-logs summary` | `docker compose logs --no-log-prefix --since 1h consumer consumer-backfill \| jq -cR 'fromjson? \| select(.message == "consumer summary") \| {ts, logger, lag, outcomes}' \| tail -10` | The consumers' minute summaries |
| `/eh-poller restart` | `docker compose --profile ingest restart poller` | Restarts one service |

## 12. Tests

What each layer covers: [test strategy](../progress/test-strategy.md).

| Skill | Command | What it runs |
| --- | --- | --- |
| `/eh-test lint` | `make lint` | Ruff (`make format` fixes formatting) |
| `/eh-test security` | `gh api 'repos/dienarvaez1/earth-hub/code-scanning/alerts?state=open'` | Open CodeQL alerts (also `dependabot/alerts`, `secret-scanning/alerts`) |
| `/eh-test` | `make test` | Unit tests: no Docker, no network |
| `/eh-test integration` | `make test-integration` | Integration, API and contract tests (`make up`) |
| `/eh-test e2e` | `make test-e2e` | Mock feed → poller → Kafka → consumer → PostgreSQL → API |
| `/eh-test dashboard` | `make test-dashboard` | The page in Chromium |
| `/eh-test disruptive` | `make test-disruptive` ⚠ | Stops and starts the shared containers (also `make smoke-restart`, `make check-db-outage`) |
| `/eh-test performance` | `make test-performance` | Timed: a year of synthetic history, the API under load |
| `/eh-test load` | `make load` | Locust against the API (`make load-ui`: its web UI) |
| `/eh-test regression` | `make regression` | Every layer in order, with reports in `reports/` (`make test-all`: all but disruptive and performance, in one process) |
| `/eh-test flake` | `make flake-check` | The end-to-end suite 5 times |
| `/eh-test traceability` | `make traceability` | The traceability matrix, from the last run |
| `/eh-test coverage` | `make coverage` | Unit tests with coverage |
| `/eh-test merge` | `uv run pytest -k merge -m "not disruptive and not performance"` | The tests matching a keyword |

## 13. Stop and clean up

| Skill | Command | What it does |
| --- | --- | --- |
| `/eh-stop` | `make ingest-stop` | Stops the pipeline; Kafka and PostgreSQL keep running |
| `/eh-stop api` / `/eh-stop ui` | `make api-stop` / `make kafka-ui-stop` | Stops the API / the viewer |
| `/eh-stop all` | `make down` | Stops everything, keeping the data |
| `/eh-stop reset` | `make reset` ⚠ | Stops everything and **deletes all data** |
