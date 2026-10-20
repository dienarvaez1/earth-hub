---
name: eh-logs
description: Read Earth-Hub's JSON logs with jq — one service's recent lines, everything about one USGS event ID across poller and consumer, errors and warnings only, rejections, or the consumer's minute summaries. Use for /eh-logs, "show the poller logs", "follow event us7000abcd", "any errors", "why was it rejected".
argument-hint: "[service | <event ID> | errors | rejected | summary]  [--since 1h]"
---

# Logs

Read-only. Every line is one JSON object (`docs/observability.md`); non-JSON lines from librdkafka are
skipped by `fromjson?`. Use `docker compose logs --no-log-prefix` and `--since` (default `1h`; take it from
the arguments if given). Services: `poller`, `consumer`, `consumer-backfill`, `catchup`, `api`, `kafka`, `postgres`.

| Argument | Command |
| --- | --- |
| a service name | `docker compose logs --no-log-prefix --since 1h <service> \| jq -cR 'fromjson? \| {ts, level, stage, message, event_id}' \| tail -40` |
| an event ID (e.g. `us7000abcd`) | `docker compose logs --no-log-prefix poller consumer consumer-backfill catchup \| jq -cR --arg id <ID> 'fromjson? \| select(.event_id == $id)'` |
| `errors` | `docker compose logs --no-log-prefix --since 1h poller consumer consumer-backfill catchup api \| jq -cR 'fromjson? \| select(.level == "WARNING" or .level == "ERROR")'` |
| `rejected` | `docker compose logs --no-log-prefix consumer consumer-backfill \| jq -cR 'fromjson? \| select(.message == "message rejected") \| {ts, event_id, reason}'` |
| `summary` | `docker compose logs --no-log-prefix --since 1h consumer consumer-backfill \| jq -cR 'fromjson? \| select(.message == "consumer summary") \| {ts, logger, lag, outcomes}' \| tail -10` |
| nothing | the last 20 lines of each pipeline service, as for a service name |

(In the table `\|` is a pipe; run it as `|`.)

An event ID that gives no lines may have aged out of the containers' logs, or arrived through backfill
(whose batches log only rejections and errors per event): then look it up with `/eh-db event <ID>`.

## Report

The lines, trimmed to what answers the question, then one line of interpretation (e.g. "published at 10:02:13,
stored 0.2 s later as applied", or "no errors in the last hour").
