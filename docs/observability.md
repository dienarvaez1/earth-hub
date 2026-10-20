# Observability

Every service logs one JSON object per line on stdout (`earth_hub/logging_config.py`). This page says what the lines carry and how to answer the usual questions with them, using `docker compose logs` and [`jq`](https://jqlang.org/). The recipes read each line as text and skip any that is not JSON (`-R 'fromjson? | ...'`): the Kafka client library (librdkafka) prints its own messages as plain text, such as `Disconnected` when a broker connection drops; `docker compose logs --no-log-prefix consumer | grep -v '^{'` shows them.

## What every line carries

| Field | On | Meaning |
| --- | --- | --- |
| `ts`, `level`, `logger`, `message` | every line | UTC time, level, module, what happened |
| `stage` | every pipeline line | where in the pipeline (below) |
| `event_id` | every line about one event | the USGS event ID, the same at every stage |
| `partition`, `offset` | consumer lines about a message | where the message sits in Kafka |
| `outcome`, `reason` | consumer results | `applied`, `stale`, `duplicate`, `rejected` or `error`; for a rejection, the rule ID and why |

The stages, in pipeline order (`earth_hub/observability.py`):

| Stage | Service | Lines |
| --- | --- | --- |
| `fetch` | poller, backfill, catch-up | reading the USGS feed or FDSN service, and its failures |
| `parse` | poller | a feature without `id`, `updated` or `time`, rejected before Kafka (`feature rejected`) |
| `publish` | poller, backfill, catch-up | each event sent to Kafka (`event published`), delivery failures, the round summary |
| `persist` | consumers | each message stored, rejected or failed, retries while the database is down, the summary |

The poller logs every event it publishes. The consumer logs every rejection (warning) and error (error) on a line of its own. It also logs every routine outcome when it runs one message at a time, as the live consumer does (DEF-036); in batches (the backfill consumer, up to 500 messages), routine outcomes are at debug level and a `batch processed` line counts them.

## The summaries

The consumer logs a `consumer summary` every minute, even when idle:

```json
{"message": "consumer summary", "stage": "persist", "messages": 2,
 "outcomes": {"applied": 1, "rejected": 1}, "totals": {"applied": 1, "rejected": 1},
 "interval_s": 60.0, "lag": 0, "lag_by_partition": {"earthquakes.raw[0]": 0, "...": 0}}
```

- `messages` and `outcomes`: since the previous summary. `totals`: since the consumer started.
- `lag`: messages in Kafka that this consumer has not read yet, per partition and in all. `null` before Kafka has assigned it partitions. A lag that keeps growing means the consumer is falling behind or stuck; one that drops back to 0 means it has caught up.

The poller's `round complete` line, once per poll (about a minute), is its summary: what that round published, found unchanged, rejected or failed to deliver, and `totals` since start.

## Questions and how to answer them

```bash
# Everything that happened to one event, in order, across services
docker compose logs --no-log-prefix poller consumer | jq -cR 'fromjson? | select(.event_id == "us7000abcd")'

# Every rejection and the rule behind it
docker compose logs --no-log-prefix consumer | jq -cR 'fromjson? | select(.message == "message rejected") | {event_id, reason}'

# Is the consumer keeping up? Its lag, minute by minute
docker compose logs --no-log-prefix consumer | jq -cR 'fromjson? | select(.message == "consumer summary") | {ts, lag, outcomes}'

# Is the database down? The consumer retries the same message, naming it
docker compose logs --no-log-prefix consumer | jq -cR 'fromjson? | select(.message | startswith("database unavailable"))'

# Is the feed failing?
docker compose logs --no-log-prefix poller | jq -cR 'fromjson? | select(.stage == "fetch" and .level != "INFO")'
```

## A controlled failure, diagnosed from the logs

`tests/e2e/test_diagnosis.py` runs this through the real poller, Kafka, consumer and PostgreSQL, then makes the diagnosis below from the log lines alone; the lines here are from that run (topic name shortened).

**The failure:** the feed lists two events, one of them (`bad01`) with latitude 95, which cannot exist.

**1. Something was rejected.** The consumer's summary counts one rejection, and its lag is 0, so it is not stuck: it read everything and turned one message away.

```json
{"level": "INFO", "message": "consumer summary", "stage": "persist", "messages": 2, "outcomes": {"applied": 1, "rejected": 1}, "lag": 0}
```

**2. Which event, and why.** The rejection line names the event, the rule and the reason, and where the message is in Kafka:

```json
{"ts": "2026-10-19T07:26:24.110046+00:00", "level": "WARNING", "logger": "earth_hub.consumers.earthquake_consumer", "message": "message rejected", "stage": "persist", "event_id": "bad01", "partition": 2, "offset": 0, "outcome": "rejected", "earthquake_id": null, "reason": "GEO-002: latitude 95.0 outside -90..90"}
```

Rule GEO-002 is in [data-quality-rules.md](data-quality-rules.md): latitude must be within -90..90.

**3. Its path.** Filtering on `bad01` shows the whole journey: the poller published it (it checks only that an event has an ID and times; data quality belongs to the consumer), and the consumer rejected it.

```json
{"ts": "2026-10-19T07:26:23.893754+00:00", "level": "INFO", "logger": "earth_hub.ingestion.poller", "message": "event published", "stage": "publish", "event_id": "bad01", "source_updated_ms": 1792391213000, "topic": "earthquakes.raw"}
{"ts": "2026-10-19T07:26:24.110046+00:00", "level": "WARNING", "logger": "earth_hub.consumers.earthquake_consumer", "message": "message rejected", "stage": "persist", "event_id": "bad01", "partition": 2, "offset": 0, "outcome": "rejected", "reason": "GEO-002: latitude 95.0 outside -90..90"}
```

**Conclusion, from the logs only:** USGS sent an event with an impossible latitude; it was published, rejected by GEO-002 and never stored, as designed; nothing else went wrong and nothing is waiting. The `raw_events` table holds the same verdict for audit (`outcome = 'rejected'`, `rule = 'GEO-002'`), and `make reconcile` counts it, but neither was needed to find it.
