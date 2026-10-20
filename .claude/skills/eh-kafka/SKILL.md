---
name: eh-kafka
description: Inspect Earth-Hub's Kafka topics with the broker's own tools — list and describe topics, end offsets per partition, consumer-group lag, read one message by partition and offset, the latest messages, or every message for one USGS event ID. Read-only. Use for /eh-kafka, "what's in earthquakes.raw", "show offset 42", "find us7000abcd in Kafka", "consumer lag".
argument-hint: "[topics | offsets [TOPIC] | lag | read TOPIC PARTITION OFFSET | tail TOPIC [N] | find ID [TOPIC]]"
---

# Kafka

Read-only: never produce, delete, or reset offsets. Run from the repository root; the tools run inside the
broker container. Define `k() { docker compose exec -T kafka /opt/kafka/bin/$1 --bootstrap-server localhost:9092 "${@:2}"; }`
in the same shell command. Topics: `earthquakes.raw` (live, group `earth-hub-consumer`) and
`earthquakes.backfill` (backfill and catch-up, group `earth-hub-consumer-backfill`), 3 partitions each, keyed by
USGS event ID.

| Argument | Command |
| --- | --- |
| `topics` (default) | `k kafka-topics.sh --list` and `k kafka-topics.sh --describe --topic <each>` |
| `offsets [TOPIC]` | `k kafka-get-offsets.sh --topic <TOPIC>` (end offset per partition = messages ever written; both topics without one) |
| `lag` | `k kafka-consumer-groups.sh --describe --group earth-hub-consumer` and `--group earth-hub-consumer-backfill` |
| `read TOPIC P O` | `k kafka-console-consumer.sh --topic <TOPIC> --partition <P> --offset <O> --max-messages 1 --timeout-ms 10000 2>/dev/null \| jq` |
| `tail TOPIC [N]` | For each partition 0–2, from `offset = end − N` (end from `kafka-get-offsets.sh`, at least 0): `k kafka-console-consumer.sh --topic <TOPIC> --partition <p> --offset <o> --max-messages <N> --timeout-ms 10000 2>/dev/null \| jq -c '{event_id, source_updated_ms, mag: .payload.properties.mag, place: .payload.properties.place}'` (N defaults to 5) |
| `find ID [TOPIC]` | For each partition 0–2: `k kafka-console-consumer.sh --topic <TOPIC> --partition <p> --offset earliest --timeout-ms 8000 --formatter-property print.partition=true --formatter-property print.offset=true 2>/dev/null \| grep -F '"event_id":"<ID>"'`, both topics without one. Scans the whole topic: say it can take a while on `earthquakes.backfill`. Also list the event's other IDs (`ids`) and search for them, since a merged event's revisions can be keyed by an old ID |

(In the table `\|` is a pipe; run it as `|`.)

Always read with `--partition`: it doesn't join a consumer group, so it never moves the pipeline's offsets or leaves a `console-consumer-*` group behind (`--from-beginning` without a partition does). Use
`--formatter-property` (not the deprecated `--property`) for print options. The message value is the envelope:
`schema_version`, `event_id`, `ids`, `source_updated_ms`, `event_time_ms`, `source`, `feed_generated_ms`,
`fetched_at`, and `payload` (the unchanged USGS feature).

For browsing in a web page instead: `make kafka-ui` → http://127.0.0.1:8080 (read-only).

## Report

The answer as a short table (topic, partition, offset, event ID, `updated`, magnitude, place). For `find`, every
revision of the event in order, and whether `raw_events` has each (`/eh-db event <ID>`) when that's the question.
