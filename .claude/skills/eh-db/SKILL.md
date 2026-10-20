---
name: eh-db
description: Query Earth-Hub's PostgreSQL with psql — counts, outcomes, rejected messages, one event's stored row and full message history, merged events, the strongest or latest events, or any read-only SQL the user gives. Use for /eh-db, "how many events are stored", "show us7000abcd in the database", "what was rejected", "run this SQL".
argument-hint: "[counts | outcomes | rejected | event <ID> | merged | latest | strongest | <SQL>]"
---

# PostgreSQL

Read-only by default: run only `SELECT` (and `EXPLAIN`). For anything that writes (`INSERT`, `UPDATE`, `DELETE`,
DDL), stop and ask: the consumer owns these tables, and the migrations own the schema.

Run from the repository root, through the container's local socket (no password needed or printed):
`docker compose exec -T postgres psql -U earth_hub_app earth_hub -c "<SQL>"`. Tables and columns:
`docs/schema.md`; more queries with their meaning: `docs/sql-examples.md` (Inspecting PostgreSQL).

| Argument | SQL |
| --- | --- |
| `counts` (default) | `SELECT (SELECT count(*) FROM earthquakes) AS events, (SELECT count(*) FROM earthquakes WHERE review_status = 'deleted') AS deleted, (SELECT count(*) FROM earthquake_ids) AS ids, (SELECT count(*) FROM raw_events) AS messages, (SELECT max(event_time) FROM earthquakes) AS newest` |
| `outcomes` | `SELECT kafka_topic, outcome, count(*) AS messages FROM raw_events GROUP BY 1, 2 ORDER BY 1, 3 DESC` |
| `rejected` | `SELECT kafka_topic, kafka_partition, kafka_offset, event_id, rule, reason FROM raw_events WHERE outcome IN ('rejected', 'error') ORDER BY received_at DESC LIMIT 20` |
| `event <ID>` | The row: `SELECT e.* FROM earthquakes e JOIN earthquake_ids i ON i.earthquake_id = e.id WHERE i.event_id = '<ID>'` (use `-x` for expanded output). Its history: `SELECT r.kafka_topic, r.kafka_partition, r.kafka_offset, r.event_id, to_timestamp(r.source_updated_ms / 1000.0) AS source_updated, r.outcome, r.reason FROM raw_events r JOIN earthquake_ids i ON i.earthquake_id = r.earthquake_id WHERE i.event_id = '<ID>' ORDER BY r.received_at` |
| `merged` | `SELECT e.preferred_event_id, array_agg(i.event_id ORDER BY i.event_id) AS ids, e.revision_count FROM earthquakes e JOIN earthquake_ids i ON i.earthquake_id = e.id GROUP BY e.id HAVING count(*) > 1 ORDER BY e.last_applied_at DESC LIMIT 20` |
| `latest` | `SELECT preferred_event_id, event_time, magnitude, place, review_status FROM earthquakes ORDER BY event_time DESC LIMIT 10` |
| `strongest` | `SELECT preferred_event_id, event_time, magnitude, mag_type, place FROM earthquakes WHERE review_status <> 'deleted' ORDER BY magnitude DESC NULLS LAST LIMIT 10` |
| any SQL | as given, if read-only |

Validate an `<ID>` before putting it in SQL: USGS IDs are letters and digits only (`^[a-z0-9]+$`, case-insensitive);
refuse anything else rather than quoting it. Times are `timestamptz`; add `SET TIME ZONE 'UTC';` before the query
for UTC output. For an interactive session, tell the user to run
`! docker compose exec postgres psql -U earth_hub_app earth_hub`.

## Report

The result as a table (trim wide rows to the columns that answer the question), then one line on what it means:
e.g. "applied 3 times, the last revision 2 h ago; the 4th message was a duplicate".
