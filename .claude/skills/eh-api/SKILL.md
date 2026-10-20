---
name: eh-api
description: Call Earth-Hub's read-only REST API (served from PostgreSQL) with curl — health, a filtered list of events, one event by any of its IDs, statistics, hourly and magnitude summaries, an export page — and summarize the JSON. Use for /eh-api, "query the API", "M5+ this week from the API", "API health", "what does the API return for us7000abcd".
argument-hint: "[health | list <params> | event <ID> | stats <params> | hourly <params> | magnitude <params> | export]"
---

# REST API

Base URL `http://127.0.0.1:8000/api/v1` (the `API_PORT` in `.env` if changed). If nothing answers, the API
isn't running: say `make api` (or `/eh-start api`) starts it, or run it yourself if the user asked for results.
Reference and examples: `docs/api.md`; interactive docs at http://127.0.0.1:8000/docs.

| Argument | Request |
| --- | --- |
| `health` (default) | `curl -s $BASE/health` (`503` when the database is down) |
| `list <params>` | `curl -s "$BASE/earthquakes?<params>"`, e.g. `min_magnitude=5&start_time=2026-10-13T00:00:00Z&sort=magnitude&limit=10`. Summarize: `jq -c '{total}, (.items[] \| {event_id, magnitude, place, event_time})'` |
| `event <ID>` | `curl -s $BASE/earthquakes/<ID>`: a former ID returns the event under its current one |
| `stats <params>` | `curl -s "$BASE/earthquakes/stats?<params>"` |
| `hourly <params>` | `curl -s "$BASE/earthquakes/summary/hourly?<params>"` (24 h by default, 31 days at most): `jq -c '.buckets[] \| [.hour, .count]'` |
| `magnitude <params>` | `curl -s "$BASE/earthquakes/summary/magnitude?<params>"`: `jq -c '.buckets[] \| [.label, .count]'` |
| `export` | `curl -s "$BASE/export/earthquakes?after_id=0&limit=2"` (what `make export-r2` pages through) |

(In the table `\|` is a pipe; run it as `|`.)

**Parameters** (list, stats, summaries): `min_magnitude`, `max_magnitude` (−2 to 10.5); `start_time` (inclusive),
`end_time` (exclusive), ISO 8601 date-times (a date alone is refused), UTC without a zone; `place` (substring,
`%` and `_` literal; URL-encode `%` as `%25`); `event_type`, `review_status`, `include_deleted=true`.
List only: `limit` 1–500, `offset`, `sort` = `event_time` | `magnitude` | `depth`, `order` = `asc` | `desc`.
Turn the user's words into these ("this week" → `start_time` = Monday 00:00 UTC; "M5+" → `min_magnitude=5`).

Errors are `{"error": {"code", "message", "details"}}`; a `422` lists every bad parameter — show its `details`.

## Report

The URL used, then the answer as a short table or a line (count, strongest, …). Don't paste long JSON.
