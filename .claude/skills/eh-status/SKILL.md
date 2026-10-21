---
name: eh-status
description: Health of the whole Earth-Hub stack in one look — which containers run and are healthy, consumer lag per topic, newest stored event, API health, the last R2 export, the mode and its hourly schedule — or a full topic-versus-database reconciliation. Use for /eh-status, "is everything running", "is the pipeline healthy", "any lag", or "reconcile".
argument-hint: "[reconcile]"
---

# Earth-Hub status

Read-only. Run from the repository root.

## Gather (in parallel)

- `docker compose --profile ingest --profile api --profile tools ps --format '{{.Service}}\t{{.Status}}'`
- Consumer lag, both groups:
  `docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group earth-hub-consumer`
  and the same with `--group earth-hub-consumer-backfill` (sum the LAG column per group).
- Database: `docker compose exec -T postgres psql -U earth_hub_app earth_hub -Atc "SELECT count(*), max(event_time), max(last_applied_at) FROM earthquakes"`
  and `... -Atc "SELECT outcome, count(*) FROM raw_events WHERE received_at > now() - interval '1 hour' GROUP BY 1"`
- API, if its container runs: `curl -s -m 5 http://127.0.0.1:8000/api/v1/health`
- Last export: `make -s export-r2-runs | tail -1` (runs are listed oldest first), only if `.env` has `R2_BUCKET` set (check with
  `grep -q '^R2_BUCKET=.' .env`; never print `.env`).
- Mode (same condition): `make -s mode`; in direct mode also the schedule's last runs,
  `gh run list --workflow direct.yml --limit 3` (filter lines with `@`).

With the argument `reconcile`: also run `make reconcile` (read-only; about 10 s) and `make reconcile-usgs`
(read-only, a few FDSN requests: Earth-Hub against USGS for the last 3 days).

If Kafka or PostgreSQL isn't running, say so and suggest `/eh-start`; skip the checks that need it.

**In direct mode the bucket is written without Docker** (`/eh-mode`): a stopped poller and catch-up are
expected then, not a fault, and PostgreSQL falls behind until `make mode-full`. Judge freshness from the
last direct round, `make -s r2-get KEY=_state/direct.json | jq -c '{seq, written_at}'` (its sequence number and time), and the last scheduled run instead;
`make reconcile-usgs` says whether anything USGS published is still missing, and `make catchup-now` closes it.

## Report

A short table: service, state (healthy / unhealthy / stopped). Then one line each: lag per group, stored
events and the newest event time (and how long ago), last hour's outcomes, API health, last export run,
the mode (and in direct mode, the last scheduled run and the last direct round).
With `reconcile`: each topic's RESULT, and any lost or pending messages. Flag anything wrong first: an
unhealthy service, lag that isn't 0, a newest event older than ~15 minutes while the poller runs, lost messages,
a failed scheduled run, or a last direct round older than ~2 hours in direct mode.
