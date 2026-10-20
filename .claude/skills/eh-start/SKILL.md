---
name: eh-start
description: Start Earth-Hub services — the infrastructure (Kafka, PostgreSQL), the live pipeline (poller, consumers, catch-up), the API, the Kafka UI or the local dashboard — and confirm they are healthy. Use for /eh-start, "start the pipeline", "bring up the API", "start everything".
argument-hint: "[infra | pipeline | api | ui | dashboard | all]  (default: infra)"
---

# Start services

Run from the repository root. Each target waits until the services are healthy.

| Argument | Command | Starts |
| --- | --- | --- |
| `infra` (default) | `make up` then `make migrate` | Kafka (KRaft) and PostgreSQL, both topics, the schema |
| `pipeline` | `make ingest` | migrate, poller, consumer, consumer-backfill, catch-up — **calls the live USGS feed** |
| `api` | `make api` | The REST API container at http://127.0.0.1:8000/docs |
| `ui` | `make kafka-ui` | The read-only Kafka viewer at http://127.0.0.1:8080 |
| `dashboard` | `make dashboard-dev` | The page at http://127.0.0.1:8787 over the real R2 bucket (read-only); runs in the foreground, so start it in the background |
| `all` | `make ingest`, then `make api` | Everything but the UI and the dashboard |

- If `.env` is missing, stop and say: `cp .env.example .env` first (never create or print `.env` yourself).
- `pipeline` and `all` start polling USGS: run them only when the user asked for that (the argument counts).
- **Before `pipeline` or `all`, check `make -s mode`.** If the bucket is in direct mode, don't start them: the
  marker doesn't stop full mode's poller, and events it stores make the handover back fail. Say so and offer
  `/eh-mode full` (which starts what it needs and restarts the poller and catch-up itself). `infra` and
  `api` are fine in either mode.
- `dashboard` needs `make dashboard-install` once, and R2 settings in `.env`.
- If a start fails, show the failing service's last 50 log lines (`docker compose logs --tail 50 <service>`).

## Report

What was started, each service's state (`docker compose ps`), and the addresses to open. Mention `/eh-stop`.
