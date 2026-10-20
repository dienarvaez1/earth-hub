---
name: eh-stop
description: Stop Earth-Hub services — the live pipeline, the API, the Kafka UI, everything — keeping data, or wipe all data with reset (after confirmation). Use for /eh-stop, "stop the pipeline", "shut everything down", "reset the database".
argument-hint: "[pipeline | api | ui | all | reset]  (default: pipeline)"
---

# Stop services

Run from the repository root.

| Argument | Command | Effect |
| --- | --- | --- |
| `pipeline` (default) | `make ingest-stop` | Stops poller, consumers and catch-up; Kafka and PostgreSQL keep running |
| `api` | `make api-stop` | Stops the API container |
| `ui` | `make kafka-ui-stop` | Stops the Kafka viewer |
| `all` | `make down` | Stops every container; **data is kept** in the Docker volumes |
| `reset` | `make reset` | Stops every container and **deletes all Kafka and PostgreSQL data** |

**`reset` is destructive and can't be undone.** Before running it, show the current counts
(`docker compose exec -T postgres psql -U earth_hub_app earth_hub -Atc "SELECT count(*) FROM earthquakes"`)
and ask the user to confirm, even if they typed `/eh-stop reset`. Afterwards, rebuilding means
`make up && make migrate`, then `make ingest` (one day of events) or `make backfill START=...` for history.

A dashboard started with `make dashboard-dev` runs in the foreground or as a background task: stop that task.

## Report

What was stopped and `docker compose ps` afterwards.
