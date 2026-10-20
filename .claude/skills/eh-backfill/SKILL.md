---
name: eh-backfill
description: Load history into Earth-Hub from the USGS FDSN service — a backfill from a start date, one catch-up pass, or progress of past backfills — and check the result. Use for /eh-backfill, "backfill 2025", "load history since June", "run catch-up", "how far did the backfill get".
argument-hint: "[START [END] | catchup | progress]"
---

# Backfill and catch-up

Run from the repository root. Both call the **live** USGS FDSN service and publish to `earthquakes.backfill`,
which `consumer-backfill` stores (it must run: `make ingest`).

| Argument | Command |
| --- | --- |
| `START [END]` (dates, e.g. `2026-01-01`) | `make backfill START=<START> [END=<END>]`. One request per week, a 1 s pause between requests: a year takes several minutes. Re-running skips finished weeks |
| `catchup` | `make catchup-once`: one `all_month` pass and one deleted-events check |
| `progress` | `docker compose exec -T postgres psql -U earth_hub_app earth_hub -c "SELECT job, status, count(*) AS windows, min(window_start), max(window_end), sum(expected) AS expected, sum(fetched) AS fetched, sum(published) AS published FROM backfill_windows GROUP BY job, status ORDER BY job, status"` |

- A backfill is a long, live operation: run it only when the user asked, with the dates they gave. Run it in
  the background and report when it ends.
- Afterwards, check the backfill consumer caught up (`/eh-status`: lag 0 for `earth-hub-consumer-backfill`)
  and run `make reconcile`.
- If it stops on a USGS outage it says "run again to resume": run the same command again.
- **Full mode only** (`make -s mode`). In direct mode, catch-up runs inside direct rounds (`/eh-mode once`), and
  a backfill or `catchup-once` would store events the handover back then finds unexpected. A long backfill
  waits until the bucket is back in full mode (`/eh-mode full`).

## Report

Weeks fetched and skipped, events published, any window that needed splitting or retrying, then the
reconciliation result.
