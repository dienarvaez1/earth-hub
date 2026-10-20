---
name: eh-export
description: Update the R2 copy of Earth-Hub's earthquakes table (and the dashboard's Parquet snapshot) from PostgreSQL through the API — a dry run of what would change, the export itself, a verifying export that lists the whole bucket, or the past runs. Use for /eh-export, "update R2", "sync the database to R2", "refresh the dashboard data", "what would the export change".
argument-hint: "[dry | run | verify | runs]  (default: dry)"
---

# Export PostgreSQL to R2

Run from the repository root. The exporter reads only the API (`GET /api/v1/export/earthquakes`), never
PostgreSQL directly, so **the API must be running**: check `curl -s -m 5 http://127.0.0.1:8000/api/v1/health`,
and if it doesn't answer, start it with `make api` first (say so).

**Full mode only.** The exporter checks the bucket's mode marker after taking the lock, and refuses in
direct mode ("the bucket is in direct mode, not full (make mode)"): there, the hourly direct rounds write the
rows and the snapshot themselves. Check `make -s mode` first; to export again, hand back with `/eh-mode full`.

| Argument | Command | Writes to R2 |
| --- | --- | --- |
| `dry` (default) | `make export-r2 DRY_RUN=1` | no: counts new, updated, unchanged and deleted rows |
| `run` | `make export-r2` | yes: uploads new and changed rows, deletes merged ones, then rewrites the Parquet snapshot if rows changed (~35 s with a few changes, ~8 s with none) |
| `verify` | `make export-r2 VERIFY=1` | yes, and lists the whole bucket (~100 s more) to remove unknown objects and re-upload missing ones |
| `runs` | `make -s export-r2-runs` | no |

- With no argument, do the dry run and show what a real run would change; run the export only when the user
  said `run` or `verify` (or confirms after the dry run).
- Only one run at a time: a lock in the bucket (`_state/lock.json`, expiring after 2 h) refuses a second one.
  If a run reports the lock is held, don't remove it; say who holds it and when it expires.
- Run it in the background if it may take long (a first full export of ~115,000 rows takes over an hour).
- The public dashboard reads the new snapshot within 5 minutes (its browser cache).

## Report

The run's line (`read … new … updated … unchanged … deleted … failed …`), its status, and whether the snapshot
was rewritten (`/eh-r2 get analytics/meta.json` shows the new run ID). A failed upload is retried by the next run.
