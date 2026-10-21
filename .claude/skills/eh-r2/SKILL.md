---
name: eh-r2
description: Look inside Earth-Hub's Cloudflare R2 bucket, read-only — list keys under a prefix, count objects and bytes, print an object (a row, a run record, the snapshot's meta.json, the gunzipped manifest), save the Parquet snapshot, or list past export runs. Use for /eh-r2, "what's in the bucket", "show the last export run", "how many rows are in R2", "download the snapshot".
argument-hint: "[ls [PREFIX] | count [earthquakes/ | PREFIX [full]] | get KEY | snapshot | runs]"
---

# R2 bucket (read-only)

Run from the repository root. Credentials come from `.env` (`R2_*`); never print them or `.env`. These commands
only list and read; writing to the bucket is `/eh-export`'s job. If they fail with "set R2_… in .env", say which
settings are missing (the message names them).

| Argument | Command |
| --- | --- |
| `ls [PREFIX]` (default: the top) | `make -s r2-ls PREFIX=<PREFIX> [LIMIT=50]` (`LIMIT=0`: all) |
| `count` or `count earthquakes/` | **Fast, no listing** (a few seconds): `make -s r2-get KEY=analytics/meta.json \| jq -c '{run_id, written_at, rows, bytes}'` (rewritten by every export and every direct round: the rows now), `make -s export-r2-runs \| tail -1` (the latest export: frozen at the last handover while in direct mode), `make -s mode`, and in direct mode `make -s r2-get KEY=_state/direct.json \| jq -c '{seq, written_at}'`. If PostgreSQL runs, also `/eh-db counts` for the comparison row |
| `count PREFIX` / `count earthquakes/ full` | `make -s r2-count PREFIX=<PREFIX>`: lists every object (`earthquakes/`: one per row, ~115,000, ~100 s). Use `full` when the snapshot and the row objects might disagree; `make reconcile-direct` checks that properly |
| `get KEY` | `make -s r2-get KEY=<KEY>`, e.g. `analytics/meta.json`, `earthquakes/<key>.json` (a record's key: the USGS ID it was created with), `runs/<run>.json`, `_state/earthquakes-manifest.json.gz` (printed gunzipped; summarize the manifest, it's large) |
| `snapshot` | `make -s r2-get KEY=analytics/earthquakes.parquet OUT=/tmp/earthquakes.parquet` (~7.5 MB), then e.g. `uv run python -c "import duckdb; print(duckdb.sql(\"SELECT count(*), max(magnitude) FROM '/tmp/earthquakes.parquet'\"))"` |
| `runs` | `make -s export-r2-runs` (the latest export runs and their counts) |

What's in the bucket (`docs/technical/architecture.md`, Export to R2): `earthquakes/<key>.json` (one per row, named by its key),
`_state/earthquakes-manifest.json.gz` (SHA-256 per row), `_state/lock.json` (only during a run),
`runs/<run_id>.json`, `analytics/earthquakes.parquet` and `analytics/meta.json` (the dashboard's snapshot).
Direct mode's state (architecture, Direct mode): `_state/mode.json` (which mode writes the bucket; `make mode`),
`_state/ids.json.gz` (the ID map), `_state/direct.json` (last sequence number, poller and catch-up state),
`_state/direct-pending.json` (only while a round writes), and `log/<date>/<first>-<last>.ndjson` (every
message processed, in order: e.g. `ls log/` with `LIMIT=1000 | tail` for the newest).

The public dashboard serves only the two `analytics/` files, at `/data/earthquakes.parquet` and
`/data/meta.json` (see `/eh-dashboard check`).

## Report

The listing or object, summarized: for `meta.json` the run, row count, size and when it was written; for a run
record its status and counts.

For `count`, a table like `/eh-db`'s, one row per source, then one line on what it means:

| Source | rows | written at (UTC) | by | log sequence |
| --- | --- | --- | --- | --- |
| `analytics/meta.json` (the dashboard's snapshot) | 115,845 | 21:34:34 | direct round `<run_id>` (or export `<run_id>`) | 62 (direct mode) |
| latest `runs/` record | 115,814 read (196 new, 54 updated, 0 deleted, 0 failed) | 19:55:49 | export `<run_id>` | – |
| PostgreSQL (`/eh-db counts`) | 115,814 | last applied 19:52:36 | full mode | – |

e.g. "the bucket holds 31 rows more than PostgreSQL: the events direct mode stored since the handover;
`make mode-full` replays them". In full mode, after an export, all three should agree. Compare with PostgreSQL (`/eh-db counts`) when the question is "is R2 up to date".
