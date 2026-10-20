---
name: eh-r2
description: Look inside Earth-Hub's Cloudflare R2 bucket, read-only — list keys under a prefix, count objects and bytes, print an object (a row, a run record, the snapshot's meta.json, the gunzipped manifest), save the Parquet snapshot, or list past export runs. Use for /eh-r2, "what's in the bucket", "show the last export run", "how many rows are in R2", "download the snapshot".
argument-hint: "[ls [PREFIX] | count [PREFIX] | get KEY | snapshot | runs]"
---

# R2 bucket (read-only)

Run from the repository root. Credentials come from `.env` (`R2_*`); never print them or `.env`. These commands
only list and read; writing to the bucket is `/eh-export`'s job. If they fail with "set R2_… in .env", say which
settings are missing (the message names them).

| Argument | Command |
| --- | --- |
| `ls [PREFIX]` (default: the top) | `make -s r2-ls PREFIX=<PREFIX> [LIMIT=50]` (`LIMIT=0`: all) |
| `count [PREFIX]` | `make -s r2-count PREFIX=<PREFIX>` (e.g. `earthquakes/`: one object per row; listing ~115,000 takes ~100 s) |
| `get KEY` | `make -s r2-get KEY=<KEY>`, e.g. `analytics/meta.json`, `earthquakes/<id>.json`, `runs/<run>.json`, `_state/earthquakes-manifest.json.gz` (printed gunzipped; summarize the manifest, it's large) |
| `snapshot` | `make -s r2-get KEY=analytics/earthquakes.parquet OUT=/tmp/earthquakes.parquet` (~7.5 MB), then e.g. `uv run python -c "import duckdb; print(duckdb.sql(\"SELECT count(*), max(magnitude) FROM '/tmp/earthquakes.parquet'\"))"` |
| `runs` | `make -s export-r2-runs` (the latest export runs and their counts) |

What's in the bucket (`docs/architecture.md`, Export to R2): `earthquakes/<id>.json` (one per row),
`_state/earthquakes-manifest.json.gz` (SHA-256 per row), `_state/lock.json` (only during a run),
`runs/<run_id>.json`, `analytics/earthquakes.parquet` and `analytics/meta.json` (the dashboard's snapshot).

The public dashboard serves only the two `analytics/` files, at `/data/earthquakes.parquet` and
`/data/meta.json` (see `/eh-dashboard check`).

## Report

The listing or object, summarized: for `meta.json` the run, row count, size and when it was written; for a run
record its status and counts. Compare with PostgreSQL (`/eh-db counts`) when the question is "is R2 up to date".
