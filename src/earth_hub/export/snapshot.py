"""The analytics snapshot: the whole `earthquakes` table as one Parquet file in R2.

The per-row objects suit incremental sync; a dashboard would need one request per row to
read them. After a run that succeeded, the exporter also writes
    analytics/earthquakes.parquet   every row it read, every published column, in key order
    analytics/meta.json             what the file holds: run, row count, SHA-256, columns
built from the same rows it uploaded, so the snapshot is exactly what the bucket holds. It
is rewritten only when the rows changed: `content_sha256` in meta.json is a fingerprint of
the manifest entries it was built from.
"""

import hashlib
import json
import tempfile
from pathlib import Path

import duckdb

SNAPSHOT_KEY = "analytics/earthquakes.parquet"
META_KEY = "analytics/meta.json"
PARQUET_TYPE = "application/vnd.apache.parquet"

TIMESTAMPTZ = "TIMESTAMP WITH TIME ZONE"  # DuckDB's own name for it, as Parquet readers report

# Every published `earthquakes` column (all but the database's `id`), in table order, with
# the type it keeps in Parquet. A test checks this against EarthquakeRow (and so the table).
COLUMNS = {
    "key": "VARCHAR",
    "preferred_event_id": "VARCHAR",
    "event_time": TIMESTAMPTZ,
    "event_time_ms": "BIGINT",
    "source_updated": TIMESTAMPTZ,
    "source_updated_ms": "BIGINT",
    "magnitude": "DOUBLE",
    "mag_type": "VARCHAR",
    "place": "VARCHAR",
    "city": "VARCHAR",
    "region": "VARCHAR",
    "country": "VARCHAR",
    "longitude": "DOUBLE",
    "latitude": "DOUBLE",
    "depth_km": "DOUBLE",
    "event_type": "VARCHAR",
    "review_status": "VARCHAR",
    "tsunami": "BOOLEAN",
    "felt": "INTEGER",
    "cdi": "DOUBLE",
    "mmi": "DOUBLE",
    "alert": "VARCHAR",
    "sig": "INTEGER",
    "net": "VARCHAR",
    "url": "VARCHAR",
    "title": "VARCHAR",
    "revision_ids": "VARCHAR[]",
    "revision_count": "INTEGER",
    "first_seen_at": TIMESTAMPTZ,
    "last_applied_at": TIMESTAMPTZ,
}


def content_hash(rows: dict[str, str]) -> str:
    """A fingerprint of the rows a snapshot holds, from their manifest entries."""
    return hashlib.sha256(json.dumps(sorted(rows.items())).encode()).hexdigest()


def _quote(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


class SnapshotFile:
    """Collects row bodies (the JSON the exporter uploads) on disk, then builds the Parquet
    file from them. About 100 MB of JSON for 115k rows, so it is not kept in memory."""

    def __init__(self):
        self._dir = tempfile.TemporaryDirectory(prefix="earth-hub-snapshot-")
        self._rows_path = Path(self._dir.name) / "rows.ndjson"
        self._file = self._rows_path.open("wb")
        self.rows = 0

    def add(self, body: bytes) -> None:
        self._file.write(body + b"\n")
        self.rows += 1

    def build(self) -> bytes:
        self._file.close()
        out = Path(self._dir.name) / "earthquakes.parquet"
        columns = "{" + ", ".join(f"'{name}': '{kind}'" for name, kind in COLUMNS.items()) + "}"
        # Explicit types: nothing is inferred, and an empty table still has every column.
        source = (
            f"read_json({_quote(self._rows_path)}, format = 'newline_delimited', "
            f"columns = {columns})"
        )
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone = 'UTC'")
            con.execute(
                f"COPY (SELECT * FROM {source} ORDER BY key) TO {_quote(out)} "
                "(FORMAT parquet, COMPRESSION zstd)"
            )
        finally:
            con.close()
        return out.read_bytes()

    def close(self) -> None:
        self._file.close()
        self._dir.cleanup()
