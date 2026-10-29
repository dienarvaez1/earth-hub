"""The analytics snapshot the exporter writes: analytics/earthquakes.parquet and meta.json."""

import hashlib
import json
from datetime import datetime, timedelta

import duckdb
import pytest
from support.export import BUCKET, NOW, make_row

from earth_hub.api.schemas import EarthquakeRow
from earth_hub.export.r2 import published
from earth_hub.export.snapshot import COLUMNS, META_KEY, SNAPSHOT_KEY, TIMESTAMPTZ, SnapshotFile

pytestmark = pytest.mark.req("REQ-EXP-009")

LATER = [lambda n=n: NOW + timedelta(minutes=n) for n in range(1, 10)]


def read_snapshot(s3, tmp_path) -> tuple[list[dict], dict[str, str]]:
    """The snapshot's rows, as Python values, and its column types."""
    path = tmp_path / "snapshot.parquet"
    path.write_bytes(s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)["Body"].read())
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    relation = con.sql(f"SELECT * FROM '{path}'")
    types = {name: str(kind) for name, kind in zip(relation.columns, relation.types, strict=True)}
    rows = [dict(zip(relation.columns, r, strict=True)) for r in relation.fetchall()]
    con.close()
    return rows, types


def as_python(row: dict) -> dict:
    """An API row with its timestamps parsed, to compare with what Parquet gives back."""
    return {
        k: datetime.fromisoformat(v) if COLUMNS[k] == TIMESTAMPTZ else v for k, v in row.items()
    }


def meta(s3) -> dict:
    return json.loads(s3.get_object(Bucket=BUCKET, Key=META_KEY)["Body"].read())


def keys(s3, prefix="analytics/") -> list[str]:
    return [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])]


def test_the_columns_are_every_table_column_but_id_in_order():
    assert list(COLUMNS) == [f for f in EarthquakeRow.model_fields if f != "id"]


def test_a_run_writes_every_row_and_column_exactly(export, api, s3, tmp_path):
    api.rows[2] = make_row(3, magnitude=None, place=None, city=None, felt=12, cdi=3.4)
    api.rows[3] = make_row(4, revision_ids=["us0004", "ak0004"], revision_count=2)
    record = export()
    assert record.status == "succeeded"
    assert record.snapshot["written"] and record.snapshot["rows"] == 7
    rows, types = read_snapshot(s3, tmp_path)
    assert types == COLUMNS
    assert rows == [as_python(published(r)) for r in api.rows]  # every value, in key order
    assert rows[0]["event_time"].microsecond == 123456


@pytest.mark.req("REQ-EXP-010")
@pytest.mark.req("REQ-EXP-013")
def test_the_snapshot_is_written_small_with_zstd(tmp_path):
    snapshot = SnapshotFile()
    for n in range(1, 3001):
        snapshot.add(json.dumps(published(make_row(n, magnitude=n % 70 / 10))).encode())
    try:
        body = snapshot.build()
    finally:
        snapshot.close()
    path, default = tmp_path / "snapshot.parquet", tmp_path / "default.parquet"
    path.write_bytes(body)
    con = duckdb.connect()
    codecs = con.sql(f"SELECT DISTINCT compression FROM parquet_metadata('{path}')").fetchall()
    con.execute(f"COPY (FROM '{path}') TO '{default}' (FORMAT parquet, COMPRESSION zstd)")
    assert codecs == [("ZSTD",)]
    assert con.sql(f"SELECT count(*) FROM '{path}'").fetchone() == (3000,)
    con.close()
    assert len(body) < default.stat().st_size  # smaller than zstd's default level


def test_meta_describes_the_file(export, s3):
    record = export()
    body = s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)["Body"].read()
    assert meta(s3) | {"content_sha256": None, "written_at": None} == {
        "table": "earthquakes",
        "run_id": record.run_id,
        "written_at": None,
        "rows": 7,
        "bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
        "content_sha256": None,
        "columns": COLUMNS,
    }


def test_an_unchanged_table_does_not_rewrite_the_snapshot(export, bucket):
    export()
    bucket.puts.clear()
    record = export(clock=LATER[0])
    assert record.snapshot == {"written": False, "rows": 7, "reason": "unchanged"}
    assert SNAPSHOT_KEY not in bucket.puts and META_KEY not in bucket.puts


def test_changes_rewrite_the_snapshot(export, api, s3, tmp_path):
    export()
    api.rows[0] = make_row(1, magnitude=4.1)
    del api.rows[4]  # merged away
    api.rows.append(make_row(12))
    record = export(clock=LATER[0])
    assert record.snapshot["written"]
    rows, _ = read_snapshot(s3, tmp_path)
    assert [r["key"] for r in rows] == [f"us{n:04d}" for n in (1, 2, 3, 4, 6, 7, 12)]
    assert rows[0]["magnitude"] == 4.1
    assert meta(s3)["run_id"] == record.run_id


def test_a_dry_run_never_writes_it(export, s3):
    record = export(dry_run=True)
    assert record.snapshot is None and keys(s3) == []


def test_a_failed_run_does_not_write_it_and_the_next_run_does(export, bucket, api, s3, tmp_path):
    bucket.failing = {"earthquakes/us0002.json"}
    record = export()
    assert record.status == "failed" and record.snapshot is None
    assert keys(s3) == []
    record = export(clock=LATER[0])
    assert record.status == "succeeded" and record.snapshot["written"]
    assert len(read_snapshot(s3, tmp_path)[0]) == 7


@pytest.mark.req("REQ-EXP-010")
def test_a_failed_snapshot_upload_fails_the_run_and_is_retried(export, bucket, s3):
    bucket.failing = {SNAPSHOT_KEY}
    record = export()
    assert record.status == "failed"
    assert any("snapshot not written" in e for e in record.errors)
    assert keys(s3) == []  # meta.json is written only after the file
    assert export(clock=LATER[0]).snapshot["written"]  # nothing else changed: still written
    assert keys(s3) == [SNAPSHOT_KEY, META_KEY]


@pytest.mark.req("REQ-EXP-010")
@pytest.mark.parametrize("damage", ["delete", "garble"])
def test_a_missing_or_unreadable_meta_rewrites_the_snapshot(export, s3, damage):
    export()
    if damage == "delete":
        s3.delete_object(Bucket=BUCKET, Key=META_KEY)
    else:
        s3.put_object(Bucket=BUCKET, Key=META_KEY, Body=b"not json")
    assert export(clock=LATER[0]).snapshot["written"]
    assert meta(s3)["rows"] == 7


def test_an_empty_table_gives_an_empty_snapshot_with_every_column(export, api, s3, tmp_path):
    api.rows.clear()
    record = export()
    assert record.status == "succeeded" and record.snapshot["rows"] == 0
    rows, types = read_snapshot(s3, tmp_path)
    assert rows == [] and types == COLUMNS
