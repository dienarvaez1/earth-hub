"""The R2 export end to end: PostgreSQL -> the real API -> the exporter -> S3 (moto).

After every run, each object, and the analytics snapshot, must equal its `earthquakes` row
in every column and type.
"""

import json
from datetime import UTC, datetime, timedelta

import boto3
import duckdb
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import select

from earth_hub.api.main import create_app
from earth_hub.database.models import Earthquake
from earth_hub.database.repository import IncomingMessage, process_message
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import Exporter, key_row_id
from earth_hub.export.snapshot import SNAPSHOT_KEY

pytestmark = pytest.mark.integration

NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)
UPDATED = 1792137700000
BUCKET = "earth-hub-test"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


class Store:
    def __init__(self, db, make_envelope):
        self.db, self.make_envelope, self.offset = db, make_envelope, 0

    def __call__(self, *features):
        with self.db.begin() as conn:
            for f in features:
                message = IncomingMessage(
                    "earthquakes.raw", 0, self.offset, b"k", self.make_envelope(f)
                )
                process_message(conn, message, now=NOW)
                self.offset += 1


def export(db, s3, tmp_path, minute=0):
    with TestClient(create_app(db), base_url="http://testserver/api/v1/") as api:
        exporter = Exporter(
            api,
            Bucket(s3, BUCKET),
            api_url="http://testserver/api/v1",
            workers=4,
            page_size=2,
            clock=lambda: NOW + timedelta(minutes=minute),
            local_dir=tmp_path,
        )
        return exporter.run()


def table_rows(db) -> dict[int, dict]:
    with db.connect() as conn:
        return {r["id"]: dict(r) for r in conn.execute(select(Earthquake.__table__)).mappings()}


def assert_snapshot_equals_table(db, s3, tmp_path):
    path = tmp_path / "snapshot.parquet"
    path.write_bytes(s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)["Body"].read())
    con = duckdb.connect()
    relation = con.sql(f"SELECT * FROM '{path}'")
    snapshot = [dict(zip(relation.columns, r, strict=True)) for r in relation.fetchall()]
    con.close()
    table = table_rows(db)
    assert [r["id"] for r in snapshot] == sorted(table)
    for got in snapshot:
        row = table[got["id"]]
        assert list(got) == list(row)
        for column, value in row.items():
            assert got[column] == value, (got["id"], column)
            assert type(got[column]) is type(value), (got["id"], column)


def assert_bucket_equals_table(db, s3):
    table = table_rows(db)
    objects = {}
    for item in s3.list_objects_v2(Bucket=BUCKET, Prefix="earthquakes/").get("Contents", []):
        objects[key_row_id(item["Key"])] = json.loads(
            s3.get_object(Bucket=BUCKET, Key=item["Key"])["Body"].read()
        )
    assert set(objects) == set(table)
    for row_id, row in table.items():
        obj = objects[row_id]
        assert list(obj) == list(row)
        for column, value in row.items():
            got = obj[column]
            if isinstance(value, datetime):
                got = datetime.fromisoformat(got)
            assert got == value, (row_id, column)
            assert type(got) is type(value), (row_id, column)


@pytest.mark.req("REQ-EXP-001", "REQ-EXP-002", "REQ-EXP-003", "REQ-EXP-004", "REQ-EXP-009")
def test_the_bucket_matches_the_table_through_revisions_deletions_and_merges(
    db, make_envelope, make_feature, s3, tmp_path
):
    store = Store(db, make_envelope)
    store(
        make_feature("ak1", updated=UPDATED, place="86 km E of Ugashik, Alaska", mag=2.37),
        make_feature("us2", updated=UPDATED, place="Fiji region", mag=None),
        make_feature("ci3", updated=UPDATED, place="10 km N of Cobb, CA", mag=-0.43),
        make_feature("us4", updated=UPDATED, place="12 km SSE of Bak’uriani, Georgia"),
        make_feature("ak5", updated=UPDATED, place="80 km E of Pilot Point, Alaska"),
    )
    first = export(db, s3, tmp_path)
    assert first.status == "succeeded"
    assert (first.counts.read, first.counts.new) == (5, 5)
    assert_bucket_equals_table(db, s3)
    assert_snapshot_equals_table(db, s3, tmp_path)

    second = export(db, s3, tmp_path, minute=1)
    assert (second.counts.unchanged, second.counts.new, second.counts.updated) == (5, 0, 0)

    store(
        make_feature("ak1", updated=UPDATED + 60_000, mag=2.41),  # a revision
        make_feature("ci3", updated=UPDATED + 60_000, status="deleted"),  # a USGS deletion
        # ak5 turns out to be us4: one row absorbs the other, which leaves the table.
        make_feature("us4", ["us4", "ak5"], updated=UPDATED + 60_000),
        make_feature("nc6", updated=UPDATED),  # a new event
    )
    third = export(db, s3, tmp_path, minute=2)
    assert third.status == "succeeded"
    c = third.counts
    assert (c.read, c.new, c.updated, c.unchanged, c.deleted, c.failed) == (5, 1, 3, 1, 1, 0)
    assert_bucket_equals_table(db, s3)
    assert third.snapshot["written"]
    assert_snapshot_equals_table(db, s3, tmp_path)
    runs = s3.list_objects_v2(Bucket=BUCKET, Prefix="runs/")["KeyCount"]
    assert runs == 3
