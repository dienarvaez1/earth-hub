"""Parity of the two modes at the store level: the same messages, in the same batches with the
same processing times, through full mode (PostgreSQL, then the real API and the exporter) and
through direct mode (the bucket), must give identical buckets.

Compared: every row object byte for byte, the manifest, the snapshot's rows and types and its
fingerprint, the ID map against `earthquake_ids`, and each message's outcome in the log against
`raw_events`. Streams are random (revisions, late and duplicate messages, ID changes, merges,
deletions, invalid messages) plus every persistence defect scenario.
"""

import gzip
import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

import boto3
import duckdb
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import select
from support.export import start_direct
from support.streams import random_stream

from earth_hub.api.main import create_app
from earth_hub.database.models import Earthquake, EarthquakeId, RawEvent
from earth_hub.database.repository import IncomingMessage, process_batch
from earth_hub.direct.runner import DirectRunner
from earth_hub.direct.store import IDS_KEY, LOG_PREFIX
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import MANIFEST_KEY, Exporter
from earth_hub.export.snapshot import META_KEY, SNAPSHOT_KEY

pytestmark = pytest.mark.integration

START = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)
UPDATED = 1792137700000


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        for name in ("full-mode", "direct-mode"):
            client.create_bucket(Bucket=name)
        yield client


class Modes:
    """Feeds batches to both modes; `compare` checks the two buckets are the same."""

    def __init__(self, db, s3, tmp_path):
        self.db, self.s3, self.tmp_path = db, s3, tmp_path
        self.offset = 0
        self.batches = 0
        (tmp_path / "direct").mkdir()
        start_direct(s3, "direct-mode")
        self.direct = DirectRunner(
            Bucket(s3, "direct-mode"), workdir=tmp_path / "direct", workers=4
        )

    def batch(self, values: list[bytes]) -> None:
        now = START + timedelta(minutes=self.batches, microseconds=self.batches * 7)
        self.batches += 1
        messages = [
            IncomingMessage("earthquakes.raw", 0, self.offset + i, b"k", v)
            for i, v in enumerate(values)
        ]
        self.offset += len(values)
        with self.db.begin() as conn:
            process_batch(conn, messages, now=now)
        self.direct.process(values, now=now)

    def export(self) -> None:
        with TestClient(create_app(self.db), base_url="http://testserver/api/v1/") as api:
            record = Exporter(
                api,
                Bucket(self.s3, "full-mode"),
                api_url="http://testserver/api/v1",
                workers=4,
                page_size=7,
                clock=lambda: START + timedelta(days=1),
                local_dir=self.tmp_path / "runs",
            ).run()
        assert record.status == "succeeded", record.errors

    def compare(self) -> None:
        self.export()
        full = contents(self.s3, "full-mode", self.tmp_path)
        direct = contents(self.s3, "direct-mode", self.tmp_path)
        assert direct["rows"].keys() == full["rows"].keys()
        for key, body in full["rows"].items():
            assert direct["rows"][key] == body, key
        assert direct["manifest"] == full["manifest"]
        assert direct["snapshot"] == full["snapshot"]
        assert direct["meta"] == full["meta"]
        ids = direct_ids(self.s3)
        assert ids == table_ids(self.db)
        # The log names each message's record as it was; the ID map, where it is now.
        now = [(*line[:4], line[4] and ids[line[4]][0]) for line in log_outcomes(self.s3)]
        assert now == raw_outcomes(self.db)


def contents(s3, bucket: str, tmp_path) -> dict:
    def get(key):
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    listed = s3.list_objects_v2(Bucket=bucket, Prefix="earthquakes/").get("Contents", [])
    rows = {item["Key"]: get(item["Key"]) for item in listed}
    manifest = json.loads(gzip.decompress(get(MANIFEST_KEY)))["rows"]
    meta = json.loads(get(META_KEY))
    path = tmp_path / f"{bucket}.parquet"
    path.write_bytes(get(SNAPSHOT_KEY))
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    relation = con.sql(f"SELECT * FROM '{path}'")
    snapshot = {
        "columns": list(zip(relation.columns, map(str, relation.types), strict=True)),
        "rows": relation.fetchall(),
    }
    con.close()
    return {
        "rows": rows,
        "manifest": manifest,
        "snapshot": snapshot,
        "meta": {k: meta[k] for k in ("rows", "content_sha256", "columns")},
    }


def direct_ids(s3) -> dict[str, tuple[str, datetime]]:
    body = json.loads(
        gzip.decompress(s3.get_object(Bucket="direct-mode", Key=IDS_KEY)["Body"].read())
    )
    return {i: (key, datetime.fromisoformat(seen)) for i, (key, seen) in body["ids"].items()}


def table_ids(db) -> dict[str, tuple[str, datetime]]:
    with db.connect() as conn:
        rows = conn.execute(
            select(EarthquakeId.event_id, Earthquake.key, EarthquakeId.first_seen_at).join(
                Earthquake, Earthquake.id == EarthquakeId.earthquake_id
            )
        ).all()
    return {r.event_id: (r.key, r.first_seen_at) for r in rows}


def log_outcomes(s3) -> list[tuple]:
    lines = []
    listed = s3.list_objects_v2(Bucket="direct-mode", Prefix=LOG_PREFIX).get("Contents", [])
    for item in sorted(listed, key=lambda i: i["Key"].rsplit("/", 1)[1]):
        body = s3.get_object(Bucket="direct-mode", Key=item["Key"])["Body"].read()
        lines += [json.loads(line) for line in body.splitlines()]
    assert [line["seq"] for line in lines] == list(range(1, len(lines) + 1))
    return [(e["event_id"], e["outcome"], e["rule"], e["reason"], e["key"]) for e in lines]


def raw_outcomes(db) -> list[tuple]:
    with db.connect() as conn:
        rows = conn.execute(
            select(
                RawEvent.event_id, RawEvent.outcome, RawEvent.rule, RawEvent.reason, Earthquake.key
            )
            .outerjoin(Earthquake, Earthquake.id == RawEvent.earthquake_id)
            .order_by(RawEvent.kafka_offset)
        ).all()
    return [tuple(r) for r in rows]


@pytest.fixture
def modes(db, s3, tmp_path):
    return Modes(db, s3, tmp_path)


@pytest.mark.req("REQ-MODE-001")
@pytest.mark.parametrize("seed", range(12))
def test_random_streams_give_the_same_bucket_in_both_modes(
    modes, make_envelope, make_feature, seed
):
    rng = random.Random(seed)
    values = random_stream(rng, f"s{seed}", make_envelope, make_feature, merges=True)
    cursor = 0
    while cursor < len(values):
        size = rng.randint(1, 25)
        modes.batch(values[cursor : cursor + size])
        cursor += size
    modes.compare()


def feature(make_feature, event_id, ids=None, **properties):
    return make_feature(event_id, ids, **{"updated": UPDATED, **properties})


@pytest.mark.req("REQ-MODE-001")
def test_revisions_id_changes_and_three_way_merges(modes, make_envelope, make_feature):
    e = make_envelope
    modes.batch([e(feature(make_feature, "ak1", mag=2.0)), e(feature(make_feature, "us2"))])
    modes.batch([e(feature(make_feature, "ci3", mag=1.1, place="10 km N of Cobb, CA"))])
    # A newer revision; an older one arriving late; an exact duplicate.
    modes.batch(
        [
            e(feature(make_feature, "ak1", mag=2.4, updated=UPDATED + 60_000)),
            e(feature(make_feature, "ak1", mag=1.0, updated=UPDATED + 30_000)),
            e(feature(make_feature, "us2")),
        ]
    )
    modes.compare()
    # The preferred ID changes: us2 is now reported as us2b, listing both.
    modes.batch([e(feature(make_feature, "us2b", ["us2b", "us2"], updated=UPDATED + 90_000))])
    # Three reports turn out to be one event, in one message; the smallest key, ak1, survives.
    modes.batch(
        [e(feature(make_feature, "ci3", ["ci3", "us2b", "ak1"], updated=UPDATED + 120_000))]
    )
    modes.compare()
    # A merge inside one batch, of a record created in that batch.
    modes.batch(
        [
            e(feature(make_feature, "nc4")),
            e(feature(make_feature, "nc5", updated=UPDATED + 5)),
            e(feature(make_feature, "nc5", ["nc5", "nc4"], updated=UPDATED + 10)),
        ]
    )
    modes.compare()


@pytest.mark.req("REQ-MODE-001", "REQ-PER-006")
def test_a_merge_keeps_the_smallest_key_even_when_created_last(modes, make_envelope, make_feature):
    """DEF-050: full mode can create records in another order than the feed's, so the survivor
    is never the first record created, in either store."""
    e = make_envelope
    modes.batch([e(feature(make_feature, "us7", mag=4.0))])
    modes.batch([e(feature(make_feature, "ak7", mag=3.0, updated=UPDATED + 5))])
    modes.batch([e(feature(make_feature, "us7", ["us7", "ak7"], updated=UPDATED + 10))])
    # Within one batch, three created in reverse key order, then one message merges them.
    modes.batch(
        [
            e(feature(make_feature, "nc9")),
            e(feature(make_feature, "nc8", updated=UPDATED + 1)),
            e(feature(make_feature, "nc7", updated=UPDATED + 2)),
            e(feature(make_feature, "nc9", ["nc9", "nc8", "nc7"], updated=UPDATED + 3)),
        ]
    )
    modes.compare()
    keys = sorted(r.key for r in rows_of(modes.db))
    assert keys == ["ak7", "nc7"]


def rows_of(db):
    with db.connect() as conn:
        return conn.execute(select(Earthquake.key)).all()


@pytest.mark.req("REQ-MODE-001")
def test_deletion_scenarios_of_the_defect_log(modes, make_envelope, make_feature):
    e = make_envelope
    # DEF-026: a duplicate deleted on its own must not hide the event it was merged into,
    # whichever arrives first.
    modes.batch([e(feature(make_feature, "aka1", ["aka1", "aka1dup"]))])
    modes.batch([e(feature(make_feature, "aka1dup", status="deleted", updated=UPDATED + 60_000))])
    modes.batch([e(feature(make_feature, "uw2dup", status="deleted", updated=UPDATED + 1))])
    modes.batch([e(feature(make_feature, "uw2", ["uw2", "uw2dup"], updated=UPDATED))])
    # DEF-027: a deletion with placeholder values (magnitude -5) is applied, the values stored
    # as unknown.
    modes.batch([e(feature(make_feature, "uw3"))])
    modes.batch(
        [e(feature(make_feature, "uw3", status="deleted", mag=-5, updated=UPDATED + 60_000))]
    )
    # A deleted event revived by a newer revision.
    modes.batch([e(feature(make_feature, "uw3", status="reviewed", updated=UPDATED + 120_000))])
    # DEF-066: the deletion the audit builds for one FDSN kept no time for. Of the event itself
    # it deletes it; of an ID merged into another event, it is that duplicate's (DEF-026).
    tx4 = feature(make_feature, "tx4")
    us5 = feature(make_feature, "us5", ["us5", "tx5"])
    modes.batch([e(tx4)])
    modes.batch([e(us5)])
    modes.batch([audit_deletion("tx4", tx4), audit_deletion("tx5", us5)])
    with modes.db.connect() as conn:
        status = dict(conn.execute(select(Earthquake.key, Earthquake.review_status)).all())
    assert (status["tx4"], status["us5"]) == ("deleted", "reviewed")  # us5 only lost a duplicate
    modes.compare()


def audit_deletion(code: str, stored_feature: dict) -> bytes:
    """What catch-up's audit publishes when USGS deleted `code` without keeping its time, for
    the stored event `stored_feature`."""
    from earth_hub.ingestion.history import Stored, deletion

    path = (
        Path(__file__).resolve().parents[1] / "fixtures/usgs/deleted_without_time_tx2026pfurqq.json"
    )
    record = json.loads(path.read_text())
    for kind in record["properties"]["products"].values():
        for product in kind:
            product.update(code=code, updateTime=UPDATED + 300_000)
    props = stored_feature["properties"]
    stored = Stored(
        stored_feature["id"],
        stored_feature["id"],
        frozenset(i for i in props["ids"].split(",") if i),
        props["time"],
        *stored_feature["geometry"]["coordinates"],
        props["type"],
        props.get("place"),
    )
    return deletion(record, stored, fetched_at=datetime(2026, 10, 31, tzinfo=UTC)).to_json()


@pytest.mark.req("REQ-MODE-001")
def test_rejected_and_unstorable_messages(modes, make_envelope, make_feature):
    e = make_envelope
    bad = feature(make_feature, "hv1")
    bad["geometry"]["coordinates"][1] = 95.0
    modes.batch(
        [
            e(bad),  # GEO-002
            b"not json",
            b"null",
            b'{"x": NaN}',
            b'{"event_id": "a\\u0000b"}',  # DEF-009: NUL, which jsonb cannot hold
            e(feature(make_feature, "hv2"), schema_version=99),  # ENV-002
            e(feature(make_feature, "hv3")),
        ]
    )
    modes.compare()


@pytest.mark.req("REQ-MODE-001")
def test_the_live_feed_fixture_one_message_per_batch(modes, make_envelope, feed_doc):
    """The live consumer's setting: batches of one, each with its own processing time."""
    for f in feed_doc["features"]:
        modes.batch([make_envelope(f)])
    modes.compare()
