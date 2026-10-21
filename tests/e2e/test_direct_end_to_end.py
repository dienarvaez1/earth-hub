"""Parity of the two modes end to end: the same mock USGS feed, month feed and FDSN service,
read by full mode's whole pipeline (poller, Kafka, consumers, PostgreSQL, API, exporter) and by
direct mode (the same poller and catch-up, publishing to the bucket), give the same rows.

The modes run on their own clocks here, so processing times (`first_seen_at`,
`last_applied_at`) differ and are left out; tests/integration/test_direct_parity.py compares
them too, with the same `now` given to both. Merges happen across feed versions: within one
version, full mode's three partitions may process messages in either order.
"""

import json

import boto3
import duckdb
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from support.export import start_direct
from support.feed import FeedServer, revised

from earth_hub.api.main import create_app
from earth_hub.database.progress import Progress
from earth_hub.direct.reconcile import reconcile as reconcile_direct
from earth_hub.direct.runner import BucketProgress, DirectPublisher, DirectRunner
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import Exporter
from earth_hub.export.snapshot import SNAPSHOT_KEY
from earth_hub.ingestion.catchup import CatchUp
from earth_hub.ingestion.fdsn import FdsnClient
from earth_hub.ingestion.poller import Poller
from earth_hub.ingestion.producer import EventPublisher, make_producer
from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client

pytestmark = pytest.mark.e2e

PROCESSING_TIMES = ("first_seen_at", "last_applied_at")
DAY_FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
MONTH_FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        for name in ("full-mode", "direct-mode"):
            client.create_bucket(Bucket=name)
        yield client


class Direct:
    """Direct mode over the same mock services: the real Poller and CatchUp."""

    def __init__(self, s3, tmp_path, feed, month, fdsn):
        start_direct(s3, "direct-mode")
        self.runner = DirectRunner(Bucket(s3, "direct-mode"), workdir=tmp_path, workers=4)
        state = self.runner.sent_state()
        self.poller = Poller(
            UsgsClient(DAY_FEED, make_http_client(transport=feed.transport), sleep=lambda s: None),
            DirectPublisher(self.runner, source="feed", state=state),
            state,
            fallback_wait_s=60,
        )
        self.catchup = catch_up(
            month,
            fdsn,
            DirectPublisher(self.runner, source="catchup"),
            BucketProgress(self.runner),
        )


def catch_up(month, fdsn, publisher, progress) -> CatchUp:
    return CatchUp(
        UsgsClient(MONTH_FEED, make_http_client(transport=month.transport), sleep=lambda s: None),
        FdsnClient(
            "https://earthquake.usgs.gov/fdsnws/event/1",
            make_http_client(transport=fdsn.transport),
            pause_s=0,
            sleep=lambda s: None,
        ),
        publisher,
        progress,
    )


def rows(s3, bucket: str) -> dict[str, dict]:
    listed = s3.list_objects_v2(Bucket=bucket, Prefix="earthquakes/").get("Contents", [])
    found = {}
    for item in listed:
        row = json.loads(s3.get_object(Bucket=bucket, Key=item["Key"])["Body"].read())
        found[item["Key"]] = {k: v for k, v in row.items() if k not in PROCESSING_TIMES}
    return found


def snapshot(s3, bucket: str, tmp_path) -> list[tuple]:
    path = tmp_path / f"{bucket}.parquet"
    path.write_bytes(s3.get_object(Bucket=bucket, Key=SNAPSHOT_KEY)["Body"].read())
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    found = con.sql(f"SELECT * EXCLUDE ({', '.join(PROCESSING_TIMES)}) FROM '{path}'").fetchall()
    con.close()
    return found


@pytest.mark.req("REQ-MODE-001", "REQ-MODE-005")
def test_the_same_usgs_input_gives_the_same_bucket_in_both_modes(
    pipeline, feed, fdsn, event, s3, tmp_path, settings
):
    month = FeedServer()
    direct = Direct(s3, tmp_path, feed, month, fdsn)
    full_catchup = catch_up(
        month,
        fdsn,
        EventPublisher(make_producer(settings.kafka_bootstrap_servers), pipeline.backfill_topic),
        Progress(pipeline.db),
    )

    def round_of_both():
        assert pipeline.poll().status == direct.poller.run_round().status
        pipeline.sync()

    a = event("ea", minutes_ago=50, mag=2.1)
    b = event("eb", minutes_ago=40)
    c = event("ec", minutes_ago=30, lat=95.0)  # rejected (GEO-002)
    d = event("ed", minutes_ago=20)
    # ed before ea: full mode may create them in either order, Kafka's partitions having none
    # between them, and the merge below must not depend on it (DEF-050).
    feed.show(d, c, b, a)
    round_of_both()
    # A revision, and a change of preferred ID.
    b2 = revised(b, by_s=60)
    b2["id"], b2["properties"]["ids"] = "eb2", ",eb2,eb,"
    feed.show(revised(a, by_s=30, mag=2.6), b2)
    feed.remove("eb")
    round_of_both()
    # Two stored events turn out to be one: ed now lists ea.
    feed.show(revised(d, by_s=300, ids=",ed,ea,"))
    round_of_both()
    # Nothing new: neither mode processes anything.
    feed.show(revised(d, by_s=300, ids=",ed,ea,"))
    round_of_both()

    # Catch-up: an old event the day feed never showed, and versions already stored.
    old = event("eold", minutes_ago=3 * 24 * 60)
    month.show(old, b2, revised(d, by_s=300, ids=",ed,ea,"))
    deleted = revised(b2, by_s=600, status="deleted")
    fdsn.put(deleted)
    assert full_catchup.catch_up() == direct.catchup.catch_up() == 1
    assert full_catchup.check_deleted() == direct.catchup.check_deleted() == 1
    pipeline.sync()

    with TestClient(create_app(pipeline.db), base_url="http://testserver/api/v1/") as api:
        record = Exporter(
            api,
            Bucket(s3, "full-mode"),
            api_url="http://testserver/api/v1",
            workers=4,
            local_dir=tmp_path / "runs",
        ).run()
    assert record.status == "succeeded", record.errors

    full = rows(s3, "full-mode")
    assert sorted(full) == [
        "earthquakes/ea.json",  # ed merged into ea, the smaller key
        "earthquakes/eb.json",  # created as eb, now eb2, deleted
        "earthquakes/eold.json",
    ]
    assert rows(s3, "direct-mode") == full
    assert snapshot(s3, "direct-mode", tmp_path) == snapshot(s3, "full-mode", tmp_path)
    assert full["earthquakes/eb.json"]["review_status"] == "deleted"
    # Both modes saw the same messages with the same outcomes (full mode: in two topics).
    full_outcomes = sorted((r["event_id"], r["outcome"], r["rule"]) for r in raw_outcomes(pipeline))
    assert direct_outcomes(s3) == full_outcomes
    assert pipeline.reconcile().ok and pipeline.reconcile(backfill=True).ok
    assert reconcile_direct(Bucket(s3, "direct-mode")).ok


def raw_outcomes(pipeline) -> list[dict]:
    from sqlalchemy import select

    from earth_hub.database.models import RawEvent

    with pipeline.db.connect() as conn:
        return [
            dict(r)
            for r in conn.execute(select(RawEvent.event_id, RawEvent.outcome, RawEvent.rule))
            .mappings()
            .all()
        ]


def direct_outcomes(s3) -> list[tuple]:
    lines = []
    for item in s3.list_objects_v2(Bucket="direct-mode", Prefix="log/").get("Contents", []):
        body = s3.get_object(Bucket="direct-mode", Key=item["Key"])["Body"].read()
        lines += [json.loads(line) for line in body.splitlines()]
    return sorted((e["event_id"], e["outcome"], e["rule"]) for e in lines)
