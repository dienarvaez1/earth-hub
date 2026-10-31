"""Milestone 9, step 6, against PostgreSQL and Kafka: a host's database rebuilt from the bucket
(direct/rebuild.py). What wiping would lose, the backup, and a load after which PostgreSQL
holds exactly the bucket."""

import base64
import gzip
import json
import random
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import boto3
import pytest
from confluent_kafka import Producer
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import create_engine, func, select, text
from support.streams import random_stream

from earth_hub.api.main import create_app
from earth_hub.database.models import Earthquake, EarthquakeId
from earth_hub.database.repository import IncomingMessage, export_cut, process_batch
from earth_hub.direct.handover import read_standby
from earth_hub.direct.rebuild import NotEmpty, backup, check, load
from earth_hub.direct.runner import DirectRunner
from earth_hub.direct.standby import Cut, Service
from earth_hub.direct.store import BucketState
from earth_hub.export import mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import Exporter
from earth_hub.ingestion.state import SentState
from earth_hub.reconcile import INVARIANTS

pytestmark = pytest.mark.integration

START = datetime(2026, 10, 30, 9, 0, tzinfo=UTC)
BUCKET = "rebuild"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


class Host:
    """A full-mode host: a database, its exports to the bucket with direct mode's state."""

    def __init__(self, db, s3, tmp_path):
        self.db, self.bucket, self.tmp = db, Bucket(s3, BUCKET), tmp_path
        self.offset = self.batches = 0

    def store(self, values: list[bytes]) -> None:
        now = START + timedelta(minutes=self.batches, microseconds=self.batches * 7)
        self.batches += 1
        messages = [
            IncomingMessage("earthquakes.raw", 0, self.offset + i, b"k", v)
            for i, v in enumerate(values)
        ]
        self.offset += len(values)
        with self.db.begin() as conn:
            process_batch(conn, messages, now=now)

    def export(self) -> None:
        @contextmanager
        def cut():
            with export_cut(self.db) as conn:
                changed = conn.execute(select(func.max(Earthquake.last_applied_at))).scalar()
                yield Cut(read_standby(conn), SentState(), True, changed)

        with TestClient(create_app(self.db), base_url="http://testserver/api/v1/") as api:
            service = Service(
                self.bucket,
                machine="host-a",
                make_exporter=lambda: exporter(api, self.bucket, self.tmp, "host-a"),
                cut=cut,
                last_change=lambda: datetime.now(UTC),
                interval=timedelta(0),
                clock=lambda: START + timedelta(days=1),
            )
            assert service.step() == "exported"

    def state(self, name: str) -> BucketState:
        (self.tmp / name).mkdir()
        state = BucketState(self.bucket, self.tmp / name, workers=4)
        state.refresh()
        return state


def exporter(api, bucket: Bucket, tmp_path, machine) -> Exporter:
    return Exporter(
        api,
        bucket,
        api_url="http://testserver/api/v1",
        workers=4,
        page_size=7,
        clock=lambda: START + timedelta(days=1),
        local_dir=tmp_path / "runs",
        machine=machine,
    )


def stream(seed: int, make_envelope, make_feature) -> list[list[bytes]]:
    rng = random.Random(seed)
    values = random_stream(rng, f"r{seed}", make_envelope, make_feature, merges=True)
    batches, cursor = [], 0
    while cursor < len(values):
        size = rng.randint(1, 20)
        batches.append(values[cursor : cursor + size])
        cursor += size
    return batches


@pytest.fixture
def host(db, s3, tmp_path):
    return Host(db, s3, tmp_path)


@pytest.fixture
def empty(make_database, settings):
    engine = create_engine(settings.database_url(make_database(migrate=True)))
    yield engine
    engine.dispose()


@pytest.mark.req("REQ-MODE-022")
@pytest.mark.parametrize("seed", range(3))
def test_a_load_gives_postgresql_exactly_the_bucket(
    host, empty, tmp_path, make_envelope, make_feature, seed
):
    """The host exported half a stream; direct mode took over and stored the rest; a new
    host's empty database loaded from the bucket then matches it in every column."""
    batches = stream(seed, make_envelope, make_feature)
    half = len(batches) // 2
    for batch in batches[:half]:
        host.store(batch)
    host.export()
    current = mode.read(host.bucket)
    mode.write(host.bucket, current, mode=mode.DIRECT, owner=None, lease_expires_at=None)
    (tmp_path / "direct").mkdir()
    runner = DirectRunner(host.bucket, workdir=tmp_path / "direct", workers=4)
    for number, batch in enumerate(batches[half:], start=half):
        runner.process(batch, now=START + timedelta(minutes=number, microseconds=number * 7))

    state = host.state("load")
    loaded = load(empty, state)
    assert loaded == len(state.order)

    with TestClient(create_app(empty), base_url="http://testserver/api/v1/") as api:
        record = exporter(api, host.bucket, tmp_path, None).run(dry_run=True)
    c = record.counts
    assert record.status == "succeeded", record.errors
    assert (c.new, c.updated, c.deleted, c.failed) == (0, 0, 0, 0)  # every row the same
    with empty.connect() as conn:
        for name, query in INVARIANTS.items():
            assert conn.execute(text(query)).scalar() == 0, name
        ids = {
            r.event_id: (r.key, r.first_seen_at)
            for r in conn.execute(
                select(EarthquakeId.event_id, Earthquake.key, EarthquakeId.first_seen_at).join(
                    Earthquake, Earthquake.id == EarthquakeId.earthquake_id
                )
            )
        }
    assert ids == {i: (k, datetime.fromisoformat(s)) for i, (k, s) in state.ids.items()}
    with pytest.raises(NotEmpty):
        load(empty, state)


@pytest.mark.req("REQ-MODE-022")
def test_check_finds_what_wiping_the_database_would_lose(host, make_envelope, make_feature):
    e = make_envelope
    host.store(
        [
            e(make_feature("us1", updated=1792137700000)),
            e(make_feature("us2", updated=1792137700000)),
        ]
    )
    host.export()
    assert check(host.db, host.state("exported")) == []
    # Stored after the export: a newer revision of us1, and a new event.
    host.store(
        [
            e(make_feature("us1", updated=1792137800000)),
            e(make_feature("us3", updated=1792137700000)),
        ]
    )
    problems = check(host.db, host.state("ahead"))
    assert problems == [
        "1 USGS IDs the bucket lacks: ['us3']",
        "1 events at a newer revision than the bucket's: ['us1']",
    ]
    host.export()
    assert check(host.db, host.state("exported-again")) == []


@pytest.mark.req("REQ-MODE-022")
def test_the_backup_holds_the_database_and_the_topics_and_restores(
    host, settings, make_temp_topic, tmp_path, make_envelope, make_feature
):
    host.store([make_envelope(make_feature("us1", updated=1792137700000))])
    topic = make_temp_topic()
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    producer.produce(topic, key=b"us1", value=b'{"a":1}', headers=[("earth-hub-seq", b"7")])
    producer.flush(10)

    directory = tmp_path / "backup"
    backup(
        directory,
        database=host.db.url.database,
        bootstrap=settings.kafka_bootstrap_servers,
        topics=[topic],
    )
    assert b"CREATE TABLE public.earthquakes" in gzip.decompress(
        (directory / "postgres.sql.gz").read_bytes()
    )
    (line,) = gzip.decompress((directory / f"{topic}.ndjson.gz").read_bytes()).splitlines()
    entry = json.loads(line)
    assert (entry["partition"], entry["offset"]) == (0, 0)
    assert base64.b64decode(entry["key"]) == b"us1"
    assert base64.b64decode(entry["value"]) == b'{"a":1}'
    assert entry["headers"] == [["earth-hub-seq", base64.b64encode(b"7").decode()]]
