"""Milestone 9's standby against PostgreSQL and Kafka: the export cut stops consumer batches, the
cut holds only what is stored, and direct mode can go on from a bucket the exporting service
wrote, giving the bucket full mode would (the unit tests are in tests/unit/test_standby.py)."""

import json
import random
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import boto3
import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import func, select, text
from sqlalchemy.exc import OperationalError
from support.streams import random_stream
from test_direct_parity import contents, direct_ids, table_ids

from earth_hub.api.main import create_app
from earth_hub.database.models import Earthquake
from earth_hub.database.repository import IncomingMessage, export_cut, process_batch
from earth_hub.direct.handover import read_standby
from earth_hub.direct.runner import DirectRunner
from earth_hub.direct.standby import Cut, Service, make_cut
from earth_hub.direct.store import STATE_KEY
from earth_hub.export import mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import Exporter
from earth_hub.ingestion.state import SentState

pytestmark = pytest.mark.integration

START = datetime(2026, 10, 30, 9, 0, tzinfo=UTC)


def message(offset: int, value: bytes) -> IncomingMessage:
    return IncomingMessage("earthquakes.raw", 0, offset, b"k", value)


@pytest.mark.req("REQ-MODE-019")
def test_no_consumer_batch_commits_while_the_cut_is_held(db, make_envelope, make_feature):
    value = make_envelope(make_feature("us1", updated=1792137700000))
    with export_cut(db), db.connect() as conn:
        conn.execute(text("SET lock_timeout = '300ms'"))
        with pytest.raises(OperationalError, match="lock timeout"):
            process_batch(conn, [message(0, value)], now=START)
        conn.rollback()
    with db.begin() as conn:  # free again once the cut ends
        assert process_batch(conn, [message(0, value)], now=START)[0].outcome == "applied"


@pytest.mark.req("REQ-MODE-019")
def test_the_cut_holds_only_stored_messages_and_checkpoints_once_catch_up_is_stored(
    db, settings, make_temp_topic, make_envelope, make_feature
):
    raw, backfill = make_temp_topic(), make_temp_topic()
    group, backfill_group = f"{raw}-group", f"{backfill}-group"
    bootstrap = settings.kafka_bootstrap_servers
    producer = Producer({"bootstrap.servers": bootstrap})
    for event in ("us1", "us2", "us3"):
        producer.produce(raw, value=make_envelope(make_feature(event, updated=1792137700000)))
    producer.produce(backfill, value=make_envelope(make_feature("us9", updated=1792137700000)))
    producer.flush(10)

    def commit(topic: str, group_id: str, offset: int) -> None:
        consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group_id})
        consumer.commit(offsets=[TopicPartition(topic, 0, offset)], asynchronous=False)
        consumer.close()

    commit(raw, group, 2)  # us3 not stored yet
    cut = make_cut(
        db,
        settings.model_copy(
            update={
                "kafka_topic_raw": raw,
                "kafka_consumer_group": group,
                "kafka_topic_backfill": backfill,
                "kafka_backfill_consumer_group": backfill_group,
            }
        ),
        lambda: datetime.now(UTC),
    )
    with cut() as c:
        assert set(c.sent.entries()) == {"us1", "us2"}
        assert not c.checkpoints  # us9, published by catch-up, not stored yet
    commit(backfill, backfill_group, 1)
    with cut() as c:
        assert c.checkpoints


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        for name in ("full-mode", "direct-mode"):
            client.create_bucket(Bucket=name)
        yield client


@pytest.mark.req("REQ-MODE-019")
@pytest.mark.parametrize("seed", range(4))
def test_direct_mode_goes_on_from_the_services_export_as_full_mode_would(
    db, s3, tmp_path, make_envelope, make_feature, seed
):
    """Full mode stores a random stream and the service exports it with direct mode's state;
    then direct mode takes the bucket over and processes the rest of the stream, while full
    mode stores the same rest and exports it to another bucket: the two buckets are the same,
    and so are direct mode's ID map and PostgreSQL's."""
    rng = random.Random(seed)
    values = random_stream(rng, f"t{seed}", make_envelope, make_feature, merges=True)
    batches, offset, cursor = [], 0, 0
    while cursor < len(values):
        size = rng.randint(1, 20)
        batches.append(values[cursor : cursor + size])
        cursor += size

    def store(batch: list[bytes], number: int) -> None:
        nonlocal offset
        now = START + timedelta(minutes=number, microseconds=number * 7)
        with db.begin() as conn:
            process_batch(conn, [message(offset + i, v) for i, v in enumerate(batch)], now=now)
        offset += len(batch)

    before, after = batches[: len(batches) // 2], batches[len(batches) // 2 :]
    for number, batch in enumerate(before):
        store(batch, number)

    direct_bucket = Bucket(s3, "direct-mode")
    clock = lambda: START + timedelta(days=1)  # noqa: E731

    @contextmanager
    def cut():
        with export_cut(db) as conn:
            changed = conn.execute(select(func.max(Earthquake.last_applied_at))).scalar()
            yield Cut(read_standby(conn), SentState(), True, changed)

    with TestClient(create_app(db), base_url="http://testserver/api/v1/") as api:

        def exporter(bucket: Bucket, machine: str | None) -> Exporter:
            return Exporter(
                api,
                bucket,
                api_url="http://testserver/api/v1",
                workers=4,
                page_size=7,
                clock=clock,
                local_dir=tmp_path / "runs",
                machine=machine,
            )

        service = Service(
            direct_bucket,
            machine="host-a",
            make_exporter=lambda: exporter(direct_bucket, "host-a"),
            cut=cut,
            last_change=lambda: START + timedelta(days=2),
            interval=timedelta(minutes=5),
            clock=clock,
        )
        assert service.step() == "exported"

        # The host stops; direct mode takes the bucket (step 5 will do this by itself).
        current = mode.read(direct_bucket)
        mode.write(direct_bucket, current, mode=mode.DIRECT, owner=None, lease_expires_at=None)
        (tmp_path / "direct").mkdir()
        runner = DirectRunner(direct_bucket, workdir=tmp_path / "direct", workers=4)
        runner.state.refresh()
        runner.state.check()  # the ID map, the manifest and the snapshot agree
        for number, batch in enumerate(after, start=len(before)):
            runner.process(batch, now=START + timedelta(minutes=number, microseconds=number * 7))
            store(batch, number)

        record = exporter(Bucket(s3, "full-mode"), None).run()
        assert record.status == "succeeded", record.errors

    full, direct = contents(s3, "full-mode", tmp_path), contents(s3, "direct-mode", tmp_path)
    assert direct["rows"] == full["rows"]
    assert direct["manifest"] == full["manifest"]
    assert direct["snapshot"] == full["snapshot"]
    assert direct["meta"] == full["meta"]
    assert direct_ids(s3) == table_ids(db)
    state = json.loads(s3.get_object(Bucket="direct-mode", Key=STATE_KEY)["Body"].read())
    assert state["seq"] == sum(map(len, after))  # the log starts at the takeover
