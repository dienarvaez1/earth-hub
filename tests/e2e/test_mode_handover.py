"""The handovers between modes, on real Kafka and PostgreSQL with the bucket on moto.

The same batches, with the same processing times, go to two databases: one that stays in full
mode throughout (the reference), and one that hands over to direct mode and back. After the
round trip, PostgreSQL must be what the reference is, every column and processing time
included, and the bucket what an export of it gives. A gap in the log, or a rule that one
mode breaks, must stop the handover and leave the bucket in direct mode; a replay repeated
after a crash must send nothing twice.
"""

import json
from datetime import UTC, datetime, timedelta

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import select

from earth_hub.api.main import create_app
from earth_hub.database.models import Earthquake, EarthquakeId, RawEvent
from earth_hub.database.repository import IncomingMessage, process_batch
from earth_hub.direct.handover import HandoverFailed, ReplayTopic, to_direct, to_full
from earth_hub.direct.runner import DirectRunner
from earth_hub.export import mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import Exporter
from earth_hub.ingestion.state import SentState

pytestmark = pytest.mark.e2e

BUCKET = "earth-hub-test"
START = datetime(2026, 10, 16, 9, 0, 0, 250000, tzinfo=UTC)
UPDATED = 1792137700000


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def reference(make_database, make_alembic_config, settings):
    """A database that stays in full mode."""
    from alembic import command

    from earth_hub.database.engine import make_engine

    name = make_database()
    command.upgrade(make_alembic_config(settings.database_url(name)), "head")
    engine = make_engine(settings, name)
    yield engine
    engine.dispose()


class RoundTrip:
    def __init__(self, pipeline, reference, s3, tmp_path, make_envelope, make_feature):
        self.pipeline, self.reference, self.s3, self.tmp = pipeline, reference, s3, tmp_path
        self.e, self.f = make_envelope, make_feature
        self.bucket = Bucket(s3, BUCKET)
        self.minute = 0
        self.offset = 0
        self.api = TestClient(create_app(pipeline.db), base_url="http://testserver/api/v1/")

    def feature(self, event_id, ids=None, **props):
        return self.e(self.f(event_id, ids, **{"updated": UPDATED, **props}))

    def now(self) -> datetime:
        self.minute += 1
        return START + timedelta(minutes=self.minute, microseconds=self.minute)

    def full(self, values) -> None:
        """A batch in full mode, in both databases."""
        now = self.now()
        for engine in (self.pipeline.db, self.reference):
            messages = [
                IncomingMessage("full-mode", 0, self.offset + i, b"k", v)
                for i, v in enumerate(values)
            ]
            with engine.begin() as conn:
                process_batch(conn, messages, now=now)
        self.offset += len(values)

    def direct(self, runner, values) -> None:
        """A round in direct mode; the reference gets it as a full-mode batch."""
        now = self.now()
        runner.process(values, now=now)
        messages = [
            IncomingMessage("full-mode", 0, self.offset + i, b"k", v) for i, v in enumerate(values)
        ]
        with self.reference.begin() as conn:
            process_batch(conn, messages, now=now)
        self.offset += len(values)

    def problems(self) -> list[str]:
        found = []
        for report in (self.pipeline.reconcile(), self.pipeline.reconcile(backfill=True)):
            if not report.ok:
                found.append(f"lost {report.lost}, {report.broken_invariants}")
        return found

    def to_direct(self):
        return to_direct(
            self.bucket,
            self.pipeline.db,
            api=self.api,
            api_url="http://testserver/api/v1",
            sent=SentState(),
            problems=self.problems,
            local_dir=self.tmp / "runs",
        )

    def to_full(self):
        return to_full(
            self.bucket,
            self.pipeline.db,
            api=self.api,
            api_url="http://testserver/api/v1",
            topic=ReplayTopic(
                self.pipeline.settings.kafka_bootstrap_servers, self.pipeline.backfill_topic
            ),
            wait=self.pipeline.sync,
            problems=self.problems,
            local_dir=self.tmp / "runs",
        )

    def runner(self) -> DirectRunner:
        workdir = self.tmp / f"runner{self.minute}"
        workdir.mkdir()
        return DirectRunner(self.bucket, workdir=workdir, workers=4)


@pytest.fixture
def trip(pipeline, reference, s3, tmp_path, make_envelope, make_feature):
    return RoundTrip(pipeline, reference, s3, tmp_path, make_envelope, make_feature)


def tables(engine) -> dict:
    """Everything PostgreSQL holds, as comparable across databases (ids and Kafka
    coordinates differ; what they point to must not)."""
    with engine.connect() as conn:
        quakes = {
            r["key"]: {c: v for c, v in r.items() if c != "id"}
            for r in conn.execute(select(Earthquake.__table__)).mappings()
        }
        ids = {
            r.event_id: (r.key, r.first_seen_at)
            for r in conn.execute(
                select(EarthquakeId.event_id, Earthquake.key, EarthquakeId.first_seen_at).join(
                    Earthquake, Earthquake.id == EarthquakeId.earthquake_id
                )
            )
        }
        raws = sorted(
            [
                tuple(r)
                for r in conn.execute(
                    select(
                        RawEvent.event_id,
                        RawEvent.outcome,
                        RawEvent.rule,
                        RawEvent.reason,
                        Earthquake.key,
                        RawEvent.source_updated_ms,
                    ).outerjoin(Earthquake, Earthquake.id == RawEvent.earthquake_id)
                )
            ],
            key=repr,
        )
    return {"earthquakes": quakes, "ids": ids, "raw_events": raws}


def phase_one(trip):
    trip.full([trip.feature("ak1"), trip.feature("us2", place="Fiji region")])
    trip.full([trip.feature("ak1", mag=3.0, updated=UPDATED + 60_000), trip.feature("ci3")])


def phase_two(trip, runner):
    """Direct mode: a revision, a new event, an ID change, a merge, a deletion, a rejection,
    a duplicate."""
    trip.direct(runner, [trip.feature("ci3", mag=1.2, updated=UPDATED + 1000), trip.feature("nc4")])
    trip.direct(runner, [trip.feature("us2b", ["us2b", "us2"], updated=UPDATED + 2000)])
    trip.direct(
        runner,
        [trip.feature("nc4", ["nc4", "ak1"], updated=UPDATED + 90_000), b"not json"],
    )
    trip.direct(
        runner,
        [
            trip.feature("ci3", status="deleted", updated=UPDATED + 5000),
            trip.feature("nc4", ["nc4", "ak1"], updated=UPDATED + 90_000),
        ],
    )


@pytest.mark.req("REQ-MODE-009", "REQ-MODE-010")
def test_full_to_direct_to_full_leaves_postgresql_as_if_it_had_stayed_in_full_mode(trip):
    phase_one(trip)
    trip.to_direct()
    assert mode.read(trip.bucket).mode == mode.DIRECT
    with pytest.raises(mode.WrongMode):
        Exporter(trip.api, trip.bucket, api_url="x", local_dir=trip.tmp).run()  # nothing written

    phase_two(trip, trip.runner())
    result = trip.to_full()
    assert result.replayed == 7 and result.already_replayed == 0
    marker = mode.read(trip.bucket)
    assert (marker.mode, marker.state, marker.checkpoint_seq) == (mode.FULL, mode.ACTIVE, 7)
    assert tables(trip.pipeline.db) == tables(trip.reference)

    # Full mode again: a direct round is refused, an export finds nothing to do.
    with pytest.raises(mode.WrongMode):
        trip.runner().process([trip.feature("zz9")], now=START)
    record = Exporter(trip.api, trip.bucket, api_url="x", local_dir=trip.tmp).run()
    assert record.status == "succeeded"
    assert (record.counts.new, record.counts.updated, record.counts.deleted) == (0, 0, 0)


@pytest.mark.req("REQ-MODE-009")
def test_the_round_trip_can_be_made_twice(trip):
    phase_one(trip)
    trip.to_direct()
    trip.direct(trip.runner(), [trip.feature("hv1")])
    trip.to_full()
    trip.full([trip.feature("hv1", mag=4.4, updated=UPDATED + 7000)])
    trip.to_direct()
    trip.direct(
        trip.runner(),
        [trip.feature("hv2"), trip.feature("hv1", ["hv1", "hv2"], updated=UPDATED + 9000)],
    )
    assert trip.to_full().replayed == 2  # only what came after the second handover
    assert tables(trip.pipeline.db) == tables(trip.reference)


@pytest.mark.req("REQ-MODE-010")
def test_a_gap_in_the_log_stops_the_handover_in_direct_mode(trip):
    phase_one(trip)
    trip.to_direct()
    phase_two(trip, trip.runner())
    gone = next(k for k in trip.bucket.keys("log/") if "000000000003-" in k)
    trip.bucket.delete([gone])
    with pytest.raises(HandoverFailed, match="the log lacks 1 entries"):
        trip.to_full()
    marker = mode.read(trip.bucket)
    assert (marker.mode, marker.state) == (mode.DIRECT, mode.ACTIVE)
    assert trip.pipeline.messages_in_topic(trip.pipeline.backfill_topic) == 0  # nothing sent


@pytest.mark.req("REQ-MODE-010")
def test_a_rule_broken_in_one_mode_stops_the_handover(trip, monkeypatch):
    """Direct mode, with DEF-026's rule broken, keeps a duplicate's deletion that full mode
    refuses: the dry run sees the difference, and the bucket stays in direct mode."""
    from earth_hub.database import revisions
    from earth_hub.processing import ledger

    phase_one(trip)
    trip.to_direct()

    def newer_always_wins(stored, incoming):
        if incoming.source_updated_ms > stored.source_updated_ms:
            return "applied", None
        return revisions.compare(stored, incoming)

    with monkeypatch.context() as patched:
        patched.setattr(ledger, "compare", newer_always_wins)
        runner = trip.runner()
        runner.process(
            [trip.feature("ak1", ["ak1", "ak1dup"], updated=UPDATED + 61_000)], now=trip.now()
        )
        runner.process(
            [trip.feature("ak1dup", status="deleted", updated=UPDATED + 62_000)], now=trip.now()
        )
    with pytest.raises(
        HandoverFailed, match="PostgreSQL differs from the bucket: new 0, updated 1"
    ):
        trip.to_full()
    assert (mode.read(trip.bucket).mode, mode.read(trip.bucket).state) == (mode.DIRECT, mode.ACTIVE)


@pytest.mark.req("REQ-MODE-011")
def test_a_replay_repeated_after_a_crash_sends_nothing_twice(trip, monkeypatch):
    phase_one(trip)
    trip.to_direct()
    phase_two(trip, trip.runner())

    real_send = ReplayTopic.send

    def crash_halfway(self, entries):
        real_send(self, entries[:3])
        raise ConnectionError("simulated crash while replaying")

    monkeypatch.setattr(ReplayTopic, "send", crash_halfway)
    with pytest.raises(ConnectionError):
        trip.to_full()
    assert mode.read(trip.bucket).state == mode.HANDING_OVER  # frozen: no direct round writes
    with pytest.raises(mode.WrongMode):
        trip.runner().process([trip.feature("zz9")], now=START)

    monkeypatch.setattr(ReplayTopic, "send", real_send)
    result = trip.to_full()
    assert (result.replayed, result.already_replayed) == (4, 3)
    assert trip.pipeline.messages_in_topic(trip.pipeline.backfill_topic) == 7
    assert tables(trip.pipeline.db) == tables(trip.reference)


@pytest.mark.req("REQ-MODE-011")
def test_a_handover_retried_after_a_failed_check_sends_nothing_twice(trip, monkeypatch):
    """DEF-047: a failed check returns the bucket to direct mode; the next attempt used to take a
    new replay_from, miss what the first had sent, and send every entry again."""
    phase_one(trip)
    trip.to_direct()
    phase_two(trip, trip.runner())
    sent = trip.pipeline.messages_in_topic(trip.pipeline.backfill_topic)

    with monkeypatch.context() as patched:
        patched.setattr(trip, "problems", lambda: ["simulated: a check failed"])
        with pytest.raises(HandoverFailed, match="simulated: a check failed"):
            trip.to_full()
    marker = mode.read(trip.bucket)
    assert (marker.mode, marker.state) == (mode.DIRECT, mode.ACTIVE)
    first = trip.pipeline.messages_in_topic(trip.pipeline.backfill_topic) - sent

    trip.direct(trip.runner(), [trip.feature("hv5")])  # direct mode carries on meanwhile
    result = trip.to_full()
    assert (result.replayed, result.already_replayed) == (1, first)
    assert trip.pipeline.messages_in_topic(trip.pipeline.backfill_topic) == sent + first + 1
    assert tables(trip.pipeline.db) == tables(trip.reference)


@pytest.mark.req("REQ-MODE-009")
def test_a_handover_to_direct_needs_full_mode_idle(trip):
    phase_one(trip)
    trip.problems = lambda: ["earthquakes.raw: 3 messages not yet stored"]
    with pytest.raises(HandoverFailed, match="not idle"):
        trip.to_direct()
    assert mode.read(trip.bucket).mode == mode.FULL


@pytest.mark.req("REQ-MODE-009", "REQ-MODE-010")
def test_the_commands_hand_over_and_back_on_the_real_wiring(
    pipeline, feed, event, s3, settings, tmp_path, monkeypatch, capsys
):
    """make mode / mode-direct / mode-full: main() as the Makefile runs it, its settings
    pointing at this test's topics, consumer groups and database, the API in process, and
    the backfill consumer running beside it as it does in Docker."""
    import threading
    from types import SimpleNamespace

    from earth_hub.direct import handover

    test_settings = settings.model_copy(
        update={
            "postgres_db": pipeline.db.url.database,
            "kafka_topic_raw": pipeline.topic,
            "kafka_topic_backfill": pipeline.backfill_topic,
            "kafka_consumer_group": pipeline.group,
            "kafka_backfill_consumer_group": pipeline.backfill_group,
            "r2_bucket": BUCKET,
        }
    )
    monkeypatch.setattr("earth_hub.config.get_settings", lambda: test_settings)
    monkeypatch.setattr("earth_hub.export.bucket.make_s3_client", lambda s: s3)
    api = SimpleNamespace(
        Client=lambda **kwargs: TestClient(
            create_app(pipeline.db), base_url="http://testserver/api/v1/"
        )
    )
    monkeypatch.setattr(handover, "httpx", api)

    feed.show(event("ak1", minutes_ago=20), event("us2", minutes_ago=10))
    pipeline.poll()
    pipeline.sync()
    assert handover.main(["status"]) == 0
    assert capsys.readouterr().out.startswith("full (active)")

    assert handover.main(["direct"]) == 0
    out = capsys.readouterr().out
    assert "direct mode from sequence 0: 2 records, 2 IDs, 2 events in the poller's state" in out

    workdir = tmp_path / "runner"
    workdir.mkdir()
    DirectRunner(Bucket(s3, BUCKET), workdir=workdir).process(
        [
            json.dumps(event_envelope(event("ci3", minutes_ago=5))).encode(),
            b"not json",
        ]
    )

    stop = threading.Event()

    def consume():
        while not stop.is_set():
            pipeline.backfill_consumer.run_once(timeout_s=0.5)

    consumer = threading.Thread(target=consume, daemon=True)
    consumer.start()
    try:
        assert handover.main(["full", "--timeout", "60"]) == 0
    finally:
        stop.set()
        consumer.join(timeout=30)
    out = capsys.readouterr().out
    assert "replayed 2 log entries (0 sent before)" in out
    assert "an export dry run finds PostgreSQL and the bucket the same" in out
    assert set(pipeline.events()) == {"ak1", "us2", "ci3"}

    assert handover.main(["status"]) == 0
    assert "full (active)" in capsys.readouterr().out
    assert handover.main(["full"]) == 1  # already in full mode
    assert "not direct" in capsys.readouterr().err


def event_envelope(feature: dict) -> dict:
    from earth_hub.ingestion.events import build_message

    return json.loads(
        build_message(feature, fetched_at=datetime.now(UTC), feed_generated_ms=None).to_json()
    )
