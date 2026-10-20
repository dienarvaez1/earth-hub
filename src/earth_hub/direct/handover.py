"""The handovers between modes (Milestone 8): `python -m earth_hub.direct.handover
status|direct|full` (make mode / mode-direct / mode-full).

The bucket's marker (export/mode.py) says which mode writes. A handover changes it only when
the other mode has everything the first one stored, and checks that before switching.

full -> direct: with full mode idle (the poller and catch-up stopped, consumers caught up),
export, so the bucket holds the table; then write direct mode's state from PostgreSQL and
Kafka: the ID map (`earthquake_ids`, and each record's `id` as its creation number), the
poller's 26-hour state (from earthquakes.raw, as a restarted poller rebuilds it) and the
catch-up checkpoints. The marker then says `direct`, with the log's sequence number then.

direct -> full: freeze the bucket (marker `handing-over`: direct rounds stop), finish a round
left pending, and read every log entry after the marker's sequence number. A gap stops it.
Each entry is replayed, in order, onto partition 0 of earthquakes.backfill, with its
processing time and sequence number in headers, so the backfill consumer stores it as
direct mode did, with the same times (earthquake_consumer.PROCESSED_AT_HEADER). Then, once
the consumers have caught up, `make reconcile` must pass and an export dry run must find
nothing to change: PostgreSQL then holds exactly what the bucket does. Only then does the
marker say `full`, with catch-up's checkpoints copied to PostgreSQL. Otherwise the bucket
goes back to direct mode, and nothing is lost: what was replayed is recognized if the
handover is run again (by its sequence number in the topic), and not sent twice.

Without a usable log (a gap), the fallback is a backfill of the direct period from FDSN
(`make backfill START=<since>`), whose differences the dry run then reports.
"""

import argparse
import json
import logging
import re
import secrets
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import httpx
from sqlalchemy import Engine, select

from earth_hub.consumers.earthquake_consumer import PROCESSED_AT_HEADER
from earth_hub.database.models import Earthquake, EarthquakeId, IngestionCheckpoint
from earth_hub.database.progress import Progress
from earth_hub.direct.runner import LOCK_TTL, DirectRunner
from earth_hub.direct.store import GZIP, IDS_KEY, LOG_PREFIX, STATE_KEY, ids_body, iso
from earth_hub.export import lock, mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import Exporter
from earth_hub.ingestion.state import SentState

logger = logging.getLogger(__spec__.name)  # the module name, also under `python -m`

SEQ_HEADER = "earth-hub-seq"
REPLAY_PARTITION = 0  # one partition keeps the log's order
_LOG_NAME = re.compile(r"/(\d+)-(\d+)\.ndjson$")


class HandoverFailed(Exception):
    """The handover stopped; the bucket is still in the mode it was in."""


@dataclass
class Result:
    mode: str
    exported: dict | None = None
    replayed: int = 0
    already_replayed: int = 0
    steps: list[str] = field(default_factory=list)

    def step(self, message: str) -> None:
        logger.info(message)
        self.steps.append(message)


class ReplayTopic:
    """Partition 0 of the backfill topic, where the log is replayed."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers, self.topic = bootstrap_servers, topic

    def end_offset(self) -> int:
        from confluent_kafka import Consumer, TopicPartition

        from earth_hub.kafka_metadata import with_retries

        consumer = Consumer({"bootstrap.servers": self.bootstrap_servers, "group.id": "replay"})
        try:
            tp = TopicPartition(self.topic, REPLAY_PARTITION)
            return with_retries(lambda: consumer.get_watermark_offsets(tp, timeout=30))[1]
        finally:
            consumer.close()

    def replayed(self, from_offset: int | None) -> set[int]:
        """The sequence numbers already on the partition since `from_offset`."""
        from confluent_kafka import Consumer, TopicPartition

        from earth_hub.kafka_metadata import with_retries

        if from_offset is None:
            return set()
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": "earth-hub-replay-check",
                "enable.auto.commit": False,
            }
        )
        found: set[int] = set()
        try:
            tp = TopicPartition(self.topic, REPLAY_PARTITION)
            high = with_retries(lambda: consumer.get_watermark_offsets(tp, timeout=30))[1]
            if from_offset >= high:
                return found
            consumer.assign([TopicPartition(self.topic, REPLAY_PARTITION, from_offset)])
            while True:
                message = consumer.poll(30)
                if message is None:
                    raise TimeoutError(f"timed out reading {self.topic}")
                if message.error():
                    raise RuntimeError(str(message.error()))
                for name, value in message.headers() or []:
                    if name == SEQ_HEADER:
                        found.add(int(value))
                if message.offset() + 1 >= high:
                    return found
        finally:
            consumer.close()

    def send(self, entries: list[dict]) -> None:
        from earth_hub.ingestion.producer import make_producer

        producer = make_producer(self.bootstrap_servers)
        errors: list[str] = []
        for entry in entries:
            producer.produce(
                self.topic,
                partition=REPLAY_PARTITION,
                key=(entry["event_id"] or "replay").encode(),
                value=replay_value(entry),
                headers=[
                    (PROCESSED_AT_HEADER, entry["processed_at"].encode()),
                    (SEQ_HEADER, str(entry["seq"]).encode()),
                ],
                on_delivery=lambda err, _msg: errors.append(str(err)) if err else None,
            )
            producer.poll(0)
        if producer.flush(60) or errors:
            raise HandoverFailed(f"replay not delivered: {errors[:3] or 'flush timed out'}")


def replay_value(entry: dict) -> bytes | None:
    """The message as direct mode received it: the envelope, or the text it could not read."""
    if entry["envelope"] is not None:
        return json.dumps(entry["envelope"], separators=(",", ":")).encode()
    return None if entry["raw_value"] is None else entry["raw_value"].encode()


def read_log(bucket: Bucket, after: int) -> list[dict]:
    """Every log entry with a sequence number above `after`, in order."""
    entries = []
    for name in sorted(bucket.keys(LOG_PREFIX)):
        match = _LOG_NAME.search(name)
        if match is None or int(match.group(2)) <= after:
            continue
        found = bucket.get(name)
        if found is None:
            continue
        entries += [e for line in found[0].splitlines() if (e := json.loads(line))["seq"] > after]
    return sorted(entries, key=lambda e: e["seq"])


def _run_id(now: datetime) -> str:
    return f"handover-{now:%Y-%m-%dT%H-%M-%SZ}-{secrets.token_hex(2)}"


def _exporter(api: httpx.Client, bucket: Bucket, api_url: str, local_dir) -> Exporter:
    return Exporter(api, bucket, api_url=api_url, local_dir=local_dir)


def to_direct(
    bucket: Bucket,
    engine: Engine,
    *,
    api: httpx.Client,
    api_url: str,
    sent: SentState,
    problems: Callable[[], list[str]],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    local_dir: Path | None = None,
) -> Result:
    """Full -> direct. `sent`: the poller's 26-hour state; `problems`: what stops full mode
    from being idle and consistent (reconcile, lag), empty when it is."""
    result = Result(mode.DIRECT)
    current = mode.require(bucket, mode.FULL)
    if found := problems():
        raise HandoverFailed(f"full mode is not idle and consistent: {'; '.join(found)}")
    record = _exporter(api, bucket, api_url, local_dir).run()
    result.exported = {"run_id": record.run_id, "status": record.status, **record.counts.__dict__}
    if record.status != "succeeded":
        raise HandoverFailed(f"the export did not succeed: {record.errors[:3]}")
    result.step(f"exported: run {record.run_id}")

    with engine.connect() as conn:
        order = {k: n for k, n in conn.execute(select(Earthquake.key, Earthquake.id)).all()}
        ids = {
            r.event_id: (r.key, iso(r.first_seen_at))
            for r in conn.execute(
                select(EarthquakeId.event_id, Earthquake.key, EarthquakeId.first_seen_at).join(
                    Earthquake, Earthquake.id == EarthquakeId.earthquake_id
                )
            )
        }
        checkpoints = {
            name: iso(value)
            for name, value in conn.execute(
                select(IngestionCheckpoint.name, IngestionCheckpoint.value)
            )
        }

    now = clock()
    run_id = _run_id(now)
    lock.acquire(bucket, run_id, iso(now), now, LOCK_TTL)
    try:
        current = mode.require(bucket, mode.FULL)  # still, now that no one else writes
        with tempfile.TemporaryDirectory() as tmp:
            runner = DirectRunner(bucket, workdir=Path(tmp))
            runner.state.refresh()
            if runner.state.pending is not None:
                raise HandoverFailed("the bucket holds an unfinished direct round")
            bucket.put(
                IDS_KEY, ids_body(max(order.values(), default=0) + 1, order, ids), content_type=GZIP
            )
            status = dict(runner.state.status)
            status.update(
                run_id=run_id, written_at=iso(now), sent=sent.entries(), checkpoints=checkpoints
            )
            bucket.put(STATE_KEY, json.dumps(status, indent=1).encode())
            runner.state.refresh()
            runner.state.check()  # the ID map against the export just made
        mode.write(
            bucket,
            current,
            mode=mode.DIRECT,
            state=mode.ACTIVE,
            since=iso(now),
            checkpoint_seq=status["seq"],
            replay_from=None,
        )
    finally:
        lock.release(bucket, run_id, LOCK_TTL)
    result.step(
        f"direct mode from sequence {status['seq']}: {len(order)} records, {len(ids)} IDs, "
        f"{len(sent)} events in the poller's state"
    )
    return result


def to_full(
    bucket: Bucket,
    engine: Engine,
    *,
    api: httpx.Client,
    api_url: str,
    topic: ReplayTopic,
    wait: Callable[[], None],
    problems: Callable[[], list[str]],
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    local_dir: Path | None = None,
) -> Result:
    """Direct -> full. `wait`: returns once the consumers have stored every replayed message;
    `problems`: what reconciliation finds wrong in full mode, empty when nothing."""
    result = Result(mode.FULL)
    current = mode.read(bucket)
    if current.mode != mode.DIRECT:
        raise mode.WrongMode(f"the bucket is in {current.mode} mode, not direct")
    now = clock()
    run_id = _run_id(now)
    with tempfile.TemporaryDirectory() as tmp:
        runner = DirectRunner(bucket, workdir=Path(tmp))
        lock.acquire(bucket, run_id, iso(now), now, LOCK_TTL)
        try:
            current = mode.read(bucket)
            if current.state == mode.ACTIVE:
                # Frozen: from here no direct round writes (each checks the marker).
                current = mode.write(
                    bucket, current, state=mode.HANDING_OVER, replay_from=topic.end_offset()
                )
            runner._prepare(in_direct_mode=False)  # finishes a round left pending
        finally:
            lock.release(bucket, run_id, LOCK_TTL)
        status = runner.state.status
    result.step(f"direct mode frozen at sequence {status['seq']}")

    def back_to_direct(reason: str) -> None:
        mode.write(bucket, mode.read(bucket), state=mode.ACTIVE)
        raise HandoverFailed(f"{reason}; the bucket is back in direct mode")

    entries = read_log(bucket, current.checkpoint_seq)
    expected = list(range(current.checkpoint_seq + 1, status["seq"] + 1))
    if [e["seq"] for e in entries] != expected:
        missing = sorted(set(expected) - {e["seq"] for e in entries})
        back_to_direct(
            f"the log lacks {len(missing)} entries (from {missing[:1]}): replay is not possible. "
            f"Backfill the direct period instead (make backfill START={current.since})"
        )
    done = topic.replayed(current.replay_from)
    to_send = [e for e in entries if e["seq"] not in done]
    topic.send(to_send)
    result.replayed, result.already_replayed = len(to_send), len(entries) - len(to_send)
    result.step(f"replayed {len(to_send)} log entries ({result.already_replayed} sent before)")

    wait()
    if found := problems():
        back_to_direct(f"reconciliation failed: {'; '.join(found)}")
    record = _exporter(api, bucket, api_url, local_dir).run(dry_run=True)
    c = record.counts
    result.exported = {"run_id": record.run_id, "status": record.status, **c.__dict__}
    if record.status != "succeeded" or c.new or c.updated or c.deleted or c.failed:
        back_to_direct(
            f"PostgreSQL differs from the bucket: new {c.new}, updated {c.updated}, deleted "
            f"{c.deleted}, failed {c.failed} {record.errors[:3]}"
        )
    result.step("an export dry run finds PostgreSQL and the bucket the same")

    progress = Progress(engine)
    for name, value in status["checkpoints"].items():
        progress.set_checkpoint(name, datetime.fromisoformat(value))
    mode.write(
        bucket,
        mode.read(bucket),
        mode=mode.FULL,
        state=mode.ACTIVE,
        since=iso(clock()),
        checkpoint_seq=status["seq"],
        replay_from=None,
    )
    result.step("full mode")
    return result


def main(argv: list[str] | None = None) -> int:
    import time

    from earth_hub.config import get_settings
    from earth_hub.database.engine import make_engine
    from earth_hub.export.bucket import MissingSettings, make_s3_client
    from earth_hub.ingestion.state import STATE_WINDOW, read_topic_since, state_from_messages
    from earth_hub.logging_config import configure_logging
    from earth_hub.reconcile import reconcile

    parser = argparse.ArgumentParser(description="Show or change which mode writes the bucket.")
    parser.add_argument("command", choices=("status", "direct", "full"))
    parser.add_argument("--timeout", type=float, default=600, help="full: seconds to wait")
    args = parser.parse_args(argv)
    configure_logging()
    settings = get_settings()
    try:
        bucket = Bucket(make_s3_client(settings), settings.r2_bucket)
    except MissingSettings as exc:
        print(f"mode: {exc}", file=sys.stderr)
        return 2
    current = mode.read(bucket)
    if args.command == "status":
        print(
            f"{current.mode} ({current.state}) since {current.since or 'the start'}; "
            f"log sequence at the last change: {current.checkpoint_seq}"
        )
        return 0

    engine = make_engine(settings)
    bootstrap = settings.kafka_bootstrap_servers
    groups = {
        settings.kafka_topic_raw: settings.kafka_consumer_group,
        settings.kafka_topic_backfill: settings.kafka_backfill_consumer_group,
    }

    def problems() -> list[str]:
        found = []
        for topic, group in groups.items():
            report = reconcile(engine, bootstrap, topic, group)
            if not report.ok:
                found.append(f"{topic}: lost {len(report.lost)}, {report.broken_invariants}")
            if report.pending:
                found.append(f"{topic}: {report.pending} messages not yet stored")
        return found

    def wait() -> None:
        deadline = time.monotonic() + args.timeout
        topic, group = settings.kafka_topic_backfill, settings.kafka_backfill_consumer_group
        while reconcile(engine, bootstrap, topic, group).pending:
            if time.monotonic() > deadline:
                raise HandoverFailed("the backfill consumer did not catch up in time")
            time.sleep(2)

    api_url = str(settings.export_api_url).rstrip("/")
    try:
        with httpx.Client(base_url=api_url + "/", timeout=60) as api:
            if args.command == "direct":
                since = datetime.now(UTC) - STATE_WINDOW
                values = read_topic_since(bootstrap, settings.kafka_topic_raw, since)
                result = to_direct(
                    bucket,
                    engine,
                    api=api,
                    api_url=api_url,
                    sent=state_from_messages(values),
                    problems=problems,
                )
            else:
                result = to_full(
                    bucket,
                    engine,
                    api=api,
                    api_url=api_url,
                    topic=ReplayTopic(bootstrap, settings.kafka_topic_backfill),
                    wait=wait,
                    problems=problems,
                )
    except (HandoverFailed, mode.WrongMode, lock.LockHeld) as exc:
        print(f"mode: {exc}", file=sys.stderr)
        return 1
    for line in result.steps:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
