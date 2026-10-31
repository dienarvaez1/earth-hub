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
from earth_hub.direct.store import (
    GZIP,
    IDS_KEY,
    LOG_PREFIX,
    PENDING_KEY,
    STATE_KEY,
    empty_status,
    ids_body,
    iso,
)
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


class HandoverDiffers(HandoverFailed):
    """Direct -> full stopped because, after the replay and with reconciliation passing, an
    export dry run found PostgreSQL and the bucket different: the one failure a rebuild of
    PostgreSQL from the bucket is the remedy for (scripts/modes.py, DEF-062)."""


# What the last `handover direct|full` ended with, for scripts/modes.py, which runs it through
# make and so cannot tell one failure from another by its exit code (DEF-062).
OUTCOME = Path("reports/handover.json")


def write_outcome(path: Path, command: str, outcome: str, reason: str | None = None) -> None:
    """`outcome`: switched, differs (HandoverDiffers) or failed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    at = iso(datetime.now(UTC))
    record = {"command": command, "outcome": outcome, "reason": reason, "at": at}
    path.write_text(json.dumps(record) + "\n")


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


def _exporter(
    api: httpx.Client, bucket: Bucket, api_url: str, local_dir, machine: str | None = None
) -> Exporter:
    return Exporter(api, bucket, api_url=api_url, local_dir=local_dir, machine=machine)


@dataclass
class Standby:
    """Direct mode's state as full mode's PostgreSQL holds it: each record's creation number
    (`earthquakes.id`), each USGS ID's record and first-seen time, catch-up's checkpoints."""

    order: dict[str, int]
    ids: dict[str, tuple[str, str]]
    checkpoints: dict[str, str]


def read_standby(conn) -> Standby:
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
        for name, value in conn.execute(select(IngestionCheckpoint.name, IngestionCheckpoint.value))
    }
    return Standby(order, ids, checkpoints)


def write_standby(
    bucket: Bucket,
    held: lock.Held,
    standby: Standby,
    sent: SentState,
    *,
    run_id: str,
    now: datetime,
    checkpoints: bool = True,
) -> dict:
    """Write direct mode's ID map and state object from full mode's, under `held`, as
    `make mode-direct` and every export of the exporting service do (REQ-MODE-019). The log's
    sequence number stays. `checkpoints=False` keeps the ones the bucket holds: catch-up's are
    copied only when everything it published is stored. Returns the state written."""
    if bucket.get(PENDING_KEY) is not None:
        raise HandoverFailed("the bucket holds an unfinished direct round")
    found = bucket.get(STATE_KEY)
    status = json.loads(found[0]) if found else empty_status()
    held.confirm()
    order = standby.order
    bucket.put(
        IDS_KEY,
        ids_body(max(order.values(), default=0) + 1, order, standby.ids),
        content_type=GZIP,
    )
    status.update(run_id=run_id, written_at=iso(now), sent=sent.entries())
    if checkpoints:
        status["checkpoints"] = standby.checkpoints
    held.confirm()
    bucket.put(STATE_KEY, json.dumps(status, indent=1).encode())
    return status


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
    machine: str | None = None,
) -> Result:
    """Full -> direct. `sent`: the poller's 26-hour state; `problems`: what stops full mode
    from being idle and consistent (reconcile, lag), empty when it is."""
    result = Result(mode.DIRECT)
    current = mode.require(bucket, mode.FULL)
    if found := problems():
        raise HandoverFailed(f"full mode is not idle and consistent: {'; '.join(found)}")
    record = _exporter(api, bucket, api_url, local_dir, machine).run()
    result.exported = {"run_id": record.run_id, "status": record.status, **record.counts.__dict__}
    if record.status != "succeeded":
        raise HandoverFailed(f"the export did not succeed: {record.errors[:3]}")
    result.step(f"exported: run {record.run_id}")

    with engine.connect() as conn:
        standby = read_standby(conn)

    now = clock()
    run_id = _run_id(now)
    held = lock.Held(bucket, run_id, iso(now), now, LOCK_TTL, clock)
    try:
        current = mode.require(bucket, mode.FULL)  # still, now that no one else writes
        with tempfile.TemporaryDirectory() as tmp:
            status = write_standby(bucket, held, standby, sent, run_id=run_id, now=now)
            runner = DirectRunner(bucket, workdir=Path(tmp))
            runner.state.refresh()
            runner.state.check()  # the ID map against the export just made
        held.confirm()
        mode.write(
            bucket,
            current,
            mode=mode.DIRECT,
            state=mode.ACTIVE,
            since=iso(now),
            checkpoint_seq=status["seq"],
            replay_from=None,
            owner=None,
            lease_expires_at=None,
        )
    finally:
        held.release()
    result.step(
        f"direct mode from sequence {status['seq']}: {len(standby.order)} records, "
        f"{len(standby.ids)} IDs, "
        f"{len(sent)} events in the poller's state"
    )
    return result


def _replay(bucket, current, status, topic, result: Result, back_to_direct) -> None:
    """Every log entry since the last handover onto the backfill topic, once (DEF-047)."""
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
    machine: str | None = None,
    rebuild: bool = False,
) -> Result:
    """Direct -> full, with `machine` (settings.machine_id) the bucket's owner. `rebuild`:
    PostgreSQL, empty, is loaded from the frozen bucket instead of replaying the log
    (direct/rebuild.py); the checks after it are the same. `wait`:
    returns once the consumers have stored every replayed message;
    `problems`: what reconciliation finds wrong in full mode, empty when nothing."""
    result = Result(mode.FULL)
    current = mode.read(bucket)
    if current.mode != mode.DIRECT:
        raise mode.WrongMode(f"the bucket is in {current.mode} mode, not direct")
    now = clock()
    run_id = _run_id(now)

    def back_to_direct(reason: str, error: type[HandoverFailed] = HandoverFailed) -> None:
        mode.write(bucket, mode.read(bucket), state=mode.ACTIVE)
        raise error(f"{reason}; the bucket is back in direct mode")

    with tempfile.TemporaryDirectory() as tmp:
        runner = DirectRunner(bucket, workdir=Path(tmp))
        held = lock.Held(bucket, run_id, iso(now), now, LOCK_TTL, clock)
        try:
            current = mode.read(bucket)
            if current.state == mode.ACTIVE:
                # Frozen: from here no direct round writes (each checks the marker). An attempt
                # whose checks failed left its replay_from: what it sent is found from there, not
                # sent twice (DEF-047). A handover that succeeds clears it.
                start = topic.end_offset() if current.replay_from is None else current.replay_from
                held.confirm()
                current = mode.write(bucket, current, state=mode.HANDING_OVER, replay_from=start)
            with runner.fenced(held):
                runner._prepare(in_direct_mode=False)  # finishes a round left pending
        finally:
            held.release()
        status = runner.state.status
        result.step(f"direct mode frozen at sequence {status['seq']}")
        if rebuild:
            from earth_hub.direct.rebuild import load

            try:
                loaded = load(engine, runner.state)
            except Exception as exc:  # noqa: BLE001 (any failure unfreezes the bucket)
                back_to_direct(f"the rebuild failed: {type(exc).__name__}: {exc}")
            result.step(f"PostgreSQL rebuilt from the bucket: {loaded} records")

    if not rebuild:
        _replay(bucket, current, status, topic, result, back_to_direct)

    wait()
    if found := problems():
        back_to_direct(f"reconciliation failed: {'; '.join(found)}")
    record = _exporter(api, bucket, api_url, local_dir).run(dry_run=True)
    c = record.counts
    result.exported = {"run_id": record.run_id, "status": record.status, **c.__dict__}
    if record.status != "succeeded" or c.new or c.updated or c.deleted or c.failed:
        back_to_direct(
            f"PostgreSQL differs from the bucket: new {c.new}, updated {c.updated}, deleted "
            f"{c.deleted}, failed {c.failed} {record.errors[:3]}",
            # A dry run that could not read a row says nothing about whether they differ.
            HandoverDiffers if record.status == "succeeded" and not c.failed else HandoverFailed,
        )
    result.step("an export dry run finds PostgreSQL and the bucket the same")

    progress = Progress(engine)
    for name, value in status["checkpoints"].items():
        progress.set_checkpoint(name, datetime.fromisoformat(value))
    switched = clock()
    mode.write(
        bucket,
        mode.read(bucket),
        mode=mode.FULL,
        state=mode.ACTIVE,
        since=iso(switched),
        checkpoint_seq=status["seq"],
        replay_from=None,
        owner=machine,
        lease_expires_at=iso(switched + mode.LEASE_TTL) if machine else None,
    )
    result.step(f"full mode, owned by {machine}" if machine else "full mode")
    return result


def _print_status(current: mode.Mode, machine: str) -> None:
    print(
        f"{current.mode} ({current.state}) since {current.since or 'the start'}; "
        f"log sequence at the last change: {current.checkpoint_seq}"
    )
    if current.owner is not None:
        live = "live" if current.lease_live(datetime.now(UTC)) else "expired"
        print(f"owner {current.owner}, lease until {current.lease_expires_at} ({live})")
    print(f"this host: {machine}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Show or change which mode writes the bucket.")
    parser.add_argument("command", choices=("status", "direct", "full"))
    parser.add_argument("--timeout", type=float, default=600, help="full: seconds to wait")
    parser.add_argument(
        "--rebuild", action="store_true", help="full: load an empty PostgreSQL from the bucket"
    )
    parser.add_argument(
        "--outcome", type=Path, default=OUTCOME, help="direct, full: where to record the outcome"
    )
    return parser


def main(argv: list[str] | None = None) -> int:  # noqa: C901 (to refactor: docs/progress/plan.md)
    import time

    from earth_hub.config import get_settings
    from earth_hub.database.engine import make_engine
    from earth_hub.export.bucket import MissingSettings, make_s3_client
    from earth_hub.ingestion.state import STATE_WINDOW, read_topic_since, state_from_messages
    from earth_hub.logging_config import configure_logging
    from earth_hub.reconcile import reconcile

    args = _parser().parse_args(argv)
    configure_logging()
    settings = get_settings()
    try:
        bucket = Bucket(make_s3_client(settings), settings.r2_bucket)
    except MissingSettings as exc:
        print(f"mode: {exc}", file=sys.stderr)
        return 2
    current = mode.read(bucket)
    if args.command == "status":
        _print_status(current, settings.machine_id)
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
                    machine=settings.machine_id,
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
                    machine=settings.machine_id,
                    rebuild=args.rebuild,
                )
    except (HandoverFailed, mode.WrongMode, lock.LockHeld) as exc:
        differs = isinstance(exc, HandoverDiffers)
        write_outcome(args.outcome, args.command, "differs" if differs else "failed", str(exc))
        print(f"mode: {exc}", file=sys.stderr)
        return 1
    write_outcome(args.outcome, args.command, "switched")
    for line in result.steps:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
