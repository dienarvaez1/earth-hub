"""A host's PostgreSQL rebuilt from the bucket (Milestone 9, step 6):
`python -m earth_hub.direct.rebuild status|check|backup DIR` (make host-rebuild, /eh-host).

A host that comes back after direct mode took over may hold events stored after its last
export; the handover to full mode would find them different from the bucket and refuse. It is
rebuilt instead, the bucket being the truth:

1. `check` (read-only) refuses when this database holds an event the bucket lacks, or holds it
   at a newer `updated`: wiping it would lose that. Every USGS ID it knows must be in the
   bucket's ID map, and each record's revision no newer than the bucket's for its preferred ID.
2. `backup DIR` saves PostgreSQL (`pg_dump`) and every message of both topics, keys, headers
   and offsets included (NDJSON), then checks the dump restores into a scratch database with
   the same row counts.
3. After a typed confirmation (scripts/modes.py), Kafka and PostgreSQL are wiped and migrated.
4. `load` (handover.to_full with rebuild=True, while the bucket is frozen): the records from
   the snapshot, with their creation numbers as ids; the USGS IDs and their first-seen times
   from the ID map; one `raw_events` row per record, on the reserved topic `bootstrap`, so
   `make reconcile`'s invariants hold. The handover then replays nothing and still checks that
   an export dry run finds PostgreSQL and the bucket the same.

The message history (`raw_events`, the topics) of the wiped host stays in its backup only.
"""

import argparse
import base64
import gzip
import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import Engine, func, insert, select, text

from earth_hub.database.models import Earthquake, EarthquakeId, RawEvent
from earth_hub.direct.store import BucketState
from earth_hub.export.bucket import Bucket
from earth_hub.export.snapshot import COLUMNS
from earth_hub.kafka_metadata import with_retries

logger = logging.getLogger(__spec__.name)  # the module name, also under `python -m`

BOOTSTRAP_TOPIC = "bootstrap"
CHUNK = 5000
TABLES = ("earthquakes", "earthquake_ids", "raw_events")


class NotEmpty(Exception):
    """The database already holds records: a rebuild loads only into an empty one."""


# 1. What wiping would lose.


def check(engine: Engine, state: BucketState) -> list[str]:
    """What this database holds that the bucket does not: empty when a rebuild loses no event
    or revision. `state`: the bucket, refreshed."""
    problems = []
    with engine.connect() as conn:
        ids = {
            event_id: key
            for event_id, key in conn.execute(
                select(EarthquakeId.event_id, Earthquake.key).join(
                    Earthquake, Earthquake.id == EarthquakeId.earthquake_id
                )
            )
        }
        records = conn.execute(
            select(Earthquake.key, Earthquake.preferred_event_id, Earthquake.source_updated_ms)
        ).all()
    missing = sorted(set(ids) - set(state.ids))
    if missing:
        problems.append(f"{len(missing)} USGS IDs the bucket lacks: {missing[:10]}")
    holders = {
        state.ids[r.preferred_event_id][0] for r in records if r.preferred_event_id in state.ids
    }
    stored = {
        key: row["source_updated_ms"] for key, row in state.rows(holders, strict=False).items()
    }
    newer = sorted(
        r.preferred_event_id
        for r in records
        if r.preferred_event_id in state.ids
        and r.source_updated_ms > stored.get(state.ids[r.preferred_event_id][0], -1)
    )
    if newer:
        problems.append(f"{len(newer)} events at a newer revision than the bucket's: {newer[:10]}")
    return problems


def is_empty(engine: Engine) -> bool:
    with engine.connect() as conn:
        return all(
            conn.execute(text(f"SELECT NOT EXISTS (SELECT 1 FROM {t})")).scalar()  # noqa: S608 (fixed names)
            for t in TABLES
        )


# 2. The backup.


def backup(
    directory: Path,
    *,
    database: str,
    bootstrap: str,
    topics: list[str],
    compose=("docker", "compose"),
) -> Path:
    """`database` and the topics into `directory`; raises if the dump does not restore."""
    directory.mkdir(parents=True, exist_ok=True)
    dump = directory / "postgres.sql.gz"
    with gzip.open(dump, "wb") as out:
        result = subprocess.run(
            [*compose, "exec", "-T", "-e", f"DB={database}", "postgres", "sh", "-c",
             'pg_dump --no-owner -U "$POSTGRES_USER" "$DB"'],
            stdout=subprocess.PIPE,
            check=True,
        )  # fmt: skip
        out.write(result.stdout)
    for topic in topics:
        _dump_topic(bootstrap, topic, directory / f"{topic}.ndjson.gz")
    _check_restore(dump, database, compose)
    return dump


def _dump_topic(bootstrap: str, topic: str, path: Path) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "earth-hub-backup",
            "enable.auto.commit": False,
        }
    )
    count = 0
    try:
        meta = consumer.list_topics(topic, timeout=30).topics.get(topic)
        partitions = sorted(meta.partitions) if meta and meta.error is None else []
        remaining = {}
        assign = []
        for p in partitions:
            low, high = with_retries(
                lambda p=p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)
            )
            if high > low:
                assign.append(TopicPartition(topic, p, low))
                remaining[p] = high
        with gzip.open(path, "wt") as out:
            if assign:
                consumer.assign(assign)
            while remaining:
                message = consumer.poll(30)
                if message is None:
                    raise TimeoutError(f"timed out reading {topic} for the backup")
                if message.error():
                    raise RuntimeError(f"error reading {topic}: {message.error()}")
                entry = {
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "timestamp": message.timestamp()[1],
                    "key": _b64(message.key()),
                    "value": _b64(message.value()),
                    "headers": [[k, _b64(v)] for k, v in (message.headers() or [])],
                }
                out.write(json.dumps(entry) + "\n")
                count += 1
                if message.offset() + 1 >= remaining.get(message.partition(), 0):
                    remaining.pop(message.partition(), None)
    finally:
        consumer.close()
    logger.info("topic saved", extra={"topic": topic, "messages": count, "path": str(path)})
    return count


def _b64(value: bytes | None) -> str | None:
    return None if value is None else base64.b64encode(value).decode()


def _check_restore(dump: Path, database: str, compose) -> None:
    """Restore into a scratch database and compare the row counts with the real one."""
    counts = "; ".join(f"SELECT '{t}', count(*) FROM {t}" for t in TABLES)  # noqa: S608 (fixed names)
    script = (
        'set -e; db=earth_hub_restore_check; dropdb -U "$POSTGRES_USER" --if-exists $db; '
        'createdb -U "$POSTGRES_USER" $db; psql -q -U "$POSTGRES_USER" -d $db -v ON_ERROR_STOP=1 '
        f'>/dev/null; for d in "$DB" $db; do psql -At -U "$POSTGRES_USER" -d "$d" -c '
        f'"{counts}"; echo --; done; dropdb -U "$POSTGRES_USER" $db'
    )
    result = subprocess.run(
        [*compose, "exec", "-T", "-e", f"DB={database}", "postgres", "sh", "-c", script],
        input=gzip.decompress(dump.read_bytes()),
        capture_output=True,
        check=True,
    )
    original, restored, _ = result.stdout.decode().split("--\n")
    if original != restored:
        raise RuntimeError(
            f"the backup does not restore the same rows: {original!r} vs {restored!r}"
        )


# 4. The load.


def load(engine: Engine, state: BucketState) -> int:
    """The bucket's records, IDs and one bootstrap message each into an empty database.
    Returns the records loaded. Raises NotEmpty."""
    if not is_empty(engine):
        raise NotEmpty("the database is not empty: wipe it first (make host-rebuild)")
    keys = sorted(state.order, key=state.order.get)
    with engine.begin() as conn:
        for start in range(0, len(keys), CHUNK):
            chunk = keys[start : start + CHUNK]
            rows = state.rows(chunk)
            conn.execute(
                insert(Earthquake),
                [{"id": state.order[k], **{c: rows[k][c] for c in COLUMNS}} for k in chunk],
            )
            conn.execute(
                insert(RawEvent),
                [
                    {
                        "kafka_topic": BOOTSTRAP_TOPIC,
                        "kafka_partition": 0,
                        "kafka_offset": state.order[k],
                        "event_id": rows[k]["preferred_event_id"],
                        "source_updated_ms": rows[k]["source_updated_ms"],
                        "outcome": "applied",
                        "reason": "loaded from the bucket by a rebuild",
                        "earthquake_id": state.order[k],
                        "received_at": rows[k]["last_applied_at"],
                    }
                    for k in chunk
                ],
            )
        ids = [
            {"event_id": i, "earthquake_id": state.order[key], "first_seen_at": seen}
            for i, (key, seen) in state.ids.items()
        ]
        for start in range(0, len(ids), CHUNK):
            conn.execute(insert(EarthquakeId), ids[start : start + CHUNK])
        for table in ("earthquakes", "raw_events"):
            conn.execute(
                text(
                    f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "  # noqa: S608 (fixed names)
                    f"GREATEST((SELECT max(id) FROM {table}), 1))"
                )
            )
        count = conn.execute(select(func.count()).select_from(Earthquake)).scalar()
    logger.info("PostgreSQL loaded from the bucket", extra={"records": count, "ids": len(ids)})
    return count


def main(argv: list[str] | None = None) -> int:
    from earth_hub.config import get_settings
    from earth_hub.database.engine import make_engine
    from earth_hub.export.bucket import MissingSettings, make_s3_client
    from earth_hub.logging_config import configure_logging

    parser = argparse.ArgumentParser(description="Rebuild this host's PostgreSQL from the bucket.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="empty, or what wiping would lose (read-only)")
    commands.add_parser("check", help="exit 1 if wiping would lose an event (read-only)")
    saved = commands.add_parser("backup", help="save PostgreSQL and the topics, check it restores")
    saved.add_argument("directory", type=Path)
    args = parser.parse_args(argv)
    configure_logging()
    settings = get_settings()
    engine = make_engine(settings)
    if args.command == "backup":
        topics = [settings.kafka_topic_raw, settings.kafka_topic_backfill]
        dump = backup(
            args.directory,
            database=settings.postgres_db,
            bootstrap=settings.kafka_bootstrap_servers,
            topics=topics,
        )
        print(f"backup: {args.directory} (PostgreSQL {dump.stat().st_size:,} bytes, restores)")
        return 0
    if is_empty(engine):
        print("rebuild: this database is empty: nothing to lose")
        return 0
    try:
        bucket = Bucket(make_s3_client(settings), settings.r2_bucket)
    except MissingSettings as exc:
        print(f"rebuild: {exc}", file=sys.stderr)
        return 2
    with tempfile.TemporaryDirectory() as tmp:
        state = BucketState(bucket, Path(tmp), workers=settings.export_workers)
        state.refresh()
        problems = check(engine, state)
    if problems:
        print("rebuild: REFUSED, wiping this database would lose:", file=sys.stderr)
        for line in problems:
            print(f"  {line}", file=sys.stderr)
        return 1
    print("rebuild: the bucket holds every event and revision this database holds")
    return 0


if __name__ == "__main__":
    sys.exit(main())
