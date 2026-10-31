"""Full mode's exporting service, which keeps direct mode ready to take over:
`python -m earth_hub.direct.standby` (the `exporter` service that make ingest starts).

When something was stored since its last export, and at most every EXPORT_INTERVAL_SECONDS, it
takes a cut (repository.export_cut: no consumer batch commits meanwhile), exports through the
API as make export-r2 does, as the bucket's owner (mode.hold), and then, still holding the
bucket's lock, writes direct mode's state from the same cut (handover.write_standby, as
make mode-direct does). If this host stops, a direct round can then go on from exactly what
the bucket holds (Milestone 9).

The state is never ahead of the export (REQ-MODE-019):
- the poller's 26-hour state comes from earthquakes.raw below the live consumer's committed
  offsets only: those messages are stored (offsets are committed after the database);
- catch-up's checkpoints are copied only when the backfill consumer has stored everything
  catch-up published (no lag, measured after reading them); otherwise the bucket keeps the
  last ones written, which are older.

Between exports it renews the lease every LEASE_RENEW. Once the bucket is not in full mode, or
another host owns it, it logs an error and stops with exit code 0, which compose's
`restart: on-failure` leaves stopped: make to-full (or /eh-host claim) and make ingest start it
again. Without R2 settings it exports nothing and stops the same way.
"""

import logging
import signal
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import Engine, func, select

from earth_hub.database.models import Earthquake
from earth_hub.database.repository import export_cut
from earth_hub.direct.handover import Standby, read_standby, write_standby
from earth_hub.export import lock, mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import Exporter, _git_commit
from earth_hub.ingestion.state import STATE_WINDOW, SentState, read_topic_since, state_from_messages
from earth_hub.kafka_metadata import with_retries
from earth_hub.reconcile import committed_offsets

logger = logging.getLogger(__spec__.name)  # the module name, also under `python -m`

POLL = timedelta(seconds=30)


class StandbyMismatch(Exception):
    """The rows exported and the cut's records differ: the state is not written."""


@dataclass
class Cut:
    standby: Standby
    sent: SentState
    checkpoints: bool  # catch-up's may be copied: all it published is stored
    changed: datetime | None  # the latest processing time stored


class Service:
    def __init__(
        self,
        bucket: Bucket,
        *,
        machine: str,
        make_exporter: Callable[[], Exporter],
        cut: Callable[[], AbstractContextManager[Cut]],
        last_change: Callable[[], datetime | None],
        interval: timedelta,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.bucket = bucket
        self.machine = machine
        self.make_exporter = make_exporter
        self.cut = cut
        self.last_change = last_change
        self.interval = interval
        self.clock = clock
        self.exported: datetime | None = None  # the latest change the bucket holds
        self.last_export: datetime | None = None  # when an export last ran
        self.renewed: datetime | None = None

    def step(self) -> str:
        """One look: `exported`, `failed`, `renewed` or `idle`. Raises mode.WrongMode (or
        NotOwner) when this host may not write, LockHeld when another run has the bucket."""
        now = self.clock()
        changed = self.last_change()
        due = self.last_export is None or now - self.last_export >= self.interval
        if changed is not None and (self.exported is None or changed > self.exported) and due:
            return self._export(now)
        if self.renewed is None or now - self.renewed >= mode.LEASE_RENEW:
            mode.hold(self.bucket, mode.require(self.bucket, mode.FULL), self.machine, now)
            self.renewed = now
            return "renewed"
        return "idle"

    def _export(self, now: datetime) -> str:
        with self.cut() as cut:
            exporter = self.make_exporter()

            def after(held: lock.Held) -> None:
                if set(exporter.manifest) != set(cut.standby.order):
                    raise StandbyMismatch(
                        f"{len(exporter.manifest)} rows exported, {len(cut.standby.order)} "
                        "records in the cut: direct mode's state not written"
                    )
                write_standby(
                    self.bucket,
                    held,
                    cut.standby,
                    cut.sent,
                    run_id=held.run_id,
                    now=self.clock(),
                    checkpoints=cut.checkpoints,
                )

            record = exporter.run(after=after)
        self.last_export = now
        if record.status != "succeeded":
            logger.error(
                "export failed; tried again in %s",
                self.interval,
                extra={"run_id": record.run_id, "errors": record.errors[:3]},
            )
            return "failed"
        self.exported, self.renewed = cut.changed, now
        c = record.counts
        logger.info(
            "exported, direct mode's state written",
            extra={
                "run_id": record.run_id,
                "new": c.new,
                "updated": c.updated,
                "deleted": c.deleted,
                "checkpoints_copied": cut.checkpoints,
            },
        )
        return "exported"

    def run(self, stop: threading.Event, poll: timedelta = POLL) -> int:
        while not stop.is_set():
            try:
                self.step()
            except mode.WrongMode as exc:
                logger.error("not exporting, stopping: %s", exc)  # expected: no traceback
                return 0
            except lock.LockHeld as exc:
                logger.info("the bucket is busy; trying again: %s", exc)
            except Exception:
                logger.exception("export cycle failed; trying again")
            stop.wait(poll.total_seconds())
        return 0


def _offsets(bootstrap: str, topic: str, group: str) -> tuple[dict[int, int], dict[int, int]]:
    """The group's committed offsets and the topic's end offsets, by partition."""
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "earth-hub-standby"})
    try:
        partitions = consumer.list_topics(topic, timeout=30).topics[topic].partitions
        ends = {
            p: with_retries(
                lambda p=p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)
            )[1]
            for p in partitions
        }
    finally:
        consumer.close()
    return committed_offsets(bootstrap, group, topic, partitions), ends


def make_cut(engine: Engine, settings, clock: Callable[[], datetime]):
    """The cut over PostgreSQL and Kafka (settings: earth_hub.config.Settings)."""
    bootstrap = settings.kafka_bootstrap_servers

    @contextmanager
    def cut() -> Iterator[Cut]:
        with export_cut(engine) as conn:
            standby = read_standby(conn)  # checkpoints first, then the lag (see the docstring)
            changed = conn.execute(select(func.max(Earthquake.last_applied_at))).scalar()
            raw, _ = _offsets(bootstrap, settings.kafka_topic_raw, settings.kafka_consumer_group)
            values = read_topic_since(
                bootstrap, settings.kafka_topic_raw, clock() - STATE_WINDOW, until=raw
            )
            committed, ends = _offsets(
                bootstrap, settings.kafka_topic_backfill, settings.kafka_backfill_consumer_group
            )
            caught_up = all(committed.get(p, 0) >= end for p, end in ends.items())
            yield Cut(standby, state_from_messages(values), caught_up, changed)

    return cut


def make_exporter(api, bucket: Bucket, api_url: str, settings) -> Exporter:
    """The service's exporter (`api`: an httpx.Client; `settings`: earth_hub.config.Settings).
    No local copy of run records (`local_dir=None`): the container's working directory is not
    writable by its user, nor visible from the host, and the bucket keeps every record under
    runs/ (DEF-063)."""
    return Exporter(
        api,
        bucket,
        api_url=api_url,
        workers=settings.export_workers,
        git_commit=_git_commit(),
        machine=settings.machine_id,
        local_dir=None,
    )


def main() -> int:
    import httpx

    from earth_hub.config import get_settings
    from earth_hub.database.engine import make_engine
    from earth_hub.export.bucket import MissingSettings, make_s3_client
    from earth_hub.logging_config import configure_logging

    configure_logging()
    settings = get_settings()
    try:
        bucket = Bucket(make_s3_client(settings), settings.r2_bucket)
    except MissingSettings as exc:
        logger.warning("no R2 settings, not exporting: %s", exc)
        return 0
    engine = make_engine(settings)
    api_url = str(settings.export_api_url).rstrip("/")
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    def last_change() -> datetime | None:
        with engine.connect() as conn:
            return conn.execute(select(func.max(Earthquake.last_applied_at))).scalar()

    with httpx.Client(
        base_url=api_url + "/", timeout=60, transport=httpx.HTTPTransport(retries=3)
    ) as api:
        service = Service(
            bucket,
            machine=settings.machine_id,
            make_exporter=lambda: make_exporter(api, bucket, api_url, settings),
            cut=make_cut(engine, settings, lambda: datetime.now(UTC)),
            last_change=last_change,
            interval=timedelta(seconds=settings.export_interval_seconds),
        )
        logger.info("exporting as %s, at most every %s", settings.machine_id, service.interval)
        return service.run(stop)


if __name__ == "__main__":
    sys.exit(main())
