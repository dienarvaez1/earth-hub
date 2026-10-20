"""Direct mode's rounds: messages in, validated, applied by the ledger, written to the bucket.

A round takes message values exactly as Kafka would carry them (EventMessage.to_json()), so it
decodes and validates them as the consumer does, and the ledger makes the same decisions as
for PostgreSQL. One round is one batch with one processing time (`now`), like process_batch.

The existing code drives it unchanged: the Poller and CatchUp publish to a DirectPublisher
instead of Kafka, and CatchUp asks BucketProgress instead of PostgreSQL what is stored. The
poller's 26-hour state, rebuilt from earthquakes.raw in full mode, is saved with each round.

Each round that writes holds the bucket's lock (export/lock.py), the one the exporter takes,
so a direct round and an export never write at once. Only one process may run direct mode;
the mode switch (Milestone 8, step 3) makes the modes take turns.
"""

import json
import logging
import secrets
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from earth_hub.database.repository import decode_value, envelope_ids
from earth_hub.database.revisions import Revision, needed
from earth_hub.direct.store import BucketState, BucketStore, Inconsistent, iso, log_key
from earth_hub.export import lock
from earth_hub.export.bucket import Bucket
from earth_hub.ingestion.events import EventMessage
from earth_hub.ingestion.producer import PublishResult
from earth_hub.ingestion.state import SentState
from earth_hub.validation.earthquake_rules import Rejected, validate

logger = logging.getLogger(__name__)

# A round takes seconds; a lock left by one that crashed should not block the next for long.
LOCK_TTL = timedelta(minutes=15)


@dataclass(frozen=True)
class Outcome:
    """A message's result, as ProcessResult in full mode, with the record's key."""

    seq: int
    outcome: str  # applied, duplicate, stale or rejected
    key: str | None = None  # the record, as it was when this message was processed
    rule: str | None = None
    reason: str | None = None


class DirectRunner:
    def __init__(
        self,
        bucket: Bucket,
        *,
        workdir: Path | None = None,
        workers: int = 16,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        lock_ttl: timedelta = LOCK_TTL,
    ):
        if workdir is None:
            self._tmp = tempfile.TemporaryDirectory(prefix="earth-hub-direct-")
            workdir = Path(self._tmp.name)
        self.bucket = bucket
        self.state = BucketState(bucket, workdir, workers=workers)
        self.clock = clock
        self.lock_ttl = lock_ttl

    def process(
        self,
        values: list[bytes],
        *,
        source: str = "feed",
        sent: SentState | None = None,
        now: datetime | None = None,
    ) -> list[Outcome]:
        """Process messages in one round. `sent`: the poller's state, saved with the round,
        including these messages. Raises LockHeld or Inconsistent before writing anything;
        any other failure leaves either nothing or a pending round the next one finishes."""
        if not values:
            return []
        now = now or self.clock()
        started = time.monotonic()
        run_id = f"{now:%Y-%m-%dT%H-%M-%SZ}-{secrets.token_hex(2)}"
        with self._locked(run_id, now):
            self._prepare()
            outcomes, pending = self._decide(values, source, sent, run_id, now)
            self.state.begin(pending)
            self.state.apply(pending)
        self._log(outcomes, source, run_id, pending, time.monotonic() - started)
        return outcomes

    # Catch-up and the poller's state.

    def sent_state(self) -> SentState:
        """The poller's 26-hour state, as the last round saved it."""
        self.state.refresh()
        return SentState.from_entries(self.state.status["sent"])

    def checkpoint(self, name: str) -> datetime | None:
        self.state.refresh()
        value = self.state.status["checkpoints"].get(name)
        return datetime.fromisoformat(value) if value else None

    def set_checkpoint(self, name: str, value: datetime) -> None:
        now = self.clock()
        with self._locked(f"{now:%Y-%m-%dT%H-%M-%SZ}-{secrets.token_hex(2)}", now):
            self._prepare()
            status = dict(self.state.status)
            status["checkpoints"] = {**status["checkpoints"], name: iso(value)}
            self.state.save_status(status)

    def needed(self, messages: list[EventMessage]) -> list[EventMessage]:
        """The messages a round would apply: Progress.needs_publish, against the bucket."""
        self.state.refresh()
        known = self.state.ids
        event_of = {i: known[i][0] for m in messages for i in m.ids if i in known}
        stored = {
            key: Revision(
                row["preferred_event_id"],
                tuple(row["revision_ids"]),
                row["source_updated_ms"],
                row["review_status"] == "deleted",
            )
            for key, row in self.state.rows(event_of.values()).items()
        }
        return needed(messages, event_of, stored)

    # A round.

    @contextmanager
    def _locked(self, run_id: str, now: datetime) -> Iterator[None]:
        lock.acquire(self.bucket, run_id, iso(now), now, self.lock_ttl)
        try:
            yield
        finally:
            lock.release(self.bucket, run_id, self.lock_ttl)

    def _prepare(self) -> None:
        """Read what changed, finish a round left pending, and check the state."""
        self.state.refresh()
        if self.state.pending is not None:
            logger.warning(
                "finishing an interrupted round", extra={"run_id": self.state.pending["run_id"]}
            )
            self.state.apply(self.state.pending)
        self.state.check()

    def _decide(self, values, source, sent, run_id, now) -> tuple[list[Outcome], dict]:
        decoded = [decode_value(v) for v in values]
        store = BucketStore(self.state)
        ledger = store.load(i for envelope, _ in decoded for i in envelope_ids(envelope))
        steps = []
        for envelope, raw_value in decoded:
            try:
                event = validate(envelope, now=now)
            except Rejected as rejected:
                rule, reason = rejected.rule, rejected.reason
                steps.append((envelope, raw_value, "rejected", None, rule, reason))
                continue
            step = ledger.apply(event, now)
            steps.append((envelope, raw_value, step.outcome, step.key, None, step.reason))
        numbers = store.write(ledger, now)
        changes = store.changes
        key_of = {n: k for k, n in self.state.order.items()} | {
            n: k for k, n in changes.order.items()
        }
        # Records created and merged away in this round, for their messages' lines.
        for k in ledger.created:
            key_of.setdefault(numbers[k], ledger.initial[k].key)

        first = self.state.status["seq"] + 1
        outcomes, lines = [], []
        for seq, (envelope, raw_value, outcome, k, rule, reason) in enumerate(steps, first):
            # The record as it was then. A key is one of its record's USGS IDs, so the ID map
            # gives where it is now (raw_events.earthquake_id in full mode, moved by merges).
            at_the_time = None if k is None else key_of[numbers[k]]
            outcomes.append(Outcome(seq, outcome, at_the_time, rule, reason))
            lines.append(
                {
                    "seq": seq,
                    "run_id": run_id,
                    "processed_at": iso(now),
                    # The envelope's (feed, catchup, deleted), else the round's.
                    "source": _source(envelope) or source,
                    "outcome": outcome,
                    "rule": rule,
                    "reason": reason,
                    "key": at_the_time,
                    "event_id": envelope.get("event_id") if isinstance(envelope, dict) else None,
                    "envelope": envelope,
                    "raw_value": raw_value,
                }
            )
        status = dict(self.state.status)
        status.update(seq=first + len(steps) - 1, run_id=run_id, written_at=iso(now))
        if sent is not None:
            status["sent"] = sent.entries()
        pending = {
            "run_id": run_id,
            "written_at": iso(now),
            "rows": {key: body.decode() for key, body in changes.rows.items()},
            "removed": sorted(changes.removed),
            "ids": changes.ids,
            "order": changes.order,
            "next": changes.next,
            "log_key": log_key(first, first + len(steps) - 1, now),
            "log": "".join(_line(line) for line in lines),
            "status": status,
        }
        return outcomes, pending

    def _log(self, outcomes, source, run_id, pending, seconds) -> None:
        routine = logging.INFO if source == "feed" else logging.DEBUG
        tally: dict[str, int] = {}
        for outcome in outcomes:
            tally[outcome.outcome] = tally.get(outcome.outcome, 0) + 1
            fields = {
                "seq": outcome.seq,
                "outcome": outcome.outcome,
                "key": outcome.key,
                "reason": outcome.reason,
            }
            if outcome.outcome == "rejected":
                logger.warning("message rejected", extra=fields)
            else:
                logger.log(routine, "message processed", extra=fields)
        logger.info(
            "direct round",
            extra={
                "run_id": run_id,
                "source": source,
                "messages": len(outcomes),
                "outcomes": tally,
                "rows_written": len(pending["rows"]),
                "rows_removed": len(pending["removed"]),
                "seconds": round(seconds, 3),
            },
        )


def _source(envelope) -> str | None:
    value = envelope.get("source") if isinstance(envelope, dict) else None
    return value if isinstance(value, str) else None


def _line(entry: dict) -> str:
    return json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n"


class DirectPublisher:
    """What the Poller and CatchUp publish to in direct mode: each publish is one round.
    Either every message is processed or none is (failed, retried next round)."""

    def __init__(self, runner: DirectRunner, *, source: str, state: SentState | None = None):
        self.runner = runner
        self.source = source
        self.state = state  # the poller's; None for catch-up, as in full mode
        self.topic = f"direct ({source})"

    def publish(self, messages: list[EventMessage]) -> PublishResult:
        if not messages:
            return PublishResult()
        sent = None
        if self.state is not None:
            # What the state will hold once these are marked sent; the poller marks its own.
            sent = SentState.from_entries(self.state.entries())
            for m in messages:
                sent.mark_sent(m.event_id, m.source_updated_ms, m.event_time_ms)
            sent.prune(self.runner.clock())
        try:
            self.runner.process([m.to_json() for m in messages], source=self.source, sent=sent)
        except (lock.LockHeld, Inconsistent) as exc:
            return PublishResult(failed=[(m, str(exc)) for m in messages])
        except Exception as exc:  # retried next round; a pending round is finished first
            logger.exception("direct round failed")
            return PublishResult(failed=[(m, f"{type(exc).__name__}: {exc}") for m in messages])
        return PublishResult(delivered=list(messages))

    def flush(self, timeout_s: float) -> int:
        return 0


class BucketProgress:
    """What CatchUp needs from database/progress.py:Progress, from the bucket."""

    def __init__(self, runner: DirectRunner):
        self.runner = runner

    def needs_publish(self, messages: list[EventMessage]) -> list[EventMessage]:
        return self.runner.needed(messages)

    def checkpoint(self, name: str) -> datetime | None:
        return self.runner.checkpoint(name)

    def set_checkpoint(self, name: str, value: datetime) -> None:
        self.runner.set_checkpoint(name, value)
