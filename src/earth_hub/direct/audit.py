"""Direct mode's audit trail: one record per execution of `python -m earth_hub.direct`.

    audit/direct/<YYYY-MM-DD>/<run_id>.json      by the day the execution started (UTC)

A record says what started the execution and how, what it did and how it ended: the command,
`started_by` (cloudflare-trigger, github-schedule, github-manual, direct-run or local), the
bucket's mode when it started, its status (running, then succeeded, failed or skipped), its
times, the messages its rounds processed by part (feed, catchup, deleted) and outcome, the
rows they wrote and removed, the log sequence numbers they took (log/ holds each message),
and, when something went wrong, the errors logged and the rejected messages (the errata).

It is written when the execution starts and when it ends; `run` (make direct-run) also
saves it every HEARTBEAT, so a record left `running` by a process that died is read as
`interrupted` once it is older than STALE. A failure to write it is logged, never raised:
the audit never stops a round. It sits beside the rows, manifest and snapshot, which full
mode's export writes identically, and is read by `python -m earth_hub.direct.audit`
(make direct-runs).
"""

import json
import logging
import os
import secrets
import subprocess
import sys
import threading
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta

from earth_hub.export.bucket import Bucket

logger = logging.getLogger(__spec__.name)  # the module name, also under `python -m`

AUDIT_PREFIX = "audit/direct/"
HEARTBEAT = timedelta(
    minutes=10
)  # make direct-run's saves: within R2's budget (test_direct_budget)
STALE = 3 * HEARTBEAT
MAX_ERRORS = 20
MAX_ERRATA = 50
STARTERS = ("cloudflare-trigger", "github-schedule", "github-manual", "direct-run", "local")


def started_by(command: str, env: Mapping[str, str] = os.environ) -> str:
    """Who started the execution. On GitHub Actions the event tells a schedule from a run
    asked for; the dashboard Worker's trigger says so in its request (EARTH_HUB_STARTED_BY,
    from direct.yml's `started_by` input). Elsewhere: make direct-run's loop, or by hand."""
    if env.get("GITHUB_ACTIONS") == "true":
        if env.get("GITHUB_EVENT_NAME") == "schedule":
            return "github-schedule"
        if env.get("EARTH_HUB_STARTED_BY") == "cloudflare-trigger":
            return "cloudflare-trigger"
        return "github-manual"
    return "direct-run" if command == "run" else "local"


def github_run(env: Mapping[str, str] = os.environ) -> str | None:
    if env.get("GITHUB_ACTIONS") != "true" or not env.get("GITHUB_RUN_ID"):
        return None
    server = env.get("GITHUB_SERVER_URL", "https://github.com")
    return f"{server}/{env.get('GITHUB_REPOSITORY')}/actions/runs/{env['GITHUB_RUN_ID']}"


def git_commit(env: Mapping[str, str] = os.environ) -> str | None:
    if env.get("GITHUB_SHA"):
        return env["GITHUB_SHA"][:7]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=False
        )
    except OSError:
        return None
    return result.stdout.strip() or None


@dataclass
class Part:
    """What the rounds of one part (feed, catchup, deleted) did."""

    rounds: int = 0
    messages: int = 0
    outcomes: dict[str, int] = field(default_factory=dict)
    rows_written: int = 0
    rows_removed: int = 0


@dataclass
class AuditRecord:
    run_id: str
    command: str
    started_by: str
    mode: str | None  # the marker when the execution started: direct, full, ...
    started_at: str
    status: str = "running"  # running, succeeded, failed, skipped
    updated_at: str | None = None
    finished_at: str | None = None
    elapsed_s: float | None = None
    parts: dict[str, Part] = field(default_factory=dict)
    log_seq: dict[str, int] | None = None  # the first and last sequence numbers it took
    errors: list[str] = field(default_factory=list)
    errata: list[dict] = field(default_factory=list)  # the rejected messages
    reason: str | None = None  # why it was skipped or failed, in a line
    github_run: str | None = None
    git_commit: str | None = None

    @property
    def key(self) -> str:
        return f"{AUDIT_PREFIX}{self.started_at[:10]}/{self.run_id}.json"

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), indent=1).encode()


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


class Audit(logging.Handler):
    """The record of one execution. As a logging handler it keeps the errors logged while it
    is attached (a feed that failed, a message that could not be published)."""

    def __init__(
        self,
        bucket: Bucket,
        *,
        command: str,
        started_by: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        github_run: str | None = None,
        git_commit: str | None = None,
        heartbeat: timedelta | None = None,
    ):
        super().__init__(level=logging.ERROR)
        self.bucket = bucket
        self.clock = clock
        self.heartbeat = heartbeat
        self._lock = threading.Lock()
        started = clock()
        self._started = started
        self._saved = started
        self.record = AuditRecord(
            run_id=f"{started:%Y-%m-%dT%H-%M-%SZ}-{secrets.token_hex(2)}",
            command=command,
            started_by=started_by,
            mode=None,
            started_at=_iso(started),
            github_run=github_run,
            git_commit=git_commit,
        )

    def start(self, mode: str | None) -> None:
        with self._lock:
            self.record.mode = mode
        self._save()

    def round(self, source: str, outcomes, rows_written: int, rows_removed: int) -> None:
        """A round the runner finished (DirectRunner.on_round): its messages counted by their
        own part (a catch-up round holds the deleted-events check's too), its rows by the
        round's."""
        with self._lock:
            part = self.record.parts.setdefault(source, Part())
            part.rounds += 1
            part.rows_written += rows_written
            part.rows_removed += rows_removed
            for outcome in outcomes:
                own = self.record.parts.setdefault(outcome.source or source, Part())
                own.messages += 1
                own.outcomes[outcome.outcome] = own.outcomes.get(outcome.outcome, 0) + 1
                if outcome.outcome == "rejected" and len(self.record.errata) < MAX_ERRATA:
                    self.record.errata.append(
                        {"seq": outcome.seq, "rule": outcome.rule, "reason": outcome.reason}
                    )
            if outcomes:
                seqs = self.record.log_seq or {"first": outcomes[0].seq, "last": outcomes[0].seq}
                seqs["first"] = min(seqs["first"], outcomes[0].seq)
                seqs["last"] = max(seqs["last"], outcomes[-1].seq)
                self.record.log_seq = seqs
            due = self.heartbeat is not None and self.clock() - self._saved >= self.heartbeat
        if due:
            self._save()

    def emit(self, record: logging.LogRecord) -> None:
        if record.name == logger.name:
            return  # its own warnings
        message = record.getMessage()
        error = getattr(record, "error", None)
        line = f"{record.name}: {message}" + (f" ({error})" if error else "")
        if record.exc_info and record.exc_info[1] is not None:
            line += f": {type(record.exc_info[1]).__name__}: {record.exc_info[1]}"
        with self._lock:
            if len(self.record.errors) < MAX_ERRORS:
                self.record.errors.append(line[:500])

    def finish(self, status: str, reason: str | None = None) -> None:
        now = self.clock()
        with self._lock:
            self.record.status = status
            self.record.reason = reason
            self.record.finished_at = _iso(now)
            self.record.elapsed_s = round((now - self._started).total_seconds(), 3)
        self._save()

    def _save(self) -> None:
        with self._lock:
            now = self.clock()
            self.record.updated_at = _iso(now)
            key, body = self.record.key, self.record.to_json()
            self._saved = now
        try:
            self.bucket.put(key, body)
        except Exception:
            logger.warning("could not save the audit record", extra={"run_id": self.record.run_id})


def list_runs(bucket: Bucket, *, limit: int = 20, now: datetime | None = None) -> list[dict]:
    """The latest records, oldest first, from the last 31 days' folders. A record still
    `running` that has not been saved for STALE belongs to a process that died: reported as
    `interrupted`."""
    now = now or datetime.now(UTC)
    found: list[str] = []
    for back in range(31):
        day = (now - timedelta(days=back)).strftime("%Y-%m-%d")
        found = sorted(bucket.keys(f"{AUDIT_PREFIX}{day}/")) + found
        if len(found) >= limit:
            break
    runs = []
    for key in found[-limit:]:
        got = bucket.get(key)
        if got is None:
            continue
        run = json.loads(got[0])
        updated = datetime.fromisoformat(run.get("updated_at") or run["started_at"])
        if run["status"] == "running" and now - updated > STALE:
            run["status"] = "interrupted"
        runs.append(run)
    return runs


def format_record(run: dict) -> str:
    parts = run.get("parts") or {}
    done = (
        "  ".join(
            f"{name} {p['messages']}"
            + (
                f" ({', '.join(f'{k} {v}' for k, v in sorted(p['outcomes'].items()))})"
                if p["outcomes"]
                else ""
            )
            for name, p in parts.items()
        )
        or "no messages"
    )
    elapsed = run.get("elapsed_s")
    line = (
        f"{run['run_id']}  {run['status']:<11} {run['started_by']:<18} "
        f"{(run.get('mode') or '-'):<7} {done}  "
        f"{'' if elapsed is None else f'{elapsed:.1f} s'}"
    )
    return line + (f"  {run['reason']}" if run.get("reason") else "")


def main(argv: list[str] | None = None) -> int:
    import argparse

    from earth_hub.config import get_settings
    from earth_hub.export.bucket import MissingSettings, make_s3_client

    parser = argparse.ArgumentParser(description="List direct mode's latest executions.")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args(argv)
    settings = get_settings()
    try:
        bucket = Bucket(make_s3_client(settings), settings.r2_bucket)
    except MissingSettings as exc:
        print(f"direct-runs: {exc}", file=sys.stderr)
        return 2
    for run in list_runs(bucket, limit=args.limit):
        print(format_record(run))
    return 0


if __name__ == "__main__":
    sys.exit(main())
