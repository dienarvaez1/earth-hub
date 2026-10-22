"""Direct mode's audit trail (src/earth_hub/direct/audit.py): who started an execution, what it
did, how it ended; saved without ever failing a round, read back with `interrupted` runs."""

import json
import logging
from datetime import UTC, datetime, timedelta

import pytest
from support.export import BUCKET, start_direct

from earth_hub.direct import audit
from earth_hub.direct.runner import DirectRunner, Outcome
from earth_hub.export.bucket import Bucket

pytestmark = pytest.mark.req("REQ-MODE-015")

T0 = datetime(2026, 10, 22, 17, 17, 40, tzinfo=UTC)


class Clock:
    def __init__(self, now=T0):
        self.now = now

    def __call__(self):
        return self.now


def saved(s3) -> list[dict]:
    listed = s3.list_objects_v2(Bucket=BUCKET, Prefix=audit.AUDIT_PREFIX).get("Contents", [])
    return [json.loads(s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read()) for o in listed]


@pytest.mark.parametrize(
    ("command", "env", "expected"),
    [
        ("once", {"GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "schedule"}, "github-schedule"),
        (
            "once",
            {
                "GITHUB_ACTIONS": "true",
                "GITHUB_EVENT_NAME": "workflow_dispatch",
                "EARTH_HUB_STARTED_BY": "cloudflare-trigger",
            },
            "cloudflare-trigger",
        ),
        (
            "once",
            {"GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "workflow_dispatch"},
            "github-manual",
        ),
        # Only a run on GitHub can say it came from the trigger.
        ("once", {"EARTH_HUB_STARTED_BY": "cloudflare-trigger"}, "local"),
        ("once", {}, "local"),
        ("run", {}, "direct-run"),
    ],
)
def test_who_started_an_execution(command, env, expected):
    assert audit.started_by(command, env) == expected
    assert expected in audit.STARTERS


def test_the_record_is_saved_at_the_start_and_at_the_end(s3):
    clock = Clock()
    trail = audit.Audit(Bucket(s3, BUCKET), command="python -m earth_hub.direct once",
                        started_by="local", clock=clock)  # fmt: skip
    trail.start("direct")
    (record,) = saved(s3)
    assert record["status"] == "running" and record["mode"] == "direct"
    assert record["started_at"] == "2026-10-22T17:17:40Z"
    key = f"audit/direct/2026-10-22/{record['run_id']}.json"
    assert record["run_id"].startswith("2026-10-22T17-17-40Z-")
    clock.now += timedelta(seconds=42.5)
    trail.finish("succeeded")
    (record,) = saved(s3)
    assert s3.head_object(Bucket=BUCKET, Key=key)
    assert record["status"] == "succeeded" and record["elapsed_s"] == 42.5
    assert record["finished_at"] == "2026-10-22T17:18:22.500000Z"


def test_rounds_are_counted_by_each_messages_part_with_the_rejected_as_errata(s3):
    trail = audit.Audit(Bucket(s3, BUCKET), command="c", started_by="local", clock=Clock())
    trail.round("feed", [Outcome(7, "applied", "ak1", source="feed")], 1, 0)
    # A catch-up round holds the deleted-events check's messages too.
    trail.round(
        "catchup",
        [
            Outcome(8, "applied", "ak2", source="catchup"),
            Outcome(9, "rejected", None, "MAG-001", "magnitude out of range", source="catchup"),
            Outcome(10, "applied", "ak3", source="deleted"),
        ],
        2,
        1,
    )
    parts = {name: vars(part) for name, part in trail.record.parts.items()}
    assert parts["feed"] == {"rounds": 1, "messages": 1, "outcomes": {"applied": 1},
                             "rows_written": 1, "rows_removed": 0}  # fmt: skip
    assert parts["catchup"]["messages"] == 2 and parts["catchup"]["rounds"] == 1
    assert parts["catchup"]["outcomes"] == {"applied": 1, "rejected": 1}
    assert (parts["catchup"]["rows_written"], parts["catchup"]["rows_removed"]) == (2, 1)
    assert parts["deleted"]["messages"] == 1 and parts["deleted"]["rounds"] == 0
    assert trail.record.log_seq == {"first": 7, "last": 10}
    assert trail.record.errata == [
        {"seq": 9, "rule": "MAG-001", "reason": "magnitude out of range"}
    ]


def test_a_long_running_execution_saves_every_heartbeat(s3):
    clock = Clock()
    trail = audit.Audit(Bucket(s3, BUCKET), command="c", started_by="direct-run", clock=clock,
                        heartbeat=audit.HEARTBEAT)  # fmt: skip
    trail.start("direct")
    clock.now += timedelta(minutes=1)
    trail.round("feed", [Outcome(1, "applied", "ak1", source="feed")], 1, 0)
    assert saved(s3)[0]["parts"] == {}  # not yet
    clock.now += audit.HEARTBEAT
    trail.round("feed", [Outcome(2, "applied", "ak2", source="feed")], 1, 0)
    (record,) = saved(s3)
    assert record["parts"]["feed"]["messages"] == 2 and record["status"] == "running"
    assert record["updated_at"] == "2026-10-22T17:28:40Z"


def test_errors_logged_while_it_runs_are_kept(s3):
    trail = audit.Audit(Bucket(s3, BUCKET), command="c", started_by="local", clock=Clock())
    poller = logging.getLogger("earth_hub.ingestion.poller")
    poller.addHandler(trail)
    try:
        poller.error("feed unavailable, skipping round", extra={"error": "USGS answered 503"})
        poller.warning("only a warning")
        for n in range(audit.MAX_ERRORS + 5):
            poller.error("again %d", n)
    finally:
        poller.removeHandler(trail)
    errors = trail.record.errors
    assert (
        errors[0]
        == "earth_hub.ingestion.poller: feed unavailable, skipping round (USGS answered 503)"
    )
    assert len(errors) == audit.MAX_ERRORS and not any("warning" in e for e in errors)


def test_a_failure_to_save_never_raises(s3, caplog):
    class Broken(Bucket):
        def put(self, *args, **kwargs):
            raise OSError("R2 unreachable")

    trail = audit.Audit(Broken(s3, BUCKET), command="c", started_by="local", clock=Clock())
    trail.start("direct")
    trail.finish("succeeded")
    assert "could not save the audit record" in caplog.text


def test_a_failing_audit_never_fails_a_round(s3, tmp_path, make_envelope, make_feature, caplog):
    start_direct(s3)
    runner = DirectRunner(Bucket(s3, BUCKET), workdir=tmp_path, workers=2)

    def broken(*args):
        raise RuntimeError("audit down")

    runner.on_round = broken
    (outcome,) = runner.process([make_envelope(make_feature("ak1"))])
    assert outcome.outcome == "applied" and outcome.source == "feed"
    assert s3.get_object(Bucket=BUCKET, Key="earthquakes/ak1.json")
    assert "could not record the round in the audit" in caplog.text


def put(s3, record: dict) -> None:
    key = f"{audit.AUDIT_PREFIX}{record['started_at'][:10]}/{record['run_id']}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(record))


def record(run_id, started_at, status, updated_at=None, **extra) -> dict:
    return {
        "run_id": run_id, "command": "c", "started_by": "cloudflare-trigger", "mode": "direct",
        "started_at": started_at, "updated_at": updated_at or started_at, "status": status,
        "finished_at": None, "elapsed_s": None, "parts": {}, "reason": None, **extra,
    }  # fmt: skip


def test_the_latest_runs_oldest_first_with_dead_ones_interrupted(s3):
    now = datetime(2026, 10, 22, 18, 0, tzinfo=UTC)
    put(s3, record("2026-10-21T23-17-40Z-aaaa", "2026-10-21T23:17:40Z", "succeeded"))
    put(s3, record("2026-10-22T16-17-40Z-bbbb", "2026-10-22T16:17:40Z", "running"))  # died
    put(s3, record("2026-10-22T17-55-00Z-cccc", "2026-10-22T17:55:00Z", "running"))  # alive
    runs = audit.list_runs(Bucket(s3, BUCKET), limit=10, now=now)
    assert [(r["run_id"][:13], r["status"]) for r in runs] == [
        ("2026-10-21T23", "succeeded"),
        ("2026-10-22T16", "interrupted"),
        ("2026-10-22T17", "running"),
    ]
    assert [r["run_id"][:13] for r in audit.list_runs(Bucket(s3, BUCKET), limit=2, now=now)] == [
        "2026-10-22T16",
        "2026-10-22T17",
    ]


def test_a_record_reads_on_one_line():
    run = record(
        "2026-10-22T17-17-40Z-a1b2", "2026-10-22T17:17:40Z", "succeeded", elapsed_s=41.2,
        parts={"feed": {"rounds": 1, "messages": 43, "outcomes": {"applied": 40, "stale": 3},
                        "rows_written": 40, "rows_removed": 0}},
    )  # fmt: skip
    line = audit.format_record(run)
    assert line.startswith("2026-10-22T17-17-40Z-a1b2  succeeded")
    assert "cloudflare-trigger" in line and "direct" in line
    assert "feed 43 (applied 40, stale 3)" in line and line.endswith("41.2 s")
