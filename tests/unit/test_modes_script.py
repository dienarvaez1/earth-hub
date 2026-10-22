"""scripts/modes.py: switching modes and backfilling in full mode, each one command with its
checks, run here against a fake shell (no Docker, Kafka, PostgreSQL or R2)."""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import modes  # noqa: E402

pytestmark = pytest.mark.req("REQ-MODE-016")

DESCRIBE = """
GROUP  TOPIC  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
{group} earthquakes.raw 0          10              {end}              {lag}  c  /1  c
{group} earthquakes.raw 1          10              10              0    c  /1  c
"""


class Fake(modes.Shell):
    """Answers the checks from `state`, and records every command that changes something."""

    def __init__(self, *, mode="direct (active) since 2026-10-21", docker=True, direct_run=False,
                 lags=None, fail=None, dry_run=False):  # fmt: skip
        self.now = 0.0
        super().__init__(dry_run=dry_run, sleep=self._sleep, clock=lambda: self.now)
        self.mode, self.docker, self.direct_run = mode, docker, direct_run
        self.lags = lags or {}  # group -> a list of lags, one per look (the last repeats)
        self.fail = fail  # a command that fails, e.g. "make mode-full"
        self.slept: list[float] = []

    def _sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds

    def read(self, *cmd):
        line = " ".join(cmd)
        if cmd[:2] == ("docker", "info"):
            return (0 if self.docker else 1), ""
        if "earth_hub.direct.handover" in line:
            return 0, f"warning: something\n{self.mode}\n"
        if cmd[0] == "pgrep":
            return (0, "4242 python -m earth_hub.direct run") if self.direct_run else (1, "")
        if "kafka-consumer-groups.sh" in line:
            group = cmd[-1]
            looks = self.lags.setdefault(group, [0])
            value = looks.pop(0) if len(looks) > 1 else looks[0]
            return 0, DESCRIBE.format(group=group, end=10 + value, lag=value)
        raise AssertionError(f"unexpected read: {line}")

    def run(self, *cmd):
        line = " ".join(cmd)
        self.ran.append(list(cmd))
        if self.dry_run:
            return
        if line == self.fail:
            raise modes.Failed(f"`{line}` failed (above)")
        if line == "make mode-full":
            self.mode = "full (active) since now"
        if line == "make mode-direct":
            self.mode = "direct (active) since now"


def ran(shell) -> list[str]:
    return [" ".join(c) for c in shell.ran]


def test_to_full_hands_over_then_waits_for_the_consumers_and_reconciles(capsys):
    shell = Fake(lags={"earth-hub-consumer": [5, 0], "earth-hub-consumer-backfill": [0]})
    assert modes.main(["to-full"], shell) == 0
    assert ran(shell) == ["make up", "make mode-full", "make reconcile"]
    assert shell.slept == [10]  # one look with lag 5, then 0
    out = capsys.readouterr().out
    assert "[1/6] Docker is running" in out and "[6/6]" in out and "Full mode: full (active)" in out


@pytest.mark.parametrize(
    ("state", "says"),
    [
        ({"docker": False}, "Docker is not running"),
        ({"mode": "full (active) since 2026-10-20"}, "already in full mode"),
        ({"mode": "direct (handing-over) since 2026-10-22"}, "mid-handover"),
        ({"direct_run": True}, "make direct-run` is running"),
    ],
)
def test_to_full_refuses_without_changing_anything(capsys, state, says):
    shell = Fake(**state)
    assert modes.main(["to-full"], shell) == 2
    assert [c for c in ran(shell) if c != "make up"] == []
    out = capsys.readouterr().out
    assert "Not started:" in out and says in out


def test_a_failed_handover_stops_there(capsys):
    shell = Fake(fail="make mode-full")
    assert modes.main(["to-full"], shell) == 1
    assert ran(shell) == ["make up", "make mode-full"]
    assert "Stopped: `make mode-full` failed" in capsys.readouterr().out


def test_consumers_that_never_catch_up_stop_it_after_the_timeout(capsys):
    shell = Fake(lags={"earth-hub-consumer": [3]})
    assert modes.main(["to-full"], shell) == 1
    assert "make reconcile" not in ran(shell)
    assert sum(shell.slept) >= 600 and len(shell.slept) == 60
    assert "did not catch up within 10 min (earth-hub-consumer 3" in capsys.readouterr().out


def test_to_direct_reconciles_hands_over_and_stops_docker(capsys):
    shell = Fake(mode="full (active) since 2026-10-22")
    assert modes.main(["to-direct"], shell) == 0
    assert ran(shell) == [
        "make reconcile", "make mode-direct", "make down", "make -s direct-runs LIMIT=3",
    ]  # fmt: skip
    assert "this computer can be closed" in capsys.readouterr().out


def test_to_direct_refuses_in_direct_mode_and_needs_a_clean_reconcile(capsys):
    assert modes.main(["to-direct"], Fake()) == 2
    shell = Fake(mode="full (active) since 2026-10-22", fail="make reconcile")
    assert modes.main(["to-direct"], shell) == 1
    assert "make mode-direct" not in ran(shell) and "make down" not in ran(shell)


def test_a_backfill_runs_in_full_mode_then_reconciles_and_exports(capsys):
    shell = Fake(
        mode="full (active) since 2026-10-22", lags={"earth-hub-consumer-backfill": [900, 40, 0]}
    )
    assert modes.main(["backfill", "2026-01-01", "2026-02-01"], shell) == 0
    assert ran(shell) == [
        "make up", "make ingest", "make backfill START=2026-01-01 END=2026-02-01",
        "make reconcile", "make export-r2",
    ]  # fmt: skip
    assert shell.slept == [10, 10]


def test_a_backfill_without_an_end_runs_to_now():
    shell = Fake(mode="full (active) since 2026-10-22")
    assert modes.main(["backfill", "2026-01-01"], shell) == 0
    assert "make backfill START=2026-01-01" in ran(shell)


@pytest.mark.parametrize(
    ("argv", "mode", "says"),
    [
        (["backfill", "2026-01-01"], "direct (active) since 2026-10-21", "full mode only"),
        (["backfill", "2026-02-01", "2026-01-01"], "full (active)", "must be after START"),
    ],
)
def test_a_backfill_refuses_direct_mode_and_a_backwards_range(capsys, argv, mode, says):
    shell = Fake(mode=mode)
    assert modes.main(argv, shell) == 2
    assert not any(c.startswith("make backfill") for c in ran(shell))
    assert says in capsys.readouterr().out


def test_a_date_that_is_not_one_is_refused_by_the_parser():
    with pytest.raises(SystemExit):
        modes.main(["backfill", "January"], Fake())


def test_a_dry_run_lists_the_steps_and_changes_nothing(capsys):
    shell = Fake(dry_run=True)
    assert modes.main(["--dry-run", "to-full"], shell) == 0
    out = capsys.readouterr().out
    assert "would wait for lag 0" in out and shell.slept == []
    assert out.rstrip().endswith("Dry run: nothing was changed.") and "Full mode:" not in out


def test_lag_reads_the_brokers_table():
    shell = Fake(lags={"earth-hub-consumer": [7]})
    assert modes.lag(shell, "earth-hub-consumer") == 7
    assert modes.parse_date("2026-01-01") == date(2026, 1, 1)
