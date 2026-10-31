"""scripts/modes.py: switching modes and backfilling in full mode, each one command with its
checks, run here against a fake shell (no Docker, Kafka, PostgreSQL or R2)."""

import json
import re
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import modes  # noqa: E402

pytestmark = pytest.mark.req("REQ-MODE-016")


@pytest.fixture(autouse=True)
def outcome_file(tmp_path, monkeypatch):
    """The handover's outcome file (DEF-062), out of reports/."""
    monkeypatch.setattr(modes, "OUTCOME", tmp_path / "handover.json")


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


# Moving full mode between hosts (Milestone 9, step 7).


class Hosts(Fake):
    """Fake adds the owner, this host and its database to what the checks read."""

    def __init__(self, *, owner=None, live=False, host="host-b", db="empty", check=0,
                 outcome="differs", **kwargs):  # fmt: skip
        super().__init__(**kwargs)
        self.owner, self.live, self.host = owner, live, host
        self.db, self.check = db, check  # db: "empty" or "data"; check: rebuild check's exit
        # What a failed `make mode-full` leaves in reports/handover.json (DEF-062): "differs",
        # "failed", "switched-then-failed" (the check against USGS after the switch), or None.
        self.outcome = outcome

    def read(self, *cmd):
        line = " ".join(cmd)
        if "earth_hub.direct.handover" in line:
            out = f"{self.mode}\n"
            if self.owner:
                state = "live" if self.live else "expired"
                out += f"owner {self.owner}, lease until 2026-10-30T12:00:00Z ({state})\n"
            return 0, out + f"this host: {self.host}\n"
        if "earth_hub.direct.rebuild status" in line:
            return self._status()
        if "earth_hub.direct.rebuild check" in line:
            return (
                self.check,
                "rebuild: REFUSED: 1 USGS IDs the bucket lacks" if self.check else "ok",
            )
        if line == "make -s direct-runs LIMIT=5":
            return 0, "run-1 succeeded"
        return super().read(*cmd)

    def _record_outcome(self):
        if self.outcome is None:  # stopped before the handover (a build, Compose)
            return
        if self.outcome == "switched-then-failed":
            self.mode, self.owner, self.live = "full (active) since now", self.host, True
            record = {"command": "full", "outcome": "switched"}
        else:
            record = {"command": "full", "outcome": self.outcome, "reason": "simulated"}
        modes.OUTCOME.write_text(json.dumps(record))

    def _status(self):
        """`rebuild status`: its exit codes are check's (DEF-060)."""
        if self.db == "empty":
            return 0, "rebuild: this database is empty: nothing to lose"
        if self.db == "unreadable":
            return 2, "rebuild: R2 settings missing"
        return (
            self.check,
            "rebuild: REFUSED: 1 USGS IDs the bucket lacks" if self.check else "rebuild: ok",
        )

    def run(self, *cmd):
        line = " ".join(cmd)
        if line == self.fail == "make mode-full" and not self.dry_run:
            self._record_outcome()
        super().run(*cmd)
        if line.startswith("make mode-full") and not self.dry_run and line != self.fail:
            self.mode = "full (active) since now"
            self.owner, self.live = self.host, True


def without_backup_dir(lines: list[str]) -> list[str]:
    return [re.sub(r"reports/backup/\S+", "reports/backup/<time>", line) for line in lines]


MODE_REQ = pytest.mark.req("REQ-MODE-024")


@MODE_REQ
def test_a_claim_on_a_new_host_loads_its_empty_database_from_the_bucket(capsys):
    shell = Hosts()
    assert modes.main(["host-claim"], shell) == 0
    assert ran(shell) == ["make up", "make migrate", "make mode-full REBUILD=1", "make reconcile"]
    assert "Claimed: full (active)" in capsys.readouterr().out


@MODE_REQ
def test_a_returning_host_whose_database_matches_hands_back_without_a_rebuild():
    shell = Hosts(db="data")
    assert modes.main(["host-claim"], shell) == 0
    assert ran(shell) == ["make up", "make migrate", "make mode-full", "make reconcile"]


@MODE_REQ
def test_a_host_whose_database_differs_is_backed_up_confirmed_wiped_and_rebuilt(monkeypatch):
    shell = Hosts(db="data", fail="make mode-full")
    asked = []
    monkeypatch.setattr(modes, "ask", lambda machine: asked.append(machine) or True)
    assert modes.main(["host-claim"], shell) == 0
    assert asked == ["host-b"]
    assert without_backup_dir(ran(shell)) == [
        "make up",
        "make migrate",
        "make mode-full",  # failed: the dry run found differences, back to direct mode
        "make ingest-stop",
        "uv run python -m earth_hub.direct.rebuild backup reports/backup/<time>",
        "make reset",
        "make up",
        "make migrate",
        "make mode-full REBUILD=1",
        "make reconcile",
    ]


@MODE_REQ
@pytest.mark.parametrize(
    ("hosts", "says"),
    [
        (
            {"mode": "full (active) since x", "owner": "host-b", "live": True},
            "owns the bucket already",
        ),
        ({"mode": "full (active) since x", "owner": "host-a", "live": True}, "lease is live"),
        ({"mode": "full (active) since x"}, "full mode with no owner"),
        ({"mode": "direct (handing-over) since x"}, "mid-handover"),
        ({"docker": False}, "Docker is not running"),
        ({"direct_run": True}, "direct-run` is running"),
    ],
)
def test_a_claim_refuses_a_wrong_start_without_changing_anything(hosts, says, capsys):
    shell = Hosts(**hosts)
    assert modes.main(["host-claim"], shell) == 2
    assert ran(shell) == []
    assert says in capsys.readouterr().out


@MODE_REQ
def test_a_database_ahead_of_the_bucket_still_tries_the_handover_first():
    # DEF-060: `rebuild status` exiting 1 (read, wiping would lose data) stopped the claim as
    # "could not read this host's database", before `make mode-full` was tried.
    shell = Hosts(db="data", check=1)
    assert modes.main(["host-claim"], shell) == 0
    assert ran(shell) == ["make up", "make migrate", "make mode-full", "make reconcile"]


@MODE_REQ
def test_a_database_ahead_that_differs_refuses_the_rebuild_and_wipes_nothing(capsys):
    shell = Hosts(db="data", check=1, fail="make mode-full")
    assert modes.main(["host-claim"], shell) == 2
    assert ran(shell)[-1] == "make ingest-stop" and "make reset" not in ran(shell)
    out = capsys.readouterr().out
    assert "would lose data" in out and "could not read" not in out


@MODE_REQ
@pytest.mark.parametrize(
    ("outcome", "says"),
    [
        (None, "stopped before the handover's comparison"),
        ("failed", "simulated"),
        ("switched-then-failed", "the handover switched, then a later step failed"),
    ],
    ids=["before the handover", "another handover failure", "after the switch"],
)
def test_a_failed_handover_that_is_not_a_difference_wipes_nothing(outcome, says, capsys):
    # DEF-062: any failure of make mode-full (here a Compose error, DEF-061) went on to the
    # rebuild; after the switch it would have wiped the database of the bucket's new owner.
    shell = Hosts(db="data", fail="make mode-full", outcome=outcome)
    assert modes.main(["host-claim"], shell) == 1
    assert ran(shell) == ["make up", "make migrate", "make mode-full"]
    out = capsys.readouterr().out
    assert says in out and "nothing was wiped" in out and "rebuilding" not in out


def test_an_outcome_left_by_an_earlier_handover_is_cleared_first(capsys):
    modes.OUTCOME.write_text(json.dumps({"command": "full", "outcome": "differs"}))
    shell = Hosts(db="data", fail="make mode-full", outcome=None)
    assert modes.main(["host-claim"], shell) == 1
    assert "make reset" not in ran(shell)


def test_a_claim_fails_when_the_database_cannot_be_read(capsys):
    shell = Hosts(db="unreadable")
    assert modes.main(["host-claim"], shell) == 1
    assert "could not read this host's database: rebuild: R2 settings missing" in (
        capsys.readouterr().out
    )
    assert "make mode-full" not in " ".join(ran(shell))


@MODE_REQ
def test_a_claim_after_a_host_stopped_takes_the_bucket_over_first():
    shell = Hosts(mode="full (active) since x", owner="host-a", live=False)
    assert modes.main(["host-claim"], shell) == 0
    assert ran(shell)[0] == "uv run python -m earth_hub.direct once"


@MODE_REQ
@pytest.mark.parametrize("why", ["the bucket lacks an event", "not confirmed"])
def test_a_rebuild_that_would_lose_data_or_is_not_confirmed_wipes_nothing(why, capsys, monkeypatch):
    shell = Hosts(db="data", check=1 if why == "the bucket lacks an event" else 0)
    monkeypatch.setattr(modes, "ask", lambda machine: False)
    assert modes.main(["host-rebuild"], shell) == 2
    assert "make reset" not in ran(shell) and "make mode-full REBUILD=1" not in ran(shell)
    out = capsys.readouterr().out
    assert ("would lose data" if why != "not confirmed" else "not confirmed") in out


@MODE_REQ
def test_a_rebuild_dry_run_lists_its_steps_and_changes_nothing(capsys):
    shell = Hosts(db="data", dry_run=True)
    assert modes.main(["--dry-run", "host-rebuild"], shell) == 0
    out = capsys.readouterr().out
    assert "would ask for this host's machine ID (host-b)" in out and "Dry run" in out
    assert shell.owner is None  # nothing happened


@MODE_REQ
def test_a_release_runs_only_on_the_owner():
    owner = Hosts(mode="full (active) since x", owner="host-b", live=True)
    assert modes.main(["host-release"], owner) == 0
    assert ran(owner) == [
        "make reconcile",
        "make mode-direct",
        "make down",
        "make -s direct-runs LIMIT=3",
    ]
    other = Hosts(mode="full (active) since x", owner="host-a", live=True)
    assert modes.main(["host-release"], other) == 2 and ran(other) == []


@MODE_REQ
def test_a_takeover_runs_direct_mode_at_once_and_only_from_full_mode():
    shell = Hosts(mode="full (active) since x", owner="host-a", live=True)
    assert modes.main(["host-takeover"], shell) == 0
    assert ran(shell) == ["uv run python -m earth_hub.direct once --take-over"]
    direct = Hosts()
    assert modes.main(["host-takeover"], direct) == 2 and ran(direct) == []


@MODE_REQ
def test_host_status_writes_nothing(capsys):
    shell = Hosts(mode="full (active) since x", owner="host-a", live=False)
    assert modes.main(["host-status"], shell) == 0
    assert ran(shell) == []
    out = capsys.readouterr().out
    assert "Owner: host-a, lease expired" in out and "This host: host-b" in out


def test_the_machine_id_prompt_with_no_input_wipes_nothing(monkeypatch, capsys):
    # DEF-059: with no terminal, input() raises EOFError; that is not the ID typed.
    def no_input(_prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", no_input)
    assert modes.ask("host-b") is False
    assert "No answer" in capsys.readouterr().out
