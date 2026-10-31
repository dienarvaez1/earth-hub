"""scripts/cutover.py: cutting full mode over from machine A to machine B, one command on each,
run here against a fake shell (no Docker, Kafka, PostgreSQL, R2 or network)."""

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cutover  # noqa: E402
import modes  # noqa: E402

pytestmark = pytest.mark.req("REQ-MODE-025")

NOW = datetime(2026, 10, 31, 3, 0, tzinfo=UTC)
ENV = {
    "R2_ACCOUNT_ID": "acct",
    "R2_ACCESS_KEY_ID": "id",
    "R2_SECRET_ACCESS_KEY": "secret",
    "R2_BUCKET": "earth-hub",
    "USGS_CONTACT": "me@example.org",
    "POSTGRES_DB": "earth_hub",
    "POSTGRES_USER": "earth_hub",
    "POSTGRES_PASSWORD": "pw",
    "EARTH_HUB_MACHINE_ID": "host-b",
}


class Machine(modes.Shell):
    """Answers every read from its fields; records what would change something."""

    def __init__(self, *, mode="direct (active) since x", owner=None, live=False, host="host-b",
                 docker=True, direct_run=False, feed="200", last_round="2026-10-31T02-17-01Z",
                 claim_dry_run=0, dry_run=False, services=cutover.SERVICES,
                 db=None):  # fmt: skip
        super().__init__(dry_run=dry_run)
        self.mode, self.owner, self.live, self.host = mode, owner, live, host
        self.docker, self.direct_run, self.feed = docker, direct_run, feed
        self.last_round, self.claim_dry_run, self.services = last_round, claim_dry_run, services
        self.db = db  # None: PostgreSQL not running; else `rebuild status`'s (exit, output)
        self.reads: list[str] = []

    def _marker(self):
        out = f"{self.mode}\n"
        if self.owner:
            out += f"owner {self.owner}, lease until t ({'live' if self.live else 'expired'})\n"
        return 0, out + f"this host: {self.host}\n"

    def _round(self):
        return 0, (
            f"{self.last_round}-ab12  skipped  cloudflare-trigger" if self.last_round else ""
        )

    def read(self, *cmd):
        line = " ".join(cmd)
        self.reads.append(line)
        answers = [
            (lambda: "earth_hub.direct.handover" in line, self._marker),
            (lambda: cmd[:2] == ("docker", "info"), lambda: (0 if self.docker else 1, "")),
            (lambda: cmd[:2] == ("uv", "--version"), lambda: (0, "uv 0.9")),
            (
                lambda: cmd[0] == "pgrep",
                lambda: (0, "42 python -m earth_hub.direct run") if self.direct_run else (1, ""),
            ),
            (
                lambda: cmd[0] == "curl",
                lambda: (0, self.feed) if self.feed else (6, "Could not resolve host"),
            ),
            (lambda: line == "make -s direct-runs LIMIT=1", self._round),
            (
                lambda: line in ("make host-release DRY_RUN=1", "make host-claim DRY_RUN=1"),
                lambda: (self.claim_dry_run, "    [1/6] ...\n    Dry run: nothing was changed."),
            ),
            (lambda: "earth_hub.direct.rebuild status" in line, lambda: self.db),
            (
                lambda: cmd[:3] == ("docker", "compose", "ps"),
                lambda: (0, "\n".join([*self.services, *(["postgres"] if self.db else [])])),
            ),
        ]
        for matches, answer in answers:
            if matches():
                return answer()
        raise AssertionError(f"unexpected read: {line}")

    def run(self, *cmd):
        self.ran.append(list(cmd))
        line = " ".join(cmd)
        if line == "make host-claim":
            self.mode, self.owner, self.live = "full (active) since now", self.host, True
        if line == "make host-release":
            self.mode, self.owner, self.live = "direct (active) since now", None, False


def ran(machine) -> list[str]:
    return [" ".join(c) for c in machine.ran]


def run_a(machine, answer="y", argv=()):
    return cutover.main(["a", *argv], machine, ask=lambda _: answer, now=lambda: NOW)


def run_b(machine, answer="y", argv=(), env=None):
    return cutover.main(
        ["b", *argv], machine, ask=lambda _: answer, env=ENV if env is None else env
    )


# A: the machine giving up full mode.


def test_a_that_owns_the_bucket_releases_it_and_stops_its_containers(capsys):
    a = Machine(mode="full (active) since x", owner="host-a", live=True, host="host-a")
    assert run_a(a) == 0
    assert ran(a) == ["make host-release", "make down"]
    out = capsys.readouterr().out
    assert "this machine owns the bucket: it releases it" in out
    assert "Done on A. Now run `make cutover-b` on B." in out


def test_a_in_direct_mode_has_nothing_to_release_and_only_stops_its_containers():
    a = Machine(host="host-a")
    assert run_a(a) == 0
    assert ran(a) == ["make down"]


def test_a_without_docker_changes_nothing_locally(capsys):
    a = Machine(host="host-a", docker=False)
    assert run_a(a) == 0
    assert ran(a) == []
    assert "nothing to stop" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("machine", "says"),
    [
        ({"direct_run": True}, "no `make direct-run` on this machine"),
        ({"mode": "full (active) since x"}, "full mode with no owner"),
        ({"mode": "direct (handing-over) since x"}, "the bucket is not mid-handover"),
        ({"mode": "full (active) since x", "owner": "host-a", "host": "host-a", "docker": False},
         "this machine owns the bucket"),
    ],
)  # fmt: skip
def test_a_refuses_after_its_pre_checks_with_nothing_changed(machine, says, capsys):
    a = Machine(**{"host": "host-a", **machine})
    assert run_a(a) == 2
    assert ran(a) == []
    out = capsys.readouterr().out
    assert f"[FAIL] {says}" in out and "pre-check(s) failed" in out


def test_a_warns_when_the_hourly_rounds_look_stopped_but_goes_on(capsys):
    a = Machine(host="host-a", last_round="2026-10-30T20-00-00Z")
    assert run_a(a) == 0
    assert "[warn] direct mode's hourly rounds ran within 2 hours" in capsys.readouterr().out


@pytest.mark.parametrize("how", ["a dry run", "an answer other than yes"])
def test_a_changes_nothing_without_confirmation(how, capsys):
    a = Machine(mode="full (active) since x", owner="host-a", live=True, host="host-a")
    if how == "a dry run":
        assert run_a(a, argv=["--dry-run"]) == 0
        assert "Dry run: stopping here" in capsys.readouterr().out
    else:
        assert run_a(a, answer="no") == 0
        assert "Not confirmed" in capsys.readouterr().out
    assert ran(a) == []
    assert "make host-release DRY_RUN=1" in a.reads  # the steps were shown


# B: the machine taking full mode.


def test_b_checks_everything_shows_the_dry_run_claims_and_verifies(capsys):
    b = Machine()
    assert run_b(b) == 0
    assert ran(b) == ["make host-claim"]
    out = capsys.readouterr().out
    assert out.index("1. Pre-checks") < out.index("2. Dry run") < out.index("3. Cutover")
    assert "[FAIL]" not in out and "[warn]" not in out
    assert "host-b owns the bucket, lease live" in out
    assert "Done on B" in out


@pytest.mark.parametrize(
    ("change", "says"),
    [
        ({"docker": False}, "Docker running"),
        ({"feed": ""}, "the USGS feed answers"),
        ({"feed": "503"}, "the USGS feed answers"),
        ({"direct_run": True}, "no `make direct-run` on this machine"),
        ({"mode": "full (active) since x", "owner": "host-b", "live": True},
         "not the owner already"),
        ({"mode": "full (active) since x", "owner": "host-a", "live": True},
         "no other host's live lease"),
        ({"mode": "full (active) since x"}, "full mode has an owner"),
    ],
)  # fmt: skip
def test_b_refuses_after_its_pre_checks_with_nothing_changed(change, says, capsys):
    b = Machine(**change)
    assert run_b(b) == 2
    assert ran(b) == []
    assert f"[FAIL] {says}" in capsys.readouterr().out


def test_b_lists_every_setting_missing_from_env(capsys):
    env = {k: v for k, v in ENV.items() if k not in ("R2_BUCKET", "USGS_CONTACT")}
    assert run_b(Machine(), env=env) == 2
    assert "[FAIL] settings in .env: empty: R2_BUCKET, USGS_CONTACT" in capsys.readouterr().out
    assert run_b(Machine(), env={}) == 2
    assert "[FAIL] .env present" in capsys.readouterr().out


def test_b_warns_when_the_machine_id_is_left_to_the_hostname(capsys):
    env = ENV | {"EARTH_HUB_MACHINE_ID": ""}
    assert run_b(Machine(), env=env) == 0
    assert "[warn] EARTH_HUB_MACHINE_ID set: empty: the hostname is used" in capsys.readouterr().out


def test_b_claims_after_a_host_that_stopped_its_lease_expired():
    b = Machine(mode="full (active) since x", owner="host-a", live=False)
    assert run_b(b) == 0
    assert ran(b) == ["make host-claim"]


@pytest.mark.parametrize("how", ["a dry run", "an answer other than yes", "a refused dry run"])
def test_b_changes_nothing_without_confirmation(how):
    b = Machine(claim_dry_run=2 if how == "a refused dry run" else 0)
    argv = ["--dry-run"] if how == "a dry run" else []
    code = run_b(b, answer="n", argv=argv)
    assert code == (2 if how == "a refused dry run" else 0)
    assert ran(b) == []
    assert "make host-claim DRY_RUN=1" in b.reads


def test_b_fails_when_the_claim_left_a_service_down(capsys):
    b = Machine(services=("poller", "consumer", "consumer-backfill", "catchup", "api"))
    assert run_b(b) == 1
    assert "not running: exporter" in capsys.readouterr().out


def test_env_file_reads_values_and_skips_comments(tmp_path):
    path = tmp_path / ".env"
    path.write_text('# a comment\nR2_BUCKET=earth-hub\nUSGS_CONTACT="me@example.org"\nEMPTY=\n')
    assert cutover.env_file(path) == {
        "R2_BUCKET": "earth-hub",
        "USGS_CONTACT": "me@example.org",
        "EMPTY": "",
    }
    assert cutover.env_file(tmp_path / "missing") == {}


# DEF-059: `! make cutover-b` (no terminal) raised EOFError at the prompt.


def no_input(_prompt):
    raise EOFError


def test_no_answer_to_the_prompt_is_a_no_with_nothing_changed(capsys):
    a = Machine(mode="full (active) since x", owner="host-a", live=True, host="host-a")
    b = Machine()
    assert cutover.main(["a"], a, ask=no_input, now=lambda: NOW) == 0
    assert cutover.main(["b"], b, ask=no_input, env=ENV) == 0
    assert capsys.readouterr().out.count("No answer: nothing was changed") == 2
    assert ran(a) == ran(b) == []


@pytest.mark.parametrize("machine", ["a", "b"])
def test_a_real_run_without_a_terminal_refuses_before_anything(machine, monkeypatch, capsys):
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False, raising=False)
    shell = Machine()
    assert cutover.main([machine], shell, env=ENV, now=lambda: NOW) == 2
    assert "run this in a terminal" in capsys.readouterr().out
    assert ran(shell) == [] and not shell.reads  # not even the pre-checks


def test_a_dry_run_needs_no_terminal(monkeypatch):
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False, raising=False)
    shell = Machine()
    assert cutover.main(["b", "--dry-run"], shell, env=ENV) == 0
    assert "make host-claim DRY_RUN=1" in shell.reads


# DEF-060: a database ahead of the bucket, found only after Kafka and PostgreSQL were started.

AHEAD = (
    1,
    '{"level": "INFO", "message": "bucket state refreshed"}\n'
    "rebuild: REFUSED, wiping this database would lose:\n"
    "  1 events at a newer revision than the bucket's: ['aka2026vnrcdp']\n",
)


@pytest.mark.parametrize(
    ("db", "mark", "says"),
    [
        (None, "ok  ", "PostgreSQL not running: compared during the claim"),
        ((0, "rebuild: this database is empty: nothing to lose\n"), "ok  ",
         "this database is empty: nothing to lose"),
        (AHEAD, "warn", "1 events at a newer revision than the bucket's: ['aka2026vnrcdp']. "
         "If the claim must rebuild it, it will refuse"),
        ((2, "rebuild: R2 settings missing\n"), "warn", "could not compare"),
    ],
    ids=["not running", "empty", "ahead", "unreadable"],
)  # fmt: skip
def test_b_says_before_the_prompt_whether_its_database_is_ahead(db, mark, says, capsys):
    b = Machine(db=db)
    assert run_b(b) == 0  # a warning: the handover may still find them the same
    out = capsys.readouterr().out
    line = next(x for x in out.splitlines() if "this host's database is not ahead" in x)
    assert line.startswith(f"  [{mark}]") and says in line
    assert out.index(line) < out.index("2. Dry run")
