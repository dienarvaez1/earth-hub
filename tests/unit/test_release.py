"""scripts/release.py, the public release: it asks before it changes anything, stops at a no,
and resumes where it stopped. Every command is faked: nothing reaches git, GitHub or R2."""

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
import release  # noqa: E402

pytestmark = pytest.mark.req("REQ-OPS-007")

NUMBERS = {
    "fix-status-case": 34,
    "merge-survivor-by-key": 36,
    "dashboard-magnitude-band": 35,
    "release-prep": 37,
    "release-public": 38,
}


class Fake(release.Shell):
    """Answers questions with `answer` and records them, and every changing command, in order."""

    def __init__(self, answer):
        super().__init__(ask=self._ask, secret=lambda question: "a-secret")
        self.answer = answer
        self.events: list[tuple[str, object]] = []

    def _ask(self, question: str) -> str:
        self.events.append(("ask", question))
        return self.answer(question)

    def read(self, *cmd, ok=(0,), cwd=None) -> str:
        line = " ".join(cmd)
        if cmd[:3] == ("git", "config", "user.email"):
            return release.NOREPLY
        if cmd[:2] == ("git", "rev-parse"):
            return "new" if cmd[2] == "HEAD" else f"tip-{cmd[2].removeprefix('origin/')}"
        if cmd[:2] == ("gitleaks", "version"):
            return "8.21.0"
        if cmd[:2] == ("git", "for-each-ref") and "refs/remotes/origin" in cmd:
            return "origin/main\norigin/old-feature"
        if cmd[:3] == ("gh", "pr", "view"):
            return f"{NUMBERS[cmd[3]]}\tOPEN\tthe title"
        if cmd[:3] == ("gh", "pr", "list") and "old-feature" in cmd:
            return "12"
        if cmd[:3] == ("gh", "pr", "checks"):
            return "all green"
        if cmd[:3] == ("gh", "repo", "view"):
            return "PRIVATE"
        if cmd[:3] == ("gh", "run", "list"):
            return "99"
        if cmd[:3] == ("gh", "secret", "list"):
            return "R2_ACCOUNT_ID\nR2_BUCKET"
        assert "--force" not in line
        return ""

    def status(self, *cmd, cwd=None, show=False) -> tuple[int, str]:
        return 0, "Token scopes: 'repo', 'workflow'"

    def change(self, *cmd, cwd=None, stdin=None) -> None:
        self.events.append(("change", cmd))


@pytest.fixture(autouse=True)
def offline(tmp_path, monkeypatch):
    """State, temporary files and .env in tmp_path; gitleaks "installed"; no waiting."""
    monkeypatch.setattr(release, "STATE", tmp_path / "state.json")
    monkeypatch.setattr(release, "REPO", tmp_path)
    monkeypatch.setattr(release.tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(release.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(release.time, "sleep", lambda s: None)
    (tmp_path / "earth-hub-gitleaks.json").write_text("[]")
    (tmp_path / ".env").write_text("R2_ACCOUNT_ID=acct\nR2_BUCKET=bucket\n")


def yes(question: str) -> str:
    return "public" if question.strip().startswith("Type") else "y"


def changes(events) -> list[tuple]:
    return [cmd for kind, cmd in events if kind == "change"]


def test_every_step_asks_before_it_changes_anything():
    sh = Fake(yes)
    state = {"done": []}
    for name, step in release.STEPS:
        sh.events = []
        step(sh, state)
        kinds = [kind for kind, _ in sh.events]
        if "change" in kinds:
            assert "ask" in kinds[: kinds.index("change")], f"{name} changes before asking"


def test_answering_no_stops_having_changed_nothing_and_a_new_run_resumes_there():
    no = Fake(lambda question: "n")
    assert release.run(no, {"done": []}) is False
    assert changes(no.events) == []
    assert no.events[0] == ("ask", "  Are both set? [y/N] ")  # settings: preflight passed
    saved = release._load()
    assert saved["done"] == ["preflight"]

    again = Fake(yes)
    assert release.run(again, saved) is True
    assert again.events[0] == ("ask", "  Are both set? [y/N] ")  # not preflight again
    assert release._load()["done"] == [name for name, _ in release.STEPS]


def test_pull_requests_merge_in_order_each_rebased_past_the_one_below():
    sh = Fake(yes)
    assert release.run(sh, {"done": []}) is True
    done = changes(sh.events)
    merged = [cmd[3] for cmd in done if cmd[:3] == ("gh", "pr", "merge")]
    assert merged == ["34", "36", "35", "37", "38"]
    rebases = [cmd[4] for cmd in done if cmd[:2] == ("git", "rebase")]
    # The first lands as it is; each later one drops the commits of the one below.
    assert rebases == [
        "tip-fix-status-case",
        "tip-merge-survivor-by-key",
        "tip-dashboard-magnitude-band",
        "tip-release-prep",
    ]
    assert all("--reset-author" in cmd[-1] for cmd in done if cmd[:2] == ("git", "rebase"))
    # Public comes after every merge but release-public's, and before the environment.
    order = [cmd[:3] for cmd in done]
    assert order.index(("gh", "repo", "edit")) > order.index(("gh", "pr", "merge"))
    env = next(i for i, cmd in enumerate(done) if "environments/r2" in " ".join(cmd))
    last_merge = max(i for i, cmd in enumerate(done) if cmd[:3] == ("gh", "pr", "merge"))
    assert order.index(("gh", "repo", "edit")) < env < last_merge


def test_going_public_needs_the_word_typed_in_full():
    sh = Fake(lambda question: "yes" if question.strip().startswith("Type") else "y")
    state = {"done": [name for name, _ in release.STEPS[:5]]}
    assert release.run(sh, state) is False
    assert not any(cmd[:3] == ("gh", "repo", "edit") for cmd in changes(sh.events))
    assert release._load()["done"][-1] == "cleanup"


def test_secrets_go_through_stdin_never_the_command_line():
    sh = Fake(yes)
    sh.change = lambda *cmd, cwd=None, stdin=None: sh.events.append(("change", (cmd, stdin)))
    release.r2_env(sh, {"done": []})
    sent = [event for kind, event in sh.events if kind == "change"]
    secrets = [(cmd, stdin) for cmd, stdin in sent if cmd[:3] == ("gh", "secret", "set")]
    assert [cmd[3] for cmd, _ in secrets] == release.R2_SECRETS
    assert all(stdin == "a-secret" and "a-secret" not in cmd for cmd, stdin in secrets)
