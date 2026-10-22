"""The public release (Milestone 7.3, docs/progress/plan.md), step by step, asking first.

    uv run python scripts/release.py            # or: make release
    uv run python scripts/release.py --dry-run  # list the steps and what each would do

Run it in a terminal: it asks before every step that changes the repository, GitHub, R2's
credentials or the dashboard, and the visibility switch needs `public` typed in full.
Answering no stops the run; running it again resumes after the last step that finished
(progress in .git/earth-hub-release.json, never committed). Git work happens in a temporary
worktree, so the checkout it runs from is left alone.

The steps:
  1. preflight   gh signed in with the repo and workflow scopes; gitleaks; a clean checkout
  2. settings    your GitHub email privacy and $0 Actions budget (you set them; it asks)
  3. audit       gitleaks over every commit of every branch and pull request
  4. merge       the pull requests below `release-public`, in order, each rebased onto main,
                 green in CI, and squash-merged
  5. cleanup     merged branches deleted from GitHub
  6. public      the repository made public
  7. contact     USGS_CONTACT as a repository variable and in .env
  8. ruleset     main protected: both CI checks required, no force push or deletion
  9. r2-env      an `r2` environment limited to main, holding the workflow's R2 credentials
 10. finish      `release-public` merged (it moves the workflow to that environment and the
                 schedule to every 15 minutes), a round run by hand, then the repository's
                 R2 secrets deleted
 11. deploy      the dashboard without its preview label, still out of search engines
"""

import argparse
import getpass
import json
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SLUG = "dienarvaez1/earth-hub"
URL = f"https://github.com/{SLUG}"
STATE = REPO / ".git" / "earth-hub-release.json"
# The branches to merge, in order; each is built on the one before.
STACK = [
    "fix-status-case",
    "merge-survivor-by-key",
    "dashboard-magnitude-band",
    "release-prep",
]
FINAL = "release-public"  # merged once the repository is public and the environment exists
CHECKS = ["Lint and unit tests", "Regression (all layers but performance)"]
R2_SECRETS = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"]
NOREPLY = "155141291+dienarvaez1@users.noreply.github.com"


class Stop(Exception):
    """The run stops here; running again resumes at this step."""


@dataclass
class Shell:
    """Runs commands. `change` runs one that changes something: skipped in a dry run.
    Questions go through `ask`, so a test can answer them."""

    dry_run: bool = False
    ask: Callable[[str], str] = input
    secret: Callable[[str], str] = getpass.getpass
    log: list[list[str]] = field(default_factory=list)  # changing commands, as run

    def read(self, *cmd: str, ok=(0,), cwd: Path = REPO) -> str:
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
        if result.returncode not in ok:
            raise Stop(f"{' '.join(cmd)} failed:\n{result.stderr.strip() or result.stdout.strip()}")
        return result.stdout.strip()

    def status(self, *cmd: str, cwd: Path = REPO, show: bool = False) -> tuple[int, str]:
        """Exit code and output of a command that changes nothing; `show` streams it instead."""
        if show:
            return subprocess.run(cmd, cwd=cwd).returncode, ""
        result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
        return result.returncode, result.stdout + result.stderr

    def change(self, *cmd: str, cwd: Path = REPO, stdin: str | None = None) -> None:
        self.log.append(list(cmd))
        shown = " ".join(cmd)
        if self.dry_run:
            print(f"    would run: {shown}")
            return
        print(f"    $ {shown}")
        result = subprocess.run(cmd, cwd=cwd, input=stdin, text=True)
        if result.returncode != 0:
            raise Stop(f"{shown} failed (exit {result.returncode})")

    def confirm(self, question: str) -> bool:
        if self.dry_run:
            print(f"    would ask: {question} [y/N]")
            return True
        return self.ask(f"  {question} [y/N] ").strip().lower() in ("y", "yes")

    def require(self, question: str) -> None:
        if not self.confirm(question):
            raise Stop("stopped at your answer; run again to resume here")


# --- the steps ---------------------------------------------------------------------------------


def preflight(sh: Shell, state: dict) -> None:
    code, text = sh.status("gh", "auth", "status")
    if code != 0 or "'repo'" not in text or "'workflow'" not in text:
        raise Stop("gh must be signed in with the repo and workflow scopes: gh auth login")
    if shutil.which("gitleaks") is None:
        print("  gitleaks is not installed (the audit needs it).")
        sh.require("Install it now with Homebrew (brew install gitleaks)?")
        sh.change("brew", "install", "gitleaks")
    if sh.read("git", "status", "--porcelain"):
        raise Stop("the checkout has uncommitted changes; commit or stash them first")
    if sh.read("git", "config", "user.email") != NOREPLY:
        print(f"  This repository's commits would carry your personal email; {NOREPLY} won't.")
        sh.require("Use the noreply address for this repository's commits?")
        sh.change("git", "config", "user.email", NOREPLY)
    sh.read("git", "fetch", "--prune", "origin")
    # The tip of each branch now: rebasing one onto main later drops the commits it shares
    # with the branch below, which has by then been squash-merged.
    if "tips" not in state:
        state["tips"] = {b: sh.read("git", "rev-parse", f"origin/{b}") for b in [*STACK, FINAL]}


def settings(sh: Shell, state: dict) -> None:
    print(
        "  Two GitHub settings only you can change:\n"
        "    https://github.com/settings/emails : turn on 'Keep my email addresses private'\n"
        "      and 'Block command line pushes that expose my email'\n"
        "    https://github.com/settings/billing : a $0 budget for Actions that stops usage\n"
        "      at the limit (it matters while the repository is private)"
    )
    sh.require("Are both set?")


def audit(sh: Shell, state: dict) -> None:
    if shutil.which("gitleaks") is None:
        print("    would scan with gitleaks (installed in preflight)")
        return
    namespace = "refs/release-audit"
    sh.read("git", "fetch", "origin", f"+refs/pull/*/head:{namespace}/pull/*")
    report = Path(tempfile.gettempdir()) / "earth-hub-gitleaks.json"
    version = sh.read("gitleaks", "version")
    mode = ["git"] if _version(version) >= (8, 19) else ["detect", "--source", "."]
    try:
        sh.read(
            "gitleaks", *mode, "--log-opts=--all", "--redact", "--no-banner",
            "--report-format", "json", "--report-path", str(report), ok=(0, 1),
        )  # fmt: skip
    finally:
        refs = sh.read("git", "for-each-ref", "--format=%(refname)", namespace).split()
        for ref in refs:
            sh.read("git", "update-ref", "-d", ref)
    findings = json.loads(report.read_text() or "[]")
    print(f"  gitleaks: {len(findings)} finding(s) over every commit of every branch and PR.")
    for f in findings:
        print(f"    {f['RuleID']:<28} {f['File']}:{f['StartLine']}  commit {f['Commit'][:7]}")
    if findings:
        print(
            "  A real secret must be revoked and replaced (rotated) before going public;\n"
            "  rewriting history does not undo a leak. False positives can be ignored."
        )
        sh.require("Are all of these false positives, or rotated already?")


def merge(sh: Shell, state: dict) -> None:
    done = state.setdefault("merged", [])
    for below, branch in zip([None, *STACK], STACK, strict=False):
        if branch in done:
            continue
        _land(sh, state, branch, below)
        done.append(branch)
        _save(state)


def cleanup(sh: Shell, state: dict) -> None:
    keep = {"main", FINAL}
    merged = []
    for ref in sh.read(
        "git", "for-each-ref", "--format=%(refname:short)", "refs/remotes/origin"
    ).split():
        branch = ref.removeprefix("origin/")
        if branch in keep or branch in ("HEAD", "origin"):
            continue
        prs = sh.read("gh", "pr", "list", "--repo", SLUG, "--head", branch, "--state", "merged",
                      "--json", "number", "-q", ".[].number")  # fmt: skip
        if prs:
            merged.append((branch, prs.split()[0]))
    if not merged:
        print("  No merged branches left on GitHub.")
        return
    for branch, number in merged:
        print(f"    {branch}  (#{number}, merged)")
    if sh.confirm(f"Delete these {len(merged)} merged branches from GitHub?"):
        sh.change("git", "push", "origin", "--delete", *[b for b, _ in merged])
    else:
        print("  Kept; this is optional.")


def public(sh: Shell, state: dict) -> None:
    visibility = sh.read("gh", "repo", "view", SLUG, "--json", "visibility", "-q", ".visibility")
    if visibility == "PUBLIC":
        print("  Already public.")
        return
    print(
        "  This makes the code and its whole history visible to everyone. It can be made\n"
        "  private again, but anything read or copied meanwhile stays out there."
    )
    answer = "public" if sh.dry_run else sh.ask("  Type 'public' to go ahead: ").strip()
    if answer != "public":
        raise Stop("not made public; run again to resume here")
    sh.change("gh", "repo", "edit", SLUG, "--visibility", "public",
              "--accept-visibility-change-consequences")  # fmt: skip


def contact(sh: Shell, state: dict) -> None:
    print(f"  USGS asks clients to say who they are: {URL} goes in the User-Agent.")
    sh.require("Set it as the repository variable USGS_CONTACT (the scheduled workflow's)?")
    sh.change("gh", "variable", "set", "USGS_CONTACT", "--repo", SLUG, "--body", URL)
    env = REPO / ".env"
    if env.exists() and f"USGS_CONTACT={URL}" not in env.read_text():
        if sh.confirm(f"Set USGS_CONTACT={URL} in your .env too?"):
            lines = [
                line
                for line in env.read_text().splitlines()
                if not line.startswith("USGS_CONTACT=")
            ]
            if not sh.dry_run:
                env.write_text("\n".join([*lines, f"USGS_CONTACT={URL}"]) + "\n")


def ruleset(sh: Shell, state: dict) -> None:
    existing = sh.read("gh", "api", f"repos/{SLUG}/rulesets", "-q", ".[].name")
    if "main" in existing.split():
        print("  A ruleset named main exists already.")
        return
    print(f"  main: no force push, no deletion, and these checks required: {', '.join(CHECKS)}.")
    print("  You (an admin) can bypass it in an emergency.")
    sh.require("Create the ruleset?")
    body = {
        "name": "main",
        "target": "branch",
        "enforcement": "active",
        "conditions": {"ref_name": {"include": ["~DEFAULT_BRANCH"], "exclude": []}},
        "rules": [
            {"type": "deletion"},
            {"type": "non_fast_forward"},
            {
                "type": "required_status_checks",
                "parameters": {
                    "strict_required_status_checks_policy": False,
                    "required_status_checks": [{"context": c} for c in CHECKS],
                },
            },
        ],
        "bypass_actors": [{"actor_id": 5, "actor_type": "RepositoryRole", "bypass_mode": "always"}],
    }
    sh.change("gh", "api", "-X", "POST", f"repos/{SLUG}/rulesets", "--input", "-",
              stdin=json.dumps(body))  # fmt: skip


def r2_env(sh: Shell, state: dict) -> None:
    print(
        "  The scheduled workflow's R2 credentials move into an `r2` environment that only\n"
        "  main can use. GitHub never shows a secret again, so enter the values: the\n"
        "  workflow's own token (Object Read & Write on the bucket), not your local one."
    )
    sh.require("Create the environment and enter the values?")
    sh.change("gh", "api", "-X", "PUT", f"repos/{SLUG}/environments/r2", "--input", "-",
              stdin=json.dumps({"deployment_branch_policy": {
                  "protected_branches": False, "custom_branch_policies": True}}))  # fmt: skip
    policies = sh.read("gh", "api", f"repos/{SLUG}/environments/r2/deployment-branch-policies",
                       "-q", ".branch_policies[].name", ok=(0, 1))  # fmt: skip
    if "main" not in policies.split():
        sh.change("gh", "api", "-X", "POST",
                  f"repos/{SLUG}/environments/r2/deployment-branch-policies",
                  "-f", "name=main", "-f", "type=branch")  # fmt: skip
    local = _dotenv(REPO / ".env")
    for name in R2_SECRETS:
        hint = (
            f" (Enter for your .env's {local[name]})"
            if name in ("R2_ACCOUNT_ID", "R2_BUCKET") and local.get(name)
            else ""
        )
        value = "" if sh.dry_run else sh.secret(f"  {name}{hint}: ").strip()
        if not value and hint:
            value = local[name]
        if not value and not sh.dry_run:
            raise Stop(f"{name} is empty; run again to resume here")
        sh.change("gh", "secret", "set", name, "--env", "r2", "--repo", SLUG, stdin=value)


def finish(sh: Shell, state: dict) -> None:
    if FINAL not in state.setdefault("merged", []):
        _land(sh, state, FINAL, STACK[-1])
        state["merged"].append(FINAL)
        _save(state)
    sh.require("Run a direct round by hand now, with the environment's credentials?")
    sh.change(
        "gh", "workflow", "run", "direct.yml", "--repo", SLUG, "--ref", "main", "-f", "job=round"
    )
    if not sh.dry_run:
        time.sleep(10)
        run = sh.read("gh", "run", "list", "--repo", SLUG, "--workflow", "direct.yml",
                      "--event", "workflow_dispatch", "-L", "1", "--json", "databaseId",
                      "-q", ".[0].databaseId")  # fmt: skip
        if sh.status("gh", "run", "watch", run, "--repo", SLUG, "--exit-status", show=True)[0]:
            raise Stop(f"the round failed: gh run view {run} --repo {SLUG} --log-failed")
    repo_secrets = sh.read(
        "gh", "secret", "list", "--repo", SLUG, "--json", "name", "-q", ".[].name"
    )
    left = [n for n in R2_SECRETS if n in repo_secrets.split()]
    if left and sh.confirm(f"The round worked. Delete the repository's own {', '.join(left)}?"):
        for name in left:
            sh.change("gh", "secret", "delete", name, "--repo", SLUG)


def deploy(sh: Shell, state: dict) -> None:
    sh.read("git", "fetch", "origin")
    print("  The dashboard, built from main: no preview label, still out of search engines.")
    sh.require("Deploy it to Cloudflare now (make dashboard-deploy)?")
    with _worktree(sh, "origin/main") as tree:
        sh.change("cp", str(REPO / ".env"), str(tree / ".env"))
        sh.change("make", "dashboard-install", cwd=tree)
        sh.change("make", "dashboard-deploy", cwd=tree)


STEPS: list[tuple[str, Callable[[Shell, dict], None]]] = [
    ("preflight", preflight),
    ("settings", settings),
    ("audit", audit),
    ("merge", merge),
    ("cleanup", cleanup),
    ("public", public),
    ("contact", contact),
    ("ruleset", ruleset),
    ("r2-env", r2_env),
    ("finish", finish),
    ("deploy", deploy),
]


# --- helpers -----------------------------------------------------------------------------------


def _pr(sh: Shell, branch: str) -> tuple[str, str]:
    out = sh.read("gh", "pr", "view", branch, "--repo", SLUG, "--json", "number,title,state",
                  "-q", '"\\(.number)\\t\\(.state)\\t\\(.title)"')  # fmt: skip
    number, pr_state, title = out.split("\t", 2)
    if pr_state != "OPEN":
        raise Stop(f"#{number} ({branch}) is {pr_state.lower()}, not open")
    return number, title


def _land(sh: Shell, state: dict, branch: str, below: str | None) -> None:
    """Rebases a pull request onto main (after the one below it merged), waits for CI,
    and squash-merges it: one question, asked before anything is pushed."""
    number, title = _pr(sh, branch)
    print(f"  #{number}  {title}")
    sh.require(f"Rebase #{number} onto main if needed, wait for CI, then squash-merge it?")
    if below is not None:
        _rebase(sh, state, branch, old_base=state["tips"][below])
    _wait_for_checks(sh, number)
    sh.change("gh", "pr", "merge", number, "--squash", "--repo", SLUG)
    sh.change("git", "push", "origin", "--delete", branch)
    sh.read("git", "fetch", "--prune", "origin")


def _rebase(sh: Shell, state: dict, branch: str, old_base: str) -> None:
    """Replays the branch's own commits, those after `old_base` (the old tip of the branch
    below, now squash-merged), onto main. Recorded, so running again does not repeat it."""
    rebased = state.setdefault("rebased", {})
    sh.read("git", "fetch", "origin")
    current = sh.read("git", "rev-parse", f"origin/{branch}")
    if rebased.get(branch) == current:
        return
    with _worktree(sh, current) as tree:
        # Authored again with the noreply address: GitHub refuses pushes of commits that
        # carry a private email once "Block command line pushes" is on.
        sh.change("git", "rebase", "--onto", "origin/main", old_base,
                  "--exec", "git commit --amend --no-edit --reset-author", cwd=tree)  # fmt: skip
        new = current if sh.dry_run else sh.read("git", "rev-parse", "HEAD", cwd=tree)
        sh.change("git", "push", f"--force-with-lease={branch}:{current}", "origin",
                  f"HEAD:refs/heads/{branch}", cwd=tree)  # fmt: skip
    rebased[branch] = new
    _save(state)


def _wait_for_checks(sh: Shell, number: str) -> None:
    if sh.dry_run:
        print(f"    would wait for CI on #{number}")
        return
    print(f"  Waiting for CI on #{number} (the regression takes about 15 minutes)...")
    for _ in range(30):  # the checks take a moment to appear after a push
        if sh.read("gh", "pr", "checks", number, "--repo", SLUG, ok=(0, 1, 8)):
            break
        time.sleep(10)
    if sh.status(
        "gh",
        "pr",
        "checks",
        number,
        "--repo",
        SLUG,
        "--watch",
        "--fail-fast",
        "--interval",
        "30",
        show=True,
    )[0]:
        raise Stop(f"CI failed on #{number}: fix it, then run again")


class _worktree:
    """A temporary detached worktree at `ref`, removed afterwards."""

    def __init__(self, sh: Shell, ref: str):
        self.sh, self.ref = sh, ref
        self.path = Path(tempfile.mkdtemp(prefix="earth-hub-release-")) / "tree"

    def __enter__(self) -> Path:
        if not self.sh.dry_run:
            self.sh.read("git", "worktree", "add", "--detach", str(self.path), self.ref)
        return self.path

    def __exit__(self, *exc) -> None:
        if not self.sh.dry_run:
            self.sh.status("git", "worktree", "remove", "--force", str(self.path))


def _version(text: str) -> tuple[int, ...]:
    digits = text.strip().lstrip("v").split("-")[0].split(".")
    return tuple(int(d) for d in digits if d.isdigit())


def _dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    pairs = (line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)
    return {k.strip(): v.strip() for k, v in pairs if not k.lstrip().startswith("#")}


def _load() -> dict:
    return json.loads(STATE.read_text()) if STATE.exists() else {"done": []}


def _save(state: dict) -> None:
    if not state.get("dry_run"):
        STATE.write_text(json.dumps(state, indent=1))


def run(sh: Shell, state: dict) -> bool:
    """Runs the steps not done yet, in order; False if one stopped."""
    for number, (name, step) in enumerate(STEPS, 1):
        if name in state["done"]:
            print(f"{number:>2}. {name}: done")
            continue
        print(f"{number:>2}. {name}")
        try:
            step(sh, state)
        except Stop as stop:
            print(f"\nStopped at {name}: {stop}")
            _save(state)
            return False
        state["done"].append(name)
        _save(state)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="The public release, step by step.")
    parser.add_argument("--dry-run", action="store_true", help="show the steps, change nothing")
    args = parser.parse_args()
    sh = Shell(dry_run=args.dry_run)
    if not args.dry_run and not sys.stdin.isatty():
        print("Run this in a terminal: it asks before every step.")
        return 2
    finished = run(sh, {"done": [], "dry_run": True} if args.dry_run else _load())
    if finished and not args.dry_run:
        print(
            "\nReleased. Remember: GitHub switches off a public repository's scheduled\n"
            "workflows after 60 days with no activity; a push re-enables them."
        )
    return 0 if finished else 1


if __name__ == "__main__":
    sys.exit(main())
