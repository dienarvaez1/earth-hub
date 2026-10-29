"""Direct mode on a schedule (.github/workflows/direct.yml, docs/technical/ci.md)."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from earth_hub.config import Settings

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"
WORKFLOW = WORKFLOWS / "direct.yml"
ROUND_CRON = "17 * * * *"  # the dashboard Worker's trigger: every hour, off the top of it
RECONCILE_CRON = "41 5 * * *"  # the same Worker's other trigger: the daily reconcile

sys.path.insert(0, str(REPO / "scripts"))
from dashboard_config import render  # noqa: E402

R2_SETTINGS = {"R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"}


@pytest.fixture(scope="module")
def workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())


def run_lines(job: dict) -> str:
    return "\n".join(step.get("run", "") for step in job["steps"])


@pytest.mark.req("REQ-CI-005")
def test_it_runs_only_when_asked(workflow):
    triggers = workflow[True]  # YAML 1.1 reads the key `on` as true
    # Never branch code, and no schedule: GitHub disables a public repository's scheduled
    # workflows after 60 days without commits, rounds included (DEF-056).
    assert set(triggers) == {"workflow_dispatch"}
    job = triggers["workflow_dispatch"]["inputs"]["job"]
    assert job["options"] == ["round", "reconcile"] and job["default"] == "round"


@pytest.mark.req("REQ-CI-005")
def test_each_job_runs_when_asked_with_the_skip_flag(workflow):
    jobs = workflow["jobs"]
    assert set(jobs) == {"round", "reconcile", "token"}
    assert jobs["round"]["if"] == "inputs.job == 'round'"  # the trigger's dispatch, or by hand
    assert jobs["reconcile"]["if"] == "inputs.job == 'reconcile'"  # the trigger's, or by hand
    for name, command in (
        ("round", "python -m earth_hub.direct once --skip-unless-direct"),
        ("reconcile", "python -m earth_hub.direct.reconcile --skip-unless-direct"),
    ):
        assert command in run_lines(jobs[name])
        assert "set -o pipefail" in run_lines(jobs[name])  # tee must not hide a failure


@pytest.mark.req("REQ-CI-006")
def test_the_worker_triggers_a_round_every_hour_and_the_reconcile_every_day():
    text = render("earth-hub-test")
    lines = [line for line in text.splitlines() if not line.strip().startswith("//")]
    config = json.loads("\n".join(lines))  # the template's comments are whole lines
    assert config["triggers"] == {"crons": [ROUND_CRON, RECONCILE_CRON]}
    assert config["vars"] == {
        "GITHUB_API": "https://api.github.com",
        "GITHUB_REPO": "dienarvaez1/earth-hub",
    }
    assert "GITHUB_TOKEN" not in json.dumps(config)  # a secret, never in the configuration
    source = (REPO / "dashboard" / "src" / "worker.ts").read_text()
    for constant in (
        'WORKFLOW = "direct.yml"',
        'REF = "main"',
        'JOB = "round"',
        'STARTED_BY = "cloudflare-trigger"',
        f'RECONCILE_CRON = "{RECONCILE_CRON}"',
        'RECONCILE = "reconcile"',
    ):
        assert f"const {constant};" in source


@pytest.mark.req("REQ-MODE-015")
def test_the_round_passes_on_who_asked_for_it(workflow):
    started_by = workflow[True]["workflow_dispatch"]["inputs"]["started_by"]
    assert started_by["type"] == "string" and started_by["default"] == ""
    (step,) = [s for s in workflow["jobs"]["round"]["steps"] if "secrets." in str(s.get("env"))]
    assert step["env"]["EARTH_HUB_STARTED_BY"] == "${{ inputs.started_by }}"  # never in run:


@pytest.mark.req("REQ-REC-003")
def test_the_daily_reconcile_also_checks_against_usgs_and_for_a_stopped_schedule(workflow):
    lines = run_lines(workflow["jobs"]["reconcile"])
    assert "python -m earth_hub.usgs_reconcile --skip-unless-direct --max-age-hours 2" in lines
    # Both checks run, and either one failing fails the run (which e-mails).
    assert lines.count("|| status=1") == 2 and "exit $status" in lines


@pytest.mark.req("REQ-CI-005")
def test_one_run_at_a_time_never_cancelled(workflow):
    assert workflow["concurrency"] == {"group": "direct-mode", "cancel-in-progress": False}
    # No longer than the 15-minute lock plus a round; a run still going delays the next.
    assert all(job["timeout-minutes"] <= 20 for job in workflow["jobs"].values())


@pytest.mark.req("REQ-CI-005")
def test_catch_up_intervals_are_shorter_than_the_schedule_and_valid_settings(workflow):
    env = workflow["env"]
    catchup = int(env["CATCHUP_INTERVAL_SECONDS"])
    deleted = int(env["DELETED_CHECK_INTERVAL_SECONDS"])
    # Runs that start a few minutes late, unevenly, must still find catch-up due about every
    # hour and the deleted-events check every day.
    assert re.fullmatch(r"(\d+,)*\d+ \* \* \* \*", ROUND_CRON)
    assert 0 < catchup <= 3600 - 10 * 60
    assert 0 < deleted <= 86400 - 60 * 60
    settings = Settings(
        _env_file=None,
        postgres_db="x",
        postgres_user="x",
        postgres_password="x",
        catchup_interval_seconds=catchup,
        deleted_check_interval_seconds=deleted,
    )
    assert settings.catchup_interval_seconds == catchup


@pytest.mark.req("REQ-CI-005")
def test_r2_credentials_reach_only_the_step_that_uses_them(workflow):
    assert "secrets" not in str(workflow.get("env", {}))
    token = workflow["jobs"]["token"]  # reads one number: no environment, no secrets
    assert "environment" not in token and "secrets." not in str(token)
    for name, job in workflow["jobs"].items():
        if name == "token":
            continue
        assert job["environment"] == "r2", name  # the environment only main can use
        assert "secrets" not in str(job.get("env", {})), name
        with_secrets = [s for s in job["steps"] if "secrets." in str(s.get("env", {}))]
        assert len(with_secrets) == 1, name
        (step,) = with_secrets
        assert R2_SETTINGS <= set(step["env"]) and "python -m earth_hub.direct" in step["run"]
    others = [p.name for p in WORKFLOWS.glob("*.y*ml") if p != WORKFLOW]
    assert others and all("secrets." not in (WORKFLOWS / n).read_text() for n in others)


@pytest.mark.req("REQ-CI-005")
def test_actions_are_pinned_and_the_token_is_read_only(workflow):
    steps = [s for job in workflow["jobs"].values() for s in job["steps"]]
    used = [s["uses"] for s in steps if "uses" in s]
    assert used and all(re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", u) for u in used), used
    assert workflow["permissions"] == {"contents": "read"}
    checkouts = [s for s in steps if s.get("uses", "").startswith("actions/checkout@")]
    assert checkouts and all(s["with"]["persist-credentials"] is False for s in checkouts)
    # The same versions as CI, so one update covers both.
    ci = (WORKFLOWS / "ci.yml").read_text()
    assert all(u in ci for u in used)


def token_check(workflow: dict, days: str) -> subprocess.CompletedProcess:
    """The token job's script, run as GitHub runs it (bash -e) with the trigger's input."""
    (step,) = workflow["jobs"]["token"]["steps"]
    assert step["env"] == {"DAYS_LEFT": "${{ inputs.token_days_left }}"}  # never in the script
    return subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        env={"PATH": os.environ["PATH"], "DAYS_LEFT": days},
        capture_output=True,
        text=True,
    )


@pytest.mark.req("REQ-CI-007")
def test_the_token_job_fails_within_14_days_of_the_expiry(workflow):
    job = workflow["jobs"]["token"]
    assert job["if"] == "inputs.token_days_left != ''"  # only when the trigger passes it
    triggers = workflow[True]["workflow_dispatch"]["inputs"]
    days_left = triggers["token_days_left"]
    assert days_left["type"] == "string" and days_left["default"] == ""
    assert token_check(workflow, "15").returncode == 0
    for days in ("14", "3", "0", "-2"):
        result = token_check(workflow, days)
        assert result.returncode == 1, days
        assert f"expires in {days} days: run make trigger-token" in result.stdout
    for junk in ("", "abc", "1-2", "3; echo hi", "$(id)", "1\n::warning::injected"):
        result = token_check(workflow, junk)
        assert result.returncode == 1 and "not a number of days" in result.stdout, junk
        assert result.stdout.splitlines() == [
            "::error::token_days_left is not a number of days"
        ]  # nothing of the input echoed, nothing run


@pytest.mark.req("REQ-CI-007")
def test_the_worker_and_the_workflow_agree_on_the_reminder():
    source = (REPO / "dashboard" / "src" / "worker.ts").read_text()
    assert "const REMIND_DAYS = 14;" in source and "const CHECK_HOUR_UTC = 8;" in source
    assert "inputs.token_days_left = String(days);" in source
    run = yaml.safe_load(WORKFLOW.read_text())["jobs"]["token"]["steps"][0]["run"]
    assert '[ "$DAYS_LEFT" -le 14 ]' in run
