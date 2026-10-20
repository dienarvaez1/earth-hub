"""Direct mode on a schedule (.github/workflows/direct.yml, docs/technical/ci.md)."""

import re
from pathlib import Path

import pytest
import yaml

from earth_hub.config import Settings

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"
WORKFLOW = WORKFLOWS / "direct.yml"
ROUND_CRON = "17 * * * *"
RECONCILE_CRON = "41 5 * * *"
R2_SETTINGS = {"R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"}


@pytest.fixture(scope="module")
def workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())


def run_lines(job: dict) -> str:
    return "\n".join(step.get("run", "") for step in job["steps"])


@pytest.mark.req("REQ-CI-005")
def test_it_runs_only_on_its_schedule_or_by_hand(workflow):
    triggers = workflow[True]  # YAML 1.1 reads the key `on` as true
    assert set(triggers) == {"schedule", "workflow_dispatch"}  # never branch code
    assert [entry["cron"] for entry in triggers["schedule"]] == [ROUND_CRON, RECONCILE_CRON]
    assert triggers["workflow_dispatch"]["inputs"]["job"]["options"] == ["round", "reconcile"]


@pytest.mark.req("REQ-CI-005")
def test_each_job_runs_on_its_own_schedule_with_the_skip_flag(workflow):
    jobs = workflow["jobs"]
    assert set(jobs) == {"round", "reconcile"}
    for name, cron, command in (
        ("round", ROUND_CRON, "python -m earth_hub.direct once --skip-unless-direct"),
        ("reconcile", RECONCILE_CRON, "python -m earth_hub.direct.reconcile --skip-unless-direct"),
    ):
        job = jobs[name]
        assert job["if"] == f"github.event.schedule == '{cron}' || inputs.job == '{name}'"
        assert command in run_lines(job)
        assert "set -o pipefail" in run_lines(job)  # tee must not hide a failure


@pytest.mark.req("REQ-CI-005")
def test_one_run_at_a_time_never_cancelled(workflow):
    assert workflow["concurrency"] == {"group": "direct-mode", "cancel-in-progress": False}
    # Shorter than the 15-minute lock and the hour between runs.
    assert all(job["timeout-minutes"] <= 20 for job in workflow["jobs"].values())


@pytest.mark.req("REQ-CI-005")
def test_catch_up_intervals_are_shorter_than_the_schedule_and_valid_settings(workflow):
    env = workflow["env"]
    catchup = int(env["CATCHUP_INTERVAL_SECONDS"])
    deleted = int(env["DELETED_CHECK_INTERVAL_SECONDS"])
    # Hourly runs that start a few minutes late, unevenly, must still find catch-up due
    # every hour and the deleted-events check every day.
    assert re.fullmatch(r"\d+ \* \* \* \*", ROUND_CRON)
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
    for name, job in workflow["jobs"].items():
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
