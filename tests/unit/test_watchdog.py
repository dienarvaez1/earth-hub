"""The watchdog (.github/workflows/watchdog.yml, scripts/watchdog.py): the repository's only
schedule, failing when the dashboard Worker's trigger has started no direct.yml run for 5 hours
(DEF-056)."""

import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO / ".github" / "workflows"
sys.path.insert(0, str(REPO / "scripts"))
import watchdog  # noqa: E402

pytestmark = pytest.mark.req("REQ-CI-009")

NOW = datetime(2026, 10, 29, 18, 47, tzinfo=UTC)


@pytest.fixture(scope="module")
def workflow() -> dict:
    return yaml.safe_load((WORKFLOWS / "watchdog.yml").read_text())


def test_the_trigger_is_alive_within_five_hours_of_its_last_run():
    alive, text = watchdog.check("2026-10-29T18:17:15Z", NOW)
    assert alive and text.endswith("29 minutes ago.")
    assert watchdog.check("2026-10-29T13:47:00Z", NOW)[0]  # 5 hours exactly
    alive, text = watchdog.check("2026-10-29T13:46:59Z", NOW)
    assert not alive and "make trigger-token" in text
    assert not watchdog.check("", NOW)[0]  # never started one


def test_a_stopped_trigger_fails_the_run_with_an_error_line(capsys):
    assert watchdog.main(["watchdog.py", "2020-01-01T00:00:00Z"]) == 1
    assert capsys.readouterr().out.startswith("::error title=The trigger stopped::")
    assert watchdog.main(["watchdog.py"]) == 1


def test_it_holds_the_repositorys_only_schedule(workflow):
    assert set(workflow[True]) == {"schedule", "workflow_dispatch"}  # `on`, read as true
    (cron,) = [entry["cron"] for entry in workflow[True]["schedule"]]
    # Hourly: GitHub skipped the first 3-hourly slot and ran the next 72 minutes late (DEF-057).
    assert cron == "47 * * * *"
    scheduled = [p.name for p in WORKFLOWS.glob("*.y*ml") if "schedule" in yaml.safe_load(
        p.read_text())[True]]  # fmt: skip
    assert scheduled == ["watchdog.yml"]


def test_it_reads_the_newest_dispatched_run_with_a_read_only_token_and_no_secrets(workflow):
    assert workflow["permissions"] == {"contents": "read", "actions": "read"}
    text = (WORKFLOWS / "watchdog.yml").read_text()
    assert "secrets." not in text
    (job,) = workflow["jobs"].values()
    used = [s["uses"] for s in job["steps"] if "uses" in s]
    assert all(re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", u) for u in used)
    assert all(u in (WORKFLOWS / "ci.yml").read_text() for u in used)  # CI's versions
    assert job["steps"][0]["with"]["persist-credentials"] is False
    run = job["steps"][-1]["run"]
    assert "--workflow direct.yml" in run and "--event workflow_dispatch" in run
    assert "--limit 1" in run and 'python3 scripts/watchdog.py "$last"' in run
