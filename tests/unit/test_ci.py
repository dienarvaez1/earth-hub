"""The CI workflow (.github/workflows/ci.yml) and the scripts it runs (docs/technical/ci.md)."""

import json
import re
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
WORKFLOW = REPO / ".github" / "workflows" / "ci.yml"
sys.path.insert(0, str(REPO / "scripts"))
import regression  # noqa: E402
import traceability  # noqa: E402


@pytest.fixture(scope="module")
def workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())


def steps(workflow: dict, job: str) -> list[dict]:
    return workflow["jobs"][job]["steps"]


def commands(workflow: dict, job: str) -> str:
    return "\n".join(step.get("run", "") for step in steps(workflow, job))


@pytest.mark.req("REQ-CI-001")
def test_every_pull_request_and_push_to_main_is_checked(workflow):
    triggers = workflow[True]  # YAML 1.1 reads the key `on` as true
    assert "pull_request" in triggers
    assert triggers["push"]["branches"] == ["main"]
    assert {"checks", "regression"} <= workflow["jobs"].keys()


@pytest.mark.req("REQ-CI-001")
def test_the_quick_job_runs_lint_and_unit_tests(workflow):
    run = commands(workflow, "checks")
    assert "make lint" in run and "make test" in run


@pytest.mark.req("REQ-CI-001")
def test_the_regression_job_runs_every_layer_but_performance_from_the_example_settings(workflow):
    run = commands(workflow, "regression")
    assert "cp .env.example .env" in run
    assert "make up" in run
    (regression_line,) = [line for line in run.splitlines() if "scripts/regression.py" in line]
    assert regression_line.split("regression.py", 1)[1].split() == ["--skip", "performance"]


@pytest.mark.req("REQ-CI-001")
def test_the_workflow_uses_no_secrets():
    assert "secrets." not in WORKFLOW.read_text()


@pytest.mark.req("REQ-CI-002")
def test_reports_are_kept_and_summarised_even_when_the_run_fails(workflow):
    job = steps(workflow, "regression")
    (upload,) = [s for s in job if s.get("uses", "").startswith("actions/upload-artifact@")]
    assert upload["if"] == "always()"
    paths = upload["with"]["path"].split()
    assert {"reports/", "docs/progress/traceability-matrix.md"} <= set(paths)
    assert upload["with"]["retention-days"] == 30
    (summary,) = [s for s in job if "GITHUB_STEP_SUMMARY" in s.get("run", "")]
    assert summary["if"] == "always()" and "regression-summary.md" in summary["run"]


@pytest.mark.req("REQ-CI-004")
def test_actions_are_pinned_to_commit_shas_and_the_token_is_read_only(workflow):
    used = [s["uses"] for job in workflow["jobs"].values() for s in job["steps"] if "uses" in s]
    assert used and all(re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", u) for u in used), used
    assert workflow["permissions"] == {"contents": "read"}
    checkouts = [
        s for job in workflow["jobs"].values() for s in job["steps"]
        if s.get("uses", "").startswith("actions/checkout@")
    ]  # fmt: skip
    assert checkouts and all(s["with"]["persist-credentials"] is False for s in checkouts)


def result(layer: str, ok: bool = True) -> regression.Result:
    return regression.Result(layer, ok, 1.0, tests=3)


@pytest.mark.req("REQ-CI-003")
def test_a_layer_left_out_is_reported_and_does_not_fail_the_run():
    text = regression.summary([result("lint"), result("unit")], [], "90.0%", True, ("performance",))
    assert "**Result: PASSED**" in text
    assert "| performance | skipped (`--skip`) |" in text


@pytest.mark.req("REQ-CI-003")
def test_a_failing_layer_still_fails_the_run_with_layers_left_out():
    text = regression.summary(
        [result("lint"), result("unit", ok=False)], ["e2e"], None, None, ("performance",)
    )
    assert "**Result: FAILED**" in text
    assert "| e2e | not run |" in text


@pytest.mark.req("REQ-CI-003")
def test_skip_accepts_only_known_layers():
    with pytest.raises(SystemExit):
        regression.main(["--skip", "performnce"])
    assert regression.layer_dirs(("performance", "lint")) == ["tests/performance"]


@pytest.fixture
def trace_repo(tmp_path, monkeypatch):
    """A repository of three requirements: one tested in the run, one only by a layer left
    out, one by nothing."""
    (tmp_path / "docs").mkdir()
    (tmp_path / "reports").mkdir()
    (tmp_path / "tests" / "slow").mkdir(parents=True)
    (tmp_path / "docs" / "requirements.md").write_text(
        "## Things\n\n| ID | Requirement |\n| --- | --- |\n"
        "| REQ-T-001 | tested |\n| REQ-T-002 | slow only |\n| REQ-T-003 | untested |\n"
    )
    (tmp_path / "tests" / "slow" / "test_slow.py").write_text('mark = "REQ-T-002"\n')
    traces = {"REQ-T-001": {"tests/test_a.py::test_a": "passed"}}
    (tmp_path / "reports" / "trace-unit.json").write_text(json.dumps(traces))
    monkeypatch.setattr(traceability, "REPO", tmp_path)
    monkeypatch.setattr(traceability, "REQUIREMENTS", tmp_path / "docs" / "requirements.md")
    monkeypatch.setattr(traceability, "MATRIX", tmp_path / "docs" / "traceability-matrix.md")
    monkeypatch.setattr(traceability, "TRACES", tmp_path / "reports")
    return tmp_path


@pytest.mark.req("REQ-CI-003")
def test_requirements_tested_only_by_a_layer_left_out_are_not_run_not_gaps(trace_repo, capsys):
    assert traceability.main(["--not-run", "tests/slow"]) == 1  # REQ-T-003 is still a gap
    matrix = (trace_repo / "docs" / "traceability-matrix.md").read_text()
    assert "| **REQ-T-002** slow only | not run (layer skipped) |" in matrix
    assert "| **REQ-T-003** untested | **NOT COVERED** |" in matrix
    assert capsys.readouterr().err.strip() == "not covered or failing: REQ-T-003"


@pytest.mark.req("REQ-CI-003")
def test_without_the_option_every_untested_requirement_is_a_gap(trace_repo, capsys):
    assert traceability.main([]) == 1
    assert "REQ-T-002, REQ-T-003" in capsys.readouterr().err


@pytest.mark.req("REQ-CI-003")
def test_three_part_requirement_ids_are_found_in_a_layer_left_out(trace_repo):
    # DEF-037: the pattern found REQ-T-002 but not REQ-DQ-GEO-002.
    (trace_repo / "tests" / "slow" / "test_rules.py").write_text(
        'pytest.mark.req("REQ-DQ-GEO-002", "REQ-E2E-011")\n'
    )
    assert traceability.marked_in(["tests/slow"]) == {"REQ-T-002", "REQ-DQ-GEO-002", "REQ-E2E-011"}


@pytest.mark.req("REQ-QA-003")
def test_make_test_and_make_coverage_run_exactly_the_regressions_unit_layer():
    # DEF-039: `make coverage` selected "not integration", so it also ran the end-to-end and
    # dashboard tests, which need Docker and a browser.
    makefile = (REPO / "Makefile").read_text()
    for target in ("test", "coverage"):
        (recipe,) = re.findall(rf"^{target}:.*\n\t(.+)$", makefile, re.M)
        assert re.search(r'-m "([^"]+)"', recipe)[1] == regression.UNIT, target
