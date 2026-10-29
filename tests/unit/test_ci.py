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
import release  # noqa: E402
import traceability  # noqa: E402
from support.shard import parse, split  # noqa: E402


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
    assert {"checks", "layers", "dashboard", "regression"} <= workflow["jobs"].keys()


@pytest.mark.req("REQ-CI-001")
def test_the_quick_job_runs_lint_and_unit_tests(workflow):
    run = commands(workflow, "checks")
    assert "make lint" in run and "make test" in run


def regression_args(workflow: dict, job: str) -> list[str]:
    (line,) = [line for line in commands(workflow, job).splitlines() if "regression.py" in line]
    return line.split("regression.py", 1)[1].split()


@pytest.mark.req("REQ-CI-001", "REQ-CI-008")
def test_the_regression_runs_every_layer_but_performance_from_the_example_settings(workflow):
    for job in ("layers", "dashboard"):
        assert "cp .env.example .env" in commands(workflow, job), job
    assert "make up" in commands(workflow, "layers")
    assert regression_args(workflow, "layers") == ["--skip", "performance", "dashboard"]
    assert regression_args(workflow, "dashboard") == [
        "--only",
        "dashboard",
        "--shard",
        '"$SHARD/2"',
    ]
    assert workflow["jobs"]["dashboard"]["strategy"]["matrix"]["shard"] == [1, 2]
    combine = regression_args(workflow, "regression")
    assert combine == ["--skip", "performance", "--combine", "parts/*/"]


@pytest.mark.req("REQ-CI-008")
def test_the_required_check_merges_every_part_and_runs_unless_cancelled(workflow):
    job = workflow["jobs"]["regression"]
    assert job["needs"] == ["layers", "dashboard"]
    assert job["if"] == "${{ !cancelled() }}"  # a skipped required check would count as passed
    (download,) = [s for s in job["steps"] if s.get("uses", "").startswith("actions/download-")]
    assert download["with"] == {"pattern": "part-*", "path": "parts/"}
    for part in ("layers", "dashboard"):
        (upload,) = [
            s for s in steps(workflow, part) if s.get("uses", "").startswith("actions/upload-")
        ]
        assert upload["if"] == "always()" and upload["with"]["name"].startswith("part-")
        assert upload["with"]["path"] == "reports/"


@pytest.mark.req("REQ-CI-008")
def test_the_release_requires_the_checks_ci_has():
    names = {job["name"] for job in yaml.safe_load(WORKFLOW.read_text())["jobs"].values()}
    assert set(release.CHECKS) <= names
    assert "Regression (all layers but performance)" in release.CHECKS  # the merged one


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


def result(layer: str, ok: bool = True, seconds: float = 1.0, shard: str = "") -> regression.Result:
    return regression.Result(layer, ok, seconds, tests=3, shard=shard)


@pytest.mark.req("REQ-CI-008")
def test_shards_split_the_tests_into_parts_that_cover_each_once():
    items = list(range(7))
    parts = [split(items, k, 3) for k in (1, 2, 3)]
    assert [kept for kept, _ in parts] == [[0, 3, 6], [1, 4], [2, 5]]
    assert sorted(i for kept, _ in parts for i in kept) == items
    assert all(sorted(kept + others) == items for kept, others in parts)
    assert parse("2/3") == (2, 3)
    for bad in ("3/2", "0/2", "1", "a/b"):
        with pytest.raises(pytest.UsageError):
            parse(bad)


@pytest.mark.req("REQ-CI-008")
def test_shards_of_a_layer_count_once_as_long_as_the_slowest():
    parts = [result("unit"), result("dashboard", seconds=5, shard="1/2"),
             result("dashboard", ok=False, seconds=9, shard="2/2")]  # fmt: skip
    unit, dashboard = regression.merge(parts)
    assert (unit.layer, unit.tests) == ("unit", 3)
    assert (dashboard.ok, dashboard.tests, dashboard.seconds) == (False, 6, 9)
    assert regression.missing_shards(parts) == []
    assert regression.missing_shards(parts[:2]) == ["dashboard (shard 2/2)"]


def write_part(directory: Path, results: list[regression.Result], traces: dict) -> Path:
    directory.mkdir()
    (directory / "regression-summary.json").write_text(
        json.dumps([r.__dict__ for r in results], default=list)
    )
    for name, trace in traces.items():
        (directory / f"trace-{name}.json").write_text(json.dumps(trace))
    return directory


@pytest.mark.req("REQ-CI-008")
def test_combine_reads_each_parts_results_and_traces(tmp_path, monkeypatch):
    monkeypatch.setattr(regression, "REPORTS", tmp_path / "reports")
    (tmp_path / "reports").mkdir()
    one = write_part(tmp_path / "one", [result("lint"), result("unit")], {"unit": {"R": {}}})
    two = write_part(tmp_path / "two", [result("dashboard", shard="1/2")], {"dashboard-1of2": {}})
    (tmp_path / "three").mkdir()  # a part that stopped before writing its summary
    parts = regression.combine([one, two, tmp_path / "three"])
    assert [(r.layer, r.shard) for r in parts] == [("lint", ""), ("unit", ""), ("dashboard", "1/2")]
    copied = sorted(p.name for p in (tmp_path / "reports").iterdir())
    assert copied == ["trace-dashboard-1of2.json", "trace-unit.json"]
    assert regression.missing_shards(parts) == ["dashboard (shard 2/2)"]


@pytest.mark.req("REQ-CI-008")
def test_a_part_takes_neither_skip_nor_combine_and_shard_needs_a_part():
    for argv in (["--only", "unit", "--skip", "lint"], ["--shard", "1/2"],
                 ["--only", "unit", "--combine", "x"]):  # fmt: skip
        with pytest.raises(SystemExit):
            regression.main(argv)


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
