"""Run the whole regression suite, layer by layer, and write reports/.

    uv run python scripts/regression.py            # or: make regression

Layers run in order and the run stops at the first failing one, so a broken unit test
doesn't spend minutes restarting containers. Each pytest layer writes JUnit XML and
requirement traces; coverage accumulates across layers. The summary goes to
reports/regression-summary.md and the traceability matrix to docs/.
"""

import json
import os
import platform
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
REPORTS = REPO / "reports"

UNIT = "not integration and not e2e and not disruptive and not performance"
LAYERS = [
    ("lint", None),
    ("unit", ["-m", UNIT]),
    ("integration", ["tests/integration", "-m", "integration"]),
    ("api", ["tests/api", "-m", "integration", "--deselect", "tests/api/test_contract.py"]),
    ("contract", ["tests/api/test_contract.py", "-m", "integration"]),
    ("e2e", ["tests/e2e", "-m", "e2e"]),
    ("disruptive", ["tests/disruptive", "--run-disruptive"]),
    ("performance", ["tests/performance", "--run-performance"]),
]


@dataclass
class Result:
    layer: str
    ok: bool
    seconds: float
    tests: int = 0
    failures: int = 0
    errors: int = 0
    skipped: int = 0
    failed_tests: tuple[str, ...] = ()


def run(cmd: list[str]) -> int:
    print("$ " + " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=REPO).returncode


def run_lint() -> Result:
    start = time.monotonic()
    ok = run(["uv", "run", "ruff", "check", "."]) == 0
    ok = run(["uv", "run", "ruff", "format", "--check", "."]) == 0 and ok
    return Result("lint", ok, time.monotonic() - start)


def run_pytest(layer: str, args: list[str], first: bool) -> Result:
    junit = REPORTS / f"junit-{layer}.xml"
    cmd = [
        "uv",
        "run",
        "pytest",
        *args,
        "-q",
        f"--junitxml={junit}",
        f"--trace-out={REPORTS / f'trace-{layer}.json'}",
        "--cov=earth_hub",
        "--cov-report=",
    ]
    if not first:
        cmd.append("--cov-append")
    start = time.monotonic()
    code = run(cmd)
    result = Result(layer, code == 0, time.monotonic() - start)
    if junit.exists():
        suite = ET.parse(junit).getroot()
        suite = suite.find("testsuite") if suite.tag == "testsuites" else suite
        result.tests = int(suite.get("tests", 0))
        result.failures = int(suite.get("failures", 0))
        result.errors = int(suite.get("errors", 0))
        result.skipped = int(suite.get("skipped", 0))
        result.failed_tests = tuple(
            f"{case.get('classname')}::{case.get('name')}"
            for case in suite.iter("testcase")
            if case.find("failure") is not None or case.find("error") is not None
        )
    return result


def tool_version(*cmd: str) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, cwd=REPO).stdout.strip()
    except FileNotFoundError:
        return "not installed"


def environment() -> dict[str, str]:
    git = lambda *a: tool_version("git", *a)  # noqa: E731
    return {
        "commit": f"{git('rev-parse', '--short', 'HEAD')} "
        f"({git('rev-parse', '--abbrev-ref', 'HEAD')})"
        + (" with uncommitted changes" if git("status", "--porcelain") else ""),
        "run at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "python": platform.python_version(),
        "os": f"{platform.system()} {platform.release()} ({platform.machine()})",
        "docker": tool_version("docker", "--version"),
        "uv": tool_version("uv", "--version"),
    }


def coverage_total() -> str | None:
    if not (REPO / ".coverage").exists():
        return None
    run(["uv", "run", "coverage", "xml", "-o", str(REPORTS / "coverage.xml"), "-q"])
    run(["uv", "run", "coverage", "html", "-d", str(REPORTS / "coverage-html"), "-q"])
    root = ET.parse(REPORTS / "coverage.xml").getroot()
    return f"{float(root.get('line-rate', 0)) * 100:.1f}%"


def summary(
    results: list[Result], skipped_layers: list[str], coverage: str | None, trace_ok: bool | None
) -> str:
    passed = all(r.ok for r in results) and not skipped_layers and trace_ok is not False
    lines = [
        "# Regression summary",
        "",
        f"**Result: {'PASSED' if passed else 'FAILED'}**",
        "",
        "| Environment | |",
        "| --- | --- |",
        *[f"| {k} | {v} |" for k, v in environment().items()],
        "",
        "| Layer | Result | Tests | Failed | Errors | Skipped | Time |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in results:
        counts = ("—",) * 4 if r.layer == "lint" else (r.tests, r.failures, r.errors, r.skipped)
        lines.append(
            f"| {r.layer} | {'pass' if r.ok else '**FAIL**'} | "
            + " | ".join(str(c) for c in counts)
            + f" | {r.seconds:.1f} s |"
        )
    for layer in skipped_layers:
        lines.append(f"| {layer} | not run | | | | | |")
    total = sum(r.tests for r in results)
    lines += ["", f"Total tests run: {total}. Line coverage across layers: {coverage or 'n/a'}."]
    if trace_ok is not None:
        lines.append(
            "Traceability: every requirement has a passing test."
            if trace_ok
            else "Traceability: **some requirements have no passing test** "
            "(see docs/traceability-matrix.md)."
        )
    failed = [t for r in results for t in r.failed_tests]
    if failed:
        lines += ["", "## Failed tests", "", *[f"- `{t}`" for t in failed]]
    if any(not r.ok for r in results if r.layer == "lint"):
        lines += ["", "Lint failed; run `make lint` for details."]
    lines += [
        "",
        "Reports: `reports/junit-<layer>.xml`, `reports/coverage-html/index.html`, "
        "`reports/coverage.xml`.",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    shutil.rmtree(REPORTS, ignore_errors=True)
    REPORTS.mkdir()
    (REPO / ".coverage").unlink(missing_ok=True)
    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    results: list[Result] = []
    first_pytest = True
    for index, (layer, args) in enumerate(LAYERS):
        print(f"\n=== {layer} ===", flush=True)
        if args is None:
            result = run_lint()
        else:
            result = run_pytest(layer, args, first_pytest)
            first_pytest = False
        results.append(result)
        if not result.ok:
            skipped = [name for name, _ in LAYERS[index + 1 :]]
            break
    else:
        skipped = []

    coverage = coverage_total()
    trace_ok = None
    if not skipped and all(r.ok for r in results):
        trace_ok = run(["uv", "run", "python", "scripts/traceability.py"]) == 0

    text = summary(results, skipped, coverage, trace_ok)
    (REPORTS / "regression-summary.md").write_text(text)
    (REPORTS / "regression-summary.json").write_text(
        json.dumps([r.__dict__ for r in results], indent=2, default=list)
    )
    print("\n" + text)
    return 0 if "**Result: PASSED**" in text else 1


if __name__ == "__main__":
    sys.exit(main())
