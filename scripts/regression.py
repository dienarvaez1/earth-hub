"""Run the whole regression suite, layer by layer, and write reports/.

    uv run python scripts/regression.py            # or: make regression
    uv run python scripts/regression.py --skip performance     # CI (.github/workflows/ci.yml)

Layers run in order and the run stops at the first failing one, so a broken unit test
doesn't spend minutes restarting containers. Each pytest layer writes JUnit XML and
requirement traces; coverage accumulates across layers. The summary goes to
reports/regression-summary.md and the traceability matrix to docs/.

`--skip` leaves out layers by choice: CI skips performance, whose timings shared runners
cannot repeat. Skipped layers are listed as such and do not fail the run; requirements
covered only by their tests show as not run, not as gaps.

CI runs the regression in parallel jobs (docs/technical/ci.md), each a part of it:
    regression.py --skip performance dashboard          the layers that need Docker
    regression.py --only dashboard --shard 1/2          half the dashboard tests (each shard)
    regression.py --skip performance --combine DIR...   the jobs' reports/, merged
A part runs no traceability check, since it sees only some of the tests. `--combine` merges
the parts' results (a layer split in shards counts once, as long as its slowest shard), their
traces and coverage, then checks traceability over all of them. A layer, or a shard of one,
that no part reports was not run, and fails the run like a failing layer.
"""

import argparse
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
COVERAGE_DATA = "coverage.data"  # the run's .coverage, kept in reports/ for --combine

UNIT = "not integration and not e2e and not disruptive and not performance and not dashboard"
LAYERS = [
    ("lint", None),
    ("unit", ["-m", UNIT]),
    ("integration", ["tests/integration", "-m", "integration"]),
    ("api", ["tests/api", "-m", "integration", "--deselect", "tests/api/test_contract.py"]),
    ("contract", ["tests/api/test_contract.py", "-m", "integration"]),
    ("e2e", ["tests/e2e", "-m", "e2e"]),
    ("dashboard", ["tests/dashboard", "-m", "dashboard"]),
    ("disruptive", ["tests/disruptive", "--run-disruptive"]),
    ("performance", ["tests/performance", "--run-performance"]),
]
NAMES = [name for name, _ in LAYERS]


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
    shard: str = ""  # "1/2": this part of the layer's tests (pytest --shard)


def run(cmd: list[str]) -> int:
    print("$ " + " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=REPO).returncode


def run_lint() -> Result:
    start = time.monotonic()
    ok = run(["uv", "run", "ruff", "check", "."]) == 0
    ok = run(["uv", "run", "ruff", "format", "--check", "."]) == 0 and ok
    return Result("lint", ok, time.monotonic() - start)


def run_pytest(layer: str, args: list[str], first: bool, shard: str = "") -> Result:
    name = layer + (f"-{shard.replace('/', 'of')}" if shard else "")  # files of each part
    junit = REPORTS / f"junit-{name}.xml"
    cmd = [
        "uv",
        "run",
        "pytest",
        *args,
        "-q",
        f"--junitxml={junit}",
        f"--trace-out={REPORTS / f'trace-{name}.json'}",
        "--cov=earth_hub",
        "--cov-report=",
        *([f"--shard={shard}"] if shard else []),
    ]
    if not first:
        cmd.append("--cov-append")
    start = time.monotonic()
    code = run(cmd)
    result = Result(layer, code == 0, time.monotonic() - start, shard=shard)
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


def merge(results: list[Result]) -> list[Result]:
    """One result per layer, in the layers' order: a layer's shards add up, and take as long
    as the slowest one (they ran side by side)."""
    merged = []
    for layer in NAMES:
        parts = [r for r in results if r.layer == layer]
        if parts:
            merged.append(
                Result(
                    layer,
                    all(r.ok for r in parts),
                    max(r.seconds for r in parts),
                    tests=sum(r.tests for r in parts),
                    failures=sum(r.failures for r in parts),
                    errors=sum(r.errors for r in parts),
                    skipped=sum(r.skipped for r in parts),
                    failed_tests=tuple(t for r in parts for t in r.failed_tests),
                )
            )
    return merged


def missing_shards(results: list[Result]) -> list[str]:
    """The shards of a split layer that no part reports, as "dashboard (shard 2/2)"."""
    missing = []
    for layer in NAMES:
        shards = {r.shard for r in results if r.layer == layer and r.shard}
        if shards:
            n = int(next(iter(shards)).split("/")[1])
            missing += [
                f"{layer} (shard {k}/{n})" for k in range(1, n + 1) if f"{k}/{n}" not in shards
            ]
    return missing


def combine(dirs: list[Path]) -> list[Result]:
    """The results of the parts whose reports/ are in `dirs`, with their traces and JUnit
    files copied to reports/ and their coverage combined into .coverage."""
    results, coverage = [], []
    for directory in dirs:
        summary_json = directory / "regression-summary.json"
        if summary_json.exists():  # a part that stopped before writing it reports nothing
            for r in json.loads(summary_json.read_text()):
                results.append(Result(**r | {"failed_tests": tuple(r["failed_tests"])}))
        for path in [*directory.glob("trace-*.json"), *directory.glob("junit-*.xml")]:
            shutil.copy(path, REPORTS / path.name)
        if (directory / COVERAGE_DATA).exists():
            coverage.append(str(directory / COVERAGE_DATA))
    if coverage:
        run(["uv", "run", "coverage", "combine", "--keep", "-q", *coverage])
    return results


def coverage_total() -> str | None:
    if not (REPO / ".coverage").exists():
        return None
    run(["uv", "run", "coverage", "xml", "-o", str(REPORTS / "coverage.xml"), "-q"])
    run(["uv", "run", "coverage", "html", "-d", str(REPORTS / "coverage-html"), "-q"])
    root = ET.parse(REPORTS / "coverage.xml").getroot()
    return f"{float(root.get('line-rate', 0)) * 100:.1f}%"


def summary(
    results: list[Result],
    skipped_layers: list[str],
    coverage: str | None,
    trace_ok: bool | None,
    left_out: tuple[str, ...] = (),
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
    for layer in left_out:
        lines.append(f"| {layer} | skipped (`--skip`) | | | | | |")
    total = sum(r.tests for r in results)
    lines += ["", f"Total tests run: {total}. Line coverage across layers: {coverage or 'n/a'}."]
    if trace_ok is not None:
        lines.append(
            "Traceability: every requirement has a passing test."
            if trace_ok
            else "Traceability: **some requirements have no passing test** "
            "(see docs/progress/traceability-matrix.md)."
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


def layer_dirs(names: tuple[str, ...]) -> list[str]:
    """The test directories of these layers, for the traceability check."""
    return [args[0] for name, args in LAYERS if name in names and args]


def run_layers(left_out: tuple[str, ...], only: list[str] | None, shard: str):
    """Run the layers in order, stopping at the first that fails: (results, layers not run)."""
    results: list[Result] = []
    first_pytest = True
    layers = [(n, a) for n, a in LAYERS if n not in left_out and (only is None or n in only)]
    for index, (layer, args) in enumerate(layers):
        print(f"\n=== {layer} ===", flush=True)
        if args is None:
            result = run_lint()
        else:
            result = run_pytest(layer, args, first_pytest, shard)
            first_pytest = False
        results.append(result)
        if not result.ok:
            return results, [name for name, _ in layers[index + 1 :]]
    return results, []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run every test layer and write reports/.")
    parser.add_argument(
        "--skip", nargs="+", default=[], choices=NAMES, metavar="LAYER", help="layers to leave out"
    )
    parser.add_argument(
        "--only", nargs="+", choices=NAMES, metavar="LAYER", help="run only these layers, a part"
    )
    parser.add_argument("--shard", metavar="K/N", help="with --only: pytest's --shard K/N")
    parser.add_argument(
        "--combine", nargs="+", type=Path, metavar="DIR", help="merge the parts' reports/"
    )
    options = parser.parse_args(argv)
    if options.only and (options.skip or options.combine):
        parser.error("--only runs a part: it takes neither --skip nor --combine")
    if options.shard and not options.only:
        parser.error("--shard needs --only")
    left_out = tuple(options.skip)
    shutil.rmtree(REPORTS, ignore_errors=True)
    REPORTS.mkdir()
    (REPO / ".coverage").unlink(missing_ok=True)
    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    if options.combine:
        parts = combine(options.combine)
        results = merge(parts)
        ran = {r.layer for r in results}
        skipped = [n for n in NAMES if n not in left_out and n not in ran] + missing_shards(parts)
    else:
        results, skipped = run_layers(left_out, options.only, options.shard or "")

    coverage = coverage_total()
    if (REPO / ".coverage").exists():
        shutil.copy(REPO / ".coverage", REPORTS / COVERAGE_DATA)
    trace_ok = None
    if not options.only and not skipped and all(r.ok for r in results):
        not_run = [f"--not-run={d}" for d in layer_dirs(left_out)]
        trace_ok = run(["uv", "run", "python", "scripts/traceability.py", *not_run]) == 0

    text = summary(results, skipped, coverage, trace_ok, () if options.only else left_out)
    (REPORTS / "regression-summary.md").write_text(text)
    (REPORTS / "regression-summary.json").write_text(
        json.dumps([r.__dict__ for r in results], indent=2, default=list)
    )
    print("\n" + text)
    return 0 if "**Result: PASSED**" in text else 1


if __name__ == "__main__":
    sys.exit(main())
