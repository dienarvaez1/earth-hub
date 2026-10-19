"""The dashboard's load and filter times at today's size and at 1M rows: three cold runs
each (a fresh browser context, nothing cached), in Chromium against `wrangler dev`.

Results go to reports/dashboard-performance.json. Limits are for a developer machine and
cover the slowest step, compiling DuckDB-WASM's 35 MB engine, which a real visitor's browser
also caches between visits (not measured here: the tests serve the engine locally).

Repeatability is checked on the page's own work (drawing once DuckDB is up, and each filter
change). The engine's compile time is recorded but not checked: it is the browser's work and
follows the machine's load (2.4 to 4.1 s between runs on the same machine).
"""

import json
import statistics
from pathlib import Path

import pytest
from support.dashboard import LocalDashboard, build, route_offline
from support.dashboard_data import synthetic

pytestmark = [pytest.mark.performance, pytest.mark.req("REQ-DASH-008")]

RUNS = 3
SIZES = {"115k": 115_000, "1M": 1_000_000}
# Milliseconds; the first map includes downloading the snapshot and starting DuckDB.
LIMITS = {
    "115k": {"first_map_ms": 8_000, "filter_ms": 300, "bucket_ms": 300},
    "1M": {"first_map_ms": 15_000, "filter_ms": 1_500, "bucket_ms": 1_500},
}
SPREAD = 0.15  # each run within ±15% of the median
REPORT = Path(__file__).resolve().parents[2] / "reports" / "dashboard-performance.json"


def measure(browser, url: str) -> dict:
    context = browser.new_context(viewport={"width": 1280, "height": 900})
    page = context.new_page()
    blocked = route_offline(page)
    try:
        page.goto(url)
        page.wait_for_function(
            "window.__earthHub.ready || window.__earthHub.error", timeout=120_000
        )
        status = page.evaluate("window.__earthHub")
        assert status["error"] is None, status["error"]

        def change(action) -> float:
            before = page.evaluate("window.__earthHub.updates")
            action()
            page.wait_for_function(f"window.__earthHub.updates > {before}", timeout=60_000)
            return page.evaluate("window.__earthHub.timings.last_update_ms")

        timings = status["timings"]
        result = {
            "rows_shown": status["mapFeatures"],
            **{k: round(v) for k, v in timings.items() if not k.startswith("last")},
            "after_engine_ms": round(timings["first_map_ms"] - timings["duckdb_instantiate_ms"]),
            "filter_ms": round(change(lambda: page.select_option("select[name=country]", "Japan"))),
            "bucket_ms": round(
                change(lambda: page.select_option("select[name=time_bucket]", "day"))
            ),
        }
        assert blocked == []
        return result
    finally:
        context.close()


@pytest.fixture(scope="module")
def results(browser, tmp_path_factory):
    build()
    out = {}
    for label, rows in SIZES.items():
        data = tmp_path_factory.mktemp(f"data-{label}")
        snapshot = synthetic(data, rows)
        local = LocalDashboard(tmp_path_factory.mktemp(f"r2-{label}"))
        local.seed(snapshot, {"run_id": "perf", "written_at": "2026-01-01T00:00:00Z", "rows": rows})
        local.start()
        try:
            runs = [measure(browser, local.url) for _ in range(RUNS)]
        finally:
            local.stop()
        out[label] = {
            "rows": rows,
            "snapshot_bytes": snapshot.stat().st_size,
            "runs": runs,
            "median": {k: statistics.median(r[k] for r in runs) for k in runs[0]},
        }
    REPORT.parent.mkdir(exist_ok=True)
    REPORT.write_text(json.dumps(out, indent=2) + "\n")
    return out


@pytest.mark.parametrize("label", SIZES)
def test_load_and_filter_times_are_within_limits(results, label):
    median = results[label]["median"]
    for metric, limit in LIMITS[label].items():
        assert median[metric] <= limit, (label, metric, median[metric], limit)


@pytest.mark.parametrize("label", SIZES)
def test_runs_are_repeatable(results, label):
    runs, median = results[label]["runs"], results[label]["median"]
    for metric in ("after_engine_ms", "filter_ms", "bucket_ms"):
        for run in runs:
            assert abs(run[metric] - median[metric]) <= SPREAD * median[metric], (
                label,
                metric,
                run,
            )
