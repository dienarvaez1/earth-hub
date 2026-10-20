"""The API under load: Locust's 50 users for 60 s over 140,000 events (scripts/load_test.py).

Every request must succeed, and each endpoint's 95th percentile stay within its budget. The
budgets are about 2.5 times the baseline measured on 20 October 2026 (developer machine,
PostgreSQL in Docker): enough room for a busy machine, not for a real regression, such as a
lost index (a place search or a lookup by ID scanning the table). The report is
reports/load-report.html.
"""

import csv
import importlib.util
from pathlib import Path

import pytest

pytestmark = [pytest.mark.performance, pytest.mark.req("REQ-API-014")]

ROOT = Path(__file__).resolve().parents[2]

# p95 in ms: the baseline, and the budget.
P95_BUDGET_MS = {
    "/earthquakes/{event_id}": 150,  # baseline 33
    "/earthquakes?min_magnitude": 250,  # 51
    "/earthquakes/summary/hourly": 200,  # 44
    "/earthquakes/summary/magnitude": 300,  # 95
    "/earthquakes/stats": 400,  # 150
    "/export/earthquakes": 400,  # 150
    "/earthquakes": 600,  # 230: counts all 140,000 rows for `total` on every page
    "/earthquakes?place": 700,  # 260: a substring search, no index can serve it
    "/earthquakes?offset": 2000,  # 730: deep pages, as documented in the limitations
    "/health": 100,  # 25
    "Aggregated": 500,  # 190
}
MIN_REQUESTS_PER_S = 15  # baseline 21.5; less means the API, not the users, set the pace


def load_test():
    spec = importlib.util.spec_from_file_location("load_test", ROOT / "scripts" / "load_test.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_api_serves_50_users_within_its_budgets():
    code = load_test().run(ui=False, users=50, spawn_rate=10, run_time="60s", events=140_000)
    with (ROOT / "reports" / "load_stats.csv").open() as f:
        stats = {row["Name"]: row for row in csv.DictReader(f)}

    total = stats["Aggregated"]
    assert int(total["Failure Count"]) == 0
    assert code == 0  # Locust's own verdict: no request failed
    assert set(P95_BUDGET_MS) <= set(stats), "an endpoint was never requested"
    over = {
        name: int(stats[name]["95%"])
        for name, budget in P95_BUDGET_MS.items()
        if int(stats[name]["95%"]) > budget
    }
    assert over == {}, f"p95 over budget (ms): {over}"
    assert float(total["Requests/s"]) >= MIN_REQUESTS_PER_S
