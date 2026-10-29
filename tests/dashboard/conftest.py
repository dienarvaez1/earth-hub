"""The dashboard layer: the built page, served by `wrangler dev` over a simulated R2 seeded
with a snapshot of the fixed dataset, driven in Chromium with nothing leaving the machine."""

import shutil
from datetime import datetime

import pytest
from support.analytics import Dashboard
from support.dashboard import DASHBOARD, LocalDashboard, build, route_offline
from support.dashboard_data import EVENTS, synthetic
from support.dashboard_data import build as build_snapshot

META = {
    "table": "earthquakes",
    "run_id": "2024-04-02T12-00-00Z-test",
    "written_at": "2024-04-02T12:00:00Z",
    "rows": len(EVENTS),
}


def pytest_collection_modifyitems(items):
    for item in items:
        if "tests/dashboard/" in str(item.path):
            item.add_marker(pytest.mark.dashboard)


@pytest.fixture(scope="session")
def built():
    if not (DASHBOARD / "node_modules").exists() or shutil.which("npx") is None:
        pytest.fail("the dashboard is not installed: run make dashboard-install")
    build()


@pytest.fixture(scope="session")
def snapshot(tmp_path_factory):
    return build_snapshot(tmp_path_factory.mktemp("snapshot"), EVENTS)


@pytest.fixture(scope="session")
def server(built, snapshot, tmp_path_factory):
    local = LocalDashboard(tmp_path_factory.mktemp("r2"))
    local.seed(snapshot, META | {"bytes": snapshot.stat().st_size})
    local.start()
    yield local
    local.stop()


class LargeServer:
    def __init__(self, server, events: int, strong: int, latest: list, largest: list):
        self.server, self.events, self.strong = server, events, strong
        self.latest, self.largest = latest, largest  # the table's two orders, from Python


@pytest.fixture(scope="session")
def large_server(built, tmp_path_factory):
    """6,000 synthetic events: more than the map shows unclustered."""
    snapshot = synthetic(tmp_path_factory.mktemp("large"), 6_000)
    dashboard = Dashboard(snapshot)
    dashboard.set_filter(event_type="earthquake")  # the page's default type
    points = dashboard.query("map_points")
    latest, largest = dashboard.query("significant"), dashboard.query("largest")
    dashboard.close()
    strong = sum(1 for p in points if p["magnitude"] is not None and p["magnitude"] >= 6)
    local = LocalDashboard(tmp_path_factory.mktemp("r2-large"))
    local.seed(snapshot, META | {"rows": 6_000})
    local.start()
    yield LargeServer(local, len(points), strong, latest, largest)
    local.stop()


class GridServer:
    def __init__(self, server, events: list, cells: list, strong: list):
        self.server = server
        self.events, self.cells, self.strong = events, cells, strong  # from Python, as the page


@pytest.fixture(scope="session")
def grid_server(built, tmp_path_factory):
    """30,000 synthetic events: more than the map is given one by one (map_grid.sql)."""
    snapshot = synthetic(tmp_path_factory.mktemp("grid"), 30_000)
    dashboard = Dashboard(snapshot)
    dashboard.set_filter(event_type="earthquake")  # the page's default type
    events, cells = dashboard.query("map_points"), dashboard.query("map_grid")
    strong = dashboard.query("map_strong")
    dashboard.close()
    local = LocalDashboard(tmp_path_factory.mktemp("r2-grid"))
    local.seed(snapshot, META | {"rows": 30_000})
    local.start()
    yield GridServer(local, events, cells, strong)
    local.stop()


@pytest.fixture(scope="session")
def empty_server(built, tmp_path_factory):
    """No snapshot in the bucket: before the first export."""
    local = LocalDashboard(tmp_path_factory.mktemp("r2-empty"))
    local.start()
    yield local
    local.stop()


@pytest.fixture
def expected(snapshot):
    """DuckDB in Python over the same file: the answers the page must show."""
    dashboard = Dashboard(snapshot)
    yield dashboard
    dashboard.close()


class Browser:
    def __init__(self, page):
        self.page = page
        self.errors: list[str] = []
        page.on("console", lambda m: m.type == "error" and self.errors.append(m.text))
        page.on("pageerror", lambda e: self.errors.append(str(e)))
        self.blocked = route_offline(page)

    def open(self, url: str, *, now: str | None = None, period: str | None = None) -> None:
        """Open the page; `now` fixes its clock (UTC, ISO 8601), `period` picks a preset."""
        if now is not None:
            self.page.clock.set_fixed_time(datetime.fromisoformat(now))
        self.page.goto(url)
        self.page.wait_for_function(
            "window.__earthHub && (window.__earthHub.ready || window.__earthHub.error)",
            timeout=60_000,
        )
        if period is not None and self.status()["error"] is None:
            self.after_update(lambda: self.page.select_option("select[name=period]", period))

    def status(self) -> dict:
        return self.page.evaluate("window.__earthHub")

    def after_update(self, action) -> dict:
        """Run `action` (a form change) and wait for the page to redraw."""
        before = self.status()["updates"]
        action()
        self.page.wait_for_function(f"window.__earthHub.updates > {before}", timeout=30_000)
        return self.status()


@pytest.fixture
def browser_page(page):
    return Browser(page)


# The fixed dataset's last day, and a period that holds all of it.
NOW = "2024-04-02T12:00:00+00:00"
EVERYTHING = "last_3_years"


@pytest.fixture
def dash(browser_page, server):
    browser_page.open(server.url, now=NOW, period=EVERYTHING)
    assert browser_page.status()["error"] is None
    yield browser_page
    assert browser_page.errors == [], "errors in the browser console"
    assert browser_page.blocked == [], "requests left the machine"
