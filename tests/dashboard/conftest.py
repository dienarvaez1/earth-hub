"""The dashboard layer: the built page, served by `wrangler dev` over a simulated R2 seeded
with a snapshot of the fixed dataset, driven in Chromium with nothing leaving the machine."""

import shutil

import pytest
from support.analytics import Dashboard
from support.dashboard import DASHBOARD, LocalDashboard, build, route_offline
from support.dashboard_data import EVENTS
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

    def open(self, url: str) -> None:
        self.page.goto(url)
        self.page.wait_for_function(
            "window.__earthHub && (window.__earthHub.ready || window.__earthHub.error)",
            timeout=60_000,
        )

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


@pytest.fixture
def dash(browser_page, server):
    browser_page.open(server.url)
    assert browser_page.status()["error"] is None
    yield browser_page
    assert browser_page.errors == [], "errors in the browser console"
    assert browser_page.blocked == [], "requests left the machine"
