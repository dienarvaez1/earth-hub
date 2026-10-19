"""The dashboard page in Chromium: DuckDB-WASM's answers against DuckDB's in Python on the
same snapshot, the form, the drawing, phone width and the empty state."""

from datetime import datetime, timedelta

import pytest

pytestmark = pytest.mark.req("REQ-DASH-007")

REPORTS = [
    "summary",
    "over_time",
    "magnitude_bins",
    "top_countries",
    "top_regions",
    "significant",
    "map_points",
    "countries",
    "regions",
    "event_types",
]
EPOCH = datetime(1970, 1, 1)


def as_page(value):
    """What the page has for a Python value: timestamps as epoch milliseconds (UTC)."""
    if isinstance(value, datetime):
        return (value - EPOCH) // timedelta(milliseconds=1)
    return value


@pytest.mark.req("REQ-DASH-001", "REQ-DASH-002", "REQ-DASH-003", "REQ-DASH-004", "REQ-DASH-005")
@pytest.mark.parametrize(
    "filters",
    [
        {},
        {"include_deleted": True, "time_bucket": "day"},
        {"time_bucket": "week", "top_n": 2},
        {"start_time": "2024-02-29 00:00:00", "end_time": "2024-03-01 00:00:00"},
        {"min_magnitude": 2.5, "max_magnitude": 5.0},
        {"country": "United States", "region": "Alaska"},
        {"country": "Unknown"},
        {"event_type": "quarry blast", "significant_magnitude": 0.5},
        {"min_magnitude": 9.0},
    ],
    ids=lambda f: ",".join(f) or "defaults",
)
def test_duckdb_wasm_gives_the_same_answers_as_the_tests(dash, expected, filters):
    got = dash.page.evaluate("(f) => window.__earthHub.queryAll(f)", filters)
    python = {
        k: datetime.fromisoformat(v) if k.endswith("_time") and v else v for k, v in filters.items()
    }
    expected.set_filter(**python)
    for name in REPORTS:
        want = [{k: as_page(v) for k, v in row.items()} for row in expected.query(name)]
        assert got[name] == want, name


def test_the_page_shows_the_snapshot(dash):
    page = dash.page
    assert page.text_content("#as-of") == "8 events, exported 2024-04-02 12:00 UTC"
    cards = {
        card.get_attribute("data-testid"): card.inner_text()
        for card in page.query_selector_all("#summary .card")
    }
    assert "7\n6 with a magnitude" in cards["summary-events"]  # deleted left out
    assert "M 5.2\nplace 6" in cards["summary-max"]
    assert "2024-02-28\nto 2024-04-02" in cards["summary-range"]
    assert "4" in cards["summary-countries"]
    assert dash.status()["mapFeatures"] == 7
    rows = page.query_selector_all("#significant tbody tr")
    assert [r.query_selector("td").inner_text() for r in rows] == [
        "2024-03-10 23:00",
        "2024-02-29 12:00",
    ]
    assert rows[0].query_selector("a").get_attribute("href").startswith("https://")


def test_the_charts_are_drawn_from_the_results(dash):
    page = dash.page
    status = dash.after_update(lambda: page.select_option("select[name=time_bucket]", "day"))
    assert status["error"] is None
    assert len(page.query_selector_all('#chart-time g[aria-label="rect"] rect')) == 35
    assert len(page.query_selector_all('#chart-magnitude g[aria-label="rect"] rect')) == 12
    countries = page.query_selector_all('#chart-countries g[aria-label="bar"] rect')
    assert len(countries) == 4
    for figure in ("time", "magnitude", "depth", "countries", "regions"):
        svg = page.query_selector(f"#chart-{figure} svg")
        assert svg.get_attribute("role") == "img" and svg.get_attribute("aria-label")


@pytest.mark.req("REQ-DASH-001")
def test_the_form_filters_the_page(dash):
    page = dash.page

    def events():
        return dash.status()["summary"]["events"]

    dash.after_update(lambda: page.select_option("select[name=country]", "Mexico"))
    assert events() == 1 and dash.status()["mapFeatures"] == 1
    # Regions follow the country.
    regions = page.eval_on_selector_all("select[name=region] option", "os => os.map(o => o.value)")
    assert regions == ["", "Baja California"]

    page.click("button[type=reset]")
    page.wait_for_function("window.__earthHub.summary.events === 7")

    dash.after_update(lambda: page.fill("input[name=min_magnitude]", "2.5"))
    assert events() == 3
    dash.after_update(lambda: page.fill("input[name=min_magnitude]", ""))
    dash.after_update(lambda: page.check("input[name=include_deleted]"))
    assert events() == 8
    dash.after_update(lambda: page.uncheck("input[name=include_deleted]"))
    # "To" includes its day: 29 February only.
    page.fill("input[name=start]", "2024-02-29")
    dash.after_update(lambda: page.fill("input[name=end]", "2024-02-29"))
    assert events() == 2


def test_no_matching_events_says_so(dash):
    page = dash.page
    dash.after_update(lambda: page.fill("input[name=min_magnitude]", "9"))
    assert dash.status()["summary"]["events"] == 0
    assert dash.status()["mapFeatures"] == 0
    for figure in ("time", "magnitude", "countries"):
        assert page.inner_text(f"#chart-{figure}") == "No events match the filters."
    assert page.inner_text("#significant caption") == "No significant events match the filters."


def test_every_control_has_a_label(dash):
    unlabeled = dash.page.evaluate(
        """() => [...document.querySelectorAll('input, select')]
              .filter(el => !el.closest('label') && !el.labels?.length)
              .map(el => el.name)"""
    )
    assert unlabeled == []


@pytest.mark.parametrize("width", [390, 768])
def test_it_fits_a_phone_and_a_tablet(browser_page, server, width):
    browser_page.page.set_viewport_size({"width": width, "height": 844})
    browser_page.open(server.url)
    page = browser_page.page
    assert page.evaluate("document.documentElement.scrollWidth") <= width
    box = page.query_selector("#map").bounding_box()
    assert box["width"] <= width - 32 + 1 and box["height"] >= 300
    assert browser_page.errors == [] and browser_page.blocked == []


def test_without_a_snapshot_the_page_says_so(browser_page, empty_server):
    browser_page.open(empty_server.url)
    assert browser_page.status()["error"] == "No data has been exported yet."
    assert browser_page.page.text_content("#as-of") == "No data has been exported yet."
    assert browser_page.blocked == []
