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
    "largest",
    "map_points",
    "countries",
    "regions",
    "event_types",
]
EPOCH = datetime(1970, 1, 1)
LARGE_NOW = "2026-01-01T12:00:00+00:00"  # the synthetic events cover 2025


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
    # Earthquakes only by default: the deleted event and the quarry blast (8) are left out.
    assert "6\n5 with a magnitude" in cards["summary-events"]
    assert "M 5.2\nplace 6" in cards["summary-max"]
    assert "2024-02-28\nto 2024-03-10" in cards["summary-range"]
    assert "4" in cards["summary-countries"]
    assert dash.status()["mapFeatures"] == 6
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
    assert (
        len(page.query_selector_all('#chart-time g[aria-label="rect"] rect')) == 12
    )  # to 10 March
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

    page.click("button[type=reset]")  # back to Today, 2 April: only event 8, a quarry blast
    page.wait_for_function("window.__earthHub.summary.events === 0")
    dash.after_update(lambda: page.select_option("select[name=period]", "last_3_years"))

    dash.after_update(lambda: page.fill("input[name=min_magnitude]", "2.5"))
    assert events() == 3
    dash.after_update(lambda: page.fill("input[name=min_magnitude]", ""))
    dash.after_update(lambda: page.check("input[name=include_deleted]"))
    assert events() == 7
    dash.after_update(lambda: page.uncheck("input[name=include_deleted]"))
    # Custom days: "To" includes its day, so 29 February only.
    dash.after_update(lambda: page.select_option("select[name=period]", "custom"))
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


@pytest.mark.req("REQ-DASH-009")
def test_a_small_result_set_shows_every_event_as_its_own_point(dash):
    status = dash.status()
    assert status["mapClustered"] is False
    assert status["mapSinglePoints"] == status["mapFeatures"] == 6


@pytest.mark.req("REQ-DASH-009")
def test_a_large_result_set_clusters_all_but_the_strong_events(browser_page, large_server):
    browser_page.open(large_server.server.url, now=LARGE_NOW, period="last_3_years")
    status = browser_page.status()
    assert status["error"] is None
    assert status["mapClustered"] is True
    assert status["mapFeatures"] == large_server.events > 5000
    # Every M 6+ event stays a single point, so no cluster can hide one.
    assert status["mapSinglePoints"] == large_server.strong > 0
    assert browser_page.errors == [] and browser_page.blocked == []


@pytest.mark.req("REQ-DASH-005")
def test_the_table_lists_the_latest_or_the_largest(browser_page, large_server):
    """On 6,000 events, where the two orders differ."""
    browser_page.open(large_server.server.url, now=LARGE_NOW, period="last_3_years")
    page = browser_page.page

    def shown():
        return page.eval_on_selector_all(
            "#significant tbody tr",
            "rows => rows.map(r => [r.cells[0].textContent, Number(r.cells[1].textContent)])",
        )

    def table(rows):
        return [[r["event_ts"].isoformat(sep=" ")[:16], r["magnitude"]] for r in rows]

    assert shown() == table(large_server.latest)
    browser_page.after_update(lambda: page.check("input[name=order][value=largest]"))
    assert shown() == table(large_server.largest)
    assert shown() != table(large_server.latest)
    browser_page.after_update(lambda: page.check("input[name=order][value=latest]"))
    assert shown() == table(large_server.latest)
    assert browser_page.errors == [] and browser_page.blocked == []


# At Sunday 10 March 2024, 12:00 UTC, the fixed dataset's non-deleted events: 1 (28 Feb,
# 23:59:59.999), 2 and 3 (29 Feb), 4 (1 Mar), 5 (Mon 4 Mar), 6 (Sun 10 Mar, 23:00); 7 is
# deleted, 8 (2 April) comes after "today".
@pytest.mark.req("REQ-DASH-010")
@pytest.mark.parametrize(
    ("period", "events", "first", "bucket"),
    [
        ("today", 1, "2024-03-10T23:00:00", "day"),  # the whole UTC day, 23:00 included
        ("this_week", 2, "2024-03-04T06:00:00", "day"),  # from Monday
        ("this_month", 3, "2024-03-01T00:00:00", "day"),
        ("last_3_months", 6, "2024-02-28T23:59:59.999", "week"),  # from 10 December
        ("this_year", 6, "2024-02-28T23:59:59.999", "week"),
        ("last_3_years", 6, "2024-02-28T23:59:59.999", "month"),
    ],
)
def test_each_preset_selects_its_days(browser_page, server, period, events, first, bucket):
    browser_page.open(server.url, now="2024-03-10T12:00:00+00:00")
    page = browser_page.page
    if period != "today":
        browser_page.after_update(lambda: page.select_option("select[name=period]", period))
    summary = browser_page.status()["summary"]
    assert summary["events"] == events
    assert summary["first_event"] == as_page(datetime.fromisoformat(first))
    assert page.input_value("select[name=time_bucket]") == bucket
    assert page.is_hidden("input[name=start]") and page.is_hidden("input[name=end]")
    assert browser_page.errors == [] and browser_page.blocked == []


@pytest.mark.req("REQ-DASH-010")
def test_today_is_the_default(browser_page, server):
    browser_page.open(server.url, now="2024-02-29T08:00:00+00:00")
    page = browser_page.page
    assert page.input_value("select[name=period]") == "today"
    assert browser_page.status()["summary"]["events"] == 2  # events 2 and 3
    assert page.input_value("select[name=time_bucket]") == "day"


@pytest.mark.req("REQ-DASH-010")
def test_custom_opens_the_calendar_from_the_last_preset(browser_page, server):
    browser_page.open(server.url, now="2024-05-31T12:00:00+00:00")
    page = browser_page.page
    browser_page.after_update(lambda: page.select_option("select[name=period]", "last_3_months"))
    browser_page.after_update(lambda: page.select_option("select[name=period]", "custom"))
    assert page.is_visible("input[name=start]") and page.is_visible("input[name=end]")
    # 31 May minus 3 months is the last day of February, never 2 March.
    assert page.input_value("input[name=start]") == "2024-02-29"
    assert page.input_value("input[name=end]") == "2024-05-31"
    assert browser_page.status()["summary"]["events"] == 5  # 2 to 6 (8 is a quarry blast)
    browser_page.after_update(lambda: page.fill("input[name=start]", "2024-03-04"))
    assert browser_page.status()["summary"]["events"] == 2  # 5 and 6


@pytest.mark.req("REQ-DASH-010")
def test_a_day_without_events_says_so(browser_page, server):
    browser_page.open(server.url, now="2024-03-20T12:00:00+00:00")
    assert browser_page.status()["summary"]["events"] == 0
    assert browser_page.page.inner_text("#chart-time") == "No events match the filters."


@pytest.mark.req("REQ-DASH-011")
def test_earthquakes_are_the_default_type(dash):
    page = dash.page
    assert page.input_value("select[name=event_type]") == "earthquake"
    assert dash.status()["summary"]["events"] == 6  # not the quarry blast
    dash.after_update(lambda: page.select_option("select[name=event_type]", ""))
    assert dash.status()["summary"]["events"] == 7  # All: the quarry blast too
    dash.after_update(lambda: page.select_option("select[name=event_type]", "quarry blast"))
    assert dash.status()["summary"]["events"] == 1
    page.click("button[type=reset]")
    page.wait_for_function("window.__earthHub.summary.events === 0")  # Today, 2 April
    assert page.input_value("select[name=event_type]") == "earthquake"
