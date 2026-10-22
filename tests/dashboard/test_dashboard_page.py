"""The dashboard page in Chromium: DuckDB-WASM's answers against DuckDB's in Python on the
same snapshot, the form, the drawing, phone width and the empty state."""

import json
from datetime import datetime, timedelta

import pytest
from support.dashboard import LocalDashboard
from support.dashboard_data import EVENTS, event
from support.dashboard_data import build as build_snapshot

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
    "date_range",
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
        {"min_magnitude": 6.0},
        {"max_magnitude": 2.5},
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
    # One segment per day and magnitude level with events: 6 events, 28 Feb to 10 Mar.
    assert len(page.query_selector_all('#chart-time g[aria-label="rect"] rect')) == 6
    assert len(page.query_selector_all('#chart-magnitude g[aria-label="rect"] rect')) == 12
    countries = page.query_selector_all('#chart-countries g[aria-label="bar"] rect')
    assert len(countries) == 4
    for figure in ("time", "magnitude", "depth", "countries", "regions"):
        svg = page.query_selector(f"#chart-{figure} svg[role=img]")
        assert svg is not None and svg.get_attribute("aria-label"), figure


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

    dash.after_update(lambda: page.select_option("select[name=magnitude]", "2.5:4.5"))
    assert events() == 1
    dash.after_update(lambda: page.select_option("select[name=magnitude]", ""))
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
    dash.after_update(lambda: page.select_option("select[name=magnitude]", "6:"))
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


@pytest.mark.req("REQ-DASH-019")
def test_a_popup_gives_the_time_in_utc_and_where_the_event_happened(browser_page, server):
    """The fixed events are in Alaska, either side of the change to summer time (10 March 2024,
    11:00 UTC): AKST is UTC-9, AKDT UTC-8."""
    browser_page.open(server.url, now="2024-03-10T12:00:00+00:00", period="this_year")
    page = browser_page.page

    def popup(key: str) -> dict:
        assert page.evaluate("key => window.__earthHub.openPopup(key)", key)
        lines = {
            name: page.inner_text(f".maplibregl-popup [data-testid=popup-{name}]")
            for name in ("details", "utc", "local")
        }
        lines["zone"] = page.get_attribute(".maplibregl-popup [data-testid=popup-local]", "title")
        page.click(".maplibregl-popup-close-button")
        return lines

    before = popup(EVENTS[1]["key"])  # 2024-02-29 00:00 UTC
    assert before["utc"] == "2024-02-29 00:00 UTC"
    assert before["local"] == "2024-02-28 15:00 AKST"
    assert before["zone"] == "America/Anchorage"
    after = popup(EVENTS[5]["key"])  # 2024-03-10 23:00 UTC, after the change
    assert (after["utc"], after["local"]) == ("2024-03-10 23:00 UTC", "2024-03-10 15:00 AKDT")
    assert after["details"].startswith("M 5.2 · ")  # magnitude and depth on their own line
    assert browser_page.errors == [] and browser_page.blocked == []


@pytest.mark.req("REQ-DASH-018")
def test_the_table_sorts_by_each_column_header(browser_page, large_server):
    browser_page.open(large_server.server.url, now=LARGE_NOW, period="last_3_years")
    page = browser_page.page
    latest = large_server.latest

    def shown() -> list[str]:
        return page.eval_on_selector_all(
            "#significant tbody tr", "rows => rows.map(r => r.cells[0].textContent)"
        )

    def times(rows) -> list[str]:
        return [r["event_ts"].isoformat(sep=" ")[:16] for r in rows]

    def header(key: str) -> str:
        return page.get_attribute(f"#significant th:has(button[data-sort={key}])", "aria-sort")

    def click(key: str) -> None:
        page.click(f"#significant button[data-sort={key}]")

    def by(field, reverse=False):
        return sorted(latest, key=lambda r: r[field], reverse=reverse)

    assert shown() == times(latest) and header("magnitude") == "none"
    for key in ("event_ts", "magnitude", "place", "depth_km"):
        click(key)
        assert shown() == times(by(key)), key
        assert header(key) == "ascending"
        click(key)
        # Descending; ties keep the table's own order, so this is not ascending reversed.
        expected = sorted(latest, key=lambda r: r[key], reverse=True)
        assert shown() == times(expected), key
        assert header(key) == "descending"
        click(key)
        assert shown() == times(latest) and header(key) == "none", key

    # Kept when the filters change; cleared when Latest or Largest picks other events.
    click("magnitude")
    browser_page.after_update(lambda: page.select_option("select[name=magnitude]", "6:"))
    assert header("magnitude") == "ascending"
    rows = page.eval_on_selector_all(
        "#significant tbody tr", "rows => rows.map(r => Number(r.cells[1].textContent))"
    )
    assert rows == sorted(rows)
    browser_page.after_update(lambda: page.check("input[name=order][value=largest]"))
    assert header("magnitude") == "none"
    # A header is a real button: reachable from the keyboard.
    page.focus("#significant button[data-sort=depth_km]")
    page.keyboard.press("Enter")
    assert header("depth_km") == "ascending"
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


# The previous day, week, month and year end before today. Each `now` puts the dataset's
# events on both sides of the period's edges: 28 Feb 23:59:59.999, midnight on 1 March.
LAST_OF_28 = "2024-02-28T23:59:59.999"


@pytest.mark.req("REQ-DASH-010")
@pytest.mark.parametrize(
    ("period", "now", "events", "first", "last", "bucket"),
    [
        # 29 February: not event 1 (the day before) nor 4 (1 March, 00:00).
        ("yesterday", "2024-03-01T12:00", 2, "2024-02-29T00:00:00", "2024-02-29T12:00:00", "day"),
        # From Wednesday 6 March: Monday 26 February to Sunday 3 March, not 5 (Monday 4 March).
        ("last_week", "2024-03-06T12:00", 4, LAST_OF_28, "2024-03-01T00:00:00", "day"),
        # February of a leap year, through the 29th, not 4 (1 March, 00:00).
        ("last_month", "2024-03-10T12:00", 3, LAST_OF_28, "2024-02-29T12:00:00", "day"),
        ("last_year", "2025-01-15T12:00", 6, LAST_OF_28, "2024-03-10T23:00:00", "week"),
    ],
)
def test_the_previous_periods_end_before_today(
    browser_page, server, period, now, events, first, last, bucket
):
    browser_page.open(server.url, now=f"{now}:00+00:00")
    page = browser_page.page
    browser_page.after_update(lambda: page.select_option("select[name=period]", period))
    summary = browser_page.status()["summary"]
    assert summary["events"] == events
    assert summary["first_event"] == as_page(datetime.fromisoformat(first))
    assert summary["last_event"] == as_page(datetime.fromisoformat(last))
    assert page.input_value("select[name=time_bucket]") == bucket
    # Custom starts from the same days.
    browser_page.after_update(lambda: page.select_option("select[name=period]", "custom"))
    assert browser_page.status()["summary"]["events"] == events
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


STOPS = [[0, "#fed976"], [2.5, "#fd8d3c"], [4.5, "#e31a1c"], [6, "#800026"]]


def rgb(hex_color: str) -> str:
    r, g, b = (int(hex_color[i : i + 2], 16) for i in (1, 3, 5))
    return f"rgb({r}, {g}, {b})"


@pytest.mark.req("REQ-DASH-012")
def test_the_legend_shows_the_magnitude_colors_beside_the_map_note(dash):
    page = dash.page
    legend = page.query_selector(".map-foot .legend")
    assert legend.is_visible()
    assert page.inner_text(".legend-title") == "Magnitude"
    ticks = page.eval_on_selector_all(".legend-tick", "ts => ts.map(t => t.textContent)")
    assert ticks == ["≤0", "2.5", "4.5", "6+"]
    assert legend.get_attribute("aria-label") == (
        "Dot color by magnitude, light to dark: ≤0, 2.5, 4.5, 6+"
    )
    # Drawn from the map's own stops, each at its place on a bar from 0 to 6.
    assert json.loads(legend.get_attribute("data-stops")) == STOPS
    gradient = page.eval_on_selector(".legend-bar", "b => getComputedStyle(b).backgroundImage")
    places = ["0%", "41.6667%", "75%", "100%"]
    expected = ", ".join(f"{rgb(c)} {at}" for (_, c), at in zip(STOPS, places, strict=True))
    assert gradient == f"linear-gradient(to right, {expected})"
    # On the note's line, to its right.
    note = page.query_selector(".map-foot .note").bounding_box()
    box = legend.bounding_box()
    assert box["x"] > note["x"] + note["width"]
    assert box["y"] < note["y"] + note["height"] and note["y"] < box["y"] + box["height"]


@pytest.mark.req("REQ-DASH-012")
def test_the_legend_fits_a_phone(browser_page, server):
    browser_page.page.set_viewport_size({"width": 390, "height": 844})
    browser_page.open(server.url)
    box = browser_page.page.query_selector(".legend").bounding_box()
    assert box["x"] >= 16 and box["x"] + box["width"] <= 390 - 16
    assert browser_page.page.evaluate("document.documentElement.scrollWidth") <= 390


def color_at(magnitude: float) -> str:
    """The map's color blend, worked out independently: linear in RGB between the stops."""
    if magnitude <= STOPS[0][0]:
        return STOPS[0][1]
    for (m0, c0), (m1, c1) in zip(STOPS, STOPS[1:], strict=False):
        if magnitude <= m1:
            t = (magnitude - m0) / (m1 - m0)
            a = [int(c0[i : i + 2], 16) for i in (1, 3, 5)]
            b = [int(c1[i : i + 2], 16) for i in (1, 3, 5)]
            # Math.round: halves round up.
            return "#" + "".join(
                f"{int(x + (y - x) * t + 0.5):02x}" for x, y in zip(a, b, strict=True)
            )
    return STOPS[-1][1]


@pytest.mark.req("REQ-DASH-013")
def test_magnitude_bars_take_the_map_color_of_their_bin(dash, expected):
    expected.set_filter(event_type="earthquake", start_time=datetime(2021, 4, 2))
    bins = expected.query("magnitude_bins")
    fills = dash.page.eval_on_selector_all(
        '#chart-magnitude g[aria-label="rect"] rect', "rs => rs.map(r => r.getAttribute('fill'))"
    )
    assert fills == [color_at((b["bin_start"] + b["bin_end"]) / 2) for b in bins]
    assert fills[0] == "#fed976"  # -0.5 to 0: below the first stop
    assert fills[-1] == color_at(5.25)  # 5.0 to 5.5: between red and dark red
    assert len(set(fills)) == len(fills)  # each bin its own shade


def test_the_color_blend_matches_the_stops():
    assert [color_at(m) for m, _ in STOPS] == [c for _, c in STOPS]
    assert color_at(-3) == "#fed976" and color_at(9) == "#800026"


LEVELS = [
    ("below_2_5", "Below 2.5", "#fed976"),
    ("from_2_5", "2.5 to 4.5", "#fd8d3c"),
    ("from_4_5", "4.5 to 6", "#e31a1c"),
    ("from_6", "6 and above", "#800026"),
    ("no_magnitude", "No magnitude", "#a8a29e"),
]


@pytest.mark.req("REQ-DASH-014")
@pytest.mark.parametrize("bucket", ["day", "month"])
def test_time_bars_are_split_by_magnitude_level(dash, expected, bucket):
    page = dash.page
    dash.after_update(lambda: page.select_option("select[name=time_bucket]", bucket))
    expected.set_filter(
        event_type="earthquake", start_time=datetime(2021, 4, 2), time_bucket=bucket
    )
    want = sorted(
        color for row in expected.query("over_time") for key, _, color in LEVELS if row[key] > 0
    )
    fills = page.eval_on_selector_all(
        '#chart-time g[aria-label="rect"] rect', "rs => rs.map(r => r.getAttribute('fill'))"
    )
    assert sorted(fills) == want  # one segment per period and level, in the level's color
    # The chart's own legend: every level, bottom to top, in the map legend's colors.
    swatches = page.eval_on_selector_all(
        "#chart-time [class*=swatch] > :is(span, div)", "els => els.map(e => e.textContent.trim())"
    )
    assert [label for _, label, _ in LEVELS] == [s for s in swatches if s]


@pytest.mark.req("REQ-DASH-014")
def test_the_levels_use_the_map_legend_colors():
    assert [c for _, _, c in LEVELS[:4]] == [c for _, c in STOPS]


@pytest.mark.req("REQ-DASH-015")
def test_custom_days_run_from_the_first_day_with_data_to_today(browser_page, server):
    # 2 April 2024, the dataset's last day; Last 3 years starts in 2021.
    browser_page.open(server.url, now="2024-04-02T12:00:00+00:00", period="last_3_years")
    page = browser_page.page
    browser_page.after_update(lambda: page.select_option("select[name=period]", "custom"))
    for name in ("start", "end"):
        assert page.get_attribute(f"input[name={name}]", "min") == "2024-02-28"
        assert page.get_attribute(f"input[name={name}]", "max") == "2024-04-02"
    # Custom starts from the last preset's days, moved up to the first day with data.
    assert page.input_value("input[name=start]") == "2024-02-28"
    # A day typed outside the limits moves to the nearest allowed one.
    browser_page.after_update(lambda: page.fill("input[name=start]", "2019-06-01"))
    assert page.input_value("input[name=start]") == "2024-02-28"
    browser_page.after_update(lambda: page.fill("input[name=end]", "2030-01-01"))
    assert page.input_value("input[name=end]") == "2024-04-02"
    assert browser_page.status()["summary"]["events"] == 6


@pytest.mark.req("REQ-DASH-015")
def test_the_magnitude_bands_are_the_legends(dash):
    page = dash.page
    options = page.eval_on_selector_all(
        "select[name=magnitude] option", "os => os.map(o => [o.value, o.textContent])"
    )
    assert options == [
        ["", "All"],
        ["2.5:", "2.5 and above"],
        [":2.5", "Below 2.5"],
        ["2.5:4.5", "2.5 to 4.5"],
        ["4.5:6", "4.5 to 6"],
        ["6:", "6 and above"],
    ]
    bands = options[2:]
    assert [label for _, label in bands] == [label for _, label, _ in LEVELS[:4]]
    assert [b for _, _, b in LEVELS[:4]] == [c for _, c in STOPS]  # the legend's colors

    def events():
        return dash.status()["summary"]["events"]

    everything = events()  # the fixture picks All
    counts = {}
    for value, _ in options[1:]:
        dash.after_update(lambda value=value: page.select_option("select[name=magnitude]", value))
        counts[value] = events()
    # Each band includes its lower edge: 2.49 and -0.43; 2.5; 4.5 and 5.2; none.
    assert counts == {"2.5:": 3, ":2.5": 2, "2.5:4.5": 1, "4.5:6": 2, "6:": 0}
    assert counts["2.5:"] == counts["2.5:4.5"] + counts["4.5:6"] + counts["6:"]
    # All also keeps the one without a magnitude.
    assert everything == sum(counts[value] for value, _ in bands) + 1


@pytest.mark.req("REQ-DASH-020")
def test_the_page_starts_on_magnitude_2_5_and_above(browser_page, server):
    browser_page.open(
        server.url, now="2024-04-02T12:00:00+00:00", period="last_3_years", magnitude=None
    )
    page = browser_page.page
    assert page.input_value("select[name=magnitude]") == "2.5:"
    # 2.5, 4.5 and 5.2: the earthquakes below 2.5 and the one without a magnitude are left out.
    assert browser_page.status()["summary"]["events"] == 3
    assert browser_page.status()["mapFeatures"] == 3
    browser_page.after_update(lambda: page.select_option("select[name=magnitude]", ""))
    assert browser_page.status()["summary"]["events"] == 6
    page.click("button[type=reset]")
    page.wait_for_function("window.__earthHub.summary.events === 0")
    assert page.input_value("select[name=magnitude]") == "2.5:"
    assert browser_page.errors == [] and browser_page.blocked == []


@pytest.mark.req("REQ-DASH-020")
def test_the_rankings_say_what_drives_their_counts(dash):
    page = dash.page
    countries = page.inner_text("#chart-countries + .note")
    assert "seismic networks" in countries and "magnitude 4.5" in countries
    assert "Unknown" in page.inner_text("#chart-regions + .note")


@pytest.mark.req("REQ-DASH-021")
def test_the_place_menus_list_unknown_first_and_us_places_with_a_prefix(dash):
    page = dash.page
    # Options count every event, the deleted one too; values stay as stored. The states
    # come after the other regions.
    assert options(page, "country") == [
        ["", "All"],
        ["Unknown", "Unknown (1)"],
        ["Japan", "Japan (1)"],
        ["Mexico", "Mexico (1)"],
        ["United States", "United States (5)"],
    ]
    assert options(page, "region") == [
        ["", "All"],
        ["Unknown", "Unknown (2)"],
        ["Baja California", "Baja California (1)"],
        ["Alaska", "US-Alaska (4)"],
        ["California", "US-California (1)"],
    ]
    dash.after_update(lambda: page.select_option("select[name=region]", "Alaska"))
    assert dash.status()["summary"]["events"] == 2  # 2.49 and 2.5: not the deleted or the blast
    # The region menu follows the country, keeping its labels.
    dash.after_update(lambda: page.select_option("select[name=country]", "United States"))
    assert [label for _, label in options(page, "region")] == [
        "All",
        "US-Alaska (4)",
        "US-California (1)",
    ]


TERRITORIES = [
    event(1, "2024-01-01T00:00:00", 3.0, "Puerto Rico", None),
    event(2, "2024-01-02T00:00:00", 3.1, "Guam", None),
    event(3, "2024-01-03T00:00:00", 3.2, "American Samoa", None),
    event(4, "2024-01-04T00:00:00", 3.3, "Northern Mariana Islands", None),
    event(5, "2024-01-05T00:00:00", 3.4, "U.S. Virgin Islands", None),
    event(6, "2024-01-06T00:00:00", 3.5, "Samoa", None),  # the independent state
    event(7, "2024-01-07T00:00:00", 3.6, "Georgia", None),  # the country, not the state
    event(8, "2024-01-08T00:00:00", 3.7, "United States", "Georgia"),
    event(9, "2024-01-09T00:00:00", 3.8, "Vanuatu", None),  # after United States
    event(10, "2024-01-09T01:00:00", 3.9, "Mexico", "Baja California"),
    event(11, "2024-01-09T02:00:00", 4.0, "United States", "Alaska"),
]


@pytest.fixture(scope="module")
def territories_server(built, tmp_path_factory):
    snapshot = build_snapshot(tmp_path_factory.mktemp("territories"), TERRITORIES)
    local = LocalDashboard(tmp_path_factory.mktemp("r2-territories"))
    meta = {
        "table": "earthquakes",
        "run_id": "2024-01-10T12-00-00Z-test",
        "written_at": "2024-01-10T12:00:00Z",
        "rows": len(TERRITORIES),
        "bytes": snapshot.stat().st_size,
    }
    local.seed(snapshot, meta)
    local.start()
    yield local
    local.stop()


@pytest.mark.req("REQ-DASH-021")
def test_us_territories_and_states_are_grouped_with_the_prefix(browser_page, territories_server):
    browser_page.open(territories_server.url, now="2024-01-10T12:00:00+00:00", period="this_year")
    page = browser_page.page
    # The territories right after United States, alphabetically by what the menu shows.
    assert options(page, "country") == [
        ["", "All"],
        ["Georgia", "Georgia (1)"],
        ["Mexico", "Mexico (1)"],
        ["Samoa", "Samoa (1)"],
        ["United States", "United States (2)"],
        ["American Samoa", "US-American Samoa (1)"],
        ["Guam", "US-Guam (1)"],
        ["Northern Mariana Islands", "US-Northern Mariana Islands (1)"],
        ["Puerto Rico", "US-Puerto Rico (1)"],
        ["U.S. Virgin Islands", "US-Virgin Islands (1)"],
        ["Vanuatu", "Vanuatu (1)"],
    ]
    # The states after the other regions.
    assert options(page, "region") == [
        ["", "All"],
        ["Unknown", "Unknown (8)"],
        ["Baja California", "Baja California (1)"],
        ["Alaska", "US-Alaska (1)"],
        ["Georgia", "US-Georgia (1)"],
    ]
    browser_page.after_update(lambda: page.select_option("select[name=country]", "Puerto Rico"))
    assert browser_page.status()["summary"]["events"] == 1
    assert browser_page.errors == [] and browser_page.blocked == []


def options(page, name):
    return page.eval_on_selector_all(
        f"select[name={name}] option", "os => os.map(o => [o.value, o.textContent])"
    )
