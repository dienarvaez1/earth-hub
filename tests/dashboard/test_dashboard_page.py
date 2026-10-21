"""The dashboard page in Chromium: DuckDB-WASM's answers against DuckDB's in Python on the
same snapshot, the form, the drawing, phone width and the empty state."""

import json
from datetime import datetime, timedelta

import pytest
from support.dashboard_data import EVENTS

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

    dash.after_update(lambda: page.select_option("select[name=min_magnitude]", "2.5"))
    assert events() == 3
    dash.after_update(lambda: page.select_option("select[name=min_magnitude]", ""))
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
    dash.after_update(lambda: page.select_option("select[name=min_magnitude]", "6"))
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
    browser_page.after_update(lambda: page.select_option("select[name=min_magnitude]", "6"))
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


def magnitude_options(page, name):
    return page.eval_on_selector_all(
        f"select[name={name}] option", "os => os.map(o => [o.value, o.textContent, o.disabled])"
    )


@pytest.mark.req("REQ-DASH-015")
def test_magnitude_lists_are_the_legend_boundaries(dash):
    page = dash.page
    # The open ends are named after the end bands, at their ends of the lists.
    assert magnitude_options(page, "min_magnitude") == [
        ["", "Below 2.5", False],
        ["2.5", "2.5", False],
        ["4.5", "4.5", False],
        ["6", "6", False],
    ]
    assert magnitude_options(page, "max_magnitude") == [
        ["2.5", "2.5", False],
        ["4.5", "4.5", False],
        ["6", "6", False],
        ["", "6 and above", False],
    ]
    assert page.input_value("select[name=min_magnitude]") == ""  # no limits by default
    assert page.input_value("select[name=max_magnitude]") == ""
    assert [b for _, _, b in LEVELS[:4]] == [c for _, c in STOPS]  # the legend's colors

    def events():
        return dash.status()["summary"]["events"]

    # 2.5 to 4.5, the orange band: only 2.5 itself (4.5 belongs to the next band).
    dash.after_update(lambda: page.select_option("select[name=min_magnitude]", "2.5"))
    dash.after_update(lambda: page.select_option("select[name=max_magnitude]", "4.5"))
    assert events() == 1
    dash.after_update(lambda: page.select_option("select[name=max_magnitude]", "6"))
    assert events() == 3  # 2.5, 4.5, 5.2
    dash.after_update(lambda: page.select_option("select[name=max_magnitude]", ""))
    dash.after_update(lambda: page.select_option("select[name=min_magnitude]", ""))
    dash.after_update(lambda: page.select_option("select[name=max_magnitude]", "2.5"))
    assert events() == 2  # 2.49 and -0.43 (no magnitude is left out)


@pytest.mark.req("REQ-DASH-015")
def test_choices_that_select_nothing_are_greyed_out(dash):
    page = dash.page
    dash.after_update(lambda: page.select_option("select[name=min_magnitude]", "4.5"))
    assert [v for v, _, off in magnitude_options(page, "max_magnitude") if off] == ["2.5", "4.5"]
    dash.after_update(lambda: page.select_option("select[name=max_magnitude]", "6"))
    assert [v for v, _, off in magnitude_options(page, "min_magnitude") if off] == ["6"]
    page.click("button[type=reset]")
    page.wait_for_function("window.__earthHub.summary.events === 0")
    for name in ("min_magnitude", "max_magnitude"):
        assert not any(off for _, _, off in magnitude_options(page, name))
