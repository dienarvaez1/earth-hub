"""The dashboard's SQL (dashboard/queries/) on a fixed dataset, with answers worked by hand.

The page runs these same files in the browser (DuckDB-WASM); here DuckDB runs them on a
snapshot built by the exporter's own code.
"""

import math
import random
from datetime import datetime

import pytest
from support.analytics import Dashboard
from support.dashboard_data import EVENTS, build, event


@pytest.fixture
def dash(tmp_path):
    dashboard = Dashboard(build(tmp_path, EVENTS))
    yield dashboard
    dashboard.close()


def number(row) -> int:
    """The test row a result comes from: support/export.py:make_row keys row n `us000n`."""
    return int(row["key"][2:])


def ids(dash, **filters):
    dash.set_filter(**filters)
    return [number(p) for p in dash.query("map_points")]


def at(text):
    return datetime.fromisoformat(text)


@pytest.mark.req("REQ-DASH-001")
@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ({}, [1, 2, 3, 4, 5, 6, 8]),  # deleted events are left out by default
        ({"include_deleted": True}, [1, 2, 3, 4, 5, 6, 7, 8]),
        # start inclusive, end exclusive, in UTC: 23:59:59.999 and the next midnight are out
        ({"start_time": at("2024-02-29"), "end_time": at("2024-03-01")}, [2, 3]),
        ({"min_magnitude": 2.5}, [2, 3, 6]),  # inclusive; no magnitude: left out
        ({"max_magnitude": 2.5}, [1, 5, 8]),  # exclusive, like the bands: 2.5 is left out
        ({"min_magnitude": 2.5, "max_magnitude": 4.5}, [2]),  # the 2.5 to 4.5 band
        ({"min_magnitude": -1, "max_magnitude": 10}, [1, 2, 3, 5, 6, 8]),
        ({"country": "Unknown"}, [4]),
        ({"country": "United States"}, [1, 2, 5, 8]),
        ({"region": "Alaska"}, [1, 2, 8]),
        ({"region": "Unknown", "country": "Japan"}, [6]),
        ({"event_type": "quarry blast"}, [8]),
        ({"country": "Atlantis"}, []),
    ],
)
def test_filters_select_the_expected_events(dash, filters, expected):
    assert ids(dash, **filters) == expected


@pytest.mark.req("REQ-DASH-002")
def test_events_per_day_in_utc_with_empty_days(dash):
    dash.set_filter(time_bucket="day")
    days = {r["period"].date().isoformat(): r["events"] for r in dash.query("over_time")}
    assert len(days) == 35  # 28 February to 2 April, every day listed
    assert sum(days.values()) == 7
    assert days["2024-02-28"] == 1 and days["2024-02-29"] == 2 and days["2024-03-01"] == 1
    assert days["2024-03-02"] == 0
    assert days["2024-03-10"] == 1  # the deleted event that day is not counted


@pytest.mark.req("REQ-DASH-002")
def test_events_per_hour_in_utc_with_empty_hours(dash):
    dash.set_filter(time_bucket="hour", start_time=at("2024-02-28"), end_time=at("2024-03-02"))
    rows = dash.query("over_time")
    hours = {r["period"].isoformat(sep=" ")[:16]: r["events"] for r in rows}
    # 28 February 23:00 to 1 March 00:00, every hour listed: 1 + 24 + 1.
    assert len(hours) == 26
    assert list(hours)[0] == "2024-02-28 23:00" and list(hours)[-1] == "2024-03-01 00:00"
    # 23:59:59.999 is still in 23:00; midnight starts its own hour.
    assert {h: n for h, n in hours.items() if n} == {
        "2024-02-28 23:00": 1,
        "2024-02-29 00:00": 1,
        "2024-02-29 12:00": 1,
        "2024-03-01 00:00": 1,
    }
    assert [r["from_2_5"] for r in rows if r["events"]] == [0, 1, 0, 0]  # 2.5 at 00:00
    assert [r["from_4_5"] for r in rows if r["events"]] == [0, 0, 1, 0]  # 4.5 at 12:00


@pytest.mark.req("REQ-DASH-002")
def test_events_per_iso_week_and_month(dash):
    dash.set_filter(time_bucket="week")
    weeks = [(r["period"].date().isoformat(), r["events"]) for r in dash.query("over_time")]
    assert weeks == [  # weeks start on Monday; Sunday 10 March is in the week of 4 March
        ("2024-02-26", 4),
        ("2024-03-04", 2),
        ("2024-03-11", 0),
        ("2024-03-18", 0),
        ("2024-03-25", 0),
        ("2024-04-01", 1),
    ]
    dash.set_filter(time_bucket="month")
    months = [(r["period"].date().isoformat(), r["events"]) for r in dash.query("over_time")]
    assert months == [("2024-02-01", 3), ("2024-03-01", 3), ("2024-04-01", 1)]


LEVELS = ("below_2_5", "from_2_5", "from_4_5", "from_6", "no_magnitude")


@pytest.mark.req("REQ-DASH-014")
def test_each_period_is_counted_by_magnitude_level(dash):
    dash.set_filter(time_bucket="month")
    months = {r["period"].date().isoformat(): r for r in dash.query("over_time")}
    levels = {month: [r[k] for k in LEVELS] for month, r in months.items()}
    # February: 2.49, 2.5 and 4.5, each band including its lower edge.
    assert levels["2024-02-01"] == [1, 1, 1, 0, 0]
    # March: no magnitude, -0.43, 5.2 (the deleted 3.0 is left out).
    assert levels["2024-03-01"] == [1, 0, 1, 0, 1]
    assert levels["2024-04-01"] == [1, 0, 0, 0, 0]  # 1.0
    for row in months.values():
        assert sum(row[k] for k in LEVELS) == row["events"]


@pytest.mark.req("REQ-DASH-014")
def test_empty_periods_have_every_level_at_zero(dash):
    dash.set_filter(time_bucket="day")
    empty = [r for r in dash.query("over_time") if r["events"] == 0]
    assert empty and all(r[k] == 0 for r in empty for k in LEVELS)


@pytest.mark.req("REQ-DASH-014")
def test_six_and_above_has_its_own_level(tmp_path):
    rows = [event(1, "2024-06-24T22:05:04", 7.5, "Venezuela", None)]
    rows += [event(2, "2024-06-24T22:04:31", 6.0, "Venezuela", None)]
    rows += [event(3, "2024-06-25T00:00:00", 5.99, "Venezuela", None)]
    dash = Dashboard(build(tmp_path, rows))
    dash.set_filter(time_bucket="month")
    (june,) = dash.query("over_time")
    assert [june[k] for k in LEVELS] == [0, 0, 1, 2, 0]
    dash.close()


@pytest.mark.req("REQ-DASH-002")
def test_the_highest_magnitude_per_period(dash):
    dash.set_filter(time_bucket="month")
    assert [r["max_magnitude"] for r in dash.query("over_time")] == [4.5, 5.2, 1.0]


@pytest.mark.req("REQ-DASH-003")
def test_magnitude_bins_of_half_a_unit(dash):
    dash.set_filter()
    bins = [(r["bin_start"], r["bin_end"], r["events"]) for r in dash.query("magnitude_bins")]
    assert bins[0] == (-0.5, 0.0, 1)  # -0.43
    assert bins[-1] == (5.0, 5.5, 1)  # 5.2
    assert len(bins) == 12  # every bin from -0.5 to 5.0, empty ones too
    counted = {start: n for start, _, n in bins if n}
    # 2.49 is in [2.0, 2.5); 2.5 and 4.5 start their own bins; the event without one is out
    assert counted == {-0.5: 1, 1.0: 1, 2.0: 1, 2.5: 1, 4.5: 1, 5.0: 1}


@pytest.mark.req("REQ-DASH-004")
def test_top_countries_with_unknown_and_ties_in_order(dash):
    dash.set_filter()
    rows = [(r["country"], r["events"]) for r in dash.query("top_countries")]
    assert rows == [("United States", 4), ("Japan", 1), ("Mexico", 1), ("Unknown", 1)]
    dash.set_filter(top_n=2)
    assert [r["country"] for r in dash.query("top_countries")] == ["United States", "Japan"]


LEVEL_KEYS = ("below_2_5", "from_2_5", "from_4_5", "from_6", "no_magnitude")


@pytest.mark.req("REQ-DASH-023")
def test_the_top_countries_and_regions_count_each_magnitude_level(dash):
    dash.set_filter()

    def levels(name, *place):
        return {
            tuple(r[p] for p in place): tuple(r[k] for k in LEVEL_KEYS) for r in dash.query(name)
        }

    # United States: 2.49, -0.43 and the 1.0 quarry blast below 2.5, and 2.5 (the deleted 3.0
    # is left out); Japan 5.2 and Mexico 4.5 in 4.5 to 6; Unknown without a magnitude.
    assert levels("top_countries", "country") == {
        ("United States",): (3, 1, 0, 0, 0),
        ("Japan",): (0, 0, 1, 0, 0),
        ("Mexico",): (0, 0, 1, 0, 0),
        ("Unknown",): (0, 0, 0, 0, 1),
    }
    assert levels("top_regions", "region", "country") == {
        ("Alaska", "United States"): (2, 1, 0, 0, 0),
        ("Baja California", "Mexico"): (0, 0, 1, 0, 0),
        ("California", "United States"): (1, 0, 0, 0, 0),
        ("Unknown", "Japan"): (0, 0, 1, 0, 0),
        ("Unknown", "Unknown"): (0, 0, 0, 0, 1),
    }
    for name in ("top_countries", "top_regions"):
        assert all(sum(r[k] for k in LEVEL_KEYS) == r["events"] for r in dash.query(name))


@pytest.mark.req("REQ-DASH-004")
def test_top_regions_with_their_country(dash):
    dash.set_filter()
    rows = [(r["region"], r["country"], r["events"]) for r in dash.query("top_regions")]
    assert rows == [
        ("Alaska", "United States", 3),
        ("Baja California", "Mexico", 1),
        ("California", "United States", 1),
        ("Unknown", "Japan", 1),
        ("Unknown", "Unknown", 1),
    ]


@pytest.mark.req("REQ-DASH-005")
def test_the_summary(dash):
    dash.set_filter()
    (summary,) = dash.query("summary")
    assert summary == {
        "events": 7,
        "with_magnitude": 6,
        "max_magnitude": 5.2,
        "max_magnitude_place": "place 6",
        "first_event": at("2024-02-28T23:59:59.999"),
        "last_event": at("2024-04-02T10:00:00"),
        "countries": 4,
        "tsunami_flagged": 0,
        "deleted": 0,
    }
    dash.set_filter(include_deleted=True)
    assert dash.query("summary")[0]["deleted"] == 1


@pytest.mark.req("REQ-DASH-005")
def test_significant_events_newest_first(dash):
    dash.set_filter()
    assert [number(r) for r in dash.query("significant")] == [6, 3]  # 4.5 is included
    dash.set_filter(significant_magnitude=5.0)
    assert [number(r) for r in dash.query("significant")] == [6]


@pytest.mark.req("REQ-DASH-005")
def test_largest_events_strongest_first(dash):
    dash.set_filter(significant_magnitude=1.0)
    assert [(number(r), r["magnitude"]) for r in dash.query("largest")] == [
        (6, 5.2),
        (3, 4.5),
        (2, 2.5),
        (1, 2.49),
        (8, 1.0),
    ]
    dash.set_filter(significant_magnitude=1.0, significant_n=2)
    assert [number(r) for r in dash.query("largest")] == [6, 3]


@pytest.mark.req("REQ-DASH-029")
def test_the_table_and_the_rankings_have_their_own_lengths(dash):
    """significant_n limits the significant events, top_n the top countries and regions."""
    dash.set_filter(significant_magnitude=1.0, significant_n=3, top_n=1)
    assert [number(r) for r in dash.query("largest")] == [6, 3, 2]
    assert [number(r) for r in dash.query("significant")] == [8, 6, 3]
    assert len(dash.query("top_countries")) == len(dash.query("top_regions")) == 1
    dash.set_filter(significant_magnitude=1.0, significant_n=50, top_n=2)
    assert len(dash.query("largest")) == 5  # every event at 1.0 and above
    assert len(dash.query("top_countries")) == 2


@pytest.mark.req("REQ-DASH-005")
def test_largest_breaks_ties_newest_first(tmp_path):
    rows = [
        event(1, "2024-01-01T00:00:00", 7.0, "Panama", None),
        event(2, "2024-06-24T22:05:04", 7.5, "Venezuela", None),
        event(3, "2024-06-24T22:04:31", 7.2, "Venezuela", None),
        event(4, "2024-03-01T00:00:00", 7.0, "Colombia", None),
        *(event(10 + i, f"2024-10-{i + 1:02d}T00:00:00", 6.0, "Russia", None) for i in range(12)),
    ]
    dash = Dashboard(build(tmp_path, rows))
    dash.set_filter(min_magnitude=6.0, max_magnitude=9.0)
    # The ten latest are all October's; the largest are June's two and the 7.0s.
    assert {number(r) for r in dash.query("significant")} == set(range(12, 22))
    assert [number(r) for r in dash.query("largest")][:4] == [2, 3, 4, 1]
    dash.close()


@pytest.mark.req("REQ-DASH-005")
def test_map_points_carry_what_the_map_shows(dash):
    dash.set_filter()
    point = dash.query("map_points")[0]
    assert point == {
        "key": "us0001",
        "longitude": -155.6123,
        "latitude": 57.4401,
        "depth_km": 10.0,
        "magnitude": 2.49,
        "event_time_ms": EVENTS[0]["event_time_ms"],
        "place": "place 1",
        "title": EVENTS[0]["title"],
        "url": EVENTS[0]["url"],
        "review_status": "reviewed",
        "tsunami": False,
    }


@pytest.mark.req("REQ-DASH-027")
@pytest.mark.parametrize("report", ["significant", "largest"])
def test_significant_events_carry_what_their_popup_shows(dash, report):
    """A row of the table opens its event's popup, so it has map_points.sql's columns."""
    dash.set_filter()
    points = {p["key"]: p for p in dash.query("map_points")}
    for row in dash.query(report):
        assert {k: row[k] for k in points[row["key"]]} == points[row["key"]]


@pytest.mark.req("REQ-DASH-005")
@pytest.mark.parametrize(
    "rows",
    [[], [event(1, "2024-01-01T00:00:00", 3.0, "Chile", None, review_status="deleted")]],
    ids=["empty table", "only deleted events"],
)
def test_no_events_give_empty_results_and_a_zero_summary(tmp_path, rows):
    dash = Dashboard(build(tmp_path, rows))
    dash.set_filter(time_bucket="day")
    for name in ("over_time", "magnitude_bins", "top_countries", "top_regions", "significant"):
        assert dash.query(name) == [], name
    (summary,) = dash.query("summary")
    assert (summary["events"], summary["max_magnitude"], summary["first_event"]) == (0, None, None)
    dash.close()


@pytest.mark.req("REQ-DASH-001")
def test_filter_options_list_every_value(dash):
    dash.set_filter()
    assert dash.query("countries") == [
        {"country": "Japan", "events": 1},
        {"country": "Mexico", "events": 1},
        {"country": "United States", "events": 4},  # not the deleted event, left out
        {"country": "Unknown", "events": 1},
    ]
    assert [r["region"] for r in dash.query("regions")] == [
        "Alaska",
        "Baja California",
        "California",
        "Unknown",
    ]
    dash.set_filter(country="United States")
    assert [r["region"] for r in dash.query("regions")] == ["Alaska", "California"]
    # Under the chosen country: the United States' 3 earthquakes and its quarry blast.
    assert dash.query("event_types") == [
        {"event_type": "earthquake", "events": 3},
        {"event_type": "quarry blast", "events": 1},
    ]


@pytest.mark.req("REQ-DASH-024")
def test_each_menu_counts_under_the_rest_of_the_filter(dash):
    def counts(name, key, **filters):
        dash.set_filter(**filters)
        return {r[key]: r["events"] for r in dash.query(name)}

    # 29 February: events 2 (Alaska, 2.5) and 3 (Baja California, Mexico, 4.5). Every option
    # stays listed, at 0 when nothing matches.
    leap = {"start_time": at("2024-02-29"), "end_time": at("2024-03-01")}
    assert counts("countries", "country", **leap) == {
        "Japan": 0, "Mexico": 1, "United States": 1, "Unknown": 0,
    }  # fmt: skip
    assert counts("event_types", "event_type", **leap) == {"earthquake": 2, "quarry blast": 0}
    # A menu's own choice leaves its counts alone; the others' choices apply.
    assert counts("countries", "country", country="Mexico", **leap) == {
        "Japan": 0, "Mexico": 1, "United States": 1, "Unknown": 0,
    }  # fmt: skip
    assert counts("event_types", "event_type", country="Mexico") == {
        "earthquake": 1, "quarry blast": 0,
    }  # fmt: skip
    assert counts("event_types", "event_type", event_type="quarry blast") == {
        "earthquake": 6, "quarry blast": 1,
    }  # fmt: skip
    # The regions of the chosen country, counted under the magnitude band and deletions.
    assert counts("regions", "region", country="United States", min_magnitude=2.5) == {
        "Alaska": 1, "California": 0,
    }  # fmt: skip
    assert counts("regions", "region", country="United States", include_deleted=True) == {
        "Alaska": 4, "California": 1,
    }  # fmt: skip
    # The type menu keeps its order (most common over all events) whatever the counts.
    dash.set_filter(event_type="quarry blast", start_time=at("2024-04-01"))
    assert [r["event_type"] for r in dash.query("event_types")] == ["earthquake", "quarry blast"]


@pytest.mark.req("REQ-DASH-015")
def test_the_date_range_runs_from_the_first_event_to_the_last(dash):
    dash.set_filter()
    assert dash.query("date_range") == [
        {"first_event": at("2024-02-28T23:59:59.999"), "last_event": at("2024-04-02T10:00:00")}
    ]


@pytest.mark.req("REQ-DASH-015")
def test_an_empty_table_has_no_date_range(tmp_path):
    dash = Dashboard(build(tmp_path, []))
    dash.set_filter()
    assert dash.query("date_range") == [{"first_event": None, "last_event": None}]
    dash.close()


@pytest.mark.req("REQ-DASH-001")
def test_an_invalid_filter_is_rejected(dash):
    with pytest.raises(Exception, match="CHECK"):
        dash.set_filter(time_bucket="year")


# --- the map when it has too many events to draw one by one (map_grid.sql and its partners) ---


def spot(row_id, longitude, latitude, magnitude, depth_km=10.0, **changes):
    return event(
        row_id, "2024-03-01T00:00:00", magnitude, "Chile", None,
        longitude=longitude, latitude=latitude, depth_km=depth_km, **changes,
    )  # fmt: skip


# Cell edges (0 and 0.25 belong to the cell they start), negative coordinates, the two ends of
# the longitudes, no magnitude, strong events at and above 6, and a deleted event.
SPOTS = [
    spot(1, 0.0, 0.0, 1.0, depth_km=0.0),
    spot(2, 0.24, 0.24, 2.0, depth_km=4.99),
    spot(3, 0.25, 0.0, 3.0, depth_km=5.0),
    spot(4, -0.01, -0.01, None),
    spot(5, 180.0, 10.0, 5.9),
    spot(6, -180.0, 10.0, 6.0),
    spot(7, 10.0, 10.0, 7.1),
    spot(8, 0.1, 0.1, 4.0, review_status="deleted"),
    spot(9, -179.5, 5.0, 2.0),
]


@pytest.fixture
def spots(tmp_path):
    dashboard = Dashboard(build(tmp_path, SPOTS))
    dashboard.set_filter()
    yield dashboard
    dashboard.close()


def keys(dash, name, **view):
    dash.set_map_view(**view)
    return [number(r) for r in dash.query(name)]


@pytest.mark.req("REQ-DASH-025")
def test_cells_count_the_events_below_the_strong_magnitude(spots):
    cells = [
        (r["cell_x"], r["cell_y"], r["longitude"], r["latitude"], r["events"], r["max_magnitude"])
        for r in spots.query("map_grid")
    ]
    assert cells == [
        (-718, 20, -179.5, 5.0, 1, 2.0),  # -179.5 / 0.25 = -718
        (-1, -1, -0.01, -0.01, 1, None),
        (0, 0, pytest.approx(0.12), pytest.approx(0.12), 2, 2.0),  # 0 and 0.24; not the deleted 0.1
        (1, 0, 0.25, 0.0, 1, 3.0),  # 0.25 starts the next cell
        (720, 40, 180.0, 10.0, 1, 5.9),
    ]
    assert keys(spots, "map_strong") == [6, 7]  # 6.0 is strong
    total = sum(c[4] for c in cells) + 2
    assert total == spots.query("summary")[0]["events"] == 8


@pytest.mark.req("REQ-DASH-025")
def test_the_cell_size_and_the_strong_magnitude_come_from_the_map_view(spots):
    spots.set_map_view(cell_degrees=1.0, strong_magnitude=5.0)
    assert [(r["cell_x"], r["cell_y"], r["events"]) for r in spots.query("map_grid")] == [
        (-180, 5, 1),
        (-1, -1, 1),
        (0, 0, 3),
    ]
    assert keys(spots, "map_strong", strong_magnitude=5.0) == [5, 6, 7]


@pytest.mark.req("REQ-DASH-025")
@pytest.mark.parametrize(
    ("view", "expected"),
    [
        ({}, [1, 2, 3, 4, 5, 9]),  # the whole world, strong and deleted events left out
        ({"west": -1, "south": -1, "east": 1, "north": 1}, [1, 2, 3, 4]),
        ({"west": 0.25, "south": 0, "east": 1, "north": 0}, [3]),  # edges included
        ({"west": 0.26, "south": -1, "east": 1, "north": 1}, []),
        ({"west": 179, "south": 0, "east": 181, "north": 20}, [5, 9]),  # across the antimeridian
        ({"west": -180, "south": -90, "east": 180, "north": 90}, [1, 2, 3, 4, 5, 9]),
        ({"west": 170, "south": -90, "east": 530, "north": 90}, [1, 2, 3, 4, 5, 9]),  # 360 wide
        ({"area_limit": 2}, [1, 2, 3]),  # one more than the limit: too many to draw
    ],
)
def test_the_area_gives_its_events_one_by_one(spots, view, expected):
    assert keys(spots, "map_area", **view) == expected


@pytest.mark.req("REQ-DASH-025")
def test_an_area_the_map_view_cannot_hold_is_rejected(spots):
    with pytest.raises(Exception, match="CHECK"):
        spots.set_map_view(west=190, south=0, east=200, north=1)  # west must be below 180


@pytest.mark.req("REQ-DASH-025")
def test_depth_bins_count_events_by_tenth_of_magnitude_and_5_km(spots):
    bins = [
        (r["magnitude_from"], r["depth_from_km"], r["events"]) for r in spots.query("depth_bins")
    ]
    assert bins == [
        (1.0, 0.0, 1),  # depth 0 starts the first bin
        (2.0, 0.0, 1),  # 4.99 km
        (2.0, 10.0, 1),
        (3.0, 5.0, 1),  # 5 km starts the next
        (5.9, 10.0, 1),
        (6.0, 10.0, 1),
        (7.1, 10.0, 1),  # no magnitude and the deleted event are left out
    ]


@pytest.mark.req("REQ-DASH-025")
def test_depth_bins_put_each_magnitude_in_its_own_tenth(tmp_path):
    """2.3 * 10 is 22.999999999999996 in floating point: it still starts at 2.3."""
    dash = Dashboard(
        build(tmp_path, [spot(n, 0, 0, m) for n, m in enumerate([2.3, 2.29, 7.1, -0.43, 4.05], 1)])
    )
    dash.set_filter()
    assert [r["magnitude_from"] for r in dash.query("depth_bins")] == [-0.5, 2.2, 2.3, 4.0, 7.1]
    dash.close()


def random_spots(rng: random.Random, count: int) -> list[dict]:
    def coordinate(limit):
        # Half on cell edges (multiples of 0.25, the ends included), half anywhere.
        if rng.random() < 0.5:
            return rng.randint(-4 * limit, 4 * limit) / 4
        return round(rng.uniform(-limit, limit), 4)

    return [
        spot(
            n,
            coordinate(180),
            coordinate(90),
            rng.choice([None, round(rng.uniform(-1, 8), 2), 6.0, 5.99]),
            review_status="deleted" if rng.random() < 0.1 else "reviewed",
        )  # fmt: skip
        for n in range(1, count + 1)
    ]


@pytest.mark.req("REQ-DASH-025")
@pytest.mark.parametrize("seed", range(20))
def test_cells_strong_events_and_areas_agree_with_every_point(tmp_path, seed):
    """On random events and filters: the cells and strong events hold every point of
    map_points.sql exactly once, in the cell its coordinates fall in, and an area holds exactly
    the points inside it."""
    rng = random.Random(seed)
    dash = Dashboard(build(tmp_path, random_spots(rng, 300)))
    dash.set_filter(
        min_magnitude=rng.choice([None, 1.0]),
        include_deleted=rng.random() < 0.5,
    )
    points = dash.query("map_points")
    strong = {p["key"] for p in points if p["magnitude"] is not None and p["magnitude"] >= 6}
    assert {r["key"] for r in dash.query("map_strong")} == strong

    expected: dict[tuple[int, int], int] = {}
    for p in points:
        if p["key"] not in strong:
            cell = (math.floor(p["longitude"] / 0.25), math.floor(p["latitude"] / 0.25))
            expected[cell] = expected.get(cell, 0) + 1
    cells = {(r["cell_x"], r["cell_y"]): r["events"] for r in dash.query("map_grid")}
    assert cells == expected
    assert sum(cells.values()) + len(strong) == dash.query("summary")[0]["events"]

    for _ in range(10):
        west = rng.uniform(-180, 179.999)
        east = west + rng.choice([rng.uniform(0, 360), rng.uniform(0, 5)])
        south = rng.uniform(-90, 90)
        north = rng.uniform(south, 90)
        dash.set_map_view(west=west, south=south, east=east, north=north)
        inside = {
            p["key"]
            for p in points
            if p["key"] not in strong
            and (p["longitude"] - west) % 360 <= east - west
            and south <= p["latitude"] <= north
        }
        assert {r["key"] for r in dash.query("map_area")} == inside
    dash.close()


@pytest.mark.req("REQ-DASH-031")
def test_the_tsunami_filter_keeps_the_flagged_events(tmp_path):
    rows = [
        event(1, "2024-03-01T00:00:00", 7.1, "Japan", None, tsunami=True),
        event(2, "2024-03-02T00:00:00", 6.4, "Japan", None),
        event(3, "2024-03-03T00:00:00", 6.9, "Chile", None, tsunami=True),
        event(4, "2024-03-04T00:00:00", 5.0, "Chile", None, tsunami=True, review_status="deleted"),
        event(5, "2024-03-05T00:00:00", 2.0, "United States", "Alaska"),
    ]
    dash = Dashboard(build(tmp_path, rows))
    dash.set_filter()
    assert dash.query("summary")[0]["events"] == 4
    dash.set_filter(tsunami_only=True)
    assert [number(p) for p in dash.query("map_points")] == [1, 3]  # the deletion stays out
    (summary,) = dash.query("summary")
    assert summary["events"] == summary["tsunami_flagged"] == 2
    # The menus count under it too, as under every other part of the filter.
    assert {r["country"]: r["events"] for r in dash.query("countries")} == {
        "Chile": 1, "Japan": 1, "United States": 0,
    }  # fmt: skip
    dash.set_filter(tsunami_only=True, include_deleted=True, country="Chile")
    assert [number(p) for p in dash.query("map_points")] == [3, 4]
    dash.close()
