"""The dashboard's SQL (dashboard/queries/) on a fixed dataset, with answers worked by hand.

The page runs these same files in the browser (DuckDB-WASM); here DuckDB runs them on a
snapshot built by the exporter's own code.
"""

from datetime import UTC, datetime

import pytest
from support.analytics import Dashboard
from support.export import make_row

from earth_hub.export.r2 import encode
from earth_hub.export.snapshot import SnapshotFile


def event(row_id, when, magnitude, country, region, **changes):
    moment = datetime.fromisoformat(when).replace(tzinfo=UTC)
    return make_row(
        row_id,
        event_time=moment.isoformat().replace("+00:00", "Z"),
        event_time_ms=int(moment.timestamp() * 1000),
        magnitude=magnitude,
        country=country,
        region=region,
        place=f"place {row_id}",
        **changes,
    )


# 29 February and the midnights around it, a Sunday at the end of an ISO week, bin edges,
# a negative magnitude, missing values, a deletion and another event type.
EVENTS = [
    event(1, "2024-02-28T23:59:59.999", 2.49, "United States", "Alaska"),
    event(2, "2024-02-29T00:00:00", 2.5, "United States", "Alaska"),
    event(3, "2024-02-29T12:00:00", 4.5, "Mexico", "Baja California"),
    event(4, "2024-03-01T00:00:00", None, None, None),
    event(5, "2024-03-04T06:00:00", -0.43, "United States", "California"),
    event(6, "2024-03-10T23:00:00", 5.2, "Japan", None),  # a Sunday
    event(7, "2024-03-10T08:00:00", 3.0, "United States", "Alaska", review_status="deleted"),
    event(8, "2024-04-02T10:00:00", 1.0, "United States", "Alaska", event_type="quarry blast"),
]


def build(tmp_path, rows):
    snapshot = SnapshotFile()
    try:
        for row in rows:
            snapshot.add(encode(row))
        path = tmp_path / "earthquakes.parquet"
        path.write_bytes(snapshot.build())
    finally:
        snapshot.close()
    return path


@pytest.fixture
def dash(tmp_path):
    dashboard = Dashboard(build(tmp_path, EVENTS))
    yield dashboard
    dashboard.close()


def ids(dash, **filters):
    dash.set_filter(**filters)
    return [p["id"] for p in dash.query("map_points")]


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
        ({"max_magnitude": 2.5}, [1, 2, 5, 8]),
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
    assert [r["id"] for r in dash.query("significant")] == [6, 3]  # 4.5 is included
    dash.set_filter(significant_magnitude=5.0)
    assert [r["id"] for r in dash.query("significant")] == [6]


@pytest.mark.req("REQ-DASH-005")
def test_map_points_carry_what_the_map_shows(dash):
    dash.set_filter()
    point = dash.query("map_points")[0]
    assert point == {
        "id": 1,
        "longitude": -155.6123,
        "latitude": 57.4401,
        "depth_km": 10.0,
        "magnitude": 2.49,
        "event_time_ms": EVENTS[0]["event_time_ms"],
        "place": "place 1",
        "title": EVENTS[0]["title"],
        "url": EVENTS[0]["url"],
        "review_status": "reviewed",
    }


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
def test_an_invalid_filter_is_rejected(dash):
    with pytest.raises(Exception, match="CHECK"):
        dash.set_filter(time_bucket="year")
