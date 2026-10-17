"""Statistics and summaries, each checked against an independent SQL query."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from .dataset import API_NOW, T0, VISIBLE_COUNT

pytestmark = pytest.mark.integration

VISIBLE = "review_status <> 'deleted'"


def sql(engine, query, **params):
    with engine.connect() as conn:
        return conn.execute(text(query), params).all()


# --- stats -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("params", "where"),
    [
        ({}, VISIBLE),
        ({"event_type": "earthquake"}, f"{VISIBLE} AND event_type = 'earthquake'"),
        ({"min_magnitude": 3}, f"{VISIBLE} AND magnitude >= 3"),
        ({"include_deleted": True}, "true"),
        (
            {"start_time": (T0 + timedelta(hours=10)).isoformat()},
            f"{VISIBLE} AND event_time >= :t",
        ),
    ],
)
def test_stats_match_sql(client, seeded_engine, params, where):
    body = client.get("/api/v1/earthquakes/stats", params=params).json()
    t = T0 + timedelta(hours=10)
    [(count, low, high, avg)] = sql(
        seeded_engine,
        "SELECT count(*), min(magnitude), max(magnitude), avg(magnitude) "
        f"FROM earthquakes WHERE {where}",
        t=t,
    )
    assert body["count"] == count
    assert body["magnitude"] == {
        "min": low,
        "max": high,
        "avg": pytest.approx(float(avg), abs=1e-4),
    }
    by_type = dict(
        sql(
            seeded_engine,
            f"SELECT event_type, count(*) FROM earthquakes WHERE {where} GROUP BY 1",
            t=t,
        )
    )
    assert body["by_event_type"] == by_type
    by_mag_type = dict(
        sql(
            seeded_engine,
            "SELECT coalesce(mag_type, 'unknown'), count(*) "
            f"FROM earthquakes WHERE {where} GROUP BY 1",
            t=t,
        )
    )
    assert body["by_magnitude_type"] == by_mag_type


def test_stats_known_values(client):
    body = client.get("/api/v1/earthquakes/stats").json()
    assert body["count"] == VISIBLE_COUNT
    assert body["magnitude"]["min"] == -0.5 and body["magnitude"]["max"] == 8.2
    assert body["by_event_type"] == {"earthquake": 16, "explosion": 2, "quarry blast": 1}
    assert body["by_magnitude_type"]["unknown"] == 1


def test_stats_with_no_matches(client):
    body = client.get("/api/v1/earthquakes/stats", params={"place": "Atlantis"}).json()
    assert body == {"count": 0, "magnitude": None, "by_event_type": {}, "by_magnitude_type": {}}


def test_stats_when_only_unknown_magnitudes_match(client):
    body = client.get(
        "/api/v1/earthquakes/stats",
        params={"start_time": "2026-10-10T22:00:00Z", "end_time": "2026-10-10T22:30:00Z"},
    ).json()
    assert body["count"] == 1 and body["magnitude"] is None


# --- hourly summary --------------------------------------------------------------------


def test_default_hourly_window_is_the_last_24_hours_with_zeros(client, seeded_engine):
    body = client.get("/api/v1/earthquakes/summary/hourly").json()
    # API_NOW is 23:30 on day one, so the window is T0 .. T0 + 24 h.
    assert body["start_time"] == "2026-10-10T00:00:00Z"
    assert body["end_time"] == "2026-10-11T00:00:00Z"
    assert len(body["buckets"]) == 24
    assert [b["hour"] for b in body["buckets"]] == [
        (T0 + timedelta(hours=h)).isoformat().replace("+00:00", "Z") for h in range(24)
    ]
    expected = dict(
        sql(
            seeded_engine,
            "SELECT date_trunc('hour', event_time AT TIME ZONE 'UTC'), count(*) FROM earthquakes "
            f"WHERE {VISIBLE} AND event_time >= :s AND event_time < :e GROUP BY 1",
            s=T0,
            e=T0 + timedelta(hours=24),
        )
    )
    for bucket in body["buckets"]:
        hour = datetime.fromisoformat(bucket["hour"]).replace(tzinfo=None)
        assert bucket["count"] == expected.get(hour, 0), bucket
    assert sum(b["count"] for b in body["buckets"]) == sum(expected.values())
    assert any(b["count"] == 0 for b in body["buckets"])  # empty hours are present


def test_hourly_counts_known_values(client):
    buckets = client.get("/api/v1/earthquakes/summary/hourly").json()["buckets"]
    by_hour = {datetime.fromisoformat(b["hour"]).hour: b["count"] for b in buckets}
    assert by_hour[0] == 2  # us01 and us17
    assert by_hour[5] == 2  # ci06 and ci07
    assert by_hour[23] == 0  # us18 is deleted


def test_hourly_explicit_range_and_partial_hours(client):
    body = client.get(
        "/api/v1/earthquakes/summary/hourly",
        params={"start_time": "2026-10-10T01:30:00Z", "end_time": "2026-10-10T03:15:00Z"},
    ).json()
    hours = [b["hour"] for b in body["buckets"]]
    assert hours == ["2026-10-10T01:00:00Z", "2026-10-10T02:00:00Z", "2026-10-10T03:00:00Z"]
    # us02 at 01:30 is inside (inclusive start); ak03 at 01:45 too; ak04 at 03:00 too.
    assert [b["count"] for b in body["buckets"]] == [2, 0, 1]


def test_hourly_respects_other_filters(client):
    body = client.get(
        "/api/v1/earthquakes/summary/hourly", params={"event_type": "explosion"}
    ).json()
    assert sum(b["count"] for b in body["buckets"]) == 1  # uw09; nn19 is outside the window


@pytest.mark.parametrize(
    ("params", "parameter"),
    [
        ({"start_time": "2026-09-01T00:00:00Z", "end_time": "2026-10-10T00:00:00Z"}, "end_time"),
        ({"start_time": "2026-10-12T00:00:00Z", "end_time": "2026-10-11T00:00:00Z"}, "start_time"),
    ],
)
def test_hourly_range_limits(client, params, parameter):
    response = client.get("/api/v1/earthquakes/summary/hourly", params=params)
    assert response.status_code == 422
    assert response.json()["error"]["details"][0]["parameter"] == parameter


def test_hourly_31_days_is_allowed(client):
    body = client.get(
        "/api/v1/earthquakes/summary/hourly",
        params={"start_time": "2026-09-10T00:00:00Z", "end_time": "2026-10-11T00:00:00Z"},
    ).json()
    assert len(body["buckets"]) == 31 * 24


# --- magnitude summary -----------------------------------------------------------------

LABELS = ["<0", "0-1", "1-2", "2-3", "3-4", "4-5", "5-6", "6-7", ">=7", "unknown"]


def test_magnitude_buckets_match_sql(client, seeded_engine):
    buckets = client.get("/api/v1/earthquakes/summary/magnitude").json()["buckets"]
    assert [b["label"] for b in buckets] == LABELS
    for bucket in buckets:
        if bucket["label"] == "unknown":
            where = "magnitude IS NULL"
        else:
            parts = []
            if bucket["min"] is not None:
                parts.append(f"magnitude >= {bucket['min']}")
            if bucket["max"] is not None:
                parts.append(f"magnitude < {bucket['max']}")
            where = " AND ".join(parts)
        [(count,)] = sql(
            seeded_engine, f"SELECT count(*) FROM earthquakes WHERE {VISIBLE} AND {where}"
        )
        assert bucket["count"] == count, bucket
    assert sum(b["count"] for b in buckets) == VISIBLE_COUNT


def test_magnitude_bucket_edges(client):
    counts = {
        b["label"]: b["count"]
        for b in client.get("/api/v1/earthquakes/summary/magnitude").json()["buckets"]
    }
    # 1.0 (nc08) is in 1-2, 6.99 (us13) in 6-7, 7.0 (us14) in >=7, -0.5 (ci05) in <0.
    assert counts == {
        "<0": 1,
        "0-1": 2,
        "1-2": 3,
        "2-3": 3,
        "3-4": 2,
        "4-5": 2,
        "5-6": 1,
        "6-7": 2,
        ">=7": 2,
        "unknown": 1,
    }


def test_magnitude_bounds_are_described(client):
    buckets = client.get("/api/v1/earthquakes/summary/magnitude").json()["buckets"]
    assert buckets[0] == {"label": "<0", "min": None, "max": 0.0, "count": 1}
    assert buckets[8]["min"] == 7.0 and buckets[8]["max"] is None


# --- empty database --------------------------------------------------------------------


def test_empty_database_gives_empty_and_zero_results(empty_client):
    assert empty_client.get("/api/v1/earthquakes").json() == {
        "items": [],
        "total": 0,
        "limit": 20,
        "offset": 0,
    }
    assert empty_client.get("/api/v1/earthquakes/stats").json() == {
        "count": 0,
        "magnitude": None,
        "by_event_type": {},
        "by_magnitude_type": {},
    }
    hourly = empty_client.get("/api/v1/earthquakes/summary/hourly").json()
    assert len(hourly["buckets"]) == 24 and all(b["count"] == 0 for b in hourly["buckets"])
    magnitude = empty_client.get("/api/v1/earthquakes/summary/magnitude").json()
    assert [b["label"] for b in magnitude["buckets"]] == LABELS
    assert all(b["count"] == 0 for b in magnitude["buckets"])
    assert empty_client.get("/api/v1/earthquakes/us01").status_code == 404


def test_health_reports_latest_event(client, empty_client):
    body = client.get("/api/v1/health").json()
    assert body["status"] == body["database"] == "ok"
    assert body["latest_event_time"] == "2026-10-11T02:00:00Z"  # ak20/us20 at T0 + 26 h
    assert empty_client.get("/api/v1/health").json()["latest_event_time"] is None


def test_api_now_is_used_for_defaults():
    assert API_NOW.tzinfo is UTC


def test_hourly_window_anchors_on_the_one_time_given(client):
    # Found by Schemathesis: an old start_time alone used to run to "now" and exceed 31 days.
    only_start = client.get(
        "/api/v1/earthquakes/summary/hourly", params={"start_time": "2000-01-01T00:00:00Z"}
    )
    assert only_start.status_code == 200
    body = only_start.json()
    assert (body["start_time"], body["end_time"]) == (
        "2000-01-01T00:00:00Z",
        "2000-01-02T00:00:00Z",
    )
    assert len(body["buckets"]) == 24

    only_end = client.get(
        "/api/v1/earthquakes/summary/hourly", params={"end_time": "2026-10-10T06:00:00Z"}
    ).json()
    assert (only_end["start_time"], only_end["end_time"]) == (
        "2026-10-09T06:00:00Z",
        "2026-10-10T06:00:00Z",
    )
