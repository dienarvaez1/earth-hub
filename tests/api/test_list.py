"""GET /api/v1/earthquakes: pagination, sorting and filters over the fixed dataset."""

from datetime import timedelta

import pytest

from .dataset import EVENT_COUNT, T0, VISIBLE, VISIBLE_COUNT

pytestmark = pytest.mark.integration

URL = "/api/v1/earthquakes"


def get(client, **params):
    response = client.get(URL, params=params)
    assert response.status_code == 200, response.text
    return response.json()


def ids(body) -> list[str]:
    return [item["event_id"] for item in body["items"]]


def all_items(client, **params) -> list[dict]:
    return get(client, limit=500, **params)["items"]


# --- shape and defaults ----------------------------------------------------------------


@pytest.mark.req("REQ-API-002", "REQ-API-013")
def test_default_page(client):
    body = get(client)
    assert (body["total"], body["limit"], body["offset"]) == (VISIBLE_COUNT, 20, 0)
    assert len(body["items"]) == VISIBLE_COUNT  # 19 visible events fit on one page
    times = [item["event_time"] for item in body["items"]]
    assert times == sorted(times, reverse=True)  # newest first by default


def test_item_fields_and_utc_times(client):
    [item] = [i for i in all_items(client) if i["event_id"] == "us01"]
    assert item["magnitude"] == 4.4 and item["mag_type"] == "mb"
    assert (item["longitude"], item["latitude"], item["depth_km"]) == (139.7, 34.7, 35.0)
    assert item["event_time"] == "2026-10-10T00:15:00Z"
    assert item["ids"] == ["us01"]
    assert item["review_status"] == "reviewed" and item["tsunami"] is False


@pytest.mark.req("REQ-API-009")
def test_deleted_events_are_hidden_unless_asked_for(client):
    assert "us18" not in ids(get(client, limit=500))
    with_deleted = get(client, limit=500, include_deleted=True)
    assert with_deleted["total"] == EVENT_COUNT and "us18" in ids(with_deleted)
    only_deleted = get(client, review_status="deleted")
    assert ids(only_deleted) == ["us18"]


# --- pagination ------------------------------------------------------------------------


@pytest.mark.req("REQ-API-002")
def test_pages_cover_everything_once_with_no_overlap(client):
    seen = []
    for offset in range(0, VISIBLE_COUNT, 4):
        page = get(client, limit=4, offset=offset)
        assert page["total"] == VISIBLE_COUNT
        seen += ids(page)
    assert len(seen) == len(set(seen)) == VISIBLE_COUNT
    assert seen == ids(get(client, limit=500))


@pytest.mark.req("REQ-API-002")
@pytest.mark.parametrize(
    ("limit", "offset", "expected"),
    [
        (1, 0, 1),
        (500, 0, VISIBLE_COUNT),
        (5, VISIBLE_COUNT - 2, 2),
        (5, VISIBLE_COUNT, 0),
        (5, VISIBLE_COUNT + 100, 0),
    ],
)
def test_page_boundaries(client, limit, offset, expected):
    body = get(client, limit=limit, offset=offset)
    assert len(body["items"]) == expected and body["total"] == VISIBLE_COUNT


# --- sorting ---------------------------------------------------------------------------


@pytest.mark.req("REQ-API-004")
@pytest.mark.parametrize(
    ("sort", "field"),
    [("event_time", "event_time"), ("magnitude", "magnitude"), ("depth", "depth_km")],
)
@pytest.mark.parametrize("order", ["asc", "desc"])
def test_sorting(client, sort, field, order):
    items = all_items(client, sort=sort, order=order)
    present = [i[field] for i in items if i[field] is not None]
    assert present == sorted(present, reverse=order == "desc")
    # Missing values always come last.
    nulls = [i for i in items if i[field] is None]
    assert items[len(items) - len(nulls) :] == nulls


@pytest.mark.req("REQ-API-004")
def test_missing_magnitude_sorts_last_in_both_directions(client):
    for order in ("asc", "desc"):
        assert ids(get(client, limit=500, sort="magnitude", order=order))[-1] == "us16"


@pytest.mark.req("REQ-API-004")
def test_ties_break_by_insertion_order_and_are_stable(client):
    # us01 and us17 share magnitude 4.4 and event time; us01 was stored first.
    for order in ("asc", "desc"):
        for sort in ("magnitude", "event_time"):
            listed = ids(get(client, limit=500, sort=sort, order=order))
            assert listed.index("us01") < listed.index("us17")
    page_ids = [ids(get(client, limit=1, offset=o, sort="magnitude")) for o in range(VISIBLE_COUNT)]
    assert [i for [i] in page_ids] == ids(get(client, limit=500, sort="magnitude"))


# --- filters ---------------------------------------------------------------------------


def expected(predicate) -> set[str]:
    return {s.event_id for s in VISIBLE if predicate(s)}


@pytest.mark.req("REQ-API-003")
@pytest.mark.parametrize(
    ("params", "predicate"),
    [
        ({"min_magnitude": 4.4}, lambda s: s.mag is not None and s.mag >= 4.4),
        ({"max_magnitude": 1.0}, lambda s: s.mag is not None and s.mag <= 1.0),
        (
            {"min_magnitude": 2, "max_magnitude": 5},
            lambda s: s.mag is not None and 2 <= s.mag <= 5,
        ),
        ({"min_magnitude": -2}, lambda s: s.mag is not None),
        ({"event_type": "explosion"}, lambda s: s.event_type == "explosion"),
        ({"event_type": "quarry blast"}, lambda s: s.event_type == "quarry blast"),
        ({"review_status": "automatic"}, lambda s: s.status == "automatic"),
        ({"place": "anchorage"}, lambda s: s.place and "anchorage" in s.place.lower()),
        ({"place": "CALIFORNIA"}, lambda s: s.place and "california" in s.place.lower()),
    ],
    ids=[
        "min-mag",
        "max-mag",
        "mag-range",
        "min-mag-excludes-unknown",
        "explosion",
        "quarry-blast",
        "automatic",
        "place-lower",
        "place-upper",
    ],
)
def test_filters(client, params, predicate):
    assert set(ids(get(client, limit=500, **params))) == expected(predicate)


@pytest.mark.req("REQ-API-003")
def test_time_window_is_start_inclusive_end_exclusive(client):
    # ci06 and ci07 happen exactly at T0 + 5 h.
    at = (T0 + timedelta(hours=5)).isoformat()
    assert {"ci06", "ci07"} <= set(ids(get(client, limit=500, start_time=at)))
    assert not {"ci06", "ci07"} & set(ids(get(client, limit=500, end_time=at)))


@pytest.mark.req("REQ-API-003")
def test_time_without_timezone_is_utc(client):
    with_z = ids(get(client, limit=500, start_time="2026-10-10T05:00:00Z"))
    naive = ids(get(client, limit=500, start_time="2026-10-10T05:00:00"))
    other_zone = ids(get(client, limit=500, start_time="2026-10-10T07:00:00+02:00"))
    assert with_z == naive == other_zone


@pytest.mark.req("REQ-API-003")
def test_combined_filters(client):
    body = get(
        client,
        min_magnitude=1,
        event_type="earthquake",
        start_time=(T0 + timedelta(hours=1)).isoformat(),
        end_time=(T0 + timedelta(hours=16)).isoformat(),
    )
    assert set(ids(body)) == {"us02", "ak03", "ak04", "nc08", "hv11", "us12", "us13", "us14"}


@pytest.mark.req("REQ-API-003")
@pytest.mark.parametrize(
    ("place", "expected_ids"),
    [
        ("50%", {"hv11"}),  # % is literal, not a wildcard
        ("Quarry_", {"uw10"}),  # _ is literal, not a single-character wildcard
        ("%", {"hv11"}),
        ("_", {"uw10"}),
        ("'; DROP TABLE earthquakes; --", set()),
        ("\\", set()),
    ],
)
def test_place_matches_special_characters_literally(client, place, expected_ids):
    assert set(ids(get(client, limit=500, place=place))) == expected_ids
    assert get(client)["total"] == VISIBLE_COUNT  # and nothing was harmed


def test_no_matches_is_an_empty_page(client):
    body = get(client, place="Atlantis")
    assert body == {"items": [], "total": 0, "limit": 20, "offset": 0}
