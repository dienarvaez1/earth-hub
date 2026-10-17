"""GET /api/v1/earthquakes/{event_id}, including former IDs of merged events."""

import pytest

pytestmark = pytest.mark.integration


def test_lookup_by_current_id(client):
    response = client.get("/api/v1/earthquakes/us02")
    assert response.status_code == 200
    body = response.json()
    assert body["event_id"] == "us02" and body["magnitude"] == 6.1 and body["ids"] == ["us02"]


def test_former_id_resolves_to_the_merged_event(client):
    old = client.get("/api/v1/earthquakes/ak20").json()
    current = client.get("/api/v1/earthquakes/us20").json()
    assert old == current
    assert current["event_id"] == "us20"
    assert current["ids"] == ["ak20", "us20"]
    assert (current["magnitude"], current["mag_type"]) == (3.4, "mww")  # the merging revision
    assert current["revision_count"] == 3


def test_deleted_event_is_still_found_by_id(client):
    body = client.get("/api/v1/earthquakes/us18").json()
    assert body["review_status"] == "deleted"


@pytest.mark.parametrize("event_id", ["nope", "US01", "us01 ", "%25", "us0*"])
def test_unknown_id_is_404_in_the_error_format(client, event_id):
    response = client.get(f"/api/v1/earthquakes/{event_id}")
    assert response.status_code == 404
    error = response.json()["error"]
    assert error["code"] == "not_found" and error["details"] == []


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/earthquakes/stats",
        "/api/v1/earthquakes/summary/hourly",
        "/api/v1/earthquakes/summary/magnitude",
    ],
)
def test_fixed_paths_are_not_taken_for_event_ids(client, path):
    response = client.get(path)
    assert response.status_code == 200
    assert "event_id" not in response.json()


def test_overlong_id_is_rejected(client):
    response = client.get("/api/v1/earthquakes/" + "x" * 101)
    assert response.status_code == 422
    assert response.json()["error"]["details"][0]["parameter"] == "event_id"
