"""API behaviour that needs no database: validation, error format, and a database that is down.

The engine points at a closed port, so any test that reached the database would get a 503;
a 422 therefore also proves validation happens before connecting.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

from earth_hub.api.main import create_app


@pytest.fixture(scope="module")
def client():
    engine = create_engine(
        "postgresql+psycopg://nobody:secret@127.0.0.1:1/none", connect_args={"connect_timeout": 1}
    )
    with TestClient(create_app(engine), raise_server_exceptions=False) as client:
        yield client
    engine.dispose()


def assert_invalid(response, *parameters):
    assert response.status_code == 422, response.text
    error = response.json()["error"]
    assert error["code"] == "invalid_parameters"
    assert error["message"] == "one or more parameters are invalid"
    assert {d["parameter"] for d in error["details"]} >= set(parameters)
    assert all(d["message"] for d in error["details"])


@pytest.mark.parametrize(
    ("params", "parameter"),
    [
        ({"limit": 0}, "limit"),
        ({"limit": 501}, "limit"),
        ({"limit": "ten"}, "limit"),
        ({"offset": -1}, "offset"),
        ({"sort": "place"}, "sort"),
        ({"sort": "magnitude; DROP TABLE earthquakes"}, "sort"),
        ({"order": "up"}, "order"),
        ({"min_magnitude": "big"}, "min_magnitude"),
        ({"min_magnitude": -2.1}, "min_magnitude"),
        ({"max_magnitude": 10.6}, "max_magnitude"),
        ({"min_magnitude": 5, "max_magnitude": 4}, "min_magnitude"),
        ({"start_time": "yesterday"}, "start_time"),
        ({"end_time": "2026-13-01T00:00:00Z"}, "end_time"),
        ({"start_time": "2026-10-10T05:00:00Z", "end_time": "2026-10-10T05:00:00Z"}, "start_time"),
        ({"start_time": "2026-10-10T06:00:00Z", "end_time": "2026-10-10T05:00:00Z"}, "start_time"),
        ({"place": ""}, "place"),
        ({"place": "x" * 201}, "place"),
        ({"event_type": ""}, "event_type"),
        ({"review_status": "manual"}, "review_status"),
        ({"include_deleted": "perhaps"}, "include_deleted"),
    ],
)
def test_invalid_list_parameters(client, params, parameter):
    assert_invalid(client.get("/api/v1/earthquakes", params=params), parameter)


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/earthquakes/stats",
        "/api/v1/earthquakes/summary/hourly",
        "/api/v1/earthquakes/summary/magnitude",
    ],
)
def test_aggregates_validate_filters_too(client, path):
    assert_invalid(client.get(path, params={"min_magnitude": 11}), "min_magnitude")


def test_several_problems_are_reported_together(client):
    response = client.get("/api/v1/earthquakes", params={"limit": 0, "offset": -5, "order": "x"})
    assert_invalid(response, "limit", "offset", "order")


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/earthquakes",
        "/api/v1/earthquakes/us01",
        "/api/v1/earthquakes/stats",
        "/api/v1/earthquakes/summary/hourly",
        "/api/v1/earthquakes/summary/magnitude",
    ],
)
def test_database_down_is_503_in_the_error_format(client, path):
    response = client.get(path)
    assert response.status_code == 503
    assert response.json() == {
        "error": {
            "code": "database_unavailable",
            "message": "the database is unavailable",
            "details": [],
        }
    }


def test_health_is_503_when_database_is_down(client):
    response = client.get("/api/v1/health")
    assert response.status_code == 503
    body = response.json()
    assert (body["status"], body["database"], body["latest_event_time"]) == (
        "unavailable",
        "unavailable",
        None,
    )


def test_no_internals_or_credentials_leak_in_errors(client):
    for path in ("/api/v1/earthquakes", "/api/v1/health"):
        text = client.get(path).text
        for secret in ("secret", "nobody", "127.0.0.1", "psycopg", "Traceback", "SELECT"):
            assert secret not in text


def test_unknown_route_and_method_use_the_error_format(client):
    missing = client.get("/api/v2/earthquakes")
    assert missing.status_code == 404 and missing.json()["error"]["code"] == "not_found"
    wrong_method = client.post("/api/v1/earthquakes")
    assert wrong_method.status_code == 405
    assert wrong_method.json()["error"]["code"] == "method_not_allowed"


def test_unexpected_error_is_a_generic_500(client, monkeypatch):
    from earth_hub.api import services

    def boom(*args, **kwargs):
        raise RuntimeError("SELECT secret FROM internals")

    monkeypatch.setattr(services, "list_earthquakes", boom)
    monkeypatch.setattr(client.app.state.engine, "connect", _fake_connect)
    response = client.get("/api/v1/earthquakes")
    assert response.status_code == 500
    assert response.json()["error"] == {
        "code": "internal_error",
        "message": "internal server error",
        "details": [],
    }
    assert "internals" not in response.text


class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _fake_connect():
    return _FakeConnection()


def test_openapi_documents_every_endpoint_and_error(client):
    spec = client.get("/openapi.json").json()
    assert set(spec["paths"]) == {
        "/api/v1/health",
        "/api/v1/earthquakes",
        "/api/v1/earthquakes/stats",
        "/api/v1/earthquakes/summary/hourly",
        "/api/v1/earthquakes/summary/magnitude",
        "/api/v1/earthquakes/{event_id}",
    }
    for path, operations in spec["paths"].items():
        responses = operations["get"]["responses"]
        assert "200" in responses, path
        assert "503" in responses, path
        if path != "/api/v1/health":
            assert {"404", "422"} <= set(responses), path
    params = {p["name"] for p in spec["paths"]["/api/v1/earthquakes"]["get"]["parameters"]}
    assert params == {
        "min_magnitude",
        "max_magnitude",
        "start_time",
        "end_time",
        "place",
        "event_type",
        "review_status",
        "include_deleted",
        "limit",
        "offset",
        "sort",
        "order",
    }
    assert spec["info"]["title"] == "Earth-Hub API"


def test_docs_page_is_served(client):
    assert client.get("/docs").status_code == 200


# --- regressions for defects found by Schemathesis ---------------------------------------


@pytest.mark.parametrize("value", ["0.5", "1792137522341", "2026-10-10", "10:00", "now"])
def test_times_must_be_iso_date_times_not_numbers_or_dates(client, value):
    # Pydantic's lax mode accepted 0.5 as a Unix timestamp; the contract says date-time.
    assert_invalid(client.get("/api/v1/earthquakes", params={"start_time": value}), "start_time")


@pytest.mark.parametrize(
    ("path", "params", "parameter"),
    [
        ("/api/v1/earthquakes", {"place": "a\x00b"}, "place"),
        ("/api/v1/earthquakes", {"event_type": "\x00"}, "event_type"),
        ("/api/v1/earthquakes/us%0001", {}, "event_id"),
    ],
)
def test_nul_characters_are_rejected_not_a_500(client, path, params, parameter):
    # PostgreSQL text cannot contain NUL, so this used to fail inside the database.
    assert_invalid(client.get(path, params=params), parameter)


def test_405_includes_the_allow_header(client):
    response = client.request("TRACE", "/api/v1/earthquakes")
    assert response.status_code == 405
    assert response.headers["allow"] == "GET"


@pytest.mark.parametrize(
    "params",
    [
        {"end_time": "0001-01-01T00:00:00Z"},
        {"start_time": "9999-12-31T23:00:00Z"},
        {"start_time": "1899-12-31T23:59:59Z"},
        {"end_time": "2100-01-01T00:00:00Z"},
    ],
)
def test_times_outside_1900_to_2099_are_rejected_not_a_500(client, params):
    # Found by Schemathesis: "24 hours before 0001-01-01" overflowed datetime.
    [name] = params
    assert_invalid(client.get("/api/v1/earthquakes/summary/hourly", params=params), name)


@pytest.mark.parametrize("offset", [10_000_001, 2**63, 1192528022417137467392])
def test_offsets_beyond_the_cap_are_rejected_not_a_500(client, offset):
    # Found by Schemathesis: an offset beyond bigint failed inside PostgreSQL.
    assert_invalid(client.get("/api/v1/earthquakes", params={"offset": offset}), "offset")
