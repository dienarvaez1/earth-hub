import json
import math
from datetime import UTC, datetime, timedelta

import pytest

from earth_hub.validation.earthquake_rules import Rejected, ms_to_datetime, validate

NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)


@pytest.fixture
def envelope(make_envelope, feed_doc):
    """The first fixture feature as the decoded message the consumer receives."""
    return json.loads(make_envelope(feed_doc["features"][0]))


def rejected_rules(envelope) -> list[str]:
    with pytest.raises(Rejected) as excinfo:
        validate(envelope, now=NOW)
    return [v.rule for v in excinfo.value.violations]


def set_time(envelope, time_ms, updated_ms=None):
    updated_ms = time_ms if updated_ms is None else updated_ms
    envelope["event_time_ms"] = envelope["payload"]["properties"]["time"] = time_ms
    envelope["source_updated_ms"] = envelope["payload"]["properties"]["updated"] = updated_ms


def test_every_fixture_event_is_valid(make_envelope, feed_doc):
    for feature in feed_doc["features"]:
        event = validate(json.loads(make_envelope(feature)), now=NOW)
        assert event.event_id == feature["id"]


def test_normalized_fields(envelope):
    feature = envelope["payload"]
    props, (lon, lat, depth) = feature["properties"], feature["geometry"]["coordinates"]
    fields = validate(envelope, now=NOW).fields

    # GeoJSON is [longitude, latitude, depth]; swapping them is a classic bug.
    assert (fields["longitude"], fields["latitude"], fields["depth_km"]) == (lon, lat, depth)
    assert fields["event_time"] == datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
        milliseconds=props["time"]
    )
    assert fields["event_time"].tzinfo is UTC
    assert fields["source_updated_ms"] == props["updated"]
    assert fields["magnitude"] == props["mag"]
    assert fields["mag_type"] == props["magType"]
    assert fields["event_type"] == "earthquake"
    assert fields["review_status"] == props["status"]
    assert fields["tsunami"] is False


def test_millisecond_conversion_is_exact():
    # 1792137522341 ms: float division would produce .341000 or .340999 µs.
    assert ms_to_datetime(1792137522341).microsecond == 341000


@pytest.mark.req("REQ-DQ-ACC-001")
def test_edge_case_values_are_accepted(make_envelope, feed_doc):
    by_id = {f["id"]: validate(json.loads(make_envelope(f)), now=NOW) for f in feed_doc["features"]}
    assert by_id["av94451518"].fields["magnitude"] == -0.34
    assert by_id["uw714120332"].fields["depth_km"] == -0.36
    assert by_id["uw714120332"].fields["event_type"] == "explosion"


def test_ids_always_include_the_preferred_id_once(envelope):
    envelope["ids"] = ["ak1", envelope["event_id"], "ak1"]
    assert validate(envelope, now=NOW).ids == (envelope["event_id"], "ak1")


# --- envelope rules --------------------------------------------------------------------


@pytest.mark.req("REQ-DQ-ENV-001")
@pytest.mark.parametrize("value", [None, [], "text", 42], ids=["null", "list", "str", "int"])
def test_non_object_is_env_001(value):
    assert rejected_rules(value) == ["ENV-001"]


@pytest.mark.req("REQ-DQ-ENV-002")
@pytest.mark.parametrize("version", [None, 2, "1"])
def test_unsupported_schema_version_is_env_002(envelope, version):
    envelope["schema_version"] = version
    assert rejected_rules(envelope) == ["ENV-002"]


@pytest.mark.req("REQ-DQ-ENV-003")
@pytest.mark.parametrize(
    "mutate",
    [
        lambda e: e.pop("event_id"),
        lambda e: e.update(event_id=""),
        lambda e: e.update(ids=[]),
        lambda e: e.update(ids="us1"),
        lambda e: e.update(ids=["us1", 2]),
        lambda e: e.update(source_updated_ms="1"),
        lambda e: e.update(event_time_ms=True),
        lambda e: e.update(payload=None),
    ],
    ids=[
        "no-id",
        "empty-id",
        "no-ids",
        "ids-str",
        "ids-int",
        "updated-str",
        "time-bool",
        "payload",
    ],
)
def test_malformed_envelope_is_env_003(envelope, mutate):
    mutate(envelope)
    assert set(rejected_rules(envelope)) == {"ENV-003"}


@pytest.mark.req("REQ-DQ-ENV-004")
def test_payload_id_mismatch_is_env_004(envelope):
    envelope["payload"]["id"] = "someone-else"
    assert rejected_rules(envelope) == ["ENV-004"]


@pytest.mark.req("REQ-DQ-ENV-003")
def test_missing_properties_is_env_003(envelope):
    envelope["payload"]["properties"] = None
    assert rejected_rules(envelope) == ["ENV-003"]


@pytest.mark.req("REQ-DQ-ENV-005")
def test_envelope_and_payload_timestamps_must_agree_env_005(envelope):
    envelope["source_updated_ms"] += 1
    assert rejected_rules(envelope) == ["ENV-005"]


# --- payload rules ---------------------------------------------------------------------


def coords(envelope, lon=None, lat=None, depth=None):
    c = envelope["payload"]["geometry"]["coordinates"]
    envelope["payload"]["geometry"]["coordinates"] = [
        c[0] if lon is None else lon,
        c[1] if lat is None else lat,
        c[2] if depth is None else depth,
    ]


@pytest.mark.req("REQ-DQ-GEO-001")
@pytest.mark.parametrize(
    "mutate",
    [
        lambda e: e["payload"].update(geometry=None),
        lambda e: e["payload"]["geometry"].update(type="LineString"),
        lambda e: e["payload"]["geometry"].update(coordinates=[1.0, 2.0]),
        lambda e: e["payload"]["geometry"].update(coordinates="1,2,3"),
        lambda e: e["payload"]["geometry"].update(coordinates=["1", 2.0, 3.0]),
        lambda e: e["payload"]["geometry"].update(coordinates=[math.nan, 2.0, 3.0]),
        lambda e: e["payload"]["geometry"].update(coordinates=[math.inf, 2.0, 3.0]),
        lambda e: e["payload"]["geometry"].update(coordinates=[True, 2.0, 3.0]),
    ],
    ids=["none", "not-point", "two-coords", "string", "string-coord", "nan", "inf", "bool"],
)
def test_bad_geometry_is_geo_001(envelope, mutate):
    mutate(envelope)
    assert rejected_rules(envelope) == ["GEO-001"]


@pytest.mark.req("REQ-DQ-GEO-002", "REQ-DQ-GEO-003", "REQ-DQ-GEO-004")
@pytest.mark.parametrize(
    ("kwargs", "rule"),
    [
        ({"lat": 90.0001}, "GEO-002"),
        ({"lat": -90.0001}, "GEO-002"),
        ({"lon": 180.0001}, "GEO-003"),
        ({"lon": -180.0001}, "GEO-003"),
        ({"depth": -10.0001}, "GEO-004"),
        ({"depth": 1000.0001}, "GEO-004"),
    ],
)
def test_coordinates_out_of_range(envelope, kwargs, rule):
    coords(envelope, **kwargs)
    assert rejected_rules(envelope) == [rule]


@pytest.mark.req("REQ-DQ-GEO-002", "REQ-DQ-GEO-003", "REQ-DQ-GEO-004")
@pytest.mark.parametrize(
    "kwargs",
    [
        {"lat": 90.0},
        {"lat": -90.0},
        {"lon": 180.0},
        {"lon": -180.0},
        {"depth": -10.0},
        {"depth": 1000.0},
        {"depth": 0},
    ],
)
def test_coordinate_boundaries_are_inclusive(envelope, kwargs):
    coords(envelope, **kwargs)
    validate(envelope, now=NOW)


@pytest.mark.req("REQ-DQ-TIME-001")
def test_event_before_1900_is_time_001(envelope):
    before_1900 = int(datetime(1899, 12, 31, 23, 59, tzinfo=UTC).timestamp() * 1000)
    set_time(envelope, before_1900)
    assert rejected_rules(envelope) == ["TIME-001"]


@pytest.mark.req("REQ-DQ-TIME-002")
def test_event_more_than_an_hour_ahead_is_time_002(envelope):
    set_time(envelope, NOW_MS + 3_600_001)
    assert rejected_rules(envelope) == ["TIME-002"]


@pytest.mark.req("REQ-DQ-TIME-002")
def test_event_up_to_an_hour_ahead_is_allowed(envelope):
    set_time(envelope, NOW_MS + 3_600_000)
    validate(envelope, now=NOW)


@pytest.mark.req("REQ-DQ-TIME-003")
def test_updated_before_event_time_is_time_003(envelope):
    set_time(envelope, NOW_MS - 1000, updated_ms=NOW_MS - 1001)
    assert rejected_rules(envelope) == ["TIME-003"]


@pytest.mark.req("REQ-DQ-TIME-001")
@pytest.mark.parametrize("huge", [10**20, -(10**20), 2**63])
def test_unrepresentable_times_are_rejected_not_crashed(envelope, huge):
    set_time(envelope, huge)
    assert "TIME-001" in rejected_rules(envelope)


@pytest.mark.req("REQ-DQ-MAG-001")
@pytest.mark.parametrize("mag", [10.51, -2.01, "4.5", True, math.nan])
def test_bad_magnitude_is_mag_001(envelope, mag):
    envelope["payload"]["properties"]["mag"] = mag
    assert rejected_rules(envelope) == ["MAG-001"]


@pytest.mark.req("REQ-DQ-MAG-001")
@pytest.mark.parametrize("mag", [None, -2.0, 10.5, 0, 7])
def test_allowed_magnitudes(envelope, mag):
    envelope["payload"]["properties"]["mag"] = mag
    assert validate(envelope, now=NOW).fields["magnitude"] == (None if mag is None else float(mag))


@pytest.mark.req("REQ-DQ-PROP-001")
@pytest.mark.parametrize("value", [None, "", 3])
def test_missing_type_is_prop_001(envelope, value):
    envelope["payload"]["properties"]["type"] = value
    assert rejected_rules(envelope) == ["PROP-001"]


@pytest.mark.req("REQ-DQ-PROP-002")
@pytest.mark.parametrize("status", [None, "manual", "REVIEWED"])
def test_unknown_status_is_prop_002(envelope, status):
    envelope["payload"]["properties"]["status"] = status
    assert rejected_rules(envelope) == ["PROP-002"]


@pytest.mark.req("REQ-DQ-ACC-001")
def test_deleted_status_is_kept(envelope):
    envelope["payload"]["properties"]["status"] = "deleted"
    assert validate(envelope, now=NOW).fields["review_status"] == "deleted"


@pytest.mark.req("REQ-DQ-PROP-003")
@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("felt", "3"),
        ("felt", 2.5),
        ("sig", "high"),
        ("cdi", "x"),
        ("mmi", math.inf),
        ("tsunami", 2),
        ("tsunami", 0.5),
        ("place", 12),
        ("magType", 1),
    ],
)
def test_wrong_optional_types_are_prop_003(envelope, name, value):
    envelope["payload"]["properties"][name] = value
    assert rejected_rules(envelope) == ["PROP-003"]


@pytest.mark.req("REQ-DQ-ACC-001")
def test_optional_fields_may_be_null(envelope):
    props = envelope["payload"]["properties"]
    for name in ("felt", "sig", "cdi", "mmi", "magType", "place", "alert", "net", "url", "title"):
        props[name] = None
    props.pop("tsunami")
    fields = validate(envelope, now=NOW).fields
    assert fields["place"] is None and fields["tsunami"] is False


def test_all_payload_violations_are_reported_together(envelope):
    coords(envelope, lat=95.0, lon=200.0)
    envelope["payload"]["properties"].update(mag=12.0, status="manual")
    with pytest.raises(Rejected) as excinfo:
        validate(envelope, now=NOW)
    assert [v.rule for v in excinfo.value.violations] == [
        "GEO-002",
        "GEO-003",
        "MAG-001",
        "PROP-002",
    ]
    assert excinfo.value.rule == "GEO-002"
    assert "GEO-003: longitude 200.0" in excinfo.value.reason
