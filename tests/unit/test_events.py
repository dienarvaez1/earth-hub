import json
from datetime import UTC, datetime

import pytest

from earth_hub.ingestion.events import (
    SCHEMA_VERSION,
    InvalidFeed,
    build_message,
    parse_feed,
    parse_ids,
)

FETCHED_AT = datetime(2026, 10, 16, 8, 10, 31, tzinfo=UTC)


def parse(doc) -> object:
    return parse_feed(json.dumps(doc).encode(), fetched_at=FETCHED_AT)


def feature_by_id(doc, event_id) -> dict:
    return next(f for f in doc["features"] if f["id"] == event_id)


def test_valid_feed_produces_one_message_per_feature(feed_doc):
    parsed = parse(feed_doc)
    assert [m.event_id for m in parsed.messages] == [f["id"] for f in feed_doc["features"]]
    assert parsed.rejected == []
    assert parsed.generated_ms == feed_doc["metadata"]["generated"]


def test_message_envelope_matches_contract(feed_doc):
    feature = feed_doc["features"][0]
    message = parse(feed_doc).messages[0]
    envelope = json.loads(message.to_json())

    assert message.key == feature["id"].encode()
    assert envelope == {
        "schema_version": SCHEMA_VERSION,
        "event_id": feature["id"],
        "ids": [feature["id"]],
        "source_updated_ms": feature["properties"]["updated"],
        "event_time_ms": feature["properties"]["time"],
        "source": "feed",
        "feed_generated_ms": feed_doc["metadata"]["generated"],
        "fetched_at": "2026-10-16T08:10:31Z",
        "payload": feature,
    }


def test_payload_is_the_unchanged_source_feature(feed_doc):
    for feature, message in zip(feed_doc["features"], parse(feed_doc).messages, strict=True):
        assert json.loads(message.to_json())["payload"] == feature


def test_event_with_several_ids_keeps_all_of_them(feed_doc):
    message = next(m for m in parse(feed_doc).messages if m.event_id == "aka2026umjevu")
    assert message.ids == ("us6000u2n8", "aka2026umjevu")


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (",us1,", ("us1",)),
        (",ak2,us1,", ("ak2", "us1")),
        (",ak2,ak2,", ("ak2", "us1")),  # duplicates collapse; preferred ID added
        ("", ("us1",)),
        (None, ("us1",)),
        (42, ("us1",)),
    ],
)
def test_parse_ids(raw, expected):
    ids = parse_ids(raw, "us1")
    assert set(ids) == set(expected)
    assert "us1" in ids and len(ids) == len(set(ids))


def test_edge_case_values_pass_through_unchanged(feed_doc):
    """Data-quality rules belong to the consumer; the poller must not drop these."""
    by_id = {m.event_id: m for m in parse(feed_doc).messages}
    assert by_id["av94451518"].payload["properties"]["mag"] == -0.34  # negative magnitude
    assert by_id["uw714120332"].payload["geometry"]["coordinates"][2] == -0.36  # above sea level
    assert by_id["uw714120332"].payload["properties"]["type"] == "explosion"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda f: f["properties"].update(mag=None, place=None, felt=None, magType=None),
        lambda f: f["geometry"].update(coordinates=[200.0, -95.0, 10.0]),  # out of range
        lambda f: f.update(geometry=None),
        lambda f: f["properties"].pop("ids"),
    ],
    ids=["optional-nulls", "invalid-coordinates", "no-geometry", "no-ids"],
)
def test_routable_features_are_published_whatever_their_data_quality(feed_doc, mutate):
    mutate(feed_doc["features"][0])
    parsed = parse(feed_doc)
    assert len(parsed.messages) == len(feed_doc["features"])
    assert parsed.rejected == []


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda f: f.pop("id"), "id must be a non-empty string"),
        (lambda f: f.update(id=""), "id must be a non-empty string"),
        (lambda f: f.update(id=123), "id must be a non-empty string"),
        (lambda f: f.update(properties=None), "properties must be an object"),
        (lambda f: f["properties"].pop("updated"), "properties.updated must be an integer"),
        (lambda f: f["properties"].update(updated="1792137700121"), "properties.updated"),
        (lambda f: f["properties"].update(updated=True), "properties.updated"),
        (lambda f: f["properties"].update(time=None), "properties.time must be an integer"),
        (lambda f: f["properties"].update(time=1.5), "properties.time"),
    ],
    ids=[
        "missing-id",
        "empty-id",
        "numeric-id",
        "no-properties",
        "missing-updated",
        "string-updated",
        "bool-updated",
        "null-time",
        "float-time",
    ],
)
def test_unroutable_feature_is_rejected_and_the_rest_still_parse(feed_doc, mutate, reason):
    mutate(feed_doc["features"][1])
    parsed = parse(feed_doc)
    assert len(parsed.messages) == len(feed_doc["features"]) - 1
    [rejected] = parsed.rejected
    assert rejected.index == 1
    assert reason in rejected.reason


def test_non_object_feature_is_rejected(feed_doc):
    feed_doc["features"].append("not a feature")
    [rejected] = parse(feed_doc).rejected
    assert (rejected.index, rejected.event_id) == (len(feed_doc["features"]) - 1, None)


def test_empty_feed_is_valid(feed_doc):
    feed_doc["features"] = []
    parsed = parse(feed_doc)
    assert parsed.messages == [] and parsed.rejected == []


@pytest.mark.parametrize(
    "body",
    [b"", b"<html>502 Bad Gateway</html>", b'{"type": "Feature', b"[]", b'{"type": "x"}'],
    ids=["empty", "html", "truncated-json", "json-array", "no-features"],
)
def test_unusable_body_raises_invalid_feed(body):
    with pytest.raises(InvalidFeed):
        parse_feed(body, fetched_at=FETCHED_AT)


def test_features_must_be_a_list():
    with pytest.raises(InvalidFeed):
        parse_feed(b'{"type": "FeatureCollection", "features": {}}', fetched_at=FETCHED_AT)


def test_missing_metadata_is_tolerated(feed_doc):
    del feed_doc["metadata"]
    parsed = parse(feed_doc)
    assert parsed.generated_ms is None
    assert json.loads(parsed.messages[0].to_json())["feed_generated_ms"] is None


def test_build_message_marks_source(feed_doc):
    message = build_message(
        feed_doc["features"][0], fetched_at=FETCHED_AT, feed_generated_ms=None, source="catchup"
    )
    assert json.loads(message.to_json())["source"] == "catchup"
