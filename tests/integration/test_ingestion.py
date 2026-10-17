"""The poller against real Kafka, with the USGS feed served from fixtures (never the live feed)."""

import json
import time
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from confluent_kafka import Consumer

from earth_hub.ingestion.poller import Poller
from earth_hub.ingestion.producer import EventPublisher, make_producer
from earth_hub.ingestion.state import (
    STATE_WINDOW,
    SentState,
    read_topic_since,
    state_from_messages,
)
from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client

pytestmark = pytest.mark.integration

URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


def serve(doc, version="v1"):
    """A mock transport that answers like the feed: 200 with the document, 304 when unchanged."""

    def handler(request):
        if request.headers.get("If-Modified-Since") == version:
            return httpx.Response(304, headers={"Last-Modified": version})
        return httpx.Response(200, json=doc, headers={"Last-Modified": version})

    return httpx.MockTransport(handler)


def make_poller(settings, topic, transport, state):
    client = UsgsClient(URL, make_http_client(transport=transport), sleep=lambda s: None)
    publisher = EventPublisher(make_producer(settings.kafka_bootstrap_servers), topic)
    return Poller(client, publisher, state, fallback_wait_s=60)


def consume_all(settings, topic, expected, timeout_s=30):
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"test-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    messages = []
    try:
        consumer.subscribe([topic])
        deadline = time.monotonic() + timeout_s
        while len(messages) < expected and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            assert not message.error(), message.error()
            messages.append(message)
    finally:
        consumer.close()
    return messages


def with_fresh_times(doc):
    """Shift event times to now, so the state window treats the fixture as current."""
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    for offset, feature in enumerate(doc["features"]):
        feature["properties"]["time"] = now_ms - offset * 60_000
        feature["properties"]["updated"] = now_ms - offset * 60_000 + 30_000
    return doc


@pytest.mark.req("REQ-ING-005")
def test_round_publishes_keyed_envelopes(settings, make_temp_topic, feed_doc):
    topic = make_temp_topic(partitions=3)
    result = make_poller(settings, topic, serve(feed_doc), SentState()).run_round()
    assert result.published == len(feed_doc["features"]) and result.failed_delivery == 0

    messages = consume_all(settings, topic, expected=len(feed_doc["features"]))
    by_key = {m.key().decode(): json.loads(m.value()) for m in messages}
    assert sorted(by_key) == sorted(f["id"] for f in feed_doc["features"])
    for feature in feed_doc["features"]:
        envelope = by_key[feature["id"]]
        assert envelope["event_id"] == feature["id"]
        assert envelope["source_updated_ms"] == feature["properties"]["updated"]
        assert envelope["payload"] == feature


@pytest.mark.req("REQ-ING-010")
def test_same_key_always_lands_on_the_same_partition(settings, make_temp_topic, feed_doc):
    topic = make_temp_topic(partitions=3)
    poller = make_poller(settings, topic, serve(feed_doc), SentState())
    poller.run_round()
    revised = json.loads(json.dumps(feed_doc))
    for feature in revised["features"]:
        feature["properties"]["updated"] += 1000
    poller._client._http = make_http_client(transport=serve(revised, "v2"))
    poller.run_round()

    messages = consume_all(settings, topic, expected=2 * len(feed_doc["features"]))
    partitions: dict[bytes, set[int]] = {}
    for message in messages:
        partitions.setdefault(message.key(), set()).add(message.partition())
    assert all(len(p) == 1 for p in partitions.values()), partitions


@pytest.mark.req("REQ-ING-008")
def test_restart_rebuilds_state_and_publishes_only_real_changes(
    settings, make_temp_topic, feed_doc
):
    topic = make_temp_topic(partitions=3)
    doc = with_fresh_times(feed_doc)
    first = make_poller(settings, topic, serve(doc), SentState()).run_round()
    assert first.published == len(doc["features"])

    # "Restart": a new poller with state rebuilt from what is actually in the topic.
    since = datetime.now(UTC) - STATE_WINDOW
    state = state_from_messages(read_topic_since(settings.kafka_bootstrap_servers, topic, since))
    assert len(state) == len(doc["features"])

    restarted = make_poller(settings, topic, serve(doc), state)
    assert restarted.run_round().published == 0

    doc["features"][3]["properties"]["updated"] += 60_000
    restarted._client._http = make_http_client(transport=serve(doc, "v2"))
    second = restarted.run_round()
    assert (second.published, second.unchanged) == (1, len(doc["features"]) - 1)

    messages = consume_all(settings, topic, expected=len(doc["features"]) + 1)
    assert len(messages) == len(doc["features"]) + 1


def test_rebuild_ignores_messages_older_than_since(settings, make_temp_topic, feed_doc):
    topic = make_temp_topic()
    make_poller(settings, topic, serve(feed_doc), SentState()).run_round()
    future = datetime.now(UTC) + timedelta(minutes=5)
    assert read_topic_since(settings.kafka_bootstrap_servers, topic, future) == []


def test_rebuild_from_empty_topic(settings, make_temp_topic):
    topic = make_temp_topic(partitions=3)
    since = datetime.now(UTC) - STATE_WINDOW
    assert read_topic_since(settings.kafka_bootstrap_servers, topic, since) == []
