import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from earth_hub.ingestion.poller import MAX_WAIT_S, MIN_WAIT_S, Poller, next_wait
from earth_hub.ingestion.producer import EventPublisher
from earth_hub.ingestion.state import SentState, state_from_messages
from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client

URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
NOW = datetime(2026, 10, 16, 8, 10, 31, tzinfo=UTC)
LAST_MODIFIED = "Fri, 16 Oct 2026 08:10:25 GMT"
EXPIRES = "Fri, 16 Oct 2026 08:11:25 GMT"


class FakeFeed:
    """Serves the current document, honouring If-Modified-Since like the real feed."""

    def __init__(self, doc):
        self.doc = doc
        self.version = LAST_MODIFIED
        self.requests: list[httpx.Request] = []
        self.next_status: list[int] = []

    def change(self, doc, version):
        self.doc, self.version = doc, version

    def handler(self, request):
        self.requests.append(request)
        if self.next_status:
            return httpx.Response(self.next_status.pop(0))
        headers = {"Last-Modified": self.version, "Expires": EXPIRES}
        if request.headers.get("If-Modified-Since") == self.version:
            return httpx.Response(304, headers=headers)
        return httpx.Response(200, content=json.dumps(self.doc).encode(), headers=headers)


def make_poller(feed, producer, state=None, **kwargs):
    http = make_http_client(transport=httpx.MockTransport(feed.handler))
    client = UsgsClient(URL, http, sleep=lambda s: None, rng=lambda: 0.0, clock=lambda: NOW)
    poller = Poller(
        client,
        EventPublisher(producer, "earthquakes.raw", flush_timeout_s=0.1),
        state or SentState(),
        fallback_wait_s=60,
        clock=lambda: NOW,
        rng=lambda: 0.0,
        **kwargs,
    )
    return poller


def published_ids(producer) -> list[str]:
    return [key.decode() for _, key, _ in producer.produced]


def revise(doc, event_id, by_ms=1000, **properties):
    feature = next(f for f in doc["features"] if f["id"] == event_id)
    feature["properties"]["updated"] += by_ms
    feature["properties"].update(properties)
    return doc


def test_first_round_publishes_every_event(feed_doc, fake_producer):
    result = make_poller(FakeFeed(feed_doc), fake_producer).run_round()
    assert result.status == "published"
    assert result.published == len(feed_doc["features"])
    assert sorted(published_ids(fake_producer)) == sorted(f["id"] for f in feed_doc["features"])


@pytest.mark.req("REQ-ING-001")
def test_304_publishes_nothing(feed_doc, fake_producer):
    feed = FakeFeed(feed_doc)
    poller = make_poller(feed, fake_producer)
    poller.run_round()
    count = len(fake_producer.produced)

    result = poller.run_round()
    assert result.status == "not_modified"
    assert feed.requests[-1].headers["If-Modified-Since"] == LAST_MODIFIED
    assert len(fake_producer.produced) == count


@pytest.mark.req("REQ-ING-004")
def test_new_feed_version_with_no_changes_publishes_nothing(feed_doc, fake_producer):
    feed = FakeFeed(feed_doc)
    poller = make_poller(feed, fake_producer)
    poller.run_round()
    feed.change(feed_doc, "Fri, 16 Oct 2026 08:11:25 GMT")

    result = poller.run_round()
    assert (result.published, result.unchanged) == (0, len(feed_doc["features"]))


@pytest.mark.req("REQ-ING-004")
def test_revision_is_published_once(feed_doc, fake_producer):
    feed = FakeFeed(feed_doc)
    poller = make_poller(feed, fake_producer)
    poller.run_round()
    fake_producer.produced.clear()

    feed.change(revise(feed_doc, "us6000u2n7", mag=4.6, status="reviewed"), "v2")
    assert poller.run_round().published == 1
    [(_, key, value)] = fake_producer.produced
    assert key == b"us6000u2n7"
    assert json.loads(value)["payload"]["properties"]["mag"] == 4.6

    feed.change(feed_doc, "v3")  # same content under a new Last-Modified
    assert poller.run_round().published == 0


@pytest.mark.req("REQ-ING-004")
def test_older_revision_is_not_published(feed_doc, fake_producer):
    feed = FakeFeed(revise(feed_doc, "us6000u2n7", by_ms=5000))
    poller = make_poller(feed, fake_producer)
    poller.run_round()
    fake_producer.produced.clear()

    feed.change(revise(feed_doc, "us6000u2n7", by_ms=-3000), "v2")  # goes backwards
    assert poller.run_round().published == 0


def test_same_event_listed_twice_publishes_newest_once(feed_doc, fake_producer):
    duplicate = json.loads(json.dumps(feed_doc["features"][0]))
    duplicate["properties"]["updated"] += 10
    feed_doc["features"].append(duplicate)
    make_poller(FakeFeed(feed_doc), fake_producer).run_round()
    values = [json.loads(v) for _, k, v in fake_producer.produced if k == duplicate["id"].encode()]
    assert [v["source_updated_ms"] for v in values] == [duplicate["properties"]["updated"]]


@pytest.mark.req("REQ-ING-008")
def test_state_rebuilt_after_restart_prevents_resending(feed_doc, fake_producer, fake_producer_cls):
    first = make_poller(FakeFeed(feed_doc), fake_producer)
    first.run_round()
    values = [value for _, _, value in fake_producer.produced]

    restarted_producer = fake_producer_cls()
    restarted = make_poller(FakeFeed(feed_doc), restarted_producer, state_from_messages(values))
    assert restarted.run_round().published == 0


@pytest.mark.req("REQ-ING-006")
def test_failed_delivery_is_retried_next_round(feed_doc, fake_producer_cls):
    target = feed_doc["features"][2]["id"]
    producer = fake_producer_cls(fail_keys={target.encode()})
    feed = FakeFeed(feed_doc)
    poller = make_poller(feed, producer)

    first = poller.run_round()
    assert first.failed_delivery == 1 and target not in published_ids(producer)

    producer.fail_keys.clear()
    producer.produced.clear()
    second = poller.run_round()
    # Last-Modified was not remembered, so the full body came back instead of a 304.
    assert "If-Modified-Since" not in feed.requests[-1].headers
    assert second.status == "published"
    assert published_ids(producer) == [target]


@pytest.mark.req("REQ-ING-003")
def test_feed_unavailable_fails_round_without_publishing(feed_doc, fake_producer):
    feed = FakeFeed(feed_doc)
    feed.next_status = [503, 503, 503]
    result = make_poller(feed, fake_producer).run_round()
    assert result.status == "failed"
    assert fake_producer.produced == []


def test_unusable_body_fails_round(fake_producer):
    def handler(request):
        return httpx.Response(200, content=b"<html>maintenance</html>")

    poller = make_poller(FakeFeed({}), fake_producer)
    poller._client._http = make_http_client(transport=httpx.MockTransport(handler))
    assert poller.run_round().status == "failed"
    assert fake_producer.produced == []


@pytest.mark.req("REQ-ING-007")
def test_rejected_features_are_counted_and_logged(feed_doc, fake_producer, caplog):
    del feed_doc["features"][0]["properties"]["updated"]
    result = make_poller(FakeFeed(feed_doc), fake_producer).run_round()
    assert result.rejected == 1
    assert result.published == len(feed_doc["features"]) - 1
    assert "feature rejected" in caplog.text


def test_empty_feed_is_a_successful_round(feed_doc, fake_producer):
    feed_doc["features"] = []
    result = make_poller(FakeFeed(feed_doc), fake_producer).run_round()
    assert (result.status, result.published) == ("published", 0)


@pytest.mark.req("REQ-OPS-005")
def test_heartbeat_written_on_success_only(feed_doc, fake_producer, tmp_path):
    heartbeat = tmp_path / "hb"
    feed = FakeFeed(feed_doc)
    feed.next_status = [500, 500, 500]
    poller = make_poller(feed, fake_producer, heartbeat_path=heartbeat)

    poller.run_round()
    assert not heartbeat.exists()
    poller.run_round()
    assert heartbeat.read_text() == NOW.isoformat()


@pytest.mark.req("REQ-ING-009", "REQ-ING-002")
def test_run_forever_stops_and_flushes(feed_doc, fake_producer):
    poller = make_poller(FakeFeed(feed_doc), fake_producer)
    waits = []

    def fake_wait(seconds):
        waits.append(seconds)
        poller.stop_event.set()
        return True

    poller.stop_event.wait = fake_wait
    poller.run_forever()
    assert waits == [pytest.approx(54 + 2)]  # Expires 08:11:25 - now 08:10:31 + jitter floor
    assert fake_producer.flush_calls[-1] == 10.0


@pytest.mark.req("REQ-ING-002")
@pytest.mark.parametrize(
    ("expires", "rng", "expected"),
    [
        (NOW + timedelta(seconds=54), 0.0, 56.0),
        (NOW + timedelta(seconds=54), 1.0, 59.0),
        (NOW - timedelta(seconds=30), 0.5, MIN_WAIT_S),  # already expired
        (NOW + timedelta(days=1), 0.5, MAX_WAIT_S),  # implausible header
        (None, 0.5, 60.0),  # fallback
    ],
)
def test_next_wait(expires, rng, expected):
    assert next_wait(expires, NOW, 60.0, lambda: rng) == pytest.approx(expected)


@pytest.mark.req("REQ-ING-002")
def test_jitter_stays_within_bounds():
    expires = NOW + timedelta(seconds=30)
    waits = [next_wait(expires, NOW, 60.0, lambda r=r: r / 10) for r in range(11)]
    assert min(waits) == pytest.approx(32.0) and max(waits) == pytest.approx(35.0)
