import json
from datetime import UTC, datetime

from earth_hub.ingestion.events import parse_feed
from earth_hub.ingestion.producer import EventPublisher

FETCHED_AT = datetime(2026, 10, 16, 8, 10, 31, tzinfo=UTC)


def messages(feed_doc):
    return parse_feed(json.dumps(feed_doc).encode(), fetched_at=FETCHED_AT).messages


def test_all_messages_delivered_with_event_id_keys(feed_doc, fake_producer):
    msgs = messages(feed_doc)
    result = EventPublisher(fake_producer, "earthquakes.raw").publish(msgs)
    assert result.delivered == msgs and result.failed == []
    assert [(t, k) for t, k, _ in fake_producer.produced] == [
        ("earthquakes.raw", m.event_id.encode()) for m in msgs
    ]
    assert [json.loads(v)["event_id"] for _, _, v in fake_producer.produced] == [
        m.event_id for m in msgs
    ]


def test_broker_error_is_reported_per_message(feed_doc, fake_producer_cls):
    msgs = messages(feed_doc)
    producer = fake_producer_cls(fail_keys={msgs[1].key})
    result = EventPublisher(producer, "t").publish(msgs)
    assert [m.event_id for m, _ in result.failed] == [msgs[1].event_id]
    assert "Not enough in-sync replicas" in result.failed[0][1]
    assert len(result.delivered) == len(msgs) - 1


def test_missing_delivery_report_counts_as_failure(feed_doc, fake_producer_cls):
    msgs = messages(feed_doc)
    producer = fake_producer_cls(drop_keys={msgs[0].key})
    result = EventPublisher(producer, "t", flush_timeout_s=0.1).publish(msgs)
    assert result.failed == [(msgs[0], "no delivery report before flush timeout")]


def test_produce_exception_counts_as_failure(feed_doc, fake_producer_cls):
    msgs = messages(feed_doc)

    class Broken(fake_producer_cls):
        def produce(self, topic, *, key, value, on_delivery):
            if key == msgs[0].key:
                raise ValueError("bad topic")
            super().produce(topic, key=key, value=value, on_delivery=on_delivery)

    result = EventPublisher(Broken(), "t").publish(msgs)
    assert [m for m, _ in result.failed] == [msgs[0]]
    assert "produce failed: bad topic" in result.failed[0][1]


def test_full_local_queue_is_retried_once(feed_doc, fake_producer_cls):
    msgs = messages(feed_doc)[:1]

    class FullOnce(fake_producer_cls):
        calls = 0

        def produce(self, topic, *, key, value, on_delivery):
            self.calls += 1
            if self.calls == 1:
                raise BufferError("queue full")
            super().produce(topic, key=key, value=value, on_delivery=on_delivery)

    producer = FullOnce()
    result = EventPublisher(producer, "t").publish(msgs)
    assert result.delivered == msgs and producer.calls == 2


def test_nothing_to_publish_does_not_flush(fake_producer):
    result = EventPublisher(fake_producer, "t").publish([])
    assert result.delivered == [] and fake_producer.flush_calls == []
