"""The whole pipeline in one object, for end-to-end tests:

    FeedServer (mock USGS) -> Poller -> Kafka (temporary topic) -> EarthquakeConsumer
    -> PostgreSQL (temporary database) -> API (TestClient)

Everything but the feed is the real code. Tests assert through the API, as a client would.
"""

import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import Producer
from fastapi.testclient import TestClient
from sqlalchemy import Engine, func, select

from earth_hub.api.main import create_app
from earth_hub.config import Settings
from earth_hub.consumers.earthquake_consumer import EarthquakeConsumer, Store, make_consumer
from earth_hub.database.models import RawEvent
from earth_hub.ingestion.poller import Poller
from earth_hub.ingestion.producer import EventPublisher, make_producer
from earth_hub.ingestion.state import STATE_WINDOW, SentState, read_topic_since, state_from_messages
from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client
from earth_hub.reconcile import Report, reconcile, topic_offsets

from .fdsn import FdsnServer
from .feed import URL, FeedServer


class _CommitsLost:
    """A Kafka consumer whose offset commits never happen: the process "crashes" after
    each database commit but before its offset commit."""

    def __init__(self, consumer):
        self._consumer = consumer

    def poll(self, timeout):
        return self._consumer.poll(timeout)

    def consume(self, num_messages, timeout):
        return self._consumer.consume(num_messages=num_messages, timeout=timeout)

    def commit(self, **kwargs):
        return None

    def close(self):
        self._consumer.close()


class Pipeline:
    def __init__(
        self,
        settings: Settings,
        topic: str,
        db: Engine,
        feed: FeedServer,
        backfill_topic: str | None = None,
        fdsn: FdsnServer | None = None,
    ):
        self.settings, self.topic, self.db, self.feed = settings, topic, db, feed
        self.backfill_topic, self.fdsn = backfill_topic, fdsn
        self.group = f"e2e-{uuid.uuid4().hex[:8]}"
        self.backfill_group = f"e2e-bf-{uuid.uuid4().hex[:8]}"
        self.poller = self._make_poller(SentState())
        self._consumer: EarthquakeConsumer | None = None
        self._backfill_consumer: EarthquakeConsumer | None = None
        self.batch_size = 1
        self._client = TestClient(create_app(db))
        self._raw_producer: Producer | None = None

    # --- driving the pipeline ------------------------------------------------------------

    def poll(self):
        """One poller round against the mock feed."""
        return self.poller.run_round()

    @property
    def consumer(self) -> EarthquakeConsumer:
        if self._consumer is None:
            self._consumer = EarthquakeConsumer(
                make_consumer(self.settings.kafka_bootstrap_servers, self.group, self.topic),
                Store(self.db),
            )
        return self._consumer

    @property
    def backfill_consumer(self) -> EarthquakeConsumer:
        """The separate, batched consumer of the backfill topic (as in docker-compose)."""
        if self._backfill_consumer is None:
            self._backfill_consumer = EarthquakeConsumer(
                make_consumer(
                    self.settings.kafka_bootstrap_servers, self.backfill_group, self.backfill_topic
                ),
                Store(self.db),
                batch_size=500,
            )
        return self._backfill_consumer

    def sync(self, timeout_s: float = 60) -> None:
        """Run the consumers until every message in each topic has a raw_events row."""
        self.consumer.batch_size = self.batch_size
        pairs = [(self.topic, lambda: self.consumer)]
        if self.backfill_topic:
            pairs.append((self.backfill_topic, lambda: self.backfill_consumer))
        deadline = time.monotonic() + timeout_s
        for topic, consumer in pairs:
            expected = self.messages_in_topic(topic)
            while self.stored(topic) < expected:
                if time.monotonic() > deadline:
                    raise AssertionError(
                        f"{topic}: stored {self.stored(topic)} of {expected} messages in time"
                    )
                consumer().run_once(timeout_s=1.0)

    def backfill(self, start, end=None, *, now=None, sweep=True):
        """Run the real backfill job against the mock FDSN service."""
        from earth_hub.database.progress import Progress
        from earth_hub.ingestion.backfill import Backfill
        from earth_hub.ingestion.fdsn import FdsnClient

        fdsn = FdsnClient(
            "https://earthquake.usgs.gov/fdsnws/event/1",
            make_http_client(transport=self.fdsn.transport),
            pause_s=0,
            sleep=lambda s: None,
        )
        publisher = EventPublisher(
            make_producer(self.settings.kafka_bootstrap_servers), self.backfill_topic
        )
        clock = (lambda: now) if now else (lambda: datetime.now(UTC))
        return Backfill(fdsn, publisher, Progress(self.db), clock=clock).run(
            start, end, sweep=sweep
        )

    def use_consumer_that_loses_commits(self) -> None:
        """Replace the consumer with one that stores messages but never commits offsets."""
        self.restart_consumer()
        self._consumer = EarthquakeConsumer(
            _CommitsLost(
                make_consumer(self.settings.kafka_bootstrap_servers, self.group, self.topic)
            ),
            Store(self.db),
        )

    def restart_poller(self) -> None:
        """A new poller, with its state rebuilt from the topic as on a real restart."""
        since = datetime.now(UTC) - STATE_WINDOW
        values = read_topic_since(self.settings.kafka_bootstrap_servers, self.topic, since)
        self.poller.shutdown()
        self.poller = self._make_poller(state_from_messages(values))

    def restart_consumer(self) -> None:
        """Close the consumer cleanly; the next one joins the same group."""
        if self._consumer is not None:
            self._consumer._consumer.close()
            self._consumer = None

    def publish_raw(self, key: bytes, value: bytes) -> None:
        """Put a message on the topic directly, e.g. a duplicate a producer retry would send."""
        if self._raw_producer is None:
            self._raw_producer = Producer(
                {"bootstrap.servers": self.settings.kafka_bootstrap_servers, "acks": "all"}
            )
        self._raw_producer.produce(self.topic, key=key, value=value)
        assert self._raw_producer.flush(30) == 0

    # --- observing it --------------------------------------------------------------------

    def get(self, path: str, expect: int = 200, **params):
        response = self._client.get(f"/api/v1{path}", params=params)
        assert response.status_code == expect, response.text
        return response.json()

    def events(self, **params) -> dict[str, dict]:
        """Events visible through the API, by event_id."""
        body = self.get("/earthquakes", limit=500, **params)
        assert body["total"] == len(body["items"]), "more than one page; raise the limit"
        return {item["event_id"]: item for item in body["items"]}

    def messages_in_topic(self, topic: str | None = None) -> int:
        return len(topic_offsets(self.settings.kafka_bootstrap_servers, topic or self.topic))

    def stored(self, topic: str | None = None) -> int:
        with self.db.connect() as conn:
            return conn.execute(
                select(func.count())
                .select_from(RawEvent)
                .where(RawEvent.kafka_topic == (topic or self.topic))
            ).scalar_one()

    def raw_outcomes(self) -> list[dict]:
        with self.db.connect() as conn:
            return [
                dict(r)
                for r in conn.execute(
                    select(RawEvent.event_id, RawEvent.outcome, RawEvent.rule)
                    .where(RawEvent.kafka_topic == self.topic)
                    .order_by(RawEvent.kafka_partition, RawEvent.kafka_offset)
                ).mappings()
            ]

    def reconcile(self, backfill: bool = False) -> Report:
        if backfill:
            return reconcile(
                self.db,
                self.settings.kafka_bootstrap_servers,
                self.backfill_topic,
                self.backfill_group,
            )
        return reconcile(self.db, self.settings.kafka_bootstrap_servers, self.topic, self.group)

    # --- lifecycle -----------------------------------------------------------------------

    def close(self) -> None:
        if self._backfill_consumer is not None:
            self._backfill_consumer._consumer.close()
            self._backfill_consumer = None
        self.restart_consumer()
        self.poller.shutdown()
        self._client.close()

    def _make_poller(self, state: SentState) -> Poller:
        client = UsgsClient(
            URL, make_http_client(transport=self.feed.transport), sleep=lambda s: None
        )
        publisher = EventPublisher(make_producer(self.settings.kafka_bootstrap_servers), self.topic)
        return Poller(client, publisher, state, fallback_wait_s=60)
