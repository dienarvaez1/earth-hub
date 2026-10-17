"""Helpers for tests that stop and start the shared Docker Compose services."""

import subprocess
import threading
import time
from pathlib import Path

import pytest
from support.feed import EventFactory, FeedServer
from support.pipeline import Pipeline

from earth_hub.consumers.earthquake_consumer import EarthquakeConsumer, Store, make_consumer

REPO = Path(__file__).resolve().parents[2]


def compose(*args: str) -> str:
    result = subprocess.run(
        ["docker", "compose", *args], cwd=REPO, capture_output=True, text=True, check=True
    )
    return result.stdout


def wait_healthy(service: str, timeout_s: float = 120) -> None:
    container = compose("ps", "-q", service).strip()
    deadline = time.monotonic() + timeout_s
    status = "unknown"
    while time.monotonic() < deadline:
        status = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
            capture_output=True,
            text=True,
        ).stdout.strip()
        if status == "healthy":
            return
        time.sleep(1)
    raise TimeoutError(f"{service} not healthy after {timeout_s}s (last status: {status})")


def wait_for(condition, timeout_s: float, message: str) -> None:
    deadline = time.monotonic() + timeout_s
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(message)
        time.sleep(0.5)


class BackgroundConsumer:
    """The real consumer, running in a thread as it does in its container."""

    def __init__(self, pipeline: Pipeline):
        settings = pipeline.settings
        self.consumer = EarthquakeConsumer(
            make_consumer(settings.kafka_bootstrap_servers, pipeline.group, pipeline.topic),
            Store(pipeline.db),
        )
        self._thread = threading.Thread(target=self.consumer.run_forever, daemon=True)

    def __enter__(self):
        self._thread.start()
        return self.consumer

    def __exit__(self, *exc):
        self.consumer.stop_event.set()
        self._thread.join(timeout=60)
        assert not self._thread.is_alive(), "consumer did not stop within 60 s"


@pytest.fixture
def feed() -> FeedServer:
    return FeedServer()


@pytest.fixture
def event(feature_template) -> EventFactory:
    return EventFactory(feature_template)


@pytest.fixture
def pipeline(settings, make_temp_topic, db, feed):
    pipe = Pipeline(settings, make_temp_topic(partitions=3), db, feed)
    yield pipe
    pipe.close()


@pytest.fixture(autouse=True)
def services_restored():
    """Whatever happens in the test, leave Kafka and PostgreSQL running and healthy."""
    yield
    compose("start", "kafka", "postgres")
    wait_healthy("kafka")
    wait_healthy("postgres")
