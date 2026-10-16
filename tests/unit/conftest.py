import pytest


class FakeProducer:
    """Stands in for confluent_kafka.Producer; delivery reports are served on flush()."""

    def __init__(self, fail_keys: set[bytes] = frozenset(), drop_keys: set[bytes] = frozenset()):
        self.fail_keys = set(fail_keys)  # broker reports an error
        self.drop_keys = set(drop_keys)  # no report before the flush timeout
        self.produced: list[tuple[str, bytes, bytes]] = []
        self._pending = []
        self.flush_calls: list[float] = []

    def produce(self, topic, *, key, value, on_delivery):
        self._pending.append((topic, key, value, on_delivery))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        self.flush_calls.append(timeout)
        still_pending = []
        for topic, key, value, callback in self._pending:
            if key in self.drop_keys:
                still_pending.append((topic, key, value, callback))
            elif key in self.fail_keys:
                callback("Broker: Not enough in-sync replicas", None)
            else:
                self.produced.append((topic, key, value))
                callback(None, None)
        self._pending = still_pending
        return len(still_pending)


@pytest.fixture
def fake_producer_cls() -> type[FakeProducer]:
    """The FakeProducer class, for tests that configure failures."""
    return FakeProducer


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()
