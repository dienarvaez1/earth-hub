import pytest
from confluent_kafka import KafkaError, KafkaException
from sqlalchemy.exc import IntegrityError, OperationalError

from earth_hub.consumers.earthquake_consumer import (
    IDLE_PING_INTERVAL_S,
    MAX_BACKOFF_S,
    EarthquakeConsumer,
    is_infrastructure_error,
)
from earth_hub.database.repository import ProcessResult


def db_down() -> OperationalError:
    return OperationalError("SELECT 1", {}, Exception("connection refused"))


class FakeError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return f"kafka error {self._code}"


class FakeMessage:
    def __init__(self, offset, *, error=None, partition=0):
        self._offset, self._error, self._partition = offset, error, partition

    def error(self):
        return self._error

    def topic(self):
        return "earthquakes.raw"

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return b"us1"

    def value(self):
        return b"{}"

    def timestamp(self):
        return (1, 1792137522341)


class FakeKafka:
    def __init__(self, messages, commit_error=None):
        self.messages = list(messages)
        self.commits: list[int] = []
        self.closed = False
        self.commit_error = commit_error

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message, asynchronous):
        assert asynchronous is False
        if self.commit_error:
            raise self.commit_error
        self.commits.append(message.offset())

    def close(self):
        self.closed = True


class FakeStore:
    """process() answers from a script: exceptions are raised, results returned."""

    def __init__(self, *script, ping_error=None):
        self.script = list(script)
        self.processed: list[int] = []
        self.errors: list[tuple[int, str]] = []
        self.record_error_failures: list[Exception] = []
        self.pings = 0
        self.ping_error = ping_error

    def process(self, message):
        self.processed.append(message.offset)
        step = self.script.pop(0) if self.script else ProcessResult("applied", 1)
        if isinstance(step, Exception):
            raise step
        return step

    def record_error(self, message, error):
        if self.record_error_failures:
            raise self.record_error_failures.pop(0)
        self.errors.append((message.offset, error))

    def ping(self):
        self.pings += 1
        if self.ping_error:
            raise self.ping_error


def make(kafka, store, **kwargs):
    sleeps = []
    consumer = EarthquakeConsumer(kafka, store, sleep=sleeps.append, **kwargs)
    return consumer, sleeps


def test_offset_committed_after_successful_processing():
    kafka, store = FakeKafka([FakeMessage(7)]), FakeStore()
    consumer, _ = make(kafka, store)
    assert consumer.run_once() is True
    assert store.processed == [7] and kafka.commits == [7]
    assert consumer.outcomes == {"applied": 1}


def test_database_outage_retries_same_message_and_commits_only_after():
    kafka = FakeKafka([FakeMessage(7)])
    store = FakeStore(db_down(), db_down(), db_down(), ProcessResult("applied", 1))
    consumer, sleeps = make(kafka, store)

    original_process = store.process

    def process(message):
        # Nothing may be committed while the database is down.
        assert kafka.commits == []
        return original_process(message)

    store.process = process
    assert consumer.run_once() is True
    assert store.processed == [7, 7, 7, 7]
    assert sleeps == [1, 2, 4]
    assert kafka.commits == [7]


def test_outage_backoff_is_capped():
    store = FakeStore(*[db_down()] * 8, ProcessResult("applied", 1))
    consumer, sleeps = make(FakeKafka([FakeMessage(1)]), store)
    consumer.run_once()
    assert sleeps == [1, 2, 4, 8, 16, MAX_BACKOFF_S, MAX_BACKOFF_S, MAX_BACKOFF_S]


def test_stopping_during_outage_commits_nothing():
    kafka = FakeKafka([FakeMessage(7)])
    store = FakeStore(*[db_down()] * 5)
    consumer, _ = make(kafka, store)
    consumer._sleep = lambda s: consumer.stop_event.set()
    assert consumer.run_once() is False
    assert kafka.commits == []


def test_unexpected_error_is_recorded_after_three_attempts_and_partition_moves_on():
    bug = ValueError("boom")
    kafka = FakeKafka([FakeMessage(7), FakeMessage(8)])
    store = FakeStore(bug, bug, bug)
    consumer, sleeps = make(kafka, store)

    consumer.run_once()
    assert store.processed == [7, 7, 7]
    assert store.errors == [(7, "ValueError: boom")]
    assert kafka.commits == [7]
    assert sleeps == []

    consumer.run_once()
    assert kafka.commits == [7, 8]
    assert consumer.outcomes == {"error": 1, "applied": 1}


def test_unexpected_error_then_success_is_not_recorded():
    store = FakeStore(ValueError("flaky"), ProcessResult("applied", 1))
    consumer, _ = make(FakeKafka([FakeMessage(7)]), store)
    consumer.run_once()
    assert store.errors == [] and consumer.outcomes == {"applied": 1}


def test_outage_while_recording_an_error_is_retried():
    bug = ValueError("boom")
    store = FakeStore(bug, bug, bug)
    store.record_error_failures = [db_down()]
    kafka = FakeKafka([FakeMessage(7)])
    consumer, sleeps = make(kafka, store)
    consumer.run_once()
    assert store.errors == [(7, "ValueError: boom")] and kafka.commits == [7]
    assert sleeps == [MAX_BACKOFF_S]


def test_integrity_error_is_not_an_outage():
    assert not is_infrastructure_error(IntegrityError("INSERT", {}, Exception("dup")))
    assert is_infrastructure_error(db_down())


def test_failed_offset_commit_does_not_crash():
    kafka = FakeKafka([FakeMessage(7)], commit_error=KafkaException("rebalance"))
    consumer, _ = make(kafka, FakeStore())
    assert consumer.run_once() is True


def test_kafka_errors_are_skipped_without_commit():
    eof = FakeMessage(0, error=FakeError(KafkaError._PARTITION_EOF))
    other = FakeMessage(0, error=FakeError(KafkaError._TRANSPORT))
    kafka, store = FakeKafka([eof, other]), FakeStore()
    consumer, _ = make(kafka, store)
    assert consumer.run_once() is False and consumer.run_once() is False
    assert store.processed == [] and kafka.commits == []


def test_already_processed_offsets_are_committed():
    kafka = FakeKafka([FakeMessage(7)])
    consumer, _ = make(kafka, FakeStore(ProcessResult("already_processed")))
    consumer.run_once()
    assert kafka.commits == [7]


def test_idle_pings_database_at_most_every_interval(tmp_path):
    clock = [1000.0]
    store = FakeStore()
    heartbeat = tmp_path / "hb"
    consumer, _ = make(FakeKafka([]), store, heartbeat_path=heartbeat, monotonic=lambda: clock[0])

    consumer.run_once()
    consumer.run_once()
    assert store.pings == 1 and heartbeat.exists()
    clock[0] += IDLE_PING_INTERVAL_S
    consumer.run_once()
    assert store.pings == 2


def test_no_heartbeat_while_idle_database_is_down(tmp_path):
    heartbeat = tmp_path / "hb"
    consumer, _ = make(FakeKafka([]), FakeStore(ping_error=db_down()), heartbeat_path=heartbeat)
    consumer.run_once()
    assert not heartbeat.exists()


def test_run_forever_closes_consumer():
    kafka = FakeKafka([FakeMessage(1)])
    consumer, _ = make(kafka, FakeStore())
    original = consumer.run_once

    def once(*args):
        original(*args)
        consumer.stop_event.set()

    consumer.run_once = once
    consumer.run_forever()
    assert kafka.closed and kafka.commits == [1]


@pytest.mark.parametrize("outcome", ["applied", "stale", "duplicate", "rejected"])
def test_outcomes_are_counted(outcome):
    consumer, _ = make(FakeKafka([FakeMessage(1)]), FakeStore(ProcessResult(outcome)))
    consumer.run_once()
    assert consumer.outcomes == {outcome: 1}
