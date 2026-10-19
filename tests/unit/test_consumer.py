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


@pytest.mark.req("REQ-PER-007")
def test_offset_committed_after_successful_processing():
    kafka, store = FakeKafka([FakeMessage(7)]), FakeStore()
    consumer, _ = make(kafka, store)
    assert consumer.run_once() == 1
    assert store.processed == [7] and kafka.commits == [7]
    assert consumer.outcomes == {"applied": 1}


@pytest.mark.req("REQ-PER-007", "REQ-PER-008")
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
    assert consumer.run_once() == 1
    assert store.processed == [7, 7, 7, 7]
    assert sleeps == [1, 2, 4]
    assert kafka.commits == [7]


def test_outage_backoff_is_capped():
    store = FakeStore(*[db_down()] * 8, ProcessResult("applied", 1))
    consumer, sleeps = make(FakeKafka([FakeMessage(1)]), store)
    consumer.run_once()
    assert sleeps == [1, 2, 4, 8, 16, MAX_BACKOFF_S, MAX_BACKOFF_S, MAX_BACKOFF_S]


@pytest.mark.req("REQ-PER-008")
def test_stopping_during_outage_commits_nothing():
    kafka = FakeKafka([FakeMessage(7)])
    store = FakeStore(*[db_down()] * 5)
    consumer, _ = make(kafka, store)
    consumer._sleep = lambda s: consumer.stop_event.set()
    assert consumer.run_once() == 0
    assert kafka.commits == []


@pytest.mark.req("REQ-PER-009")
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


@pytest.mark.req("REQ-PER-008")
def test_integrity_error_is_not_an_outage():
    assert not is_infrastructure_error(IntegrityError("INSERT", {}, Exception("dup")))
    assert is_infrastructure_error(db_down())


def test_failed_offset_commit_does_not_crash():
    kafka = FakeKafka([FakeMessage(7)], commit_error=KafkaException("rebalance"))
    consumer, _ = make(kafka, FakeStore())
    assert consumer.run_once() == 1


def test_kafka_errors_are_skipped_without_commit():
    eof = FakeMessage(0, error=FakeError(KafkaError._PARTITION_EOF))
    other = FakeMessage(0, error=FakeError(KafkaError._TRANSPORT))
    kafka, store = FakeKafka([eof, other]), FakeStore()
    consumer, _ = make(kafka, store)
    assert consumer.run_once() == 0 and consumer.run_once() == 0
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


@pytest.mark.req("REQ-OPS-005")
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


# --- batches -----------------------------------------------------------------------------


class BatchKafka(FakeKafka):
    def consume(self, num_messages, timeout):
        batch, self.messages = self.messages[:num_messages], self.messages[num_messages:]
        return batch

    def commit(self, *, message=None, offsets=None, asynchronous):
        assert asynchronous is False
        if self.commit_error:
            raise self.commit_error
        if offsets is not None:
            self.commits.append(sorted((tp.partition, tp.offset) for tp in offsets))
        else:
            self.commits.append(message.offset())


class BatchStore(FakeStore):
    def __init__(self, *batch_script, **kwargs):
        super().__init__(**kwargs)
        self.batch_script = list(batch_script)
        self.batches: list[list[int]] = []

    def process_batch(self, messages):
        self.batches.append([m.offset for m in messages])
        step = self.batch_script.pop(0) if self.batch_script else None
        if isinstance(step, Exception):
            raise step
        return [ProcessResult("applied", 1) for _ in messages]


def make_batch(kafka, store, batch_size=500):
    sleeps = []
    consumer = EarthquakeConsumer(kafka, store, sleep=sleeps.append, batch_size=batch_size)
    return consumer, sleeps


@pytest.mark.req("REQ-PER-007", "REQ-PER-015")
def test_batch_commits_each_partitions_next_offset_after_the_database():
    messages = [FakeMessage(o, partition=p) for p, o in [(0, 4), (1, 9), (0, 5), (2, 1), (1, 10)]]
    kafka, store = BatchKafka(messages), BatchStore()
    consumer, _ = make_batch(kafka, store)
    assert consumer.run_once() == 5
    assert store.batches == [[4, 9, 5, 1, 10]]
    assert kafka.commits == [[(0, 6), (1, 11), (2, 2)]]  # highest offset + 1, per partition
    assert consumer.outcomes == {"applied": 5}


def test_batch_size_limits_each_round():
    kafka, store = BatchKafka([FakeMessage(o) for o in range(7)]), BatchStore()
    consumer, _ = make_batch(kafka, store, batch_size=3)
    assert [consumer.run_once() for _ in range(4)] == [3, 3, 1, 0]


@pytest.mark.req("REQ-PER-008")
def test_database_outage_retries_the_whole_batch_and_commits_after():
    kafka = BatchKafka([FakeMessage(o) for o in range(3)])
    store = BatchStore(db_down(), db_down())
    consumer, sleeps = make_batch(kafka, store)
    assert consumer.run_once() == 3
    assert store.batches == [[0, 1, 2]] * 3 and sleeps == [1, 2]
    assert kafka.commits == [[(0, 3)]]


@pytest.mark.req("REQ-PER-016")
def test_failed_batch_is_redone_one_by_one_isolating_the_bad_message():
    kafka = BatchKafka([FakeMessage(o) for o in range(4)])
    bug = ValueError("boom")
    # The batch fails; then message 2 fails three times on its own, the rest succeed.
    store = BatchStore(bug)
    script = {2: [bug, bug, bug]}

    def process(message):
        store.processed.append(message.offset)
        errors = script.get(message.offset)
        if errors:
            raise errors.pop(0)
        return ProcessResult("applied", 1)

    store.process = process
    consumer, _ = make_batch(kafka, store)
    assert consumer.run_once() == 4
    assert store.errors == [(2, "ValueError: boom")]
    assert consumer.outcomes == {"applied": 3, "error": 1}
    assert kafka.commits == [[(0, 4)]]


def test_stopping_during_a_batch_outage_commits_nothing():
    kafka = BatchKafka([FakeMessage(o) for o in range(3)])
    consumer, _ = make_batch(kafka, BatchStore(*[db_down()] * 5))
    consumer._sleep = lambda s: consumer.stop_event.set()
    assert consumer.run_once() == 0 and kafka.commits == []


def test_kafka_errors_in_a_batch_are_skipped():
    eof = FakeMessage(0, error=FakeError(KafkaError._PARTITION_EOF))
    kafka, store = BatchKafka([eof, FakeMessage(1)]), BatchStore()
    consumer, _ = make_batch(kafka, store)
    assert consumer.run_once() == 1 and store.batches == [[1]]


# --- observability: event IDs, stages, the periodic summary --------------------------------


class ScriptedBatchStore(BatchStore):
    def __init__(self, *results):
        super().__init__()
        self.results = list(results)

    def process_batch(self, messages):
        self.batches.append([m.offset for m in messages])
        return [self.results.pop(0) for _ in messages]


def records(caplog, message):
    return [r for r in caplog.records if r.getMessage() == message]


@pytest.mark.req("REQ-OBS-001")
def test_every_message_line_has_its_event_id_stage_and_position(caplog):
    caplog.set_level("DEBUG")
    consumer, _ = make(FakeKafka([FakeMessage(3)]), FakeStore())
    consumer.run_once()
    (line,) = records(caplog, "message processed")
    assert (line.event_id, line.stage, line.partition, line.offset) == ("us1", "persist", 0, 3)


@pytest.mark.req("REQ-OBS-001")
def test_in_batches_rejections_and_errors_get_their_own_lines(caplog):
    caplog.set_level("INFO")
    store = ScriptedBatchStore(
        ProcessResult("applied", 1),
        ProcessResult("rejected", reason="GEO-002: latitude 95.0 is outside -90..90"),
        ProcessResult("error", reason="ValueError: boom"),
    )
    consumer, _ = make_batch(BatchKafka([FakeMessage(o) for o in (1, 2, 3)]), store)
    consumer.run_once()
    assert records(caplog, "message processed") == []  # routine: debug only in batches
    (rejected,) = records(caplog, "message rejected")
    assert rejected.levelname == "WARNING"
    assert (rejected.event_id, rejected.stage, rejected.offset) == ("us1", "persist", 2)
    assert rejected.reason.startswith("GEO-002:")
    (failed,) = records(caplog, "message failed")
    assert failed.levelname == "ERROR" and failed.offset == 3


@pytest.mark.req("REQ-OBS-001")
def test_error_lines_name_the_event(caplog):
    store = FakeStore(ValueError("bad"), ValueError("bad"), ValueError("bad"))
    consumer, _ = make(FakeKafka([FakeMessage(5)]), store)
    consumer.run_once()
    lines = records(caplog, "unexpected error processing message")
    assert len(lines) == 3 and all(r.event_id == "us1" and r.stage == "persist" for r in lines)
    (failed,) = records(caplog, "message failed")
    assert failed.reason == "ValueError: bad"


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.mark.req("REQ-OBS-002")
def test_a_summary_follows_every_interval(caplog):
    caplog.set_level("INFO")
    clock = Clock()
    kafka = FakeKafka([FakeMessage(o) for o in range(1, 5)])
    store = FakeStore(ProcessResult("applied", 1), ProcessResult("rejected", reason="r"))
    consumer, _ = make(kafka, store, monotonic=clock, summary_interval_s=60)
    consumer.run_once()
    consumer.run_once()
    assert records(caplog, "consumer summary") == []  # not yet due
    clock.now += 60
    consumer.run_once()
    (first,) = records(caplog, "consumer summary")
    assert first.stage == "persist"
    assert (first.messages, first.outcomes) == (3, {"applied": 2, "rejected": 1})
    assert first.lag is None  # a fake Kafka cannot say
    clock.now += 60
    consumer.run_once()
    second = records(caplog, "consumer summary")[1]
    assert (second.messages, second.outcomes) == (1, {"applied": 1})  # since the last one
    assert second.totals == {"applied": 3, "rejected": 1}  # since start


@pytest.mark.req("REQ-OBS-002")
def test_an_idle_consumer_still_reports(caplog):
    caplog.set_level("INFO")
    clock = Clock()
    consumer, _ = make(FakeKafka([]), FakeStore(), monotonic=clock, summary_interval_s=60)
    clock.now += 61
    consumer.run_once()
    (summary,) = records(caplog, "consumer summary")
    assert (summary.messages, summary.outcomes) == (0, {})


class LaggingKafka(FakeKafka):
    """Partition 0 ends at offset 120 and the consumer is at 100; partition 1 is unread."""

    def assignment(self):
        from confluent_kafka import TopicPartition

        return [TopicPartition("earthquakes.raw", 0), TopicPartition("earthquakes.raw", 1)]

    def position(self, partitions):
        from confluent_kafka import TopicPartition

        return [
            TopicPartition("earthquakes.raw", 0, 100),
            TopicPartition("earthquakes.raw", 1, -1001),
        ]

    def get_watermark_offsets(self, tp, timeout, cached):
        return {0: (0, 120), 1: (40, 55)}[tp.partition]


@pytest.mark.req("REQ-OBS-002")
def test_the_summary_reports_lag_per_partition(caplog):
    caplog.set_level("INFO")
    clock = Clock()
    consumer, _ = make(LaggingKafka([]), FakeStore(), monotonic=clock, summary_interval_s=1)
    clock.now += 2
    consumer.run_once()
    (summary,) = records(caplog, "consumer summary")
    # Partition 1 has no position yet: everything from its first offset is still to read.
    assert summary.lag_by_partition == {"earthquakes.raw[0]": 20, "earthquakes.raw[1]": 15}
    assert summary.lag == 35
