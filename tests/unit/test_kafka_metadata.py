import pytest
from confluent_kafka import KafkaError, KafkaException

from earth_hub.kafka_metadata import with_retries


def kafka_error(code) -> KafkaException:
    return KafkaException(KafkaError(code))


def run(lookup, timeout_s=5.0):
    clock = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    return with_retries(
        lookup, timeout_s=timeout_s, sleep=sleep, monotonic=lambda: clock[0]
    ), sleeps


@pytest.mark.req("REQ-PER-013", "REQ-ING-008")
@pytest.mark.parametrize(
    "code",
    [KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE, KafkaError._TRANSPORT],
)
def test_temporary_errors_are_retried_with_backoff(code):
    attempts = iter([kafka_error(code), kafka_error(code), "ok"])

    def lookup():
        step = next(attempts)
        if isinstance(step, Exception):
            raise step
        return step

    result, sleeps = run(lookup)
    assert result == "ok" and sleeps == [0.1, 0.2]


def test_other_errors_are_raised_at_once():
    def lookup():
        raise kafka_error(KafkaError.TOPIC_AUTHORIZATION_FAILED)

    with pytest.raises(KafkaException):
        run(lookup)


def test_gives_up_at_the_deadline():
    def lookup():
        raise kafka_error(KafkaError.NOT_LEADER_FOR_PARTITION)

    with pytest.raises(KafkaException):
        _, sleeps = run(lookup, timeout_s=1.0)
