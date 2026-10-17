import pytest

from earth_hub.config import Settings
from earth_hub.healthcheck import CONSUMER_MAX_AGE_S, is_healthy, main, max_age_s


@pytest.mark.parametrize(
    ("age_s", "max_age", "healthy"),
    [(0, 180, True), (180, 180, True), (181, 180, False), (None, 180, False)],
)
def test_is_healthy(age_s, max_age, healthy):
    now = 10_000.0
    mtime = None if age_s is None else now - age_s
    assert is_healthy(mtime, now, max_age) is healthy


@pytest.mark.parametrize(("interval", "expected"), [(10, 180), (60, 180), (200, 600)])
def test_poller_max_age_is_three_rounds(interval, expected):
    settings = Settings(
        _env_file=None,
        postgres_db="d",
        postgres_user="u",
        postgres_password="p",
        poll_interval_seconds=interval,
    )
    assert max_age_s("poller", settings) == expected
    assert max_age_s("consumer", settings) == CONSUMER_MAX_AGE_S


def test_unknown_service_is_a_usage_error():
    assert main(["api"]) == 2
