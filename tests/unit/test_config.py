import pytest
from pydantic import ValidationError

from earth_hub.config import Settings

REQUIRED = {
    "POSTGRES_DB": "earth_hub",
    "POSTGRES_USER": "earth_hub_app",
    "POSTGRES_PASSWORD": "s3cret-p@ss/word",
}


@pytest.fixture
def env(monkeypatch):
    """Start from an environment with no Earth-Hub settings, then set the required ones."""
    for name in Settings.model_fields:
        monkeypatch.delenv(name.upper(), raising=False)
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


def load() -> Settings:
    # _env_file=None keeps a developer's real .env out of unit tests.
    return Settings(_env_file=None)


def test_defaults_are_host_side_values(env):
    settings = load()
    assert settings.postgres_host == "localhost"
    assert settings.postgres_port == 5433
    assert settings.kafka_bootstrap_servers == "localhost:29092"
    assert settings.kafka_topic_raw == "earthquakes.raw"
    assert str(settings.usgs_feed_url).endswith("/summary/all_day.geojson")
    assert settings.poll_interval_seconds == 60


def test_values_come_from_environment(env):
    env.setenv("POSTGRES_HOST", "db.internal")
    env.setenv("POSTGRES_PORT", "6543")
    env.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    env.setenv("POLL_INTERVAL_SECONDS", "30")
    settings = load()
    assert settings.postgres_host == "db.internal"
    assert settings.postgres_port == 6543
    assert settings.kafka_bootstrap_servers == "kafka:9092"
    assert settings.poll_interval_seconds == 30


@pytest.mark.parametrize("missing", sorted(REQUIRED))
def test_missing_required_setting_fails(env, missing):
    env.delenv(missing)
    with pytest.raises(ValidationError, match=missing.lower()):
        load()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("POSTGRES_PORT", "0"),
        ("POSTGRES_PORT", "not-a-port"),
        ("API_PORT", "70000"),
        ("APP_ENV", "staging"),
        ("USGS_FEED_URL", "not a url"),
        ("POLL_INTERVAL_SECONDS", "5"),
        ("POSTGRES_DB", ""),
        ("KAFKA_BOOTSTRAP_SERVERS", ""),
    ],
)
def test_invalid_setting_fails(env, name, value):
    env.setenv(name, value)
    with pytest.raises(ValidationError, match=name.lower()):
        load()


def test_database_url_is_built_from_parts(env):
    url = load().database_url()
    assert url.drivername == "postgresql+psycopg"
    assert (url.username, url.host, url.port, url.database) == (
        "earth_hub_app",
        "localhost",
        5433,
        "earth_hub",
    )
    assert url.password == REQUIRED["POSTGRES_PASSWORD"]


def test_database_url_can_target_another_database(env):
    assert load().database_url("earth_hub_test").database == "earth_hub_test"


def test_special_characters_in_password_are_escaped(env):
    rendered = load().database_url().render_as_string(hide_password=False)
    assert "s3cret-p%40ss%2Fword@localhost" in rendered


def test_password_never_appears_in_text_output(env):
    settings = load()
    password = REQUIRED["POSTGRES_PASSWORD"]
    for text in (repr(settings), str(settings), str(settings.database_url())):
        assert password not in text
    assert password not in settings.model_dump_json()
