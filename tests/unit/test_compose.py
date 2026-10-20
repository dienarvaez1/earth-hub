"""Settings in docker-compose.yml that behaviour depends on."""

from pathlib import Path

import pytest
import yaml
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def services() -> dict:
    return yaml.safe_load((REPO / "docker-compose.yml").read_text())["services"]


@pytest.mark.req("REQ-OBS-004")
def test_the_live_consumer_runs_one_message_at_a_time(services):
    # Batches log routine outcomes at debug level only, so live events would not be
    # traceable from the feed to the database at the default level (DEF-036).
    assert services["consumer"]["environment"]["CONSUMER_BATCH_SIZE"] == "1"
    assert "KAFKA_CONSUMER_TOPIC" not in services["consumer"]["environment"]


@pytest.mark.req("REQ-OBS-004")
def test_only_the_backfill_consumer_takes_batches(services):
    backfill = services["consumer-backfill"]["environment"]
    assert "CONSUMER_BATCH_SIZE" not in backfill  # from .env
    assert int(dotenv_values(REPO / ".env.example")["CONSUMER_BATCH_SIZE"]) > 1
    # Both still reach Kafka and PostgreSQL by their in-network names.
    for name in ("consumer", "consumer-backfill"):
        assert services[name]["environment"]["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:9092"
