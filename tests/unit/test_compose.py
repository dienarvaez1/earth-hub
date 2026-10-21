"""Settings in docker-compose.yml that behaviour depends on."""

import re
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


def recipe(target: str) -> list[str]:
    text = (REPO / "Makefile").read_text()
    (body,) = re.findall(rf"^{target}:.*\n((?:\t.*\n?)+)", text, re.M)
    return [line.strip() for line in body.splitlines()]


@pytest.mark.req("REQ-REC-002")
def test_each_switch_catches_up_and_checks_against_usgs_once_it_succeeded():
    direct = recipe("mode-direct")
    handover = next(i for i, line in enumerate(direct) if "handover direct" in line)
    after = direct[handover + 1 :]
    assert any(line.startswith("uv run python -m earth_hub.direct once --all") for line in after)
    assert any(line.startswith("uv run python -m earth_hub.usgs_reconcile") for line in after)
    assert "exit 1" in direct[handover]  # a failed handover stops before them

    full = recipe("mode-full")
    handover = next(i for i, line in enumerate(full) if "handover full" in line)
    after = full[handover + 1 :]
    assert any("earth_hub.ingestion.catchup --once" in line for line in after)
    assert any("earth_hub.usgs_reconcile --wait" in line for line in after)

    now = " ".join(recipe("catchup-now"))
    assert "direct once --all" in now and "earth_hub.ingestion.catchup --once" in now


@pytest.mark.req("REQ-MODE-010")
def test_every_target_that_starts_our_services_builds_their_image_first(services):
    # DEF-046: `make mode-full` started the consumers from an image built before the replay
    # headers existed, so the replay was stored at its own time instead of direct mode's.
    ours = {name for name, s in services.items() if s.get("image") == "earth-hub:dev"} - {"migrate"}
    recipes = re.findall(r"^([a-z0-9-]+):.*\n((?:\t.*\n?)+)", (REPO / "Makefile").read_text(), re.M)
    starting = {}
    for target, recipe in recipes:
        lines = recipe.splitlines()
        for i, line in enumerate(lines):
            if " up " in line and ours & set(line.split()):
                starting[target] = any(" build" in earlier for earlier in lines[:i])
    assert {"ingest", "api", "mode-full"} <= set(starting)
    assert [target for target, built in starting.items() if not built] == []
