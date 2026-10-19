import json
import logging
import sys

import pytest

from earth_hub.logging_config import JsonFormatter, configure_logging


def make_record(**extra) -> logging.LogRecord:
    record = logging.makeLogRecord(
        {"name": "earth_hub.test", "levelname": "INFO", "msg": "event %s", "args": ("stored",)}
    )
    record.__dict__.update(extra)
    return record


@pytest.mark.req("REQ-OPS-004")
def test_record_is_one_json_object_with_core_fields():
    entry = json.loads(JsonFormatter().format(make_record()))
    assert entry["level"] == "INFO"
    assert entry["logger"] == "earth_hub.test"
    assert entry["message"] == "event stored"
    assert entry["ts"].endswith("+00:00")


@pytest.mark.req("REQ-OPS-004")
def test_extra_fields_are_included():
    entry = json.loads(JsonFormatter().format(make_record(event_id="us7000abcd", stage="persist")))
    assert entry["event_id"] == "us7000abcd"
    assert entry["stage"] == "persist"


def test_exception_is_included():
    try:
        raise ValueError("bad payload")
    except ValueError:
        record = logging.makeLogRecord({"msg": "failed", "exc_info": sys.exc_info()})
    entry = json.loads(JsonFormatter().format(record))
    assert "ValueError: bad payload" in entry["exc_info"]


def test_configure_logging_writes_json_to_stdout(capsys):
    configure_logging("INFO")
    logging.getLogger("earth_hub.test").info("ready", extra={"stage": "startup"})
    line = capsys.readouterr().out.strip().splitlines()[-1]
    assert json.loads(line)["stage"] == "startup"


@pytest.fixture(autouse=True)
def restore_root_logger():
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield
    root.handlers[:], root.level = handlers, level


@pytest.mark.req("REQ-OBS-001")
@pytest.mark.parametrize(
    ("logger", "stage"),
    [
        ("earth_hub.ingestion.usgs_client", "fetch"),
        ("earth_hub.ingestion.backfill", "fetch"),
        ("earth_hub.ingestion.state", "publish"),
        ("earth_hub.consumers.earthquake_consumer", "persist"),
        ("earth_hub.database.repository", "persist"),
    ],
)
def test_pipeline_lines_without_a_stage_get_their_modules(logger, stage):
    record = make_record()
    record.name = logger
    assert json.loads(JsonFormatter().format(record))["stage"] == stage


@pytest.mark.req("REQ-OBS-001")
def test_a_stated_stage_wins_and_other_loggers_get_none():
    record = make_record(stage="publish")
    record.name = "earth_hub.ingestion.backfill"
    assert json.loads(JsonFormatter().format(record))["stage"] == "publish"
    assert "stage" not in json.loads(JsonFormatter().format(make_record()))  # earth_hub.test
