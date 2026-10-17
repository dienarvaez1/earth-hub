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
