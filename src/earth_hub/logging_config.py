"""JSON log lines on stdout, one object per record."""

import json
import logging
import sys
from datetime import UTC, datetime

# Attributes every LogRecord has; anything else came from `extra=` and is logged as a field.
_STANDARD_ATTRS = set(logging.makeLogRecord({}).__dict__) | {"message", "taskName"}

# The stage of a pipeline line that does not name one (earth_hub.observability), by module.
DEFAULT_STAGES = {
    "earth_hub.ingestion.usgs_client": "fetch",
    "earth_hub.ingestion.fdsn": "fetch",
    "earth_hub.ingestion.poller": "fetch",
    "earth_hub.ingestion.backfill": "fetch",
    "earth_hub.ingestion.catchup": "fetch",
    "earth_hub.ingestion.state": "publish",
    "earth_hub.ingestion.producer": "publish",
    "earth_hub.ingestion.__main__": "publish",
    "earth_hub.consumers": "persist",
    "earth_hub.database": "persist",
    "earth_hub.validation": "persist",
}


def default_stage(logger_name: str | None) -> str | None:
    if not logger_name:
        return None
    for prefix, stage in DEFAULT_STAGES.items():
        if logger_name == prefix or logger_name.startswith(prefix + "."):
            return stage
    return None


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS:
                entry[key] = value
        if "stage" not in entry and (stage := default_stage(record.name)) is not None:
            entry["stage"] = stage
        if record.exc_info:
            entry["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
