"""A controlled failure, diagnosed from the logs alone (docs/observability.md).

An event with an impossible latitude goes through the real poller, Kafka, consumer and
PostgreSQL. The test then answers "what happened to it?" from the JSON log lines only, the
way an operator would with `jq`: no database, no API.
"""

import json

import pytest

from earth_hub.logging_config import JsonFormatter

pytestmark = [pytest.mark.e2e, pytest.mark.req("REQ-QA-002")]


def json_lines(caplog) -> list[dict]:
    """The captured records as the services print them: one JSON object per line."""
    formatter = JsonFormatter()
    return [json.loads(formatter.format(r)) for r in caplog.records]


def journey(lines: list[dict], event_id: str) -> list[dict]:
    """What `jq 'select(.event_id == "...")'` shows: the event's lines, in order."""
    return [line for line in lines if line.get("event_id") == event_id]


@pytest.mark.req("REQ-OBS-001", "REQ-OBS-002", "REQ-OBS-003")
def test_a_rejected_event_is_diagnosed_from_the_logs(pipeline, feed, event, caplog):
    caplog.set_level("INFO")
    feed.show(event("good01"), event("bad01", lat=95.0))
    pipeline.poll()
    pipeline.sync()
    pipeline.consumer.log_summary()
    lines = json_lines(caplog)

    # 1. Follow the bad event: the poller published it, the consumer rejected it.
    bad = journey(lines, "bad01")
    assert [(line["stage"], line["message"]) for line in bad] == [
        ("publish", "event published"),
        ("persist", "message rejected"),
    ]
    rejected = bad[-1]
    assert rejected["level"] == "WARNING"
    # 2. Why: the rule and the reason, with where the message sits in Kafka.
    assert rejected["reason"].startswith("GEO-002:")
    assert "95" in rejected["reason"]
    assert {"partition", "offset"} <= rejected.keys()

    # The good event went the same way and was stored.
    good = journey(lines, "good01")
    assert [(line["stage"], line["outcome"]) for line in good if "outcome" in line] == [
        ("persist", "applied")
    ]

    # 3. The consumer's summary counts it, and nothing is left to read.
    (summary,) = [line for line in lines if line["message"] == "consumer summary"]
    assert summary["outcomes"] == {"applied": 1, "rejected": 1}
    assert summary["lag"] == 0

    # Every pipeline line says which stage it comes from.
    pipeline_lines = [line for line in lines if line["logger"].startswith("earth_hub.")]
    assert pipeline_lines and all("stage" in line for line in pipeline_lines)
