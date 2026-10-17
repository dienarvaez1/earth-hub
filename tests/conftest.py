import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from earth_hub.ingestion.events import build_message

FIXTURES = Path(__file__).resolve().parent / "fixtures"
_FEED_SAMPLE = json.loads((FIXTURES / "usgs" / "all_day_sample.geojson").read_text())


@pytest.fixture
def feed_doc() -> dict:
    """A trimmed real all_day feed with 6 events covering known edge cases. Safe to mutate."""
    return copy.deepcopy(_FEED_SAMPLE)


FETCHED_AT = datetime(2026, 10, 16, 8, 10, 31, tzinfo=UTC)
# A "now" just after the fixture events, for validating them as the consumer would.
FIXTURE_NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)


@pytest.fixture
def make_envelope():
    """Build an earthquakes.raw message from a feature with the poller's own code, so
    consumer tests always use the real producer contract. Returns bytes."""

    def make(feature: dict, **envelope_overrides) -> bytes:
        message = build_message(feature, fetched_at=FETCHED_AT, feed_generated_ms=None)
        if not envelope_overrides:
            return message.to_json()
        envelope = json.loads(message.to_json())
        envelope.update(envelope_overrides)
        return json.dumps(envelope).encode()

    return make


@pytest.fixture
def make_feature(feed_doc):
    """A copy of the first fixture feature with a new id and optional property changes."""

    def make(event_id: str, ids: list[str] | None = None, **properties) -> dict:
        feature = copy.deepcopy(feed_doc["features"][0])
        feature["id"] = event_id
        feature["properties"]["ids"] = "," + ",".join(ids or [event_id]) + ","
        feature["properties"].update(properties)
        return feature

    return make
