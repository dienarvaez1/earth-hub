import copy
import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parent / "fixtures"
_FEED_SAMPLE = json.loads((FIXTURES / "usgs" / "all_day_sample.geojson").read_text())


@pytest.fixture
def feed_doc() -> dict:
    """A trimmed real all_day feed with 6 events covering known edge cases. Safe to mutate."""
    return copy.deepcopy(_FEED_SAMPLE)
