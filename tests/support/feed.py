"""A controllable stand-in for the USGS feed, and a builder for its events.

Event times are relative to the real clock, because the consumer validates them against
"now" (rule TIME-002) and the poller forgets events older than its state window.
"""

import copy
import json
from datetime import UTC, datetime, timedelta

import httpx

URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class EventFactory:
    def __init__(self, template: dict):
        self._template = template
        self._now = datetime.now(UTC).replace(microsecond=0)

    def __call__(
        self,
        event_id: str,
        *,
        minutes_ago: float = 60,
        mag: float | None = 2.5,
        ids: list[str] | None = None,
        updated_s: int = 30,
        lon: float = -150.0,
        lat: float = 60.0,
        depth: float = 10.0,
        **properties,
    ) -> dict:
        feature = copy.deepcopy(self._template)
        time = self._now - timedelta(minutes=minutes_ago)
        feature["id"] = event_id
        feature["geometry"]["coordinates"] = [lon, lat, depth]
        feature["properties"].update(
            mag=mag,
            time=ms(time),
            updated=ms(time + timedelta(seconds=updated_s)),
            ids="," + ",".join(ids or [event_id]) + ",",
            place=f"Test place for {event_id}",
            status="reviewed",
            type="earthquake",
        )
        feature["properties"].update(properties)
        return feature


def revised(feature: dict, *, by_s: int = 60, **properties) -> dict:
    """The same event, revised by USGS `by_s` seconds later."""
    f = copy.deepcopy(feature)
    f["properties"]["updated"] += by_s * 1000
    f["properties"].update(properties)
    return f


class FeedServer:
    """Serves a FeatureCollection like the real feed: 200 with Last-Modified, 304 when the
    client's If-Modified-Since matches the current version."""

    def __init__(self):
        self._features: dict[str, dict] = {}
        self._version = 0
        self.requests = 0
        self.transport = httpx.MockTransport(self._handle)

    def show(self, *features: dict) -> None:
        """Add or replace events (by id) in the feed, as a new feed version."""
        for feature in features:
            self._features[feature["id"]] = feature
        self._version += 1

    def remove(self, event_id: str) -> None:
        self._features.pop(event_id, None)
        self._version += 1

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests += 1
        last_modified = f"v{self._version}"
        if request.headers.get("If-Modified-Since") == last_modified:
            return httpx.Response(304, headers={"Last-Modified": last_modified})
        document = {
            "type": "FeatureCollection",
            "metadata": {"generated": ms(datetime.now(UTC)), "count": len(self._features)},
            "features": list(self._features.values()),
        }
        return httpx.Response(
            200, content=json.dumps(document).encode(), headers={"Last-Modified": last_modified}
        )
