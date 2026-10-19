"""Shared pieces for exporter tests: a fake export API, a bucket that can fail, a row."""

from datetime import UTC, datetime

import httpx

from earth_hub.export.bucket import Bucket

NOW = datetime(2026, 10, 18, 9, 0, tzinfo=UTC)
BUCKET = "earth-hub-test"


def make_row(row_id: int, **changes) -> dict:
    row = {
        "id": row_id,
        "preferred_event_id": f"us{row_id:04d}",
        "event_time": "2026-10-17T20:26:51.123456Z",
        "event_time_ms": 1792268811123,
        "source_updated": "2026-10-17T20:31:02.040000Z",
        "source_updated_ms": 1792269062040,
        "magnitude": 2.37,
        "mag_type": "ml",
        "place": "86 km E of Ugashik, Alaska",
        "city": "Ugashik",
        "region": "Alaska",
        "country": "United States",
        "longitude": -155.6123,
        "latitude": 57.4401,
        "depth_km": 10.0,
        "event_type": "earthquake",
        "review_status": "reviewed",
        "tsunami": False,
        "felt": None,
        "cdi": None,
        "mmi": None,
        "alert": None,
        "sig": 85,
        "net": "ak",
        "url": "https://earthquake.usgs.gov/earthquakes/eventpage/ak0001",
        "title": "M 2.4 - 86 km E of Ugashik, Alaska",
        "revision_ids": [f"us{row_id:04d}"],
        "revision_count": 1,
        "first_seen_at": "2026-10-17T20:27:00.000001Z",
        "last_applied_at": "2026-10-17T20:31:05.5Z",
    }
    return row | changes


class FakeApi:
    """GET export/earthquakes over a list of rows, with the real paging contract."""

    def __init__(self, rows):
        self.rows = rows
        self.fail_after_pages: int | None = None
        self.pages = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/export/earthquakes"
        self.pages += 1
        if self.fail_after_pages is not None and self.pages > self.fail_after_pages:
            raise KeyboardInterrupt
        after = int(request.url.params["after_id"])
        limit = int(request.url.params["limit"])
        page = sorted((r for r in self.rows if r["id"] > after), key=lambda r: r["id"])[:limit]
        return httpx.Response(
            200,
            json={"rows": page, "next_after_id": page[-1]["id"] if len(page) == limit else None},
        )

    def client(self) -> httpx.Client:
        return httpx.Client(
            base_url="http://api.test/api/v1/", transport=httpx.MockTransport(self.handler)
        )


class FlakyBucket(Bucket):
    """Fails uploads of chosen keys, once each."""

    def __init__(self, client, name, failing: set[str]):
        super().__init__(client, name)
        self.failing = set(failing)
        self.puts: list[str] = []

    def put(self, key, body, **kwargs):
        self.puts.append(key)
        if key in self.failing:
            self.failing.discard(key)
            raise ConnectionError("simulated network failure")
        return super().put(key, body, **kwargs)
