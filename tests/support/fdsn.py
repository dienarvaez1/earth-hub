"""A stand-in for the USGS FDSN event service, faithful to the behaviour that matters here.

Measured against the real service (2026-10-17):
- starttime and endtime lose their milliseconds: the service compares at whole seconds;
- both edges are inclusive at that precision: start_s*1000 <= event_ms <= end_s*1000;
- a query matching more than 20,000 events is refused with 400;
- includedeleted is false, true or only, and /count honours it the same way as /query;
- query?eventid=X answers one GeoJSON Feature, found by any of its IDs, or 404 (also for a
  deleted event unless includedeleted is true or only);
- a deletion can keep no `id`, `time`, `updated` or `ids`, only its products' `code` and
  `updateTime` (tx2026pfurqq, 2026-10-30): no search by time returns it, a lookup by that code
  does (DEF-066).
"""

import copy
import json
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs

import httpx

CAP = 20_000


def _codes(event: dict) -> list[str]:
    """The `code` of each of an event's products, as USGS lists them."""
    products = event["properties"].get("products") or {}
    return [p["code"] for kind in products.values() for p in kind if p.get("code")]


def _seconds(value: str) -> int:
    """Parse a query time the way the service does: whole seconds, UTC unless stated."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp())  # floor: the milliseconds are dropped


class FdsnServer:
    def __init__(self, events: Iterable[dict] = ()):
        self._events: dict[str, dict] = {}
        self.requests: list[httpx.Request] = []
        # Scripted failures: callables(request) -> Response or None (None = answer normally).
        self.interceptors: list[Callable[[httpx.Request], httpx.Response | None]] = []
        self.count_bias = 0  # add to /count, to simulate a count/query mismatch
        self.transport = httpx.MockTransport(self._handle)
        for event in events:
            self.put(event)

    def put(self, *events: dict) -> None:
        for event in events:
            # A deletion USGS kept no `id` for is known by its products' code (DEF-066), or by
            # nothing at all (REQ-BF-014): then only a search by time finds it.
            key = event["id"] or next(iter(_codes(event)), f"(no id #{len(self._events)})")
            self._events[key] = copy.deepcopy(event)

    def matching(self, params: dict[str, str]) -> list[dict]:
        start = _seconds(params["starttime"]) * 1000
        end = _seconds(params["endtime"]) * 1000
        deleted = params.get("includedeleted", "false")
        updated_after = (
            _seconds(params["updatedafter"]) * 1000 if "updatedafter" in params else None
        )
        out = []
        for event in self._events.values():
            props = event["properties"]
            is_deleted = props.get("status") == "deleted"
            if deleted == "false" and is_deleted or deleted == "only" and not is_deleted:
                continue
            if props.get("time") is None:  # no search by time returns it, only its ID
                continue
            if not start <= props["time"] <= end:
                continue
            if updated_after is not None and props["updated"] < updated_after:
                continue
            out.append(event)
        return sorted(out, key=lambda e: (e["properties"]["time"], e["id"]))

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        for intercept in list(self.interceptors):
            response = intercept(request)
            if response is not None:
                return response
        params = {k: v[-1] for k, v in parse_qs(request.url.query.decode()).items()}
        method = request.url.path.rsplit("/", 1)[-1]
        if "eventid" in params:
            return self._event(params)
        matches = self.matching(params)
        if method == "count":
            return httpx.Response(
                200, json={"count": len(matches) + self.count_bias, "maxAllowed": CAP}
            )
        if len(matches) > CAP:
            return httpx.Response(
                400, text=f"{len(matches)} matching events exceeds search limit of {CAP}"
            )
        if "limit" in params:
            matches = matches[: int(params["limit"])]
        if not matches:
            return httpx.Response(204)
        body = {
            "type": "FeatureCollection",
            "metadata": {"count": len(matches)},
            "features": matches,
        }
        return httpx.Response(200, content=json.dumps(body).encode())

    def _event(self, params: dict[str, str]) -> httpx.Response:
        wanted = params["eventid"]
        for event in self._events.values():
            ids = {i for i in (event["properties"].get("ids") or "").split(",") if i}
            if wanted == event["id"] or wanted in ids or wanted in _codes(event):
                deleted = event["properties"].get("status") == "deleted"
                if deleted and params.get("includedeleted", "false") == "false":
                    break
                return httpx.Response(200, content=json.dumps(event).encode())
        return httpx.Response(404, text=f"No event found for {wanted}")

    def requests_for(self, method: str) -> list[dict[str, str]]:
        return [
            {k: v[-1] for k, v in parse_qs(r.url.query.decode()).items()}
            for r in self.requests
            if r.url.path.endswith("/" + method)
        ]


def fail_times(times: int, status: int = 503, when=lambda request: True, headers=None):
    """An interceptor that answers `status` for the first `times` matching requests."""
    remaining = [times]

    def intercept(request: httpx.Request):
        if remaining[0] > 0 and when(request):
            remaining[0] -= 1
            return httpx.Response(status, headers=headers or {})
        return None

    return intercept


def at(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def event_at(
    template: dict,
    event_id: str,
    when: datetime,
    *,
    updated: timedelta = timedelta(minutes=5),
    **props,
) -> dict:
    """A USGS-shaped event at an exact time (milliseconds kept)."""
    f = copy.deepcopy(template)
    f["id"] = event_id
    f["properties"].update(
        time=at(when),
        updated=at(when + updated),
        ids=f",{event_id},",
        status="reviewed",
        type="earthquake",
        place=f"Test place for {event_id}",
    )
    f["properties"].update(props)
    return f
