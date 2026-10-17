from datetime import UTC, datetime, timedelta

import httpx
import pytest

from earth_hub.ingestion.fdsn import (
    EMPTY_COLLECTION,
    FdsnClient,
    FdsnTooSlow,
    FdsnUnavailable,
    fdsn_time,
    retry_after_seconds,
)
from earth_hub.ingestion.usgs_client import USER_AGENT, make_http_client, user_agent

BASE = "https://earthquake.usgs.gov/fdsnws/event/1"
START = datetime(2026, 1, 4, tzinfo=UTC)
END = datetime(2026, 1, 11, tzinfo=UTC)
NOW = datetime(2026, 10, 17, 8, 0, tzinfo=UTC)


class Clock:
    def __init__(self):
        self.t = 100.0
        self.sleeps: list[float] = []

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.t += seconds

    def monotonic(self):
        return self.t


def client_for(handler, clock=None, **kwargs) -> tuple[FdsnClient, Clock, list[httpx.Request]]:
    clock = clock or Clock()
    requests = []

    def record(request):
        requests.append(request)
        return handler(request)

    http = make_http_client(transport=httpx.MockTransport(record))
    client = FdsnClient(
        BASE,
        http,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        clock=lambda: NOW,
        rng=lambda: 0.0,
        **kwargs,
    )
    return client, clock, requests


@pytest.mark.req("REQ-BF-002")
def test_times_are_whole_seconds_in_utc():
    assert fdsn_time(START) == "2026-01-04T00:00:00"
    from datetime import timezone

    assert (
        fdsn_time(datetime(2026, 1, 4, 2, tzinfo=timezone(timedelta(hours=2))))
        == "2026-01-04T00:00:00"
    )


@pytest.mark.req("REQ-BF-002")
def test_sub_second_edges_are_refused_rather_than_silently_dropped():
    with pytest.raises(ValueError, match="not whole seconds"):
        fdsn_time(datetime(2026, 1, 3, 23, 59, 59, 999000, tzinfo=UTC))
    with pytest.raises(ValueError, match="timezone-aware"):
        fdsn_time(datetime(2026, 1, 4))


@pytest.mark.req("REQ-BF-003", "REQ-BF-008")
def test_query_parameters():
    def handler(request):
        if request.url.path.endswith("/count"):
            return httpx.Response(200, json={"count": 0, "maxAllowed": 20000})
        return httpx.Response(200, content=EMPTY_COLLECTION)

    client, _, requests = client_for(handler)
    client.query(START, END)
    client.count(START, END)
    query, count = (dict(r.url.params) for r in requests)
    assert requests[0].url.path.endswith("/query") and requests[1].url.path.endswith("/count")
    assert query == {
        "format": "geojson",
        "starttime": "2026-01-04T00:00:00",
        "endtime": "2026-01-11T00:00:00",
        "includedeleted": "true",
        "orderby": "time-asc",
    }
    assert "limit" not in query  # a capped result would silently drop events
    assert {k: v for k, v in count.items()} == {k: v for k, v in query.items() if k != "orderby"}


def test_updated_after_and_deleted_only():
    client, _, requests = client_for(lambda r: httpx.Response(200, json={"count": 3}))
    assert client.count(START, END, include_deleted="only", updated_after=START) == 3
    params = dict(requests[0].url.params)
    assert params["includedeleted"] == "only" and params["updatedafter"] == "2026-01-04T00:00:00"


def test_204_means_no_events():
    client, _, _ = client_for(lambda r: httpx.Response(204))
    assert client.query(START, END) == EMPTY_COLLECTION


@pytest.mark.req("REQ-BF-007")
def test_requests_are_spaced_by_the_pause():
    client, clock, _ = client_for(lambda r: httpx.Response(200, json={"count": 0}), pause_s=1.5)
    for _ in range(4):
        client.count(START, END)
        clock.t += 0.2  # the caller's own work between requests
    assert clock.sleeps == [pytest.approx(1.3)] * 3  # first request waits for nothing


@pytest.mark.req("REQ-BF-007")
def test_retry_after_is_honoured():
    answers = iter(
        [httpx.Response(429, headers={"Retry-After": "20"}), httpx.Response(200, json={"count": 1})]
    )
    client, clock, _ = client_for(lambda r: next(answers), pause_s=0)
    assert client.count(START, END) == 1
    assert clock.sleeps == [20.0]


@pytest.mark.req("REQ-BF-007")
def test_server_errors_back_off_then_give_up():
    client, clock, requests = client_for(lambda r: httpx.Response(503), pause_s=0, backoff_s=2.0)
    with pytest.raises(FdsnUnavailable, match="after 3 attempts"):
        client.count(START, END)
    assert len(requests) == 3 and clock.sleeps == [2.0, 4.0]


def test_client_errors_are_not_retried():
    client, _, requests = client_for(
        lambda r: httpx.Response(400, text="exceeds search limit"), pause_s=0
    )
    with pytest.raises(FdsnUnavailable, match="not retried"):
        client.query(START, END)
    assert len(requests) == 1


@pytest.mark.req("REQ-BF-013")
@pytest.mark.parametrize(
    "answer",
    [httpx.ReadTimeout("slow"), httpx.Response(504, text="504 Gateway Time-out")],
    ids=["read-timeout", "504"],
)
def test_timeouts_and_504_are_too_slow_and_not_retried(answer):
    """The USGS gateway gives up at 60 s and caches the 504: asking again is pointless."""

    def handler(request):
        if isinstance(answer, Exception):
            raise answer
        return answer

    client, clock, requests = client_for(handler, pause_s=0)
    with pytest.raises(FdsnTooSlow):
        client.query(START, END)
    assert len(requests) == 1 and clock.sleeps == []


def test_connection_errors_are_still_retried():
    answers = iter([httpx.ConnectError("refused"), httpx.Response(200, json={"count": 2})])

    def handler(request):
        answer = next(answers)
        if isinstance(answer, Exception):
            raise answer
        return answer

    client, _, _ = client_for(handler, pause_s=0)
    assert client.count(START, END) == 2


def test_empty_window_is_refused():
    client, _, _ = client_for(lambda r: httpx.Response(200))
    with pytest.raises(ValueError):
        client.count(END, START)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("30", 30.0),
        ("0", 0.0),
        (None, None),
        ("soon", None),
        ("Sat, 17 Oct 2026 08:01:00 GMT", 60.0),
    ],
)
def test_retry_after_parsing(header, expected):
    assert retry_after_seconds(header, NOW) == expected


@pytest.mark.req("REQ-OPS-006")
def test_user_agent_contact_is_optional():
    assert user_agent("") == USER_AGENT
    assert user_agent("https://github.com/example/earth-hub").endswith(
        "(local-first earthquake data platform; +https://github.com/example/earth-hub)"
    )
    with make_http_client("ops@example.org") as http:
        assert "+ops@example.org" in http.headers["User-Agent"]
