from datetime import UTC, datetime

import httpx
import pytest

from earth_hub.ingestion.usgs_client import (
    USER_AGENT,
    FeedUnavailable,
    UsgsClient,
    make_http_client,
    parse_http_date,
)

URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
NOW = datetime(2026, 10, 16, 8, 10, 31, tzinfo=UTC)
HEADERS = {
    "Last-Modified": "Fri, 16 Oct 2026 08:10:25 GMT",
    "Expires": "Fri, 16 Oct 2026 08:11:25 GMT",
}


def scripted(*steps):
    """An httpx transport that answers each request with the next step (Response or exception)."""
    requests = []
    remaining = list(steps)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        step = remaining.pop(0)
        if isinstance(step, Exception):
            raise step
        return step

    return httpx.MockTransport(handler), requests


def make_client(transport, sleeps=None, **kwargs) -> UsgsClient:
    http = make_http_client(transport=transport)
    return UsgsClient(
        URL,
        http,
        sleep=(sleeps.append if sleeps is not None else lambda s: None),
        rng=lambda: 0.5,
        clock=lambda: NOW,
        **kwargs,
    )


def test_200_returns_body_and_cache_headers():
    transport, requests = scripted(httpx.Response(200, content=b"{}", headers=HEADERS))
    response = make_client(transport).fetch()
    assert response.not_modified is False
    assert response.body == b"{}"
    assert response.last_modified == HEADERS["Last-Modified"]
    assert response.expires == datetime(2026, 10, 16, 8, 11, 25, tzinfo=UTC)
    assert response.fetched_at == NOW
    assert "If-Modified-Since" not in requests[0].headers
    assert requests[0].headers["User-Agent"] == USER_AGENT


def test_conditional_request_and_304():
    transport, requests = scripted(httpx.Response(304, headers=HEADERS))
    response = make_client(transport).fetch(if_modified_since=HEADERS["Last-Modified"])
    assert response.not_modified is True
    assert response.body is None
    assert requests[0].headers["If-Modified-Since"] == HEADERS["Last-Modified"]


@pytest.mark.parametrize(
    "first_failure",
    [
        httpx.ReadTimeout("timed out"),
        httpx.ConnectTimeout("timed out"),
        httpx.ConnectError("connection refused"),
        httpx.Response(500),
        httpx.Response(503),
        httpx.Response(429),
    ],
    ids=["read-timeout", "connect-timeout", "connect-error", "500", "503", "429"],
)
def test_transient_failure_then_success_recovers(first_failure):
    sleeps = []
    transport, requests = scripted(first_failure, httpx.Response(200, content=b"{}"))
    response = make_client(transport, sleeps).fetch()
    assert response.body == b"{}"
    assert len(requests) == 2
    assert sleeps == [1.5]  # backoff 1 s * 2**0 * (1 + jitter 0.5)


def test_three_server_errors_give_up_with_exponential_backoff():
    sleeps = []
    transport, requests = scripted(httpx.Response(500), httpx.Response(502), httpx.Response(500))
    with pytest.raises(FeedUnavailable, match="after 3 attempts: HTTP 500"):
        make_client(transport, sleeps).fetch()
    assert len(requests) == 3
    assert sleeps == [1.5, 3.0]  # no sleep after the last attempt


@pytest.mark.parametrize("status", [400, 403, 404, 410])
def test_client_errors_are_not_retried(status):
    transport, requests = scripted(httpx.Response(status))
    with pytest.raises(FeedUnavailable, match=f"HTTP {status} \\(not retried\\)"):
        make_client(transport).fetch()
    assert len(requests) == 1


def test_attempts_are_configurable():
    transport, requests = scripted(httpx.ReadTimeout("t"))
    with pytest.raises(FeedUnavailable, match="after 1 attempts"):
        make_client(transport, max_attempts=1).fetch()
    assert len(requests) == 1


def test_timeouts_are_set():
    timeout = make_http_client().timeout
    assert (timeout.connect, timeout.read) == (5.0, 15.0)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Fri, 16 Oct 2026 08:11:25 GMT", datetime(2026, 10, 16, 8, 11, 25, tzinfo=UTC)),
        (None, None),
        ("", None),
        ("not a date", None),
        ("0", None),
    ],
)
def test_parse_http_date(value, expected):
    assert parse_http_date(value) == expected
