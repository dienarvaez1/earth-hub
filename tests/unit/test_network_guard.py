"""The guard in tests/conftest.py that keeps unit tests off the network."""

import socket

import httpx
import pytest

from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client

LIVE_FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


@pytest.mark.req("REQ-QA-001")
def test_live_usgs_feed_is_unreachable_from_unit_tests():
    with pytest.raises(RuntimeError, match="must not use the network"), make_http_client() as http:
        UsgsClient(LIVE_FEED, http, max_attempts=1).fetch()


@pytest.mark.req("REQ-QA-001")
def test_dns_and_raw_sockets_are_blocked():
    with pytest.raises(RuntimeError, match="must not use the network"):
        socket.getaddrinfo("earthquake.usgs.gov", 443)
    sock = socket.socket()
    try:
        with pytest.raises(RuntimeError, match="must not use the network"):
            sock.connect(("8.8.8.8", 53))
    finally:
        sock.close()


@pytest.mark.req("REQ-QA-001")
def test_loopback_is_still_allowed():
    sock = socket.socket()
    try:
        # Nothing listens on port 1, so this is refused, not blocked by the guard.
        with pytest.raises(ConnectionRefusedError):
            sock.connect(("127.0.0.1", 1))
    finally:
        sock.close()


@pytest.mark.req("REQ-QA-001")
def test_mock_transports_are_unaffected():
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"ok": True}))
    with make_http_client(transport=transport) as http:
        assert http.get(LIVE_FEED).json() == {"ok": True}
