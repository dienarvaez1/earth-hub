"""The dashboard Worker's cron trigger: it asks GitHub to run one direct round (DEF-052).
A local stand-in plays GitHub's API, so nothing leaves the machine."""

import json
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from support.dashboard import LocalDashboard

pytestmark = pytest.mark.req("REQ-CI-006")

WORKFLOW = "/repos/dienarvaez1/earth-hub/actions/workflows/direct.yml"
DISPATCHES = WORKFLOW + "/dispatches"


class FakeGitHub:
    """Records every request; answers a dispatch with `status` (204: GitHub's answer), and the
    workflow with the token's `expiry` header, as GitHub states it on every reply."""

    def __init__(self):
        self.requests: list[dict] = []
        self.status = 204
        self.expiry: str | None = None
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 (http.server's name)
                fake.requests.append(
                    {"method": "GET", "path": self.path, "headers": dict(self.headers)}
                )
                self.send_response(200)
                if fake.expiry is not None:
                    self.send_header("GitHub-Authentication-Token-Expiration", fake.expiry)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b"{}")

            def do_POST(self):  # noqa: N802 (http.server's name)
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                fake.requests.append(
                    {
                        "method": "POST",
                        "path": self.path,
                        "headers": dict(self.headers),
                        "body": json.loads(body),
                    }
                )
                self.send_response(fake.status)
                self.end_headers()
                if fake.status != 204:
                    self.wfile.write(b'{"message": "Bad credentials"}')

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()


@pytest.fixture(scope="module")
def github():
    fake = FakeGitHub()
    yield fake
    fake.close()


def worker(state, github, *token: str) -> LocalDashboard:
    local = LocalDashboard(state)
    local.start("--test-scheduled", "--var", f"GITHUB_API:{github.url}", *token)
    return local


def fire(local: LocalDashboard, at: str = "2026-10-22T09:17:00+00:00") -> httpx.Response:
    """What Cloudflare does every hour: run the scheduled handler, as at `at`."""
    time = int(datetime.fromisoformat(at).timestamp() * 1000)
    params = {"cron": "17 * * * *", "time": str(time)}
    # The runtime's own endpoint (`--test-scheduled`); it takes the event's time in ms.
    return httpx.get(f"{local.url}/cdn-cgi/local/scheduled", params=params, timeout=30)


@pytest.fixture(scope="module")
def with_token(built, github, tmp_path_factory):
    local = worker(tmp_path_factory.mktemp("trigger"), github, "--var", "GITHUB_TOKEN:t0ken")
    yield local
    local.stop()


def test_each_trigger_asks_github_for_one_round_on_main(with_token, github):
    github.requests.clear()
    github.status = 204
    assert fire(with_token).status_code == 200
    (request,) = github.requests
    assert request["path"] == DISPATCHES
    assert request["body"] == {"ref": "main", "inputs": {"job": "round"}}
    headers = {k.lower(): v for k, v in request["headers"].items()}
    assert headers["authorization"] == "Bearer t0ken"
    assert headers["accept"] == "application/vnd.github+json"
    assert headers["x-github-api-version"] == "2022-11-28"
    assert headers["user-agent"] == "earth-hub-trigger"


@pytest.mark.req("REQ-CI-007")
@pytest.mark.parametrize(
    ("expiry", "days"),
    [
        ("2026-11-05 07:00:00 UTC", "13"),  # 13 days and 22 h 43 min after 08:17
        ("2026-11-05 00:00:00 -0700", "13"),  # the same moment, in another zone
        ("2027-10-22 08:17:00 UTC", "365"),
        (None, None),  # a token that never expires: nothing passed
    ],
)
def test_once_a_day_the_trigger_passes_the_days_the_token_has_left(
    with_token, github, expiry, days
):
    github.requests.clear()
    github.expiry = expiry
    try:
        assert fire(with_token, at="2026-10-22T08:17:00+00:00").status_code == 200
    finally:
        github.expiry = None
    check, dispatch = github.requests
    assert check["method"] == "GET" and check["path"] == WORKFLOW  # starts nothing
    assert check["headers"]["Authorization"] == "Bearer t0ken"
    inputs = {"job": "round"} | ({"token_days_left": days} if days else {})
    assert dispatch["path"] == DISPATCHES and dispatch["body"]["inputs"] == inputs


@pytest.mark.req("REQ-CI-007")
def test_the_other_hours_only_start_the_round(with_token, github):
    github.requests.clear()
    github.expiry = "2026-11-05 07:00:00 UTC"
    try:
        assert fire(with_token, at="2026-10-22T09:17:00+00:00").status_code == 200
    finally:
        github.expiry = None
    (dispatch,) = github.requests
    assert dispatch["method"] == "POST" and dispatch["body"]["inputs"] == {"job": "round"}


def test_a_refusal_fails_the_trigger(with_token, github):
    github.requests.clear()
    github.status = 401
    try:
        assert fire(with_token).status_code >= 500  # Cloudflare records the run as failed
    finally:
        github.status = 204
    assert len(github.requests) == 1


def test_without_a_token_nothing_is_asked(built, github, tmp_path):
    local = worker(tmp_path, github)
    try:
        github.requests.clear()
        assert fire(local).status_code == 200
        assert github.requests == []
    finally:
        local.stop()
