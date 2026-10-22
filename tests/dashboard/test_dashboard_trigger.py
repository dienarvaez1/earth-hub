"""The dashboard Worker's cron trigger: it asks GitHub to run one direct round (DEF-052).
A local stand-in plays GitHub's API, so nothing leaves the machine."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from support.dashboard import LocalDashboard

pytestmark = pytest.mark.req("REQ-CI-006")

DISPATCHES = "/repos/dienarvaez1/earth-hub/actions/workflows/direct.yml/dispatches"


class FakeGitHub:
    """Records every request; answers `status` (204: GitHub's answer to a dispatch)."""

    def __init__(self):
        self.requests: list[dict] = []
        self.status = 204
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 (http.server's name)
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                fake.requests.append(
                    {"path": self.path, "headers": dict(self.headers), "body": json.loads(body)}
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


def fire(local: LocalDashboard) -> httpx.Response:
    """What Cloudflare does every hour: run the scheduled handler."""
    return httpx.get(f"{local.url}/__scheduled", params={"cron": "17 * * * *"}, timeout=30)


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
