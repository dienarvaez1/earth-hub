"""scripts/trigger_token.py: the dashboard trigger's token, checked and stored without ever
being shown (docs/technical/ci.md#the-trigger)."""

import subprocess
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import trigger_token  # noqa: E402

pytestmark = pytest.mark.req("REQ-CI-006")

TOKEN = "github_pat_0123456789abcdef"
WORKFLOW = "/repos/dienarvaez1/earth-hub/actions/workflows/direct.yml"


def github(status: int, seen: list | None = None) -> httpx.Client:
    def answer(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return httpx.Response(status, json={})

    return httpx.Client(transport=httpx.MockTransport(answer))


class Wrangler:
    def __init__(self, returncode=0, stderr=""):
        self.calls: list[dict] = []
        self.returncode, self.stderr = returncode, stderr

    def __call__(self, args, **kwargs):
        self.calls.append({"args": args, **kwargs})
        return subprocess.CompletedProcess(args, self.returncode, "", self.stderr)


def run(capsys, *, token=TOKEN, status=200, wrangler=None, seen=None):
    opened: list[str] = []
    wrangler = wrangler or Wrangler()
    code = trigger_token.main(
        ask=lambda prompt: token,
        open_url=opened.append,
        client=github(status, seen),
        run=wrangler,
    )
    out = capsys.readouterr()
    assert TOKEN not in out.out + out.err  # never shown
    return code, opened, wrangler


def test_the_form_asks_only_for_actions_on_this_owner():
    query = parse_qs(urlparse(trigger_token.token_form("dienarvaez1/earth-hub")).query)
    assert query["actions"] == ["write"] and query["target_name"] == ["dienarvaez1"]
    assert set(query) == {"name", "description", "target_name", "actions"}


def test_a_good_token_is_checked_then_stored_through_standard_input(capsys):
    seen: list[httpx.Request] = []
    code, opened, wrangler = run(capsys, seen=seen)
    assert code == 0
    assert opened and opened[0].startswith(
        "https://github.com/settings/personal-access-tokens/new?"
    )
    (request,) = seen
    assert request.method == "GET" and request.url.path == WORKFLOW  # starts nothing
    assert request.headers["authorization"] == f"Bearer {TOKEN}"
    (call,) = wrangler.calls
    assert call["args"] == ["npx", "--no-install", "wrangler", "secret", "put", "GITHUB_TOKEN"]
    assert call["input"] == TOKEN + "\n" and call["cwd"] == trigger_token.DASHBOARD
    assert TOKEN not in " ".join(call["args"])


@pytest.mark.parametrize(
    ("status", "says"),
    [(401, "does not accept"), (403, "repository"), (404, "repository"), (500, "500")],
)
def test_a_token_github_refuses_is_not_stored(capsys, status, says):
    wrangler = Wrangler()
    opened: list[str] = []
    code = trigger_token.main(
        ask=lambda prompt: TOKEN, open_url=opened.append, client=github(status), run=wrangler
    )
    out = capsys.readouterr().out
    assert code == 1 and wrangler.calls == []
    assert "Not stored" in out and says in out and TOKEN not in out


def test_nothing_pasted_changes_nothing(capsys):
    code, _, wrangler = run(capsys, token="   ")
    assert code == 1 and wrangler.calls == []


def test_a_wrangler_failure_is_reported_without_the_token(capsys):
    wrangler = Wrangler(returncode=1, stderr=f"error: not logged in ({TOKEN})")
    code, _, _ = run(capsys, wrangler=wrangler)
    assert code == 1  # run() checked the token was not shown, even from wrangler's message


def test_github_out_of_reach_is_reported(capsys):
    def fail(request):
        raise httpx.ConnectError("down")

    code = trigger_token.main(
        ask=lambda prompt: TOKEN,
        open_url=lambda url: None,
        client=httpx.Client(transport=httpx.MockTransport(fail)),
        run=Wrangler(),
    )
    assert code == 1 and "could not reach GitHub" in capsys.readouterr().out
