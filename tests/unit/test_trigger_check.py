"""scripts/trigger_check.py: what `make trigger-check` makes of the Worker preview's output.
The check itself runs on Cloudflare; tests/dashboard/test_dashboard_trigger.py runs the handler."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import trigger_check  # noqa: E402

pytestmark = pytest.mark.req("REQ-CI-010")


def test_an_accepted_token_passes_and_says_its_days_left():
    lines = [
        "[wrangler:info] Ready on http://127.0.0.1:8799\n",
        "[wrangler:info] GET /__scheduled 200 OK (603ms)\n",
        "trigger check: OK, GitHub accepts the token for direct.yml (364 days left)\n",
    ]
    assert trigger_check.verdict(lines) == (
        True,
        "trigger check: OK, GitHub accepts the token for direct.yml (364 days left)",
    )


def test_a_refused_or_missing_token_fails():
    refused = ["✘ [ERROR] Error: trigger check: GitHub answered 401\n"]
    assert trigger_check.verdict(refused) == (False, "trigger check: GitHub answered 401")
    missing = ["Uncaught Error: trigger check: GITHUB_TOKEN is not set\n"]
    assert trigger_check.verdict(missing)[0] is False


def test_silence_fails():
    works, text = trigger_check.verdict(["[wrangler:info] Ready on http://127.0.0.1:8799\n"])
    assert not works and "said nothing" in text
