"""The dashboard trigger's GitHub token (docs/technical/ci.md#the-trigger), step by step.

    uv run python scripts/trigger_token.py   # or: make trigger-token

Run it in a terminal. It opens GitHub's form for a fine-grained token with its fields filled
in (you pick the repository and the expiry), reads the token without echoing it, checks that
it can see this repository's Actions, and stores it as the dashboard Worker's GITHUB_TOKEN
secret through wrangler's standard input, then prints its expiry. The token is never printed,
written to a file or put on a command line. Run it again to replace the token before it
expires: from 14 days before, the trigger's daily check fails and GitHub e-mails you.
"""

import getpass
import re
import subprocess
import sys
import webbrowser
from collections.abc import Callable
from http import HTTPStatus
from pathlib import Path
from urllib.parse import urlencode

import httpx

DASHBOARD = Path(__file__).resolve().parents[1] / "dashboard"
TEMPLATE = DASHBOARD / "wrangler.template.jsonc"
SECRET = "GITHUB_TOKEN"  # noqa: S105 (the secret's name, not its value)
WORKFLOW = "direct.yml"


def setting(name: str) -> str:
    """A var of the Worker's configuration (GITHUB_REPO, GITHUB_API), as the trigger uses it."""
    match = re.search(rf'"{name}":\s*"([^"]+)"', TEMPLATE.read_text())
    if match is None:
        raise SystemExit(f"{name} is missing from {TEMPLATE.name}")
    return match.group(1)


def token_form(repo: str) -> str:
    """GitHub's new fine-grained token form, filled in: only Actions, read and write."""
    owner = repo.split("/", maxsplit=1)[0]
    fields = {
        "name": "earth-hub dashboard trigger",
        "description": f"Cloudflare cron starts {WORKFLOW} rounds",
        "target_name": owner,
        "actions": "write",
    }
    return f"https://github.com/settings/personal-access-tokens/new?{urlencode(fields)}"


def check(token: str, repo: str, api: str, client: httpx.Client) -> tuple[str | None, str | None]:
    """What to fix (None when the token can see the workflow), and the token's expiry as GitHub
    states it (None when it never expires). Starts nothing."""
    try:
        response = client.get(
            f"{api}/repos/{repo}/actions/workflows/{WORKFLOW}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=30,
        )
    except httpx.HTTPError as error:
        return f"could not reach GitHub ({type(error).__name__}); try again", None
    expiry = response.headers.get("GitHub-Authentication-Token-Expiration")
    if response.status_code == HTTPStatus.OK:
        return None, expiry
    if response.status_code == HTTPStatus.UNAUTHORIZED:
        return "GitHub does not accept this token: copy it again, whole", None
    if response.status_code in (403, 404):
        return (
            f"the token cannot see {repo}'s Actions: give it access to that repository and "
            "Actions: Read and write"
        ), None
    return f"GitHub answered {response.status_code}; try again", None


def store(token: str, run: Callable[..., subprocess.CompletedProcess]) -> str | None:
    """Stores the token as the Worker's secret; None on success, otherwise what went wrong."""
    result = run(
        ["npx", "--no-install", "wrangler", "secret", "put", SECRET],
        cwd=DASHBOARD,
        input=token + "\n",
        text=True,
        capture_output=True,
    )
    if result.returncode == 0:
        return None
    detail = (result.stderr or result.stdout or "").replace(token, "<token>").strip()
    return f"wrangler could not store it: {detail[-400:]}"


def main(
    *,
    ask: Callable[[str], str] = getpass.getpass,
    open_url: Callable[[str], object] = webbrowser.open,
    client: httpx.Client | None = None,
    run: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> int:
    repo, api = setting("GITHUB_REPO"), setting("GITHUB_API")
    form = token_form(repo)
    print(f"1. Create the token. Opening GitHub's form (or open it yourself):\n   {form}")
    print("   Name, description and Actions: Read and write are filled in. Pick:")
    print(f"   - Repository access: Only select repositories -> {repo.split('/')[1]}")
    print("   - Expiration: the longest allowed, and note the date: rounds stop when it passes")
    print("   Then Generate token, and copy it.")
    open_url(form)
    token = ask("2. Paste the token (it is not shown): ").strip()
    if not token:
        print("No token given; nothing changed.")
        return 1
    if not token.startswith("github_pat_"):
        print("   That does not look like a fine-grained token (github_pat_...); checking anyway.")
    print("3. Checking that it can see the workflow (this starts nothing)...")
    problem, expiry = check(token, repo, api, client or httpx.Client())
    if problem is not None:
        print(f"   Not stored: {problem}.")
        return 1
    print(f"   It can. 4. Storing it as the dashboard Worker's {SECRET} secret...")
    problem = store(token, run)
    if problem is not None:
        print(f"   {problem}")
        print("   Not logged in to Cloudflare? cd dashboard && npx wrangler login, then again.")
        return 1
    print(
        f"   Stored. `cd dashboard && npx wrangler secret list` lists {SECRET} (never its value)."
    )
    if expiry is None:
        print("   GitHub states no expiry for it: it never expires.")
    else:
        print(f"   It expires {expiry}. From 14 days before, the trigger's daily check fails")
        print("   and GitHub e-mails you to run this again.")
    print("The next trigger uses it. If the trigger is not deployed yet: make dashboard-deploy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
