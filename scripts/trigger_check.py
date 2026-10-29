"""Whether the dashboard Worker's deployed GitHub token works, starting nothing
(docs/technical/ci.md#the-trigger).

    uv run python scripts/trigger_check.py   # or: make trigger-check

A Worker secret cannot be read back, so this asks the Worker itself. It runs the Worker on
Cloudflare with the deployed Worker's secrets (`wrangler dev --remote --test-scheduled`, a
preview: the deployed Worker is not changed), fires its scheduled handler with the cron
"check", which no deployed trigger has, and prints what the handler says: it reads direct.yml
with the token, as the daily expiry check does, and starts nothing. The token never leaves
Cloudflare. Exits 0 when GitHub accepts the token, 1 otherwise.
"""

import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx

DASHBOARD = Path(__file__).resolve().parents[1] / "dashboard"
MARK = "trigger check:"
TIMEOUT_S = 120


def verdict(lines: list[str]) -> tuple[bool, str]:
    """(whether the token works, what to say), from the preview's output."""
    for line in lines:
        if MARK in line:
            text = line[line.index(MARK) :].strip()
            return text.startswith(f"{MARK} OK"), text
    return False, "trigger check: the Worker said nothing about the token (see the output above)"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    port = free_port()
    process = subprocess.Popen(
        ["npx", "--no-install", "wrangler", "dev", "--remote", "--test-scheduled",
         "--ip", "127.0.0.1", "--port", str(port)],
        cwd=DASHBOARD, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )  # fmt: skip
    lines: list[str] = []
    reader = threading.Thread(target=lambda: lines.extend(process.stdout), daemon=True)
    reader.start()
    try:
        deadline = time.monotonic() + TIMEOUT_S
        while time.monotonic() < deadline and not any("Ready on" in line for line in lines):
            if process.poll() is not None:
                break
            time.sleep(1)
        else:
            if time.monotonic() >= deadline:
                print("trigger check: wrangler did not start in time", file=sys.stderr)
                return 1
        if process.poll() is not None:
            print("".join(line for line in lines if "@" not in line), file=sys.stderr)
            print(
                "trigger check: wrangler stopped (logged in? npx wrangler login)", file=sys.stderr
            )
            return 1
        httpx.get(f"http://127.0.0.1:{port}/__scheduled", params={"cron": "check"}, timeout=60)
        while time.monotonic() < deadline and not any(MARK in line for line in lines):
            time.sleep(0.5)
    finally:
        process.terminate()
        process.wait(timeout=30)
        reader.join(timeout=5)
    works, text = verdict(lines)
    print(text)
    return 0 if works else 1


if __name__ == "__main__":
    sys.exit(main())
