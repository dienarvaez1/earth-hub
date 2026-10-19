"""The dashboard served locally: `wrangler dev` with a simulated R2 (Miniflare) seeded with a
snapshot, never real Cloudflare. Used by tests/dashboard/ and the performance tests."""

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

REPO = Path(__file__).resolve().parents[2]
DASHBOARD = REPO / "dashboard"
BUCKET = "earth-hub-test"
DUCKDB_DIST = DASHBOARD / "node_modules" / "@duckdb" / "duckdb-wasm" / "dist"
# No telemetry or update checks from wrangler during tests.
ENV = os.environ | {"WRANGLER_SEND_METRICS": "false", "WRANGLER_LOG": "warn", "CI": "1"}

sys.path.insert(0, str(REPO / "scripts"))
from dashboard_config import render  # noqa: E402


def npx(*args: str, timeout: float = 300) -> None:
    subprocess.run(
        ["npx", "--no-install", *args],
        cwd=DASHBOARD,
        env=ENV,
        check=True,
        timeout=timeout,
        capture_output=True,
    )


def build() -> None:
    """The page (vite build into dashboard/dist), as deployed."""
    subprocess.run(
        ["npm", "run", "-s", "build"], cwd=DASHBOARD, env=ENV, check=True, capture_output=True
    )


def duckdb_version() -> str:
    package = DASHBOARD / "node_modules" / "@duckdb" / "duckdb-wasm" / "package.json"
    return json.loads(package.read_text())["version"]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class LocalDashboard:
    """`wrangler dev` on a free port, with its own simulated R2 in `state`."""

    def __init__(self, state: Path):
        self.state = state
        self.config = DASHBOARD / f"wrangler.test-{os.getpid()}-{id(self)}.jsonc"
        self.config.write_text(render(BUCKET, remote=False))
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.process: subprocess.Popen | None = None

    def put(self, key: str, path: Path, content_type: str) -> None:
        npx(
            "wrangler", "r2", "object", "put", f"{BUCKET}/{key}",
            "--file", str(path), "--content-type", content_type,
            "--local", "--persist-to", str(self.state), "-c", str(self.config),
        )  # fmt: skip

    def seed(self, parquet: Path, meta: dict) -> None:
        meta_path = self.state / "meta.json"
        self.state.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(json.dumps(meta))
        self.put("analytics/earthquakes.parquet", parquet, "application/vnd.apache.parquet")
        self.put("analytics/meta.json", meta_path, "application/json")

    def start(self) -> None:
        self.process = subprocess.Popen(
            [
                "npx", "--no-install", "wrangler", "dev", "-c", str(self.config),
                "--local", "--persist-to", str(self.state),
                "--ip", "127.0.0.1", "--port", str(self.port),
                "--show-interactive-dev-session=false",
            ],
            cwd=DASHBOARD,
            env=ENV,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )  # fmt: skip
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f"wrangler dev exited: {self.process.stderr.read().decode()}")
            try:
                if httpx.get(self.url + "/", timeout=2).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.3)
        raise RuntimeError("wrangler dev did not start within 60 s")

    def stop(self) -> None:
        if self.process is not None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
        self.config.unlink(missing_ok=True)


def route_offline(page, *, style: dict | None = None) -> list[str]:
    """Serve DuckDB-WASM from node_modules and a plain map style, and block every other
    request off this machine; returns the blocked URLs, which a test can assert is empty."""
    blocked: list[str] = []
    prefix = f"https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@{duckdb_version()}/dist/"

    def handle(route):
        url = route.request.url
        if url.startswith(prefix):
            name = url[len(prefix) :].split("?")[0]
            kind = "application/wasm" if name.endswith(".wasm") else "text/javascript"
            return route.fulfill(path=str(DUCKDB_DIST / name), content_type=kind)
        if url.startswith("https://tiles.openfreemap.org/styles/"):
            return route.fulfill(json=style or PLAIN_STYLE)
        if url.startswith("https://tiles.openfreemap.org/fonts/"):
            return route.fulfill(body=b"", content_type="application/x-protobuf")
        if url.startswith(("http://127.0.0.1", "blob:", "data:")):
            return route.continue_()
        blocked.append(url)
        return route.abort()

    page.route("**/*", handle)
    return blocked


# A map style with no tiles: the events layers still draw on it.
PLAIN_STYLE = {
    "version": 8,
    "glyphs": "https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf",
    "sources": {},
    "layers": [{"id": "background", "type": "background", "paint": {"background-color": "#eee"}}],
}
