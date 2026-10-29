"""Download DuckDB-WASM's Parquet extension into the dashboard's static files.

    uv run python scripts/fetch_duckdb_extensions.py      # make dashboard-build runs it

DuckDB-WASM loads Parquet support as an extension. The page serves it itself, from
/duckdb-extensions/, instead of from extensions.duckdb.org, so the dashboard depends on one
outside service less. The files are DuckDB's own, signed builds (DuckDB checks the signature
when it loads them), pinned here by SHA-256 and not committed.
"""

import hashlib
import sys
import urllib.request
from pathlib import Path

# The DuckDB engine inside the @duckdb/duckdb-wasm version in dashboard/package.json; a
# browser test fails if they drift apart.
ENGINE = "v1.5.6"
SHA256 = {
    "wasm_eh": "33094a01b3a1dc27c4158f4c1fa13c45c240eea6ba07197a19a1925f002ca473",
    "wasm_mvp": "a7a7fc4e56683c67d887f76e50a5a24da04bfab05c5ac77713614197db0d92c2",
}
SOURCE = "https://extensions.duckdb.org"
TARGET = Path(__file__).resolve().parents[1] / "dashboard" / "public" / "duckdb-extensions"
NAME = "parquet.duckdb_extension.wasm"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fetch(platform: str, expected: str) -> str:
    path = TARGET / ENGINE / platform / NAME
    if path.exists() and digest(path) == expected:
        return f"{path.relative_to(TARGET)} present"
    url = f"{SOURCE}/{ENGINE}/{platform}/{NAME}"
    # The server refuses Python's default User-Agent.
    headers = {"User-Agent": "earth-hub-dashboard-build"}
    request = urllib.request.Request(url, headers=headers)  # noqa: S310 (a fixed https:// URL)
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 (the same URL)
        body = response.read()
    actual = hashlib.sha256(body).hexdigest()
    if actual != expected:
        raise SystemExit(f"{url}: SHA-256 {actual}, expected {expected}; not saved")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return f"{path.relative_to(TARGET)} downloaded ({len(body)} bytes)"


def main() -> int:
    for platform, expected in SHA256.items():
        print(fetch(platform, expected))
    return 0


if __name__ == "__main__":
    sys.exit(main())
