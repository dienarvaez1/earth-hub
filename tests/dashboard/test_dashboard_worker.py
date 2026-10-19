"""The dashboard's Worker: only the snapshot and its metadata are reachable, with caching
headers, conditional requests and ranges."""

import httpx
import pytest

pytestmark = pytest.mark.req("REQ-DASH-006")


@pytest.fixture
def get(server):
    def request(path, method="GET", **headers):
        return httpx.request(method, server.url + path, headers=headers, timeout=30)

    return request


def test_the_snapshot_is_served_exactly(get, snapshot):
    response = get("/data/earthquakes.parquet")
    assert response.status_code == 200
    assert response.content == snapshot.read_bytes()
    assert response.headers["content-type"] == "application/vnd.apache.parquet"
    assert response.headers["content-length"] == str(snapshot.stat().st_size)
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["cache-control"] == "public, max-age=300"
    assert response.headers["etag"]


def test_meta_is_served_as_json(get):
    response = get("/data/meta.json")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["rows"] == 8


def test_a_matching_etag_gets_304_without_a_body(get):
    etag = get("/data/earthquakes.parquet").headers["etag"]
    response = get("/data/earthquakes.parquet", **{"If-None-Match": etag})
    assert response.status_code == 304
    assert response.content == b""
    assert response.headers["etag"] == etag


def test_ranges(get, snapshot):
    body = snapshot.read_bytes()
    first = get("/data/earthquakes.parquet", Range="bytes=0-3")
    assert first.status_code == 206
    assert first.content == b"PAR1" == body[:4]
    assert first.headers["content-range"] == f"bytes 0-3/{len(body)}"
    last = get("/data/earthquakes.parquet", Range="bytes=-8")
    assert last.status_code == 206
    assert last.content == body[-8:]


def test_an_unsatisfiable_range_gets_416(get, snapshot):
    size = snapshot.stat().st_size
    response = get("/data/earthquakes.parquet", Range=f"bytes={size + 10}-")
    assert response.status_code == 416
    assert response.headers["content-range"] == f"bytes */{size}"


def test_head_has_the_headers_and_no_body(get, snapshot):
    response = get("/data/earthquakes.parquet", method="HEAD")
    assert response.status_code == 200
    assert response.content == b""
    assert response.headers["content-length"] == str(snapshot.stat().st_size)


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE"])
def test_writes_are_refused(get, method):
    response = get("/data/meta.json", method=method)
    assert response.status_code == 405
    assert response.headers["allow"] == "GET, HEAD"


@pytest.mark.parametrize(
    "path",
    [
        "/data/other.json",
        "/data/",
        "/analytics/earthquakes.parquet",
        "/_state/earthquakes-manifest.json.gz",
        "/_state/lock.json",
        "/earthquakes/1.json",
        "/runs/",
        "/data/%2e%2e/_state/lock.json",
    ],
)
def test_nothing_else_in_the_bucket_is_reachable(get, path):
    response = get(path)
    assert response.status_code == 404
    assert b"PAR1" not in response.content


def test_the_page_and_its_files_are_static(get):
    page = get("/")
    assert page.status_code == 200
    assert "<title>Earth-Hub Earthquakes</title>" in page.text
    extension = get("/duckdb-extensions/v1.5.6/wasm_eh/parquet.duckdb_extension.wasm")
    assert extension.status_code == 200
    assert extension.content[:4] == b"\0asm"


def test_without_a_snapshot_the_data_paths_say_so(empty_server):
    response = httpx.get(empty_server.url + "/data/meta.json", timeout=30)
    assert response.status_code == 404
    assert "no snapshot yet" in response.text
    assert response.headers["cache-control"] == "no-store"
