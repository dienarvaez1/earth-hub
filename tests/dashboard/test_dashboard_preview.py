"""What the public page carries beyond the reports: the licenses of the software it ships
(DEF-035), and the requests that keep it out of search engines: it is shared by link, not
promoted."""

import json
import os
import re
import subprocess

import httpx
import pytest
from support.dashboard import DASHBOARD, ENV

NOTICES = "/third-party-licenses.txt"
# Each package: its name and version, and its license identifier, between two rules.
HEADING = re.compile(r"^={78}\n([^\n]+) \(([^\n()]+)\)\n={78}$", re.M)


@pytest.fixture
def get(server):
    return lambda path: httpx.get(server.url + path, timeout=30)


def sections(text: str) -> dict[str, tuple[str, str]]:
    """{title: (license identifier, text)}, one per package."""
    headings = list(HEADING.finditer(text))
    ends = [h.start() for h in headings[1:]] + [len(text)]
    pairs = zip(headings, ends, strict=True)
    return {h[1]: (h[2], text[h.end() : end].strip()) for h, end in pairs}


@pytest.mark.req("REQ-DASH-016")
def test_the_licenses_of_the_shipped_software_are_served(get):
    response = get(NOTICES)
    assert response.status_code == 200
    found = sections(response.text)
    names = {title.rsplit(" ", 1)[0] for title in found if title[-1].isdigit()}
    dependencies = json.loads((DASHBOARD / "package.json").read_text())["dependencies"]
    assert set(dependencies) <= names  # and what they bundle, such as d3 and apache-arrow
    assert "DuckDB Parquet extension" in found
    for title, (identifier, text) in found.items():
        assert identifier != "unknown" and len(text) > 100, title
        assert re.search(r"copyright", text, re.I), title


@pytest.mark.req("REQ-DASH-016")
def test_the_footer_links_to_the_licenses(dash):
    link = dash.page.locator("footer a#licenses")
    assert link.is_visible()
    assert link.get_attribute("href") == NOTICES


@pytest.mark.req("REQ-DASH-032")
def test_the_footer_links_to_the_project_on_github(dash):
    link = dash.page.locator("footer a#source")
    assert link.is_visible() and link.inner_text() == "Earth-Hub on GitHub"
    assert link.get_attribute("href") == "https://github.com/dienarvaez1/earth-hub"
    assert link.get_attribute("rel") == "noopener"


@pytest.mark.req("REQ-DASH-016")
def test_a_package_without_a_license_text_fails_the_build(built, tmp_path):
    result = subprocess.run(
        ["npx", "--no-install", "vite", "build", "--outDir", str(tmp_path / "out")],
        cwd=DASHBOARD,
        env=ENV | {"DASHBOARD_LICENSES": str(tmp_path)},  # no texts at all
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode != 0
    assert "no license text for @duckdb__duckdb-wasm.txt" in result.stdout + result.stderr
    assert os.listdir(DASHBOARD / "dist")  # the real build is untouched


@pytest.mark.req("REQ-DASH-017")
@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/robots.txt",
        NOTICES,
        "/duckdb-extensions/v1.5.6/wasm_eh/parquet.duckdb_extension.wasm",
        "/data/meta.json",
        "/data/earthquakes.parquet",
        "/data/anything-else",
    ],
)
def test_every_response_asks_search_engines_not_to_index(get, path):
    assert get(path).headers["x-robots-tag"] == "noindex, nofollow"


@pytest.mark.req("REQ-DASH-017")
def test_every_page_asset_asks_search_engines_not_to_index(get):
    assets = re.findall(r'(?:src|href)="(/assets/[^"]+)"', get("/").text)
    assert assets
    for asset in assets:
        assert get(asset).headers["x-robots-tag"] == "noindex, nofollow", asset


@pytest.mark.req("REQ-DASH-017")
def test_robots_txt_disallows_everything(get):
    response = get("/robots.txt")
    assert response.status_code == 200
    rules = [line for line in response.text.splitlines() if not line.startswith("#")]
    assert rules == ["User-agent: *", "Disallow: /"]


@pytest.mark.req("REQ-DASH-017")
def test_the_page_asks_not_to_be_indexed_and_is_no_longer_a_preview(dash):
    robots = dash.page.locator('meta[name="robots"]').get_attribute("content")
    assert robots == "noindex, nofollow"
    assert dash.page.locator("#preview").count() == 0
    assert dash.page.locator("h1").inner_text() == "Earth-Hub earthquakes"
