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


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=DASHBOARD, capture_output=True, text=True, check=False
    ).stdout.strip()


@pytest.mark.req("REQ-DASH-032")
def test_the_footer_shows_the_build_and_links_to_it_on_github(dash):
    # The page was built from this checkout (the `built` fixture): git describes the same.
    repo = "https://github.com/dienarvaez1/earth-hub"
    described = git("describe", "--tags", "--always", "--dirty")
    link = dash.page.locator("footer a#source")
    assert link.is_visible() and link.inner_text() == described
    expected = (
        f"{repo}/releases/tag/{described}"
        if re.fullmatch(r"v\d+\.\d+\.\d+", described)
        else f"{repo}/commit/{git('rev-parse', 'HEAD')}"
    )
    assert link.get_attribute("href") == expected
    assert link.get_attribute("title") == "Earth-Hub on GitHub"
    assert link.get_attribute("rel") == "noopener"


@pytest.mark.req("REQ-DASH-033")
def test_the_footer_credits_usgs_and_says_what_the_data_is_not(dash):
    usgs = dash.page.locator("footer #disclaimer a#usgs")
    assert usgs.is_visible() and usgs.inner_text() == "U.S. Geological Survey"
    assert usgs.get_attribute("href") == "https://earthquake.usgs.gov/"
    live = dash.page.locator("footer #disclaimer a#usgs-live")
    assert live.get_attribute("href") == "https://earthquake.usgs.gov/earthquakes/map/"
    text = " ".join(dash.page.inner_text("footer #disclaimer").split())
    for phrase in (
        "Earthquake data courtesy of the U.S. Geological Survey (USGS).",
        "for research purposes",
        "not affiliated with or endorsed by USGS",
        "not a complete or exact copy of its service",
        "missing or inaccurate if any part of this system fails",
        "derived by Earth-Hub",
        "Provided as is, without warranty.",
        "Not for emergency or safety decisions",
    ):
        assert phrase in text, phrase


# Records any use of the browser's location or site storage, before the page's own scripts run.
WATCH_PRIVACY = """
window.__privacy = [];
const record = (what) => window.__privacy.push(what);
for (const name of ["getCurrentPosition", "watchPosition"]) {
  navigator.geolocation[name] = () => record("geolocation." + name);
}
for (const area of ["localStorage", "sessionStorage"]) {
  const store = window[area];
  store.setItem = () => record(area + ".setItem");
}
const open = indexedDB.open.bind(indexedDB);
indexedDB.open = (...args) => (record("indexedDB.open"), open(...args));
"""


@pytest.mark.req("REQ-DASH-034")
def test_a_visit_sets_no_cookie_stores_nothing_and_never_asks_for_the_location(dash, server):
    page = dash.page
    page.context.add_init_script(WATCH_PRIVACY)  # from the next load on
    dash.open(server.url)  # loaded again, watched this time, then filtered and redrawn
    dash.after_update(lambda: page.select_option("select[name=period]", "last_3_years"))
    assert dash.status()["error"] is None
    link = page.locator("footer a#privacy")
    assert link.get_attribute("href") == "/privacy" and "location is never requested" in (
        page.inner_text("footer")
    )
    assert page.evaluate("window.__privacy") == []  # no location, no storage
    assert page.evaluate("[localStorage.length, sessionStorage.length]") == [0, 0]
    assert page.evaluate("document.cookie") == ""
    assert page.context.cookies() == []


@pytest.mark.req("REQ-DASH-034")
def test_the_privacy_notice_says_what_is_and_is_not_collected(browser_page, server):
    response = browser_page.page.goto(f"{server.url}/privacy")
    assert response.ok and response.headers["x-robots-tag"] == "noindex, nofollow"
    assert "set-cookie" not in response.headers
    page = browser_page.page
    assert page.title() == "Privacy Notice · Earth-Hub"
    text = " ".join(page.inner_text("main").split())
    for phrase in (
        "Effective date: 29 October 2026",
        "does not request, access, collect, record or store the geographic location of your device",
        "never invokes your browser's Geolocation API",
        "does not record your IP address or any location derived from it",
        "set, read or use cookies",
        "Cloudflare, Inc.",
        "jsDelivr",
        "OpenFreeMap",
        "clearing your browser's cache",
    ):
        assert phrase in text, phrase
    assert page.locator("a#back").get_attribute("href") == "/"
    assert page.locator("a#contact-link").get_attribute("href") == (
        "https://github.com/dienarvaez1/earth-hub/issues"
    )


@pytest.mark.req("REQ-DASH-016")
def test_a_package_without_a_license_text_fails_the_build(built, tmp_path):
    result = subprocess.run(
        ["npx", "--no-install", "vite", "build", "--outDir", str(tmp_path / "out")],
        cwd=DASHBOARD,
        env=ENV | {"DASHBOARD_LICENSES": str(tmp_path)},  # no texts at all
        check=False,
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
