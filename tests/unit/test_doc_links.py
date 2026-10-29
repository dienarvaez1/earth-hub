"""The documentation's relative links and its counts. External links are not checked: unit
tests have no network."""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
PAGES = sorted(
    [REPO / "README.md", REPO / "CLAUDE.md", *(REPO / "docs").rglob("*.md")]
    + [*(REPO / "dashboard").glob("**/README.md")],
)
PAGES = [p for p in PAGES if "node_modules" not in p.parts]
FENCE = re.compile(r"^```.*?^```", re.M | re.S)
CODE = re.compile(r"`[^`\n]*`")
LINK = re.compile(r"\]\(([^)\s]+)\)")
HEADING = re.compile(r"^#{1,6} (.+?)\s*#*$", re.M)


def prose(text: str) -> str:
    """The text without code blocks or inline code, where links are not links."""
    return CODE.sub("", FENCE.sub("", text))


def anchors(page: Path) -> set[str]:
    """The heading anchors GitHub generates for a page, duplicates numbered as it does."""
    seen: dict[str, int] = {}
    found = set()
    for heading in HEADING.findall(FENCE.sub("", page.read_text())):
        text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", heading)  # a link keeps its text
        slug = re.sub(r"[^\w\- ]", "", text.lower()).replace(" ", "-")
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        found.add(slug if count == 0 else f"{slug}-{count}")
    return found


def links(page: Path) -> list[str]:
    return [
        target
        for target in LINK.findall(prose(page.read_text()))
        if not re.match(r"[a-z]+:", target)  # http:, https:, mailto:
    ]


def test_the_pages_are_found():
    names = {p.relative_to(REPO).as_posix() for p in PAGES}
    assert {"README.md", "docs/technical/architecture.md", "docs/progress/defects.md"} <= names


@pytest.mark.req("REQ-DOC-001")
@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.relative_to(REPO).as_posix())
def test_every_relative_link_and_anchor_resolves(page):
    broken = []
    for target in links(page):
        path, _, anchor = target.partition("#")
        linked = (page.parent / path).resolve() if path else page
        if (
            not linked.exists()
            or REPO not in linked.parents
            or anchor
            and (linked.suffix != ".md" or anchor not in anchors(linked))
        ):
            broken.append(target)
    assert broken == []


def test_the_anchors_follow_github(tmp_path):
    page = tmp_path / "page.md"
    page.write_text(
        "# Earth Hub\n## Tests and QA\n### `make regression`: every layer\n"
        "## Tests and QA\n```\n# not a heading\n```\n## See [the API](api.md)\n"
    )
    assert anchors(page) == {
        "earth-hub",
        "tests-and-qa",
        "make-regression-every-layer",
        "tests-and-qa-1",
        "see-the-api",
    }


def count(pattern: str, path: str) -> int:
    return len(re.findall(pattern, (REPO / path).read_text(), re.M))


@pytest.mark.req("REQ-DOC-005")
def test_the_readme_counts_match_the_requirements_and_the_defect_log():
    readme = (REPO / "README.md").read_text()
    requirements = count(
        r"^\| REQ-[A-Z0-9-]+ \|", "docs/progress/requirements.md"
    )  # as conftest.py
    defects = count(r"^\| \[?DEF-\d+", "docs/progress/defects.md")
    claimed = {int(n) for n in re.findall(r"\b(\d+) requirements\b", readme)}
    assert claimed == {requirements}
    claimed = {int(n) for n in re.findall(r"\b(\d+) defects\b", readme)}
    assert claimed == {defects}
