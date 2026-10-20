"""The Claude Code skills (.claude/skills/) and the cheat sheet (docs/overview/cheatsheet.md): every
skill is listed, every skill and make target they name exists, and the docs name only skills,
with arguments, that exist."""

import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
SKILLS = REPO / ".claude" / "skills"
CHEATSHEET = REPO / "docs" / "overview" / "cheatsheet.md"
SKILL_FILES = sorted(SKILLS.glob("*/SKILL.md"))
DOCS = sorted([REPO / "README.md", *(REPO / "docs").rglob("*.md")])
TARGETS = set(re.findall(r"^([a-z0-9-]+):", (REPO / "Makefile").read_text(), re.M))

pytestmark = pytest.mark.req("REQ-DOC-006")


def frontmatter(path: Path) -> dict:
    text = path.read_text()
    assert text.startswith("---\n"), path
    return yaml.safe_load(text.split("---\n", 2)[1])


def skills_named(text: str) -> set[str]:
    return set(re.findall(r"`/(eh(?:-[a-z0-9]+)?)\b", text))


def make_targets(text: str) -> set[str]:
    """Targets of `make` commands written as code, not the word "make" in prose."""
    code = " ".join(re.findall(r"`([^`]+)`", text))
    return set(re.findall(r"\bmake (?:-s )?([a-z][a-z0-9-]*)", code))


def test_there_are_skills():
    assert len(SKILL_FILES) >= 10


@pytest.mark.parametrize("path", SKILL_FILES, ids=lambda p: p.parent.name)
def test_each_skill_is_named_after_its_folder_and_says_when_to_use_it(path):
    meta = frontmatter(path)
    assert meta["name"] == path.parent.name
    assert len(meta["description"]) > 80 and "Use " in meta["description"]


def test_the_cheat_sheet_lists_every_skill_and_only_skills_that_exist():
    table = CHEATSHEET.read_text().split("## Skills", 1)[1].split("\n## ", 1)[0]
    listed = {re.match(r"\| `/(eh[a-z0-9-]*)", row)[1] for row in table.splitlines()[4:] if row}
    assert listed == {p.parent.name for p in SKILL_FILES}
    for path in [CHEATSHEET, *SKILL_FILES]:
        assert skills_named(path.read_text()) <= listed, path


@pytest.mark.parametrize("path", [CHEATSHEET, *SKILL_FILES], ids=lambda p: p.parent.name)
def test_every_make_target_named_exists(path):
    assert make_targets(path.read_text()) - TARGETS == set()


def skill_calls(text: str) -> set[tuple[str, str]]:
    """(skill, first argument) for each `/eh-x word ...` written as code."""
    return set(re.findall(r"`/(eh(?:-[a-z0-9]+)?) ([a-z][a-z0-9-]*)", text))


@pytest.mark.parametrize("path", DOCS, ids=lambda p: p.relative_to(REPO).as_posix())
def test_the_docs_name_only_skills_and_arguments_that_exist(path):
    skills = {p.parent.name: p.read_text() for p in SKILL_FILES}
    text = path.read_text()
    assert skills_named(text) <= set(skills), path
    # A skill whose argument hint takes free text (e.g. <keyword>, <SQL>) accepts any word.
    free = {p.parent.name for p in SKILL_FILES if "<" in frontmatter(p).get("argument-hint", "")}
    unknown = {
        (name, arg)
        for name, arg in skill_calls(text)
        if name not in free and not re.search(rf"`{re.escape(arg)}(?![\w-])", skills[name])
    }
    assert unknown == set()
