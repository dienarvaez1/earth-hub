"""The docs stay condensed: each drift-prone fact has its homes, counts live only in the README,
and every page people read keeps to its word budget.

A fact restated where its change does not reach becomes wrong there: "after the next hour" in the
cutover guide, months after rounds went to every 4 hours; "190 of them" in the README, at 204
requirements. A fact that changes is stated in its homes below, and only there; other pages name
it and link. Changing a fact = changing it in its homes (and here, if a home moves).

History is left out on purpose: the plan history, the defect log and the generated traceability
matrix record what was true then.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
HISTORY = {
    "docs/progress/history.md",
    "docs/progress/defects.md",
    "docs/progress/traceability-matrix.md",
}
PAGES = sorted(
    p.relative_to(REPO).as_posix()
    for p in [
        REPO / "README.md",
        REPO / "CLAUDE.md",
        *(REPO / "docs").rglob("*.md"),
        *(REPO / ".claude" / "skills").glob("*/SKILL.md"),
    ]
    if p.relative_to(REPO).as_posix() not in HISTORY
)


def text(page: str) -> str:
    return (REPO / page).read_text()


# --- one home per fact -------------------------------------------------------------------------

# fact -> (pattern, the pages allowed to state it). Lists shrink as pages are condensed.
FACTS = {
    "round and reconcile times": (
        r"5 \*/4 \* \* \*|00:05|10 0 \* \* \*|00:10 UTC|08:05",
        {
            "CLAUDE.md",
            "docs/overview/limitations.md",
            "docs/technical/architecture.md",
            "docs/technical/ci.md",
            "docs/technical/deployment.md",
            "docs/progress/requirements.md",
        },
    ),
    "the 5-hour staleness limit": (
        r"\b5 hours\b",
        {
            "CLAUDE.md",
            "docs/technical/architecture.md",
            "docs/technical/ci.md",
            "docs/technical/deployment.md",
            "docs/progress/requirements.md",
            "docs/progress/test-strategy.md",
        },
    ),
    "the 20-minute lease": (
        r"\b20 minutes\b",
        {
            "docs/technical/architecture.md",
            "docs/technical/ci.md",
            "docs/technical/deployment.md",
            "docs/progress/requirements.md",
        },
    ),
    "the export interval's default": (
        r"300 by default|EXPORT_INTERVAL_SECONDS[^.]{0,30}\b300\b",
        {"docs/technical/deployment.md", "docs/progress/requirements.md"},
    ),
    "the audit's lookup limit": (
        r"at most 50|50 a day",
        {"docs/technical/architecture.md", "docs/progress/requirements.md"},
    ),
    # Left at 2 in ci.md after the workflow moved to 5 (#70).
    "the daily reconcile's age limit": (r"max-age-hours \d", {"docs/technical/ci.md"}),
    # "Up to an hour late" outlived the hourly rounds in the README and ci.md (#70).
    "how late direct mode is": (
        r"(?i)up to (?:an|one|two|three|four|five|six|\d+) hours? (?:late|after)",
        {"docs/overview/limitations.md"},
    ),
}


@pytest.mark.req("REQ-DOC-007")
@pytest.mark.parametrize("fact", FACTS)
def test_each_fact_is_stated_only_in_its_homes(fact):
    pattern, homes = FACTS[fact]
    stated = {page for page in PAGES if re.search(pattern, text(page))}
    assert stated <= homes, f"{fact} restated outside its homes: name it and link instead"


# --- counts only in the README -----------------------------------------------------------------

NUMBER_WORDS = (
    r"(?:one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|"
    r"fifteen|sixteen|seventeen|eighteen|nineteen|twenty(?:-\w+)?|thirty(?:-\w+)?|all but \w+)"
)
COUNTED = r"(?:requirements|defects)"


def count_problems(body: str, *, readme: bool) -> list[str]:
    """Counts of requirements or defects that no test checks: in words, hidden in a phrase, or
    outside the README."""
    problems = re.findall(rf"\b{NUMBER_WORDS}\b[^.\n]{{0,40}}\b{COUNTED}\b", body, re.I)
    problems += re.findall(rf"\b{NUMBER_WORDS} (?:were|was) found\b", body, re.I)
    problems += re.findall(rf"\b{COUNTED}\b[^\n]{{0,80}}?\b\d[\d,]* of them\b", body, re.I)
    if not readme:
        problems += re.findall(rf"\b\d[\d,]* {COUNTED}\b", body)
    return problems


@pytest.mark.req("REQ-DOC-008")
@pytest.mark.parametrize("page", PAGES)
def test_counts_of_requirements_and_defects_live_only_in_the_readme(page):
    problems = count_problems(text(page), readme=page == "README.md")
    assert not problems, f"counts belong in the README, as 'N requirements': {problems[:3]}"


@pytest.mark.req("REQ-DOC-008")
@pytest.mark.parametrize(
    "sentence",
    [
        # Each wrong on main on 2026-10-31.
        "- the [requirements](docs/progress/requirements.md), 190 of them, and the",
        "All but seven were found before their code was merged",
        "Twenty-one were found after merge (DEF-034 to DEF-039, DEF-053 to DEF-067)",
    ],
)
def test_the_counts_that_slipped_through_before_are_caught(sentence):
    assert count_problems(sentence, readme=True)


# --- word budgets ------------------------------------------------------------------------------

# Today's sizes, rounded up: a ceiling while the pages are condensed, lowered as they shrink.
BUDGETS = {
    "README.md": 1800,
    "CLAUDE.md": 2900,
    "docs/overview/cheatsheet.md": 2900,
    "docs/overview/demo.md": 1600,
    "docs/overview/limitations.md": 1900,
    "docs/technical/architecture.md": 7700,
    "docs/technical/deployment.md": 1800,
    "docs/technical/ci.md": 2300,
    "docs/progress/plan.md": 1600,
    "docs/progress/test-strategy.md": 2800,
    ".claude/skills/eh-api/SKILL.md": 400,
    ".claude/skills/eh-backfill/SKILL.md": 400,
    ".claude/skills/eh-dashboard/SKILL.md": 600,
    ".claude/skills/eh-db/SKILL.md": 500,
    ".claude/skills/eh-export/SKILL.md": 600,
    ".claude/skills/eh-host/SKILL.md": 800,
    ".claude/skills/eh-kafka/SKILL.md": 500,
    ".claude/skills/eh-logs/SKILL.md": 400,
    ".claude/skills/eh-mode/SKILL.md": 700,
    ".claude/skills/eh-poller/SKILL.md": 400,
    ".claude/skills/eh-r2/SKILL.md": 600,
    ".claude/skills/eh-reconcile/SKILL.md": 600,
    ".claude/skills/eh-start/SKILL.md": 400,
    ".claude/skills/eh-status/SKILL.md": 600,
    ".claude/skills/eh-stop/SKILL.md": 400,
    ".claude/skills/eh-test/SKILL.md": 800,
    ".claude/skills/eh-usgs/SKILL.md": 400,
    ".claude/skills/eh/SKILL.md": 300,
}


@pytest.mark.req("REQ-DOC-009")
def test_every_skill_has_a_budget():
    skills = {p for p in PAGES if p.startswith(".claude/skills/")}
    assert skills <= set(BUDGETS), f"give a new skill a budget: {sorted(skills - set(BUDGETS))}"


@pytest.mark.req("REQ-DOC-009")
@pytest.mark.parametrize("page", BUDGETS)
def test_each_page_keeps_to_its_word_budget(page):
    words = len(text(page).split())
    assert words <= BUDGETS[page], f"{page}: {words} words, budget {BUDGETS[page]}"
