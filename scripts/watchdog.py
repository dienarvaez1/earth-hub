"""The watchdog (.github/workflows/watchdog.yml, docs/technical/ci.md#the-watchdog): fails when
the dashboard Worker's cron trigger has started no direct.yml run for MAX_AGE (DEF-056).

    python3 scripts/watchdog.py <createdAt of direct.yml's newest workflow_dispatch run>

direct.yml has no schedule, so GitHub never disables it for inactivity; this check holds the
only schedule, and is all that stops if GitHub does. Rounds run, and skip, in either mode, so
the check holds in both. Standard library only: the workflow installs nothing.
"""

import sys
from datetime import UTC, datetime, timedelta

MAX_AGE = timedelta(hours=2)  # two hourly rounds missed


def check(created_at: str, now: datetime) -> tuple[bool, str]:
    """(whether the trigger is alive, what to say)."""
    if not created_at:
        return False, "The dashboard Worker's trigger has never started a direct.yml run."
    started = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    minutes = int((now - started).total_seconds() // 60)
    if now - started > MAX_AGE:
        return False, (
            f"The dashboard Worker's trigger has started no direct.yml run since {created_at} "
            f"({minutes} minutes): check its cron trigger and its GitHub token "
            "(make trigger-token)."
        )
    return True, f"The last direct.yml run started at {created_at}, {minutes} minutes ago."


def main(argv: list[str]) -> int:
    alive, text = check(argv[1] if len(argv) > 1 else "", datetime.now(UTC))
    print(text if alive else f"::error title=The trigger stopped::{text}")
    return 0 if alive else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
