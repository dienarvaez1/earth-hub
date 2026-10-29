"""`pytest --shard K/N`: run only the Kth of N parts of the tests collected, so CI can run
the dashboard layer in parallel jobs (.github/workflows/ci.yml). Every test is in exactly
one part: the Kth, (K+N)th, ... in collection order, which every part collects alike."""

import pytest


def parse(spec: str) -> tuple[int, int]:
    """ "2/3" as (2, 3); anything else, or K outside 1 to N, is a usage error."""
    try:
        k, n = (int(part) for part in spec.split("/"))
    except ValueError:
        raise pytest.UsageError(f"--shard takes K/N, such as 1/2, not {spec!r}") from None
    if not 1 <= k <= n:
        raise pytest.UsageError(f"--shard {spec}: K must be from 1 to N")
    return k, n


def split(items: list, k: int, n: int) -> tuple[list, list]:
    """(the items this part runs, the others)."""
    kept = [item for index, item in enumerate(items) if index % n == k - 1]
    return kept, [item for index, item in enumerate(items) if index % n != k - 1]


class Shard:
    def __init__(self, k: int, n: int):
        self.k, self.n = k, n

    @pytest.hookimpl(trylast=True)  # after -m and -k have deselected theirs
    def pytest_collection_modifyitems(self, config, items):
        kept, others = split(items, self.k, self.n)
        if others:
            config.hook.pytest_deselected(items=others)
            items[:] = kept
