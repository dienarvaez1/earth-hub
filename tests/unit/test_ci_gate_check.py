"""Deliberately failing: proves CI reports a failure (closed without merging)."""


def test_ci_shows_a_failure():
    assert 1 + 1 == 3
