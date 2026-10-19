"""What the pipeline's log lines carry, and the consumer's periodic summary.

Every line about one event has `event_id` and `stage`, so filtering the logs on an event ID
shows its whole path (docs/observability.md). The stages, in pipeline order:

    fetch     poller: reading the USGS feed (no event yet)
    parse     poller: a feature without id, updated or time, rejected before Kafka
    publish   poller: sent to Kafka, or failed to be
    persist   consumer: validated and stored, rejected by a rule, or failed

The names are stable: tests and the docs rely on them.
"""

import time
from collections.abc import Callable

STAGES = ("fetch", "parse", "publish", "persist")
SUMMARY_INTERVAL_S = 60.0


class Summary:
    """Outcome counts since the last summary and since start, reported every interval."""

    def __init__(
        self,
        interval_s: float = SUMMARY_INTERVAL_S,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.interval_s = interval_s
        self._monotonic = monotonic
        self._next = monotonic() + interval_s
        self.window: dict[str, int] = {}
        self.totals: dict[str, int] = {}

    def count(self, outcome: str) -> None:
        self.window[outcome] = self.window.get(outcome, 0) + 1
        self.totals[outcome] = self.totals.get(outcome, 0) + 1

    def due(self) -> bool:
        return self._monotonic() >= self._next

    def take(self) -> dict:
        """The summary's fields; starts a new window."""
        fields = {
            "messages": sum(self.window.values()),
            "outcomes": dict(sorted(self.window.items())),
            "totals": dict(sorted(self.totals.items())),
            "interval_s": self.interval_s,
        }
        self.window = {}
        self._next = self._monotonic() + self.interval_s
        return fields
