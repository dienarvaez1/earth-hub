"""A full year of history through the whole backfill pipeline, timed.

The synthetic FDSN service generates each window's events on demand (one every ~3 minutes,
139,579 in total: last year's real count), with the real service's boundary semantics.
"""

import time
from datetime import UTC, datetime, timedelta

import pytest
from support.fdsn import FdsnServer, at

pytestmark = [pytest.mark.performance, pytest.mark.req("REQ-QA-002")]

YEAR_COUNT = 139_579  # events USGS recorded from 2025-10-17 to 2026-10-17
BUDGET_S = 180


class SyntheticYear(FdsnServer):
    """Event i happens at start + i * step (+7 s, so none sits exactly on a Sunday)."""

    def __init__(self, template: dict, start: datetime, end: datetime, count: int):
        super().__init__()
        self._template, self._start, self._count = template, start, count
        self._step_ms = int((end - start).total_seconds() * 1000) // count

    def _time_ms(self, i: int) -> int:
        return at(self._start) + 7000 + i * self._step_ms

    def matching(self, params):
        from support.fdsn import _seconds

        lo_ms, hi_ms = _seconds(params["starttime"]) * 1000, _seconds(params["endtime"]) * 1000
        if "updatedafter" in params:  # nothing was revised during the run
            return []
        if params.get("includedeleted") == "only":  # the synthetic year has no deletions
            return []
        first = max(0, -(-(lo_ms - self._time_ms(0)) // self._step_ms))
        last = min(self._count - 1, (hi_ms - self._time_ms(0)) // self._step_ms)
        events = []
        for i in range(first, last + 1):
            t = self._time_ms(i)
            event = {
                "type": "Feature",
                "id": f"y{i:06d}",
                "geometry": self._template["geometry"],
                "properties": {
                    **self._template["properties"],
                    "time": t,
                    "updated": t + 300_000,
                    "ids": f",y{i:06d},",
                    "mag": round((i % 90) / 10 - 1, 1),
                    "status": "reviewed",
                    "type": "earthquake",
                },
            }
            events.append(event)
        return events


@pytest.mark.req("REQ-BF-011", "REQ-E2E-009")
def test_a_year_backfills_within_three_minutes_with_exact_totals(pipeline, feature_template):
    now = datetime.now(UTC).replace(microsecond=0)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    pipeline.fdsn = SyntheticYear(feature_template, start, now - timedelta(minutes=10), YEAR_COUNT)

    began = time.monotonic()
    report = pipeline.backfill(start, now)
    published_at = time.monotonic()
    pipeline.sync(timeout_s=BUDGET_S)
    finished = time.monotonic()

    total, fetch_s, store_s = finished - began, published_at - began, finished - published_at
    print(
        f"\nyear backfill: {report.windows_done} windows, {report.published} events; "
        f"fetch+publish {fetch_s:.1f} s, consume+store {store_s:.1f} s, total {total:.1f} s "
        f"({YEAR_COUNT / total:.0f} events/s)"
    )
    assert report.windows_done == 42 and report.sweep["windows_done"] == 1  # weeks, then sweep
    assert report.published == YEAR_COUNT and report.fetched == YEAR_COUNT
    assert pipeline.get("/earthquakes/stats")["count"] == YEAR_COUNT
    assert (
        sum(b["count"] for b in pipeline.get("/earthquakes/summary/magnitude")["buckets"])
        == YEAR_COUNT
    )
    reconciled = pipeline.reconcile(backfill=True)
    assert reconciled.ok and reconciled.outcomes == {"applied": YEAR_COUNT}
    assert total <= BUDGET_S, f"took {total:.0f} s, budget {BUDGET_S} s"
