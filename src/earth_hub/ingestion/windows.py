"""Backfill time windows: calendar weeks starting Sunday 00:00:00 UTC.

Each window ends at exactly the instant the next one starts, so together they cover the range
with no gap. FDSN treats both edges as inclusive at whole-second precision: an event inside the
boundary second belongs to the later window only, and an event at exactly Sunday 00:00:00.000
may be returned by both, which is harmless (duplicates are never stored twice).
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

WEEK = timedelta(days=7)
DAY = timedelta(days=1)
SUNDAY = 6  # datetime.weekday()


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime
    # The window was cut short at "now": the week isn't over, so a later run fetches it again.
    open: bool = False

    def split(self) -> list["Window"]:
        """Smaller windows covering exactly the same range: days, then halves down to 1 s."""
        span = self.end - self.start
        step = DAY if span > DAY else max(timedelta(seconds=1), _whole_seconds(span / 2))
        if step >= span:
            raise ValueError(f"window {self} cannot be split further")
        edges = [self.start]
        while edges[-1] + step < self.end:
            edges.append(edges[-1] + step)
        edges.append(self.end)
        return [
            Window(a, b, open=self.open and b == self.end)
            for a, b in zip(edges, edges[1:], strict=False)
        ]


def _whole_seconds(delta: timedelta) -> timedelta:
    return timedelta(seconds=int(delta.total_seconds()))


def floor_to_second(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(microsecond=0)


def week_start(value: datetime) -> datetime:
    """The Sunday 00:00:00 UTC on or before `value`."""
    utc = value.astimezone(UTC)
    days_back = (utc.weekday() - SUNDAY) % 7
    return (utc - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)


def weekly_windows(start: datetime, end: datetime) -> list[Window]:
    """Windows from `start` to `end`, cut at every Sunday 00:00:00 UTC in between.

    The first window runs from `start` to the following Sunday (2026-01-01 was a Thursday, so
    the first 2026 window is Thu 1 Jan .. Sun 4 Jan). Later windows are full weeks. The last
    one ends at `end`; if that is before its week is over, it is `open`.
    """
    start, end = floor_to_second(start), floor_to_second(end)
    if end <= start:
        raise ValueError(f"backfill range {start} .. {end} is empty")
    windows = []
    cursor = start
    boundary = week_start(start) + WEEK
    while cursor < end:
        window_end = min(boundary, end)
        windows.append(Window(cursor, window_end, open=window_end < boundary))
        cursor, boundary = window_end, boundary + WEEK
    return windows
