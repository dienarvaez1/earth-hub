from datetime import UTC, datetime, timedelta

import pytest

from earth_hub.ingestion.windows import Window, week_start, weekly_windows

NOW = datetime(2026, 10, 17, 7, 30, 12, 500000, tzinfo=UTC)
JAN_1 = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.req("REQ-BF-001")
def test_2026_up_to_today_is_42_windows_starting_on_jan_1():
    windows = weekly_windows(JAN_1, NOW)
    assert len(windows) == 42
    first, second, last = windows[0], windows[1], windows[-1]
    assert (first.start, first.end) == (JAN_1, datetime(2026, 1, 4, tzinfo=UTC))  # Thu -> Sun
    assert (second.start, second.end) == (
        datetime(2026, 1, 4, tzinfo=UTC),
        datetime(2026, 1, 11, tzinfo=UTC),
    )
    assert (last.start, last.end) == (
        datetime(2026, 10, 11, tzinfo=UTC),
        NOW.replace(microsecond=0),
    )


@pytest.mark.req("REQ-BF-001", "REQ-BF-002")
def test_windows_start_on_sunday_midnight_utc_and_meet_exactly():
    windows = weekly_windows(JAN_1, NOW)
    for window in windows[1:]:
        assert window.start.weekday() == 6  # Sunday
        assert (window.start.hour, window.start.minute, window.start.second) == (0, 0, 0)
        assert window.start.tzinfo is UTC
    for previous, following in zip(windows, windows[1:], strict=False):
        assert previous.end == following.start  # no gap, and no "minus 1 ms"
    assert all(w.start.microsecond == w.end.microsecond == 0 for w in windows)
    assert all(w.end - w.start <= timedelta(days=7) for w in windows)


@pytest.mark.req("REQ-BF-001")
def test_full_weeks_are_done_and_the_current_week_is_open():
    windows = weekly_windows(JAN_1, NOW)
    assert [w.open for w in windows] == [False] * 41 + [True]
    # A range ending exactly on a Sunday has no open window.
    assert not weekly_windows(JAN_1, datetime(2026, 1, 11, tzinfo=UTC))[-1].open


def test_a_start_on_sunday_midnight_is_its_own_week():
    sunday = datetime(2026, 1, 4, tzinfo=UTC)
    windows = weekly_windows(sunday, sunday + timedelta(days=14))
    assert [(w.start, w.end) for w in windows] == [
        (sunday, sunday + timedelta(days=7)),
        (sunday + timedelta(days=7), sunday + timedelta(days=14)),
    ]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (datetime(2026, 1, 1, 15, 30, tzinfo=UTC), datetime(2025, 12, 28, tzinfo=UTC)),
        (datetime(2026, 1, 4, tzinfo=UTC), datetime(2026, 1, 4, tzinfo=UTC)),
        (datetime(2026, 1, 3, 23, 59, 59, 999999, tzinfo=UTC), datetime(2025, 12, 28, tzinfo=UTC)),
    ],
)
def test_week_start(value, expected):
    assert week_start(value) == expected


@pytest.mark.req("REQ-BF-001")
def test_local_times_are_converted_to_utc_weeks():
    # 2026-01-04 01:00 in UTC+2 is still Saturday 23:00 UTC: the previous week.
    from datetime import timezone

    local = datetime(2026, 1, 4, 1, 0, tzinfo=timezone(timedelta(hours=2)))
    assert week_start(local) == datetime(2025, 12, 28, tzinfo=UTC)


def test_empty_range_is_refused():
    with pytest.raises(ValueError):
        weekly_windows(NOW, NOW)


@pytest.mark.req("REQ-BF-004")
def test_split_covers_exactly_the_same_range():
    week = Window(datetime(2026, 1, 4, tzinfo=UTC), datetime(2026, 1, 11, tzinfo=UTC))
    days = week.split()
    assert len(days) == 7
    assert days[0].start == week.start and days[-1].end == week.end
    assert all(a.end == b.start for a, b in zip(days, days[1:], strict=False))
    hours = days[0].split()
    assert hours[0].start == days[0].start and hours[-1].end == days[0].end
    assert all(a.end == b.start for a, b in zip(hours, hours[1:], strict=False))


def test_split_keeps_open_only_on_the_last_part():
    window = Window(
        datetime(2026, 10, 11, tzinfo=UTC), datetime(2026, 10, 14, 12, tzinfo=UTC), open=True
    )
    parts = window.split()
    assert [p.open for p in parts] == [False] * (len(parts) - 1) + [True]


def test_one_second_window_cannot_be_split():
    start = datetime(2026, 1, 4, tzinfo=UTC)
    with pytest.raises(ValueError):
        Window(start, start + timedelta(seconds=1)).split()
