"""The backfill job against the faithful FDSN stand-in, with no database or Kafka."""

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from support.fdsn import FdsnServer, event_at, fail_times
from support.memory import MemoryProgress

from earth_hub.ingestion.backfill import Backfill, IncompleteWindow
from earth_hub.ingestion.fdsn import FdsnClient
from earth_hub.ingestion.producer import EventPublisher
from earth_hub.ingestion.usgs_client import make_http_client

JAN_1 = datetime(2026, 1, 1, tzinfo=UTC)
SUN_4 = datetime(2026, 1, 4, tzinfo=UTC)
SUN_11 = datetime(2026, 1, 11, tzinfo=UTC)
END = datetime(2026, 2, 1, tzinfo=UTC)  # Sunday: 5 windows, none open
MS = timedelta(milliseconds=1)


@pytest.fixture
def server():
    return FdsnServer()


@pytest.fixture
def make_job(server, fake_producer):
    def make(progress=None, now=END + timedelta(hours=1), **client_kwargs):
        progress = progress or MemoryProgress()
        fdsn = FdsnClient(
            "https://earthquake.usgs.gov/fdsnws/event/1",
            make_http_client(transport=server.transport),
            pause_s=0,
            sleep=lambda s: None,
            **client_kwargs,
        )
        job = Backfill(
            fdsn, EventPublisher(fake_producer, "earthquakes.backfill"), progress, clock=lambda: now
        )
        return job, progress

    return make


def regular_queries(server) -> list[dict]:
    """The non-deleted query of each window (each window also has a deleted-only one)."""
    return [q for q in server.requests_for("query") if q["includedeleted"] == "false"]


def published_ids(producer) -> list[str]:
    return [json.loads(value)["event_id"] for _, _, value in producer.produced]


# --- boundaries (the concern this design answers) -----------------------------------------


@pytest.mark.req("REQ-BF-002")
def test_events_at_awkward_boundary_times_are_never_missed(
    server, make_job, fake_producer, feature_template
):
    tricky = {
        "just_before_sunday": SUN_4 - MS,
        "last_ms_of_saturday": SUN_4 - timedelta(microseconds=1000),
        "exactly_sunday_midnight": SUN_4,
        "just_after_sunday": SUN_4 + MS,
        "inside_boundary_second": SUN_4 + timedelta(milliseconds=670),
        "saturday_23_59_59_500": SUN_11 - timedelta(milliseconds=500),
        "first_instant_of_range": JAN_1,
        "last_instant_of_range": END,
    }
    server.put(*[event_at(feature_template, name, when) for name, when in tricky.items()])
    make_job()[0].run(JAN_1, END, sweep=False)
    # Each at least once; one sitting exactly on a boundary may come twice, which the consumer
    # stores once (tested end to end).
    assert set(published_ids(fake_producer)) == set(tricky)
    assert published_ids(fake_producer).count("exactly_sunday_midnight") == 2
    assert published_ids(fake_producer).count("inside_boundary_second") == 1


@pytest.mark.req("REQ-BF-002")
def test_the_classic_minus_one_millisecond_mistake_really_loses_events(server, feature_template):
    """Proves the stand-in reproduces the real service: a week ending at 23:59:59.999 is read
    as 23:59:59, so an event in the last second of Saturday falls between two weeks."""
    server.put(event_at(feature_template, "lost", SUN_4 - timedelta(milliseconds=500)))
    previous_week = server.matching(
        {"starttime": "2025-12-28T00:00:00", "endtime": "2026-01-03T23:59:59.999"}
    )
    next_week = server.matching(
        {"starttime": "2026-01-04T00:00:00", "endtime": "2026-01-11T00:00:00"}
    )
    assert previous_week == next_week == []
    correct = server.matching(
        {"starttime": "2025-12-28T00:00:00", "endtime": "2026-01-04T00:00:00"}
    )
    assert [e["id"] for e in correct] == ["lost"]


@pytest.mark.req("REQ-BF-001", "REQ-BF-003")
def test_one_count_and_one_query_per_week(server, make_job, feature_template):
    server.put(
        *[
            event_at(feature_template, f"e{d}", JAN_1 + timedelta(days=d, hours=6))
            for d in range(31)
        ]
    )
    make_job()[0].run(JAN_1, END, sweep=False)
    queries = regular_queries(server)
    assert [(q["starttime"], q["endtime"]) for q in queries] == [
        ("2026-01-01T00:00:00", "2026-01-04T00:00:00"),
        ("2026-01-04T00:00:00", "2026-01-11T00:00:00"),
        ("2026-01-11T00:00:00", "2026-01-18T00:00:00"),
        ("2026-01-18T00:00:00", "2026-01-25T00:00:00"),
        ("2026-01-25T00:00:00", "2026-02-01T00:00:00"),
    ]
    # Each week: non-deleted and deleted events asked for separately, each counted first.
    every = server.requests_for("query")
    assert [q["includedeleted"] for q in every] == ["false", "only"] * 5
    assert [q["includedeleted"] for q in server.requests_for("count")] == ["false", "only"] * 5
    assert all("limit" not in q for q in every)


# --- completeness --------------------------------------------------------------------------


@pytest.mark.req("REQ-BF-003")
def test_fewer_events_than_counted_fails_the_window(server, make_job, feature_template):
    server.put(event_at(feature_template, "e1", SUN_4 + timedelta(days=1)))
    server.count_bias = 1  # /count reports one more than /query returns
    job, progress = make_job()
    with pytest.raises(IncompleteWindow, match="said 1, query returned 0, 3 times"):
        job.run(JAN_1, END, sweep=False)
    assert progress.windows == {}  # never recorded as done


@pytest.mark.req("REQ-BF-003")
def test_a_window_that_comes_back_short_once_is_fetched_again(server, make_job, feature_template):
    server.put(event_at(feature_template, "e1", SUN_4 + timedelta(days=1)))
    short_once = lambda r: (  # noqa: E731
        "/count" in r.url.path and r.url.params.get("starttime") == "2026-01-04T00:00:00"
    )
    calls = [0]

    def overcount_first_time(request):
        if short_once(request) and calls[0] == 0:
            calls[0] += 1
            return httpx.Response(200, json={"count": 2, "maxAllowed": 20000})
        return None

    server.interceptors.append(overcount_first_time)
    job, progress = make_job()
    report = job.run(JAN_1, END, sweep=False)
    assert report.windows_done == 5 and report.fetched == 1


@pytest.mark.req("REQ-BF-014")
def test_a_feature_without_an_id_is_logged_once_and_counted_with_its_window(
    server, make_job, fake_producer, feature_template, caplog
):
    """USGS sends some deletions with an empty ID; the backfill drops them, but says so, even when
    the window is fetched a second time."""
    anonymous = event_at(feature_template, "", SUN_4 + timedelta(days=2), status="deleted")
    server.put(event_at(feature_template, "e1", SUN_4 + timedelta(days=1)), anonymous)
    calls = [0]

    def overcount_first_time(request):
        if "/count" in request.url.path and calls[0] == 0:
            calls[0] += 1
            return httpx.Response(200, json={"count": 2, "maxAllowed": 20000})
        return None

    server.interceptors.append(overcount_first_time)
    job, progress = make_job()
    report = job.run(SUN_4, SUN_11, sweep=False)
    assert (report.published, report.rejected) == (1, 1)
    [(_, counts)] = progress.windows.values()
    assert (counts.fetched, counts.published, counts.rejected) == (2, 1, 1)
    [line] = [r for r in caplog.records if r.getMessage() == "feature rejected"]
    assert (line.stage, line.job, line.start, line.event_id) == ("parse", "backfill", SUN_4, "")
    assert "id must be a non-empty string" in line.reason
    assert published_ids(fake_producer) == ["e1"]


@pytest.mark.req("REQ-BF-004")
def test_week_over_the_cap_is_split_into_days(
    server, make_job, fake_producer, feature_template, monkeypatch
):
    monkeypatch.setattr("support.fdsn.CAP", 3)
    monkeypatch.setattr("earth_hub.ingestion.backfill.MAX_RESULTS", 3)
    busy = [
        event_at(feature_template, f"swarm{i}", SUN_4 + timedelta(hours=12 * i)) for i in range(10)
    ]
    server.put(*busy)
    job, _ = make_job()
    report = job.run(SUN_4, SUN_11, sweep=False)
    assert report.windows_split == 1
    # Events exactly at midnight are returned by both adjacent days; none is missing.
    assert set(published_ids(fake_producer)) == {e["id"] for e in busy}


@pytest.mark.req("REQ-BF-005")
def test_interrupted_backfill_resumes_without_refetching(
    server, make_job, fake_producer, feature_template
):
    server.put(
        *[
            event_at(feature_template, f"e{d}", JAN_1 + timedelta(days=d, hours=6))
            for d in range(31)
        ]
    )
    third_week = lambda r: r.url.params.get("starttime") == "2026-01-11T00:00:00"  # noqa: E731
    server.interceptors.append(fail_times(3, 503, when=third_week))
    job, progress = make_job()
    with pytest.raises(Exception, match="failed after 3 attempts"):
        job.run(JAN_1, END, sweep=False)
    assert len(progress.windows) == 2

    server.requests.clear()
    report = make_job(progress)[0].run(JAN_1, END, sweep=False)
    assert report.windows_skipped == 2 and report.windows_done == 3
    fetched = [q["starttime"] for q in regular_queries(server)]
    assert fetched == ["2026-01-11T00:00:00", "2026-01-18T00:00:00", "2026-01-25T00:00:00"]
    assert sorted(set(published_ids(fake_producer))) == sorted(f"e{d}" for d in range(31))


@pytest.mark.req("REQ-BF-001", "REQ-BF-005")
def test_the_open_week_is_fetched_again_next_time(server, make_job, feature_template):
    now = datetime(2026, 1, 14, 12, tzinfo=UTC)  # Wednesday
    progress = MemoryProgress()
    make_job(progress, now=now)[0].run(SUN_4, sweep=False)
    ((window, _),) = [v for (job, start), v in progress.windows.items() if start == SUN_11]
    assert window.open and window.end == now

    server.requests.clear()
    later = datetime(2026, 1, 20, tzinfo=UTC)
    make_job(progress, now=later)[0].run(SUN_4, sweep=False)
    assert [q["starttime"] for q in regular_queries(server)] == [
        "2026-01-11T00:00:00",  # the formerly open week, now complete
        "2026-01-18T00:00:00",
    ]


# --- what gets published -------------------------------------------------------------------


@pytest.mark.req("REQ-BF-009")
def test_only_new_or_newer_events_are_published(server, make_job, fake_producer, feature_template):
    when = SUN_4 + timedelta(days=2)
    same = event_at(feature_template, "same", when)
    newer = event_at(feature_template, "newer", when)
    fresh = event_at(feature_template, "fresh", when)
    server.put(same, newer, fresh)
    stored = {"same": same["properties"]["updated"], "newer": newer["properties"]["updated"] - 1000}
    report = make_job(MemoryProgress(stored))[0].run(SUN_4, SUN_11, sweep=False)
    assert sorted(published_ids(fake_producer)) == ["fresh", "newer"]
    assert (report.published, report.skipped) == (2, 1)


@pytest.mark.req("REQ-BF-008", "REQ-DQ-ACC-001")
def test_deleted_events_are_included(server, make_job, fake_producer, feature_template):
    server.put(event_at(feature_template, "gone", SUN_4 + timedelta(days=1), status="deleted"))
    make_job()[0].run(SUN_4, SUN_11, sweep=False)
    [(_, _, value)] = fake_producer.produced
    assert json.loads(value)["payload"]["properties"]["status"] == "deleted"


@pytest.mark.req("REQ-BF-010")
def test_messages_are_marked_backfill_and_go_to_the_backfill_topic(
    server, make_job, fake_producer, feature_template
):
    server.put(event_at(feature_template, "e1", SUN_4 + timedelta(days=1)))
    make_job()[0].run(SUN_4, SUN_11, sweep=False)
    [(topic, key, value)] = fake_producer.produced
    assert (topic, key) == ("earthquakes.backfill", b"e1")
    assert json.loads(value)["source"] == "backfill"


@pytest.mark.req("REQ-BF-006")
def test_final_sweep_catches_events_revised_into_an_already_fetched_week(
    server, feature_template, fake_producer
):
    """USGS revises an event's time during the backfill, moving it into a week already done."""
    progress = MemoryProgress()
    started = datetime(2026, 2, 1, 6, tzinfo=UTC)
    quake = event_at(
        feature_template, "moved", SUN_11 + timedelta(hours=1), updated=timedelta(hours=1)
    )
    server.put(quake)

    # While the third week is being fetched, the revision lands: time moves back a day.
    def revise_during_third_week(request):
        if "/query" in str(request.url) and "starttime=2026-01-11" in str(request.url):
            revised = json.loads(json.dumps(quake))
            revised["properties"]["time"] = int((SUN_11 - timedelta(hours=2)).timestamp() * 1000)
            revised["properties"]["updated"] = int(
                (started + timedelta(minutes=10)).timestamp() * 1000
            )
            server.put(revised)
            server.interceptors.remove(revise_during_third_week)
        return None

    server.interceptors.append(revise_during_third_week)
    fdsn = FdsnClient(
        "https://earthquake.usgs.gov/fdsnws/event/1",
        make_http_client(transport=server.transport),
        pause_s=0,
        sleep=lambda s: None,
    )
    job = Backfill(
        fdsn, EventPublisher(fake_producer, "earthquakes.backfill"), progress, clock=lambda: started
    )
    report = job.run(JAN_1, END)
    # The original version was not in week 3 any more when fetched; week 2 was already done.
    # Without the sweep the event would be missing entirely.
    assert report.sweep["published"] == 1
    assert published_ids(fake_producer) == ["moved"]
    [sweep_query] = [q for q in regular_queries(server) if "updatedafter" in q]
    assert sweep_query["updatedafter"] == "2026-02-01T06:00:00"
    assert (sweep_query["starttime"], sweep_query["endtime"]) == (
        "2026-01-01T00:00:00",
        "2026-02-01T00:00:00",
    )


def too_slow_for(days: int):
    """Like the real gateway: a 504 for any query spanning more than `days` days."""
    import httpx

    def intercept(request):
        params = request.url.params
        if not request.url.path.endswith("/query"):
            return None
        span = datetime.fromisoformat(params["endtime"]) - datetime.fromisoformat(
            params["starttime"]
        )
        return (
            httpx.Response(504, text="504 Gateway Time-out")
            if span > timedelta(days=days)
            else None
        )

    return intercept


@pytest.mark.req("REQ-BF-013", "REQ-BF-002")
def test_a_week_usgs_times_out_on_is_split_into_days(
    server, make_job, fake_producer, feature_template
):
    events = [
        event_at(feature_template, f"e{h}", SUN_4 + timedelta(hours=5 * h, seconds=3))
        for h in range(33)
    ]
    server.put(*events)
    server.interceptors.append(too_slow_for(days=3))
    job, progress = make_job()
    report = job.run(SUN_4, SUN_11, sweep=False)

    queries = [(q["starttime"], q["endtime"]) for q in regular_queries(server)]
    assert queries[0] == ("2026-01-04T00:00:00", "2026-01-11T00:00:00")  # the week, once
    assert queries[1:] == [
        (f"2026-01-{d:02d}T00:00:00", f"2026-01-{d + 1:02d}T00:00:00") for d in range(4, 11)
    ]  # then its seven days, on the same boundaries
    assert report.windows_split == 1 and report.windows_done == 7
    assert sorted(set(published_ids(fake_producer))) == sorted(e["id"] for e in events)


@pytest.mark.req("REQ-BF-013", "REQ-BF-005")
def test_resuming_a_week_done_partly_in_days_continues_with_the_days(
    server, make_job, feature_template
):
    server.put(
        *[event_at(feature_template, f"e{d}", SUN_4 + timedelta(days=d, hours=6)) for d in range(7)]
    )
    server.interceptors.append(too_slow_for(days=1))
    fifth_day = lambda r: r.url.params.get("starttime") == "2026-01-08T00:00:00"  # noqa: E731
    server.interceptors.insert(0, fail_times(3, 503, when=fifth_day))
    job, progress = make_job()
    with pytest.raises(Exception, match="failed after 3 attempts"):
        job.run(SUN_4, SUN_11, sweep=False)
    assert len(progress.windows) == 4  # Jan 4-7 done

    server.requests.clear()
    make_job(progress)[0].run(SUN_4, SUN_11, sweep=False)
    queries = [q["starttime"] for q in regular_queries(server)]
    assert queries == ["2026-01-08T00:00:00", "2026-01-09T00:00:00", "2026-01-10T00:00:00"]


def test_a_window_still_too_slow_at_one_hour_stops_the_backfill(server, make_job, feature_template):
    import httpx

    server.put(event_at(feature_template, "e1", SUN_4 + timedelta(hours=1)))
    server.interceptors.append(
        lambda r: httpx.Response(504) if r.url.path.endswith("/query") else None
    )
    from earth_hub.ingestion.fdsn import FdsnTooSlow

    with pytest.raises(FdsnTooSlow):
        make_job()[0].run(SUN_4, SUN_4 + timedelta(days=1), sweep=False)


@pytest.mark.req("REQ-BF-008", "REQ-BF-013")
def test_deleted_and_regular_events_are_never_mixed_in_one_query(
    server, make_job, fake_producer, feature_template
):
    """USGS is 20-100x slower on includedeleted=true and returns stubs with no time; the
    backfill must get both kinds without ever asking for them mixed."""
    alive = event_at(feature_template, "alive", SUN_4 + timedelta(days=1))
    gone = event_at(feature_template, "gone", SUN_4 + timedelta(days=2), status="deleted")
    server.put(alive, gone)
    server.interceptors.append(
        lambda r: httpx.Response(504) if r.url.params.get("includedeleted") == "true" else None
    )
    report = make_job()[0].run(SUN_4, SUN_11, sweep=False)
    assert sorted(published_ids(fake_producer)) == ["alive", "gone"]
    assert report.windows_split == 0 and report.fetched == 2
    assert {r.url.params.get("includedeleted") for r in server.requests} == {"false", "only"}
