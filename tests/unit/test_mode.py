"""The bucket's mode marker, who may write in each mode, and `make direct-once`. The handovers
themselves run on real Kafka and PostgreSQL: tests/e2e/test_mode_handover.py."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from support.export import BUCKET, start_direct
from support.fdsn import FdsnServer
from support.feed import EventFactory, FeedServer

from earth_hub.consumers.earthquake_consumer import PROCESSED_AT_HEADER, _processed_at
from earth_hub.direct.runner import DirectRunner
from earth_hub.export import mode
from earth_hub.export.bucket import Bucket, PreconditionFailed
from earth_hub.export.lock import LOCK_KEY


def test_no_marker_means_full_mode(s3):
    assert mode.read(Bucket(s3, BUCKET)) == mode.Mode()


@pytest.mark.req("REQ-MODE-009")
def test_the_marker_changes_only_from_what_was_read(s3):
    bucket = Bucket(s3, BUCKET)
    first = mode.read(bucket)
    direct = mode.write(bucket, first, mode=mode.DIRECT, checkpoint_seq=4)
    assert mode.read(bucket) == direct and direct.checkpoint_seq == 4
    with pytest.raises(PreconditionFailed):
        mode.write(bucket, first, mode=mode.FULL)  # stale: someone changed it since
    with pytest.raises(PreconditionFailed):
        mode.write(bucket, mode.Mode(), mode=mode.FULL)  # "none there" is stale too


@pytest.mark.req("REQ-MODE-009")
def test_the_exporter_writes_nothing_in_direct_mode(s3, export, bucket):
    start_direct(s3)
    with pytest.raises(mode.WrongMode, match="in direct mode, not full"):
        export()
    listed = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])]
    assert listed == [mode.MODE_KEY]  # no row, no run record, the lock released
    assert export(dry_run=True).status == "succeeded"  # a dry run writes nothing: allowed


@pytest.mark.req("REQ-MODE-009")
@pytest.mark.parametrize("marker", [None, mode.Mode(mode.DIRECT, mode.HANDING_OVER)])
def test_direct_rounds_write_nothing_unless_the_bucket_is_in_direct_mode(
    s3, tmp_path, make_envelope, make_feature, marker
):
    if marker is not None:
        s3.put_object(Bucket=BUCKET, Key=mode.MODE_KEY, Body=marker.body())
    runner = DirectRunner(Bucket(s3, BUCKET), workdir=tmp_path)
    with pytest.raises(mode.WrongMode):
        runner.process([make_envelope(make_feature("ak1"))])
    listed = {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])}
    assert LOCK_KEY not in listed and not any(k.startswith("earthquakes/") for k in listed)


class FakeMessage:
    def __init__(self, headers):
        self._headers = headers

    def headers(self):
        return self._headers


@pytest.mark.req("REQ-MODE-010")
@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        (
            [(PROCESSED_AT_HEADER, b"2026-10-16T09:00:00.250001Z")],
            datetime(2026, 10, 16, 9, 0, 0, 250001, tzinfo=UTC),
        ),
        (
            [("other", b"x"), (PROCESSED_AT_HEADER, b"2026-10-16T09:00:00+00:00")],
            datetime(2026, 10, 16, 9, tzinfo=UTC),
        ),
        (None, None),
        ([(PROCESSED_AT_HEADER, b"2026-10-16T09:00:00+02:00")], None),  # not UTC
        ([(PROCESSED_AT_HEADER, b"2026-10-16T09:00:00")], None),  # no time zone
        ([(PROCESSED_AT_HEADER, b"yesterday")], None),
        ([(PROCESSED_AT_HEADER, b"\xff")], None),
    ],
)
def test_a_replayed_message_carries_its_processing_time(headers, expected):
    assert _processed_at(FakeMessage(headers)) == expected


@pytest.fixture
def direct_main(s3, monkeypatch, feature_template):
    """`python -m earth_hub.direct` with the bucket on moto and the feeds mocked."""
    from earth_hub.config import Settings
    from earth_hub.direct import __main__ as entry

    settings = Settings(
        _env_file=None, postgres_db="x", postgres_user="x", postgres_password="x", r2_bucket=BUCKET
    )
    feed = FeedServer()
    feed.show(EventFactory(feature_template)("ak1", minutes_ago=10))
    month = FeedServer()
    fdsn = FdsnServer()
    monkeypatch.setattr("earth_hub.config.get_settings", lambda: settings)
    monkeypatch.setattr("earth_hub.export.bucket.make_s3_client", lambda settings: s3)

    import httpx

    def client(contact="", timeout_s=15.0, **kwargs):
        def route(request):
            url = str(request.url)
            server = fdsn if "fdsnws" in url else month if "all_month" in url else feed
            return server.transport.handle_request(request)

        return httpx.Client(transport=httpx.MockTransport(route))

    monkeypatch.setattr("earth_hub.ingestion.usgs_client.make_http_client", client)
    return entry.main


def deleted_checked_recently(s3) -> None:
    """Direct mode's state with the deleted-events check an hour old: not due (catch-up is)."""
    state = {
        "seq": 0,
        "run_id": None,
        "written_at": None,
        "sent": {},
        "checkpoints": {"deleted": (datetime.now(UTC) - timedelta(hours=1)).isoformat()},
    }
    s3.put_object(Bucket=BUCKET, Key="_state/direct.json", Body=json.dumps(state))


@pytest.mark.req("REQ-MODE-012")
def test_direct_once_refuses_a_bucket_in_full_mode(direct_main, s3, capsys):
    assert direct_main(["once"]) == 2
    assert "in full mode, not direct" in capsys.readouterr().err
    assert s3.list_objects_v2(Bucket=BUCKET)["KeyCount"] == 0


@pytest.mark.req("REQ-MODE-012")
def test_direct_once_runs_the_poller_and_catch_up_when_due(direct_main, s3, capsys):
    start_direct(s3)
    deleted_checked_recently(s3)
    assert direct_main(["once"]) == 0
    out = capsys.readouterr().out
    assert "poller published, 1 events processed" in out
    assert "catchup: 0 events processed" in out and "deleted" not in out
    row = json.loads(s3.get_object(Bucket=BUCKET, Key="earthquakes/ak1.json")["Body"].read())
    assert row["key"] == "ak1"


@pytest.mark.req("REQ-REC-002")
def test_direct_once_all_runs_catch_up_and_the_deleted_check_even_when_not_due(
    direct_main, s3, capsys
):
    start_direct(s3)
    deleted_checked_recently(s3)
    assert direct_main(["once", "--all"]) == 0
    out = capsys.readouterr().out
    assert "catchup: 0 events processed" in out and "deleted: 0 events processed" in out


@pytest.mark.req("REQ-MODE-012")
def test_a_long_running_direct_process_is_told_when_a_handover_freezes_the_bucket(
    s3, tmp_path, make_envelope, make_feature
):
    from earth_hub.direct.runner import DirectPublisher
    from earth_hub.ingestion.events import build_message

    start_direct(s3)
    stopped = []
    runner = DirectRunner(Bucket(s3, BUCKET), workdir=tmp_path)
    publisher = DirectPublisher(runner, source="feed", on_wrong_mode=lambda: stopped.append(1))
    message = build_message(
        make_feature("ak1"), fetched_at=datetime.now(UTC), feed_generated_ms=None
    )
    assert publisher.publish([message]).delivered == [message]
    frozen = mode.read(Bucket(s3, BUCKET))
    mode.write(Bucket(s3, BUCKET), frozen, state=mode.HANDING_OVER)
    assert len(publisher.publish([message]).failed) == 1
    assert stopped == [1]


@pytest.mark.req("REQ-MODE-014")
@pytest.mark.parametrize("marker", [None, mode.Mode(mode.DIRECT, mode.HANDING_OVER)])
def test_a_scheduled_round_skips_a_bucket_not_in_direct_mode(direct_main, s3, capsys, marker):
    if marker is not None:
        s3.put_object(Bucket=BUCKET, Key=mode.MODE_KEY, Body=marker.body())
    before = s3.list_objects_v2(Bucket=BUCKET)["KeyCount"]
    assert direct_main(["once", "--skip-unless-direct"]) == 0
    assert "direct: skipped: the bucket is in" in capsys.readouterr().out
    assert s3.list_objects_v2(Bucket=BUCKET)["KeyCount"] == before


@pytest.mark.req("REQ-MODE-014")
def test_a_scheduled_round_runs_as_direct_once_in_direct_mode(direct_main, s3, capsys):
    start_direct(s3)
    deleted_checked_recently(s3)
    assert direct_main(["once", "--skip-unless-direct"]) == 0
    out = capsys.readouterr().out
    assert "poller published, 1 events processed" in out and "skipped" not in out
    assert s3.get_object(Bucket=BUCKET, Key="earthquakes/ak1.json")


@pytest.mark.req("REQ-MODE-014")
@pytest.mark.parametrize(("flags", "code"), [(["--skip-unless-direct"], 0), ([], 1)])
def test_a_handover_during_a_scheduled_round_is_a_skip_and_catch_up_does_not_run(
    direct_main, s3, capsys, monkeypatch, flags, code
):
    start_direct(s3)

    def frozen(self, *args, **kwargs):  # a handover froze the bucket just before this round
        raise mode.WrongMode("the bucket is in direct, handing-over mode, not direct")

    monkeypatch.setattr(DirectRunner, "process", frozen)
    assert direct_main(["once", *flags]) == code
    out = capsys.readouterr().out
    assert "events processed" in out and "catchup" not in out
    assert ("left direct mode during the run" in out) == bool(flags)


@pytest.mark.req("REQ-MODE-014")
def test_missing_settings_still_fail_a_scheduled_round(direct_main, monkeypatch, capsys):
    from earth_hub.export.bucket import MissingSettings

    def missing(settings):
        raise MissingSettings("set R2_BUCKET, R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY")

    monkeypatch.setattr("earth_hub.export.bucket.make_s3_client", missing)
    assert direct_main(["once", "--skip-unless-direct"]) == 2
    assert "set R2_BUCKET" in capsys.readouterr().err
