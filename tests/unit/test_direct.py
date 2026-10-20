"""Direct mode's rounds on an in-memory S3 (moto): what a round writes, how an interrupted one
is finished, what it refuses, and the poller and catch-up running on it unchanged. Parity
with full mode is tested against PostgreSQL in tests/integration/test_direct_parity.py."""

import gzip
import json
import random
from datetime import UTC, datetime, timedelta

import duckdb
import pytest
from support.export import BUCKET
from support.fdsn import FdsnServer, event_at
from support.feed import FeedServer
from support.streams import random_stream

from earth_hub.direct import runner as runner_module
from earth_hub.direct.runner import BucketProgress, DirectPublisher, DirectRunner
from earth_hub.direct.store import (
    IDS_KEY,
    PENDING_KEY,
    STATE_KEY,
    Inconsistent,
    log_key,
    record_of,
    row_body,
)
from earth_hub.export.bucket import Bucket, PreconditionFailed
from earth_hub.export.lock import LOCK_KEY, LockHeld
from earth_hub.export.r2 import MANIFEST_KEY, row_key
from earth_hub.export.snapshot import META_KEY, SNAPSHOT_KEY, SnapshotFile
from earth_hub.ingestion.catchup import CatchUp
from earth_hub.ingestion.fdsn import FdsnClient
from earth_hub.ingestion.poller import Poller
from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client

NOW = datetime(2026, 10, 16, 9, 0, 0, 123456, tzinfo=UTC)
UPDATED = 1792137700000


class FailingBucket(Bucket):
    """Fails the first `operation` (put or delete) of a key starting with `prefix`, once."""

    def __init__(self, client, name):
        super().__init__(client, name)
        self.fail: tuple[str, str] | None = None

    def _trip(self, operation: str, key: str) -> None:
        if self.fail is not None and self.fail[0] == operation and key.startswith(self.fail[1]):
            self.fail = None
            raise ConnectionError(f"simulated failure: {operation} {key}")

    def put(self, key, body, **kwargs):
        self._trip("put", key)
        return super().put(key, body, **kwargs)

    def delete(self, keys):
        keys = list(keys)
        for key in keys:
            self._trip("delete", key)
        return super().delete(keys)


@pytest.fixture(autouse=True)
def fixed_run_ids(monkeypatch):
    """Run IDs end in random characters; fixed, two runs of the same rounds write the same."""
    monkeypatch.setattr(runner_module.secrets, "token_hex", lambda n: "0000")


@pytest.fixture
def make_runner(s3, tmp_path):
    count = [0]

    def make(bucket: Bucket | None = None, **kwargs) -> DirectRunner:
        count[0] += 1
        workdir = tmp_path / f"runner{count[0]}"
        workdir.mkdir()
        return DirectRunner(bucket or Bucket(s3, BUCKET), workdir=workdir, workers=2, **kwargs)

    return make


@pytest.fixture
def runner(make_runner):
    return make_runner()


def get(s3, key: str) -> bytes:
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def everything(s3) -> dict[str, bytes]:
    listed = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    return {item["Key"]: get(s3, item["Key"]) for item in listed}


def feature(make_feature, event_id, ids=None, **properties):
    return make_feature(event_id, ids, **{"updated": UPDATED, **properties})


def rounds(make_envelope, make_feature) -> list[list[bytes]]:
    """Three rounds: new events, then revisions and a merge, then more."""
    e = make_envelope
    return [
        [e(feature(make_feature, "ak1")), e(feature(make_feature, "us2", place="Fiji region"))],
        [
            e(feature(make_feature, "ak1", mag=3.1, updated=UPDATED + 60_000)),
            e(feature(make_feature, "us2", ["us2", "ak1"], updated=UPDATED + 90_000)),  # merge
            e(feature(make_feature, "ci3")),
        ],
        [e(feature(make_feature, "ci3", updated=UPDATED + 1)), b"not json"],
    ]


def process_all(runner, batches) -> None:
    for minute, values in enumerate(batches):
        runner.process(values, now=NOW + timedelta(minutes=minute))


@pytest.mark.req("REQ-MODE-003")
def test_a_round_writes_rows_ids_manifest_snapshot_log_and_state(
    runner, s3, make_envelope, make_feature
):
    process_all(runner, rounds(make_envelope, make_feature))

    stored = everything(s3)
    assert sorted(k for k in stored if k.startswith("earthquakes/")) == [
        row_key("ak1"),  # the merge's survivor: the older record, its key kept
        row_key("ci3"),
    ]
    survivor = json.loads(stored[row_key("ak1")])
    # 2 revisions of ak1 + 1 of us2, merged, then the merging message applied: 4.
    assert (survivor["preferred_event_id"], survivor["revision_count"]) == ("us2", 4)
    assert set(json.loads(gzip.decompress(stored[MANIFEST_KEY]))["rows"]) == {"ak1", "ci3"}
    ids = json.loads(gzip.decompress(stored[IDS_KEY]))
    assert {i: k for i, (k, _) in ids["ids"].items()} == {"ak1": "ak1", "us2": "ak1", "ci3": "ci3"}
    assert ids["records"] == {"ak1": 1, "ci3": 3}  # creation order, as full mode's ids
    assert PENDING_KEY not in stored and LOCK_KEY not in stored

    logs = sorted(k for k in stored if k.startswith("log/"))
    assert logs == [
        "log/2026-10-16/000000000001-000000000002.ndjson",
        "log/2026-10-16/000000000003-000000000005.ndjson",
        "log/2026-10-16/000000000006-000000000007.ndjson",
    ]
    lines = [json.loads(line) for key in logs for line in stored[key].splitlines()]
    assert [(e["seq"], e["event_id"], e["outcome"], e["key"]) for e in lines] == [
        (1, "ak1", "applied", "ak1"),
        (2, "us2", "applied", "us2"),
        (3, "ak1", "applied", "ak1"),
        (4, "us2", "applied", "ak1"),  # merged into ak1
        (5, "ci3", "applied", "ci3"),
        (6, "ci3", "applied", "ci3"),
        (7, None, "rejected", None),
    ]
    assert lines[0]["processed_at"] == "2026-10-16T09:00:00.123456Z"
    assert lines[0]["source"] == "feed" and lines[0]["envelope"]["event_id"] == "ak1"
    assert (lines[6]["rule"], lines[6]["raw_value"]) == ("ENV-001", "not json")
    status = json.loads(stored[STATE_KEY])
    assert status["seq"] == 7


@pytest.mark.req("REQ-MODE-008")
def test_records_read_back_from_the_snapshot_give_the_same_objects(
    runner, s3, make_envelope, make_feature
):
    rng = random.Random(7)
    values = random_stream(rng, "rt", make_envelope, make_feature, merges=True)
    unusual = feature(make_feature, "rt-unicode", place="Ñuñoa, Región Metropolitana, Chile")
    unusual["properties"].update(mag=-0.37, felt=None, cdi=3.1, tsunami=1, alert="green")
    values.append(make_envelope(unusual))
    for minute, start in enumerate(range(0, len(values), 9)):
        runner.process(values[start : start + 9], now=NOW + timedelta(minutes=minute, seconds=0.5))

    runner.state.refresh()
    keys = set(runner.state.manifest)
    assert len(keys) > 10
    for key, row in runner.state.rows(keys).items():
        assert row_body(record_of(row)) == get(s3, row_key(key)), key


@pytest.mark.req("REQ-MODE-001")
def test_the_updated_snapshot_equals_a_rebuild_from_the_objects(
    runner, s3, make_envelope, make_feature, tmp_path
):
    process_all(runner, rounds(make_envelope, make_feature))
    rebuilt = SnapshotFile()
    for key in sorted(k for k in everything(s3) if k.startswith("earthquakes/")):
        rebuilt.add(get(s3, key))
    (tmp_path / "rebuilt.parquet").write_bytes(rebuilt.build())
    rebuilt.close()
    (tmp_path / "updated.parquet").write_bytes(get(s3, SNAPSHOT_KEY))

    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    a = con.sql(f"SELECT * FROM '{tmp_path / 'rebuilt.parquet'}'")
    b = con.sql(f"SELECT * FROM '{tmp_path / 'updated.parquet'}'")
    assert (a.columns, a.types, a.fetchall()) == (b.columns, b.types, b.fetchall())
    meta = json.loads(get(s3, META_KEY))
    assert meta["rows"] == 2 and meta["run_id"].startswith("2026-10-16T09-02-00Z")


STEPS = {
    "a row object": ("put", "earthquakes/"),
    "a removed row": ("delete", "earthquakes/us2"),  # merged away in round 2
    "the ID map": ("put", IDS_KEY),
    "the manifest": ("put", MANIFEST_KEY),
    "the snapshot": ("put", SNAPSHOT_KEY),
    "the snapshot's meta.json": ("put", META_KEY),
    "the log": ("put", "log/"),
    "the state": ("put", STATE_KEY),
    "removing the redo record": ("delete", PENDING_KEY),
}


@pytest.mark.req("REQ-MODE-002")
@pytest.mark.parametrize("step", STEPS, ids=list(STEPS))
def test_a_round_interrupted_at_any_write_is_finished_by_the_next(
    s3, make_runner, make_envelope, make_feature, step
):
    """The bucket ends byte for byte as if nothing had been interrupted."""
    batches = rounds(make_envelope, make_feature)
    process_all(make_runner(), batches)
    expected = everything(s3)
    for key in expected:
        s3.delete_object(Bucket=BUCKET, Key=key)

    failing = FailingBucket(s3, BUCKET)
    crashing = make_runner(failing)
    crashing.process(batches[0], now=NOW)
    failing.fail = STEPS[step]
    with pytest.raises(ConnectionError):
        crashing.process(batches[1], now=NOW + timedelta(minutes=1))
    assert PENDING_KEY in everything(s3)
    assert LOCK_KEY not in everything(s3)  # released on the way out
    # The next round, by a new process, finishes the interrupted one first.
    make_runner().process(batches[2], now=NOW + timedelta(minutes=2))
    assert everything(s3) == expected


@pytest.mark.req("REQ-MODE-002")
def test_a_round_never_starts_on_top_of_unfinished_writes(runner, s3):
    runner.state.refresh()
    s3.put_object(Bucket=BUCKET, Key=PENDING_KEY, Body=b"{}")  # another process's
    with pytest.raises(PreconditionFailed):
        runner.state.begin({"run_id": "mine"})


@pytest.mark.req("REQ-MODE-004")
def test_a_round_waits_while_an_export_holds_the_lock(runner, s3, make_envelope, make_feature):
    lock = {"run_id": "export", "started_at": "x", "expires_at": "2099-01-01T00:00:00Z"}
    s3.put_object(Bucket=BUCKET, Key=LOCK_KEY, Body=json.dumps(lock))
    with pytest.raises(LockHeld, match="run export"):
        runner.process([make_envelope(feature(make_feature, "ak1"))], now=NOW)
    assert list(everything(s3)) == [LOCK_KEY]


@pytest.mark.req("REQ-MODE-004")
@pytest.mark.parametrize("damage", ["manifest", "id map"])
def test_direct_mode_refuses_a_bucket_whose_parts_disagree(
    runner, s3, make_envelope, make_feature, damage
):
    process_all(runner, rounds(make_envelope, make_feature)[:1])
    if damage == "manifest":  # as after an export whose snapshot was not written
        rows = json.loads(gzip.decompress(get(s3, MANIFEST_KEY)))
        rows["rows"]["zz9"] = "0" * 64
        s3.put_object(
            Bucket=BUCKET, Key=MANIFEST_KEY, Body=gzip.compress(json.dumps(rows).encode())
        )
    else:
        ids = json.loads(gzip.decompress(get(s3, IDS_KEY)))
        del ids["records"]["us2"]
        s3.put_object(Bucket=BUCKET, Key=IDS_KEY, Body=gzip.compress(json.dumps(ids).encode()))
    before = everything(s3)
    with pytest.raises(Inconsistent):
        runner.process([make_envelope(feature(make_feature, "ci3"))], now=NOW)
    assert everything(s3) == before


@pytest.mark.req("REQ-MODE-004")
def test_a_runner_reads_what_another_wrote_since_its_last_round(
    s3, make_runner, make_envelope, make_feature
):
    batches = rounds(make_envelope, make_feature)
    process_all(make_runner(), batches)
    expected = everything(s3)
    for key in expected:
        s3.delete_object(Bucket=BUCKET, Key=key)

    first, second = make_runner(), make_runner()
    first.process(batches[0], now=NOW)
    second.process(batches[1], now=NOW + timedelta(minutes=1))  # first's cache is now stale
    first.process(batches[2], now=NOW + timedelta(minutes=2))
    assert everything(s3) == expected


# The poller and catch-up, unchanged, on direct mode.


def test_rounds_with_nothing_to_process_write_nothing(runner, s3):
    assert runner.process([], now=NOW) == []
    assert everything(s3) == {}


@pytest.mark.req("REQ-MODE-005")
def test_the_poller_runs_unchanged_and_its_state_survives_a_restart(
    s3, make_runner, feature_template
):
    from support.feed import EventFactory, revised

    events = EventFactory(feature_template)
    feed = FeedServer()
    a, b = events("ak1", minutes_ago=30), events("us2", minutes_ago=90)
    feed.show(a, b)

    def poller(runner):
        state = runner.sent_state()
        client = UsgsClient(
            "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
            make_http_client(transport=feed.transport),
            sleep=lambda s: None,
        )
        publisher = DirectPublisher(runner, source="feed", state=state)
        return Poller(client, publisher, state, fallback_wait_s=60)

    first = poller(make_runner())
    assert first.run_round().published == 2
    feed.show(revised(a, mag=3.3))
    assert first.run_round().published == 1

    # A new process: the state comes from the bucket, so nothing is sent again.
    restarted = poller(make_runner())
    feed.show(b)  # a new feed version, nothing new in it
    result = restarted.run_round()
    assert (result.published, result.unchanged) == (0, 2)
    saved = json.loads(get(s3, STATE_KEY))
    assert set(saved["sent"]) == {"ak1", "us2"}
    assert saved["seq"] == 3


@pytest.mark.req("REQ-MODE-005")
def test_a_failed_round_is_reported_failed_and_retried_by_the_poller(
    s3, make_runner, feature_template
):
    from support.feed import EventFactory

    feed = FeedServer()
    feed.show(EventFactory(feature_template)("ak1", minutes_ago=30))
    runner = make_runner()
    state = runner.sent_state()
    client = UsgsClient(
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
        make_http_client(transport=feed.transport),
        sleep=lambda s: None,
    )
    poller = Poller(
        client, DirectPublisher(runner, source="feed", state=state), state, fallback_wait_s=60
    )
    lock = {"run_id": "export", "started_at": "x", "expires_at": "2099-01-01T00:00:00Z"}
    s3.put_object(Bucket=BUCKET, Key=LOCK_KEY, Body=json.dumps(lock))
    assert poller.run_round().failed_delivery == 1
    s3.delete_object(Bucket=BUCKET, Key=LOCK_KEY)
    assert poller.run_round().published == 1  # Last-Modified was not kept: fetched again


@pytest.mark.req("REQ-MODE-005")
def test_catch_up_runs_unchanged_and_skips_what_full_mode_would(
    s3, make_runner, feature_template, make_envelope
):
    from earth_hub.ingestion.events import build_message

    now = datetime.now(UTC).replace(microsecond=0)
    stored_same = event_at(feature_template, "same", now - timedelta(days=3))
    stored_older = event_at(feature_template, "older", now - timedelta(days=4))
    new = event_at(feature_template, "new", now - timedelta(days=5))
    runner = make_runner()
    newer = json.loads(json.dumps(stored_older))
    newer["properties"]["updated"] += 60_000
    runner.process([make_envelope(stored_same), make_envelope(stored_older)], now=now)

    month = FeedServer()
    month.show(stored_same, newer, new)
    deleted = event_at(feature_template, "same", now - timedelta(days=3))
    deleted["properties"].update(status="deleted", updated=deleted["properties"]["updated"] + 5)
    fdsn = FdsnServer([deleted])

    def job(runner):
        return CatchUp(
            UsgsClient(
                "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
                make_http_client(transport=month.transport),
                sleep=lambda s: None,
            ),
            FdsnClient(
                "https://earthquake.usgs.gov/fdsnws/event/1",
                make_http_client(transport=fdsn.transport),
                pause_s=0,
                sleep=lambda s: None,
            ),
            DirectPublisher(runner, source="catchup"),
            BucketProgress(runner),
            clock=lambda: now,
        )

    assert job(runner).catch_up() == 2  # "same" is stored at that version
    assert job(runner).check_deleted() == 1
    # The same messages, through the rule full mode uses, against what is stored.
    message = build_message(stored_same, fetched_at=now, feed_generated_ms=None)
    assert runner.needed([message]) == []

    restarted = make_runner()
    assert BucketProgress(restarted).checkpoint("catchup") == now
    assert BucketProgress(restarted).checkpoint("deleted") == now
    lines = [
        json.loads(line)
        for key, body in everything(s3).items()
        if key.startswith("log/")
        for line in body.splitlines()
    ]
    assert sorted((e["source"], e["event_id"]) for e in lines if e["source"] != "feed") == [
        ("catchup", "new"),
        ("catchup", "older"),
        ("deleted", "same"),
    ]
    assert json.loads(get(s3, row_key("same")))["review_status"] == "deleted"


def test_log_names_sort_by_sequence():
    assert log_key(1, 12, NOW) < log_key(13, 13, NOW) < log_key(100, 250, NOW)


@pytest.mark.req("REQ-MODE-002")
def test_gzipped_state_holds_no_time_so_the_same_rows_give_the_same_bytes(
    runner, s3, make_envelope, make_feature
):
    """DEF-041: gzip writes the current time into its header unless told otherwise."""
    process_all(runner, rounds(make_envelope, make_feature)[:1])
    for key in (MANIFEST_KEY, IDS_KEY):
        assert get(s3, key)[4:8] == b"\0\0\0\0", key  # the header's MTIME field
