"""DEF-058: a writer paused past its lock's expiry writes nothing once it wakes. Every write
confirms the lock first (export/lock.py:Held), so the lock taken over between any two writes
of a direct round or an export stops the writer at its next one; and `make repair-rows`
(direct/repair.py) fixes a row object left stale by a write already in flight."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from support.export import BUCKET, start_direct

from earth_hub.direct import repair as repair_module
from earth_hub.direct import runner as runner_module
from earth_hub.direct.reconcile import reconcile
from earth_hub.direct.repair import repair
from earth_hub.direct.runner import DirectRunner
from earth_hub.direct.store import IDS_KEY, PENDING_KEY, STATE_KEY, Inconsistent
from earth_hub.export import lock
from earth_hub.export.bucket import Bucket
from earth_hub.export.lock import LOCK_KEY, Held, LockLost
from earth_hub.export.mode import MODE_KEY, Mode
from earth_hub.export.r2 import MANIFEST_KEY, Exporter
from earth_hub.export.snapshot import META_KEY, SNAPSHOT_KEY

NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)
TTL = timedelta(minutes=15)
UPDATED = 1792137700000


class Clock:
    def __init__(self, now: datetime):
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class PausingBucket(Bucket):
    """After the first write (put or delete) of a key starting with `prefix`, the writer
    "sleeps": the clock moves past its lock's time and another run takes the lock. The writes
    that succeed after that are recorded; the lock's own are left out."""

    def __init__(self, client, name, clock: Clock):
        super().__init__(client, name)
        self.clock = clock
        self.pause_after: tuple[str, str] | None = None
        self.after_pause: list[str] = []
        self.paused = False

    def _wrote(self, operation: str, key: str) -> None:
        if self.paused:
            if key != LOCK_KEY:
                self.after_pause.append(f"{operation} {key}")
            return
        if self.pause_after is None:
            return
        wanted, prefix = self.pause_after
        if operation == wanted and key.startswith(prefix):
            self.paused = True
            self.clock.now += timedelta(hours=1)
            lock.acquire(Bucket(self.client, self.name), "other", "", self.clock.now, TTL)

    def put(self, key, body, **kwargs):
        etag = super().put(key, body, **kwargs)
        self._wrote("put", key)
        return etag

    def delete(self, keys):
        keys = list(keys)
        failed = super().delete(keys)
        for key in keys:
            self._wrote("delete", key)
        return failed


def everything(s3) -> dict[str, bytes]:
    listed = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    return {
        item["Key"]: s3.get_object(Bucket=BUCKET, Key=item["Key"])["Body"].read() for item in listed
    }


def workdir(tmp_path, name: str):
    path = tmp_path / name
    path.mkdir()
    return path


def wipe(s3) -> None:
    for key in everything(s3):
        s3.delete_object(Bucket=BUCKET, Key=key)


@pytest.fixture(autouse=True)
def fixed_run_ids(monkeypatch):
    monkeypatch.setattr(runner_module.secrets, "token_hex", lambda n: "0000")
    monkeypatch.setattr(repair_module.secrets, "token_hex", lambda n: "0000")


# The lock as a run holds it.


@pytest.mark.req("REQ-MODE-017")
def test_a_young_lock_is_confirmed_without_a_request_and_renewed_after_a_third(s3):
    bucket = Bucket(s3, BUCKET)
    clock = Clock(NOW)
    held = Held(bucket, "mine", "", NOW, TTL, clock)
    first = s3.get_object(Bucket=BUCKET, Key=LOCK_KEY)["ETag"]
    clock.now += TTL / 3 - timedelta(seconds=1)
    held.confirm()
    assert s3.get_object(Bucket=BUCKET, Key=LOCK_KEY)["ETag"] == first  # no request
    clock.now += timedelta(seconds=1)
    held.confirm()
    renewed = json.loads(s3.get_object(Bucket=BUCKET, Key=LOCK_KEY)["Body"].read())
    assert renewed["expires_at"] == (clock.now + TTL).isoformat().replace("+00:00", "Z")


@pytest.mark.req("REQ-MODE-017")
def test_a_lock_taken_over_while_its_holder_slept_stops_it_for_good(s3):
    bucket = Bucket(s3, BUCKET)
    clock = Clock(NOW)
    held = Held(bucket, "mine", "", NOW, TTL, clock)
    clock.now += timedelta(hours=1)  # asleep past its time
    lock.acquire(bucket, "other", "", clock.now, TTL)
    with pytest.raises(LockLost):
        held.confirm()
    clock.now = NOW  # even a clock that says otherwise
    with pytest.raises(LockLost):
        held.confirm()
    held.release()  # leaves the other run's lock
    assert json.loads(s3.get_object(Bucket=BUCKET, Key=LOCK_KEY)["Body"].read())["run_id"] == (
        "other"
    )


@pytest.mark.req("REQ-MODE-017")
def test_a_failed_renewal_is_tried_again_while_the_lock_still_has_time(s3):
    class Flaky(Bucket):
        down = False

        def put(self, key, body, **kwargs):
            if self.down:
                raise ConnectionError("simulated")
            return super().put(key, body, **kwargs)

    bucket = Flaky(s3, BUCKET)
    clock = Clock(NOW)
    held = Held(bucket, "mine", "", NOW, TTL, clock)
    bucket.down = True
    clock.now += TTL / 2  # past a third, under two thirds: still ours for 7½ minutes
    held.confirm()
    clock.now = NOW + TTL * 2 / 3  # no longer sure of it
    with pytest.raises(ConnectionError):
        held.confirm()
    bucket.down = False
    held.confirm()  # renewed: nobody could take it before it expired


# A direct round paused between two writes.


def feature(make_feature, event_id, ids=None, **properties):
    return make_feature(event_id, ids, **{"updated": UPDATED, **properties})


def rounds(make_envelope, make_feature) -> list[list[bytes]]:
    """New events; then revisions and a merge, which removes a row; then more."""
    e = make_envelope
    return [
        [e(feature(make_feature, "ak1")), e(feature(make_feature, "us2", place="Fiji region"))],
        [
            e(feature(make_feature, "ak1", mag=3.1, updated=UPDATED + 60_000)),
            e(feature(make_feature, "us2", ["us2", "ak1"], updated=UPDATED + 90_000)),
            e(feature(make_feature, "ci3")),
        ],
        [e(feature(make_feature, "ci3", updated=UPDATED + 1))],
    ]


# The second round's writes, in order (direct/store.py:BucketState.begin and apply).
STEPS = {
    "the redo record": ("put", PENDING_KEY),
    "a row object": ("put", "earthquakes/"),
    "a removed row": ("delete", "earthquakes/"),
    "the ID map": ("put", IDS_KEY),
    "the manifest": ("put", MANIFEST_KEY),
    "the snapshot": ("put", SNAPSHOT_KEY),
    "the snapshot's meta.json": ("put", META_KEY),
    "the log": ("put", "log/"),
    "the state": ("put", STATE_KEY),
    "removing the redo record": ("delete", PENDING_KEY),
}


@pytest.mark.req("REQ-MODE-017")
@pytest.mark.parametrize("step", STEPS, ids=list(STEPS))
def test_a_round_paused_past_its_lock_writes_nothing_once_it_wakes(
    s3, tmp_path, make_envelope, make_feature, step
):
    """Asleep after any write, its lock taken over: it writes nothing more, and the next round
    (after the other run) finishes its writes, so the bucket ends as if it had never paused."""
    batches = rounds(make_envelope, make_feature)
    start_direct(s3)
    plain = DirectRunner(Bucket(s3, BUCKET), workdir=workdir(tmp_path, "a"), workers=1)
    for minute, values in enumerate(batches):
        plain.process(values, now=NOW + timedelta(minutes=minute))
    expected = everything(s3)
    wipe(s3)

    start_direct(s3)
    clock = Clock(NOW)
    bucket = PausingBucket(s3, BUCKET, clock)
    # One worker: the row objects are written one after another, so none is in flight.
    sleeper = DirectRunner(bucket, workdir=workdir(tmp_path, "b"), workers=1, clock=clock)
    sleeper.process(batches[0], now=NOW)
    clock.now = NOW + timedelta(minutes=1)
    bucket.pause_after = STEPS[step]
    if step == "removing the redo record":  # its last write: nothing left to stop
        sleeper.process(batches[1], now=clock.now)
    else:
        with pytest.raises(LockLost):
            sleeper.process(batches[1], now=clock.now)
    assert bucket.paused and bucket.after_pause == []

    lock.release(Bucket(s3, BUCKET), "other", TTL)  # the other run, done
    DirectRunner(Bucket(s3, BUCKET), workdir=workdir(tmp_path, "c"), workers=1).process(
        batches[2], now=NOW + timedelta(minutes=2)
    )
    assert everything(s3) == expected


# An export paused in the middle of a page.


@pytest.mark.req("REQ-MODE-017")
def test_an_export_paused_inside_a_page_writes_no_more_rows(api, s3, tmp_path):
    """Before DEF-058 the lock was renewed once a page: a run asleep after its third row wrote
    the rest of the page on waking, over the other run's."""
    clock = Clock(NOW)
    bucket = PausingBucket(s3, BUCKET, clock)
    bucket.pause_after = ("put", "earthquakes/")
    exporter = Exporter(
        api.client(),
        bucket,
        api_url="http://api.test/api/v1",
        workers=1,
        page_size=100,  # every row in one page
        clock=clock,
        local_dir=tmp_path,
    )
    record = exporter.run()
    assert record.status == "failed"
    assert any("LockLost" in e for e in record.errors)
    rows = [k for k in everything(s3) if k.startswith("earthquakes/")]
    assert len(rows) == 1 and len(api.rows) > 1
    assert bucket.after_pause == [f"put runs/{record.run_id}.json"]  # its own record only
    assert json.loads(everything(s3)[LOCK_KEY])["run_id"] == "other"


# Repairing what a write in flight left.


@pytest.fixture
def stored(s3, tmp_path, make_envelope, make_feature):
    """A direct-mode bucket after the three rounds."""
    start_direct(s3)
    runner = DirectRunner(Bucket(s3, BUCKET), workdir=workdir(tmp_path, "r"), workers=1)
    for minute, values in enumerate(rounds(make_envelope, make_feature)):
        runner.process(values, now=NOW + timedelta(minutes=minute))
    return everything(s3)


@pytest.mark.req("REQ-MODE-017")
def test_repair_rows_writes_stale_and_missing_row_objects_again_from_the_snapshot(s3, stored):
    rows = sorted(k for k in stored if k.startswith("earthquakes/"))
    s3.put_object(Bucket=BUCKET, Key=rows[0], Body=b'{"stale":true}')
    s3.delete_object(Bucket=BUCKET, Key=rows[1])
    assert not reconcile(Bucket(s3, BUCKET), deep=True).ok

    dry = repair(Bucket(s3, BUCKET), dry_run=True)
    assert dry.differing == sorted(
        k.removeprefix("earthquakes/").removesuffix(".json") for k in rows[:2]
    )
    assert everything(s3)[rows[0]] == b'{"stale":true}'  # a dry run writes nothing

    report = repair(Bucket(s3, BUCKET), workers=2)
    assert report.ok and report.repaired == 2
    assert everything(s3) == stored  # exactly the objects, and no lock left
    assert reconcile(Bucket(s3, BUCKET), deep=True).ok


@pytest.mark.req("REQ-MODE-017")
def test_repair_rows_works_in_full_mode_and_writes_only_what_the_manifest_vouches_for(s3, stored):
    s3.put_object(Bucket=BUCKET, Key=MODE_KEY, Body=Mode().body())  # full mode
    key = next(k for k in sorted(stored) if k.startswith("earthquakes/"))
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"{}")
    assert repair(Bucket(s3, BUCKET)).repaired == 1
    assert everything(s3)[key] == stored[key]


@pytest.mark.req("REQ-MODE-017")
@pytest.mark.parametrize("refusal", ["a handover", "a stale snapshot", "the lock held"])
def test_repair_rows_refuses_without_writing(s3, stored, refusal):
    key = next(k for k in sorted(stored) if k.startswith("earthquakes/"))
    s3.put_object(Bucket=BUCKET, Key=key, Body=b"{}")
    if refusal == "a handover":
        mode = json.loads(stored[MODE_KEY]) | {"state": "handing-over"}
        s3.put_object(Bucket=BUCKET, Key=MODE_KEY, Body=json.dumps(mode))
        error = runner_module.mode.WrongMode
    elif refusal == "a stale snapshot":
        meta = json.loads(stored[META_KEY]) | {"content_sha256": "0" * 64}
        s3.put_object(Bucket=BUCKET, Key=META_KEY, Body=json.dumps(meta))
        error = Inconsistent
    else:
        lock.acquire(Bucket(s3, BUCKET), "export", "", datetime.now(UTC), TTL)
        error = lock.LockHeld
    before = everything(s3)
    with pytest.raises(error):
        repair(Bucket(s3, BUCKET))
    assert everything(s3) == before
    assert everything(s3)[key] == b"{}"


@pytest.mark.req("REQ-MODE-017")
def test_the_weekly_deep_check_is_in_the_scheduled_reconcile():
    from pathlib import Path

    workflow = (Path(__file__).resolve().parents[2] / ".github/workflows/direct.yml").read_text()
    assert 'if [ "$(date -u +%u)" = 7 ]; then deep="--deep"; fi' in workflow
    assert "earth_hub.direct.reconcile --skip-unless-direct $deep" in workflow
