"""Milestone 9, step 5: direct mode takes the bucket over from a full-mode host whose lease
expired (direct/takeover.py), on moto. The bucket a host's exports leave is built here by
direct rounds, then handed to a host in the marker: the same objects, as the parity tests show
(tests/integration/test_standby_cut.py checks it from a real export)."""

import gzip
import json
from datetime import UTC, datetime, timedelta

import pytest
from support.export import BUCKET, start_direct

from earth_hub.direct import runner as runner_module
from earth_hub.direct.reconcile import reconcile
from earth_hub.direct.runner import DirectRunner
from earth_hub.direct.store import PENDING_KEY, STATE_KEY
from earth_hub.direct.takeover import Refused, take_over
from earth_hub.export import lock, mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.mode import LEASE_TTL, MODE_KEY, Mode
from earth_hub.export.r2 import MANIFEST_KEY, fingerprint, manifest_body, row_key

NOW = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)
UPDATED = 1792137700000


@pytest.fixture(autouse=True)
def fixed_run_ids(monkeypatch):
    monkeypatch.setattr(runner_module.secrets, "token_hex", lambda n: "0000")


def everything(s3) -> dict[str, bytes]:
    listed = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    return {
        item["Key"]: s3.get_object(Bucket=BUCKET, Key=item["Key"])["Body"].read() for item in listed
    }


def manifest_rows(s3) -> dict[str, str]:
    body = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read()
    return json.loads(gzip.decompress(body))["rows"]


@pytest.fixture
def owned(s3, tmp_path, make_envelope, make_feature):
    """A bucket with records, direct mode's state describing it, owned by host-a in full
    mode with a lease that ended at NOW."""
    start_direct(s3)
    (tmp_path / "w").mkdir()
    runner = DirectRunner(Bucket(s3, BUCKET), workdir=tmp_path / "w", workers=2)
    e = make_envelope
    runner.process(
        [
            e(make_feature("ak1", updated=UPDATED)),
            e(make_feature("us2", updated=UPDATED, place="Fiji region")),
            e(make_feature("ci3", updated=UPDATED)),
        ],
        now=NOW - timedelta(hours=2),
    )
    seq = json.loads(everything(s3)[STATE_KEY])["seq"]
    lease = mode.iso(NOW)
    s3.put_object(
        Bucket=BUCKET,
        Key=MODE_KEY,
        Body=Mode(owner="host-a", lease_expires_at=lease, checkpoint_seq=seq).body(),
    )
    return everything(s3)


def clock_at(moment: datetime):
    return lambda: moment


@pytest.mark.req("REQ-MODE-020")
def test_an_expired_lease_is_taken_over_and_direct_mode_goes_on(
    owned, s3, tmp_path, make_envelope, make_feature
):
    took = take_over(Bucket(s3, BUCKET), clock=clock_at(NOW + timedelta(seconds=1)), workers=2)
    assert (took.previous_owner, took.lease_expires_at, took.rolled_back) == (
        "host-a",
        mode.iso(NOW),
        0,
    )
    marker = mode.read(Bucket(s3, BUCKET))
    assert (marker.mode, marker.state, marker.owner, marker.lease_expires_at) == (
        mode.DIRECT,
        mode.ACTIVE,
        None,
        None,
    )
    assert marker.checkpoint_seq == json.loads(owned[STATE_KEY])["seq"]
    assert lock.LOCK_KEY not in everything(s3)
    # Only the marker changed; then a direct round runs on the bucket as it was.
    assert {k: v for k, v in everything(s3).items() if k != MODE_KEY} == {
        k: v for k, v in owned.items() if k != MODE_KEY
    }
    (tmp_path / "next").mkdir()
    DirectRunner(Bucket(s3, BUCKET), workdir=tmp_path / "next", workers=2).process(
        [make_envelope(make_feature("nc4", updated=UPDATED))], now=NOW + timedelta(minutes=5)
    )
    assert reconcile(Bucket(s3, BUCKET), deep=True).ok


@pytest.mark.req("REQ-MODE-020")
@pytest.mark.parametrize(
    "case", ["a live lease", "no owner", "direct mode", "a handover", "an export running"]
)
def test_a_bucket_not_due_for_a_takeover_is_left_alone(owned, s3, case):
    bucket = Bucket(s3, BUCKET)
    current = mode.read(bucket)
    at = NOW + timedelta(seconds=1)
    if case == "a live lease":
        at = NOW - timedelta(seconds=1)
    elif case == "no owner":
        mode.write(bucket, current, owner=None, lease_expires_at=None)
    elif case == "direct mode":
        mode.write(bucket, current, mode=mode.DIRECT, owner=None, lease_expires_at=None)
    elif case == "a handover":
        mode.write(bucket, current, state=mode.HANDING_OVER)
    else:
        lock.acquire(bucket, "export", "", at, timedelta(minutes=30))
    before = everything(s3)
    if case == "an export running":
        with pytest.raises(lock.LockHeld):
            take_over(bucket, clock=clock_at(at))
    else:
        assert take_over(bucket, clock=clock_at(at)) is None
    assert everything(s3) == before


@pytest.mark.req("REQ-MODE-020")
def test_take_over_now_does_not_wait_for_the_lease(owned, s3):
    took = take_over(Bucket(s3, BUCKET), clock=clock_at(NOW - LEASE_TTL / 2), force=True)
    assert took is not None and mode.read(Bucket(s3, BUCKET)).mode == mode.DIRECT


@pytest.mark.req("REQ-MODE-020")
def test_of_two_racing_takeovers_one_takes_the_bucket(owned, s3, monkeypatch):
    """The second reads the marker before the first writes it, then again under the lock."""
    bucket = Bucket(s3, BUCKET)
    real_read = mode.read
    reads = []

    def read(b):
        reads.append(1)
        if len(reads) == 1:  # the second takeover's first look, then the first one runs
            current = real_read(b)
            assert take_over(bucket, clock=clock_at(NOW + timedelta(seconds=1))) is not None
            return current
        return real_read(b)

    monkeypatch.setattr(mode, "read", read)
    assert take_over(bucket, clock=clock_at(NOW + timedelta(seconds=2))) is None
    assert mode.read(bucket).mode == mode.DIRECT


@pytest.mark.req("REQ-MODE-021")
def test_an_export_left_unfinished_is_rolled_back_before_the_takeover(owned, s3):
    """The host died after uploading a changed row, a new one, and a manifest checkpoint, but
    before the snapshot: the bucket goes back to its last complete export."""
    rows = manifest_rows(s3)
    changed = next(iter(sorted(rows)))
    s3.put_object(Bucket=BUCKET, Key=row_key(changed), Body=b'{"newer":true}')
    s3.put_object(Bucket=BUCKET, Key=row_key("us9999"), Body=b'{"new":true}')
    partial = rows | {
        changed: fingerprint(b'{"newer":true}'),
        "us9999": fingerprint(b'{"new":true}'),
    }
    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=manifest_body(partial, "export-x", "t"))
    assert not reconcile(Bucket(s3, BUCKET)).ok

    took = take_over(Bucket(s3, BUCKET), clock=clock_at(NOW + timedelta(seconds=1)), workers=2)
    assert took.rolled_back == 2  # one written again, one removed
    after = everything(s3)
    assert row_key("us9999") not in after
    assert after[row_key(changed)] == owned[row_key(changed)]
    assert manifest_rows(s3) == rows
    assert mode.read(Bucket(s3, BUCKET)).mode == mode.DIRECT
    assert reconcile(Bucket(s3, BUCKET), deep=True).ok


@pytest.mark.req("REQ-MODE-021")
@pytest.mark.parametrize("damage", ["an unfinished direct round", "a snapshot that lost a record"])
def test_a_bucket_that_cannot_be_made_consistent_is_refused(owned, s3, tmp_path, damage):
    if damage == "an unfinished direct round":
        s3.put_object(Bucket=BUCKET, Key=PENDING_KEY, Body=b"{}")
    else:  # the ID map names a record the snapshot does not hold: nothing to roll back to
        ids = json.loads(gzip.decompress(owned["_state/ids.json.gz"]))
        ids["records"]["us9999"] = 99
        s3.put_object(
            Bucket=BUCKET, Key="_state/ids.json.gz", Body=gzip.compress(json.dumps(ids).encode())
        )
    before = everything(s3)
    with pytest.raises(Refused):
        take_over(Bucket(s3, BUCKET), clock=clock_at(NOW + timedelta(seconds=1)), workers=2)
    assert everything(s3) == before
