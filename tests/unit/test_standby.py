"""Milestone 9, steps 2 to 4, without Docker (moto and the fake export API): the bucket's owner
and lease (export/mode.py), direct mode's state written after each export
(direct/handover.py:write_standby) and the exporting service's cadence (direct/standby.py).
The cut over PostgreSQL and Kafka is tested in tests/integration/test_standby_cut.py."""

import json
import threading
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import pytest
from support.export import BUCKET, start_direct

from earth_hub.direct.handover import HandoverFailed, Standby, write_standby
from earth_hub.direct.standby import Cut, Service
from earth_hub.direct.store import IDS_KEY, PENDING_KEY, STATE_KEY
from earth_hub.export import lock, mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.mode import LEASE_RENEW, LEASE_TTL, MODE_KEY, Mode, NotOwner
from earth_hub.export.r2 import Exporter
from earth_hub.ingestion.state import SentState

NOW = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)


class Clock:
    def __init__(self, now: datetime = NOW):
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def marker(s3) -> dict:
    return json.loads(s3.get_object(Bucket=BUCKET, Key=MODE_KEY)["Body"].read())


def full_mode(s3, **fields) -> None:
    s3.put_object(Bucket=BUCKET, Key=MODE_KEY, Body=Mode(**fields).body())


def objects(s3, prefix: str = "") -> list[str]:
    listed = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
    return [item["Key"] for item in listed]


def exporter(api, s3, clock, machine="host-a", tmp_path=None) -> Exporter:
    return Exporter(
        api.client(),
        Bucket(s3, BUCKET),
        api_url="http://api.test/api/v1",
        workers=2,
        clock=clock,
        local_dir=tmp_path,
        machine=machine,
    )


# The owner and the lease.


@pytest.mark.req("REQ-MODE-018")
def test_a_marker_from_before_has_no_owner(s3):
    s3.put_object(Bucket=BUCKET, Key=MODE_KEY, Body=b'{"mode": "full", "state": "active"}')
    current = mode.read(Bucket(s3, BUCKET))
    assert (current.owner, current.lease_expires_at) == (None, None)
    assert not current.lease_live(NOW)


@pytest.mark.req("REQ-MODE-018")
def test_the_lease_is_taken_renewed_only_after_five_minutes_and_refused_to_another_host(s3):
    bucket = Bucket(s3, BUCKET)
    held = mode.hold(bucket, mode.read(bucket), "host-a", NOW)
    assert (held.owner, held.lease_expires_at) == ("host-a", mode.iso(NOW + LEASE_TTL))
    assert held.lease_live(NOW + LEASE_TTL - timedelta(seconds=1))
    assert not held.lease_live(NOW + LEASE_TTL)

    soon = mode.hold(bucket, mode.read(bucket), "host-a", NOW + LEASE_RENEW / 2)
    assert soon.lease_expires_at == held.lease_expires_at  # no write
    later = mode.hold(bucket, mode.read(bucket), "host-a", NOW + LEASE_RENEW)
    assert later.lease_expires_at == mode.iso(NOW + LEASE_RENEW + LEASE_TTL)

    with pytest.raises(NotOwner, match="owned by host-a"):
        mode.hold(bucket, mode.read(bucket), "host-b", NOW + timedelta(hours=1))  # even expired
    assert marker(s3)["owner"] == "host-a"


@pytest.mark.req("REQ-MODE-018")
def test_an_export_takes_an_unowned_bucket_and_renews_its_lease_when_done(api, s3, tmp_path):
    record = exporter(api, s3, Clock(), tmp_path=tmp_path).run()
    assert record.status == "succeeded"
    assert (marker(s3)["owner"], marker(s3)["lease_expires_at"]) == (
        "host-a",
        mode.iso(NOW + LEASE_TTL),
    )
    # A run longer than the renewal interval renews it as it ends.
    clock = Clock(NOW + timedelta(minutes=10))
    exporter(api, s3, clock, tmp_path=tmp_path).run()
    assert marker(s3)["lease_expires_at"] == mode.iso(clock.now + LEASE_TTL)


@pytest.mark.req("REQ-MODE-018")
def test_another_hosts_export_writes_nothing(api, s3, tmp_path):
    full_mode(s3, owner="host-a", lease_expires_at=mode.iso(NOW + LEASE_TTL))
    before = marker(s3)
    with pytest.raises(NotOwner):
        exporter(api, s3, Clock(), machine="host-b", tmp_path=tmp_path).run()
    assert objects(s3, "earthquakes/") == [] and objects(s3, "analytics/") == []
    assert lock.LOCK_KEY not in objects(s3)  # released
    assert marker(s3) == before


@pytest.mark.req("REQ-MODE-018")
def test_a_dry_run_and_an_export_without_a_machine_leave_the_marker_alone(api, s3, tmp_path):
    full_mode(s3, owner="host-a", lease_expires_at=mode.iso(NOW))
    before = marker(s3)
    assert exporter(api, s3, Clock(), machine="host-b", tmp_path=tmp_path).run(dry_run=True)
    assert exporter(api, s3, Clock(), machine=None, tmp_path=tmp_path).run().status == ("succeeded")
    assert marker(s3) == before


@pytest.mark.req("REQ-MODE-018")
def test_a_direct_round_skipped_in_full_mode_names_the_owner_and_the_lease(s3):
    full_mode(s3, owner="host-a", lease_expires_at=mode.iso(NOW + LEASE_TTL))
    with pytest.raises(mode.WrongMode) as raised:
        mode.require(Bucket(s3, BUCKET), mode.DIRECT)
    assert str(raised.value) == (
        f"the bucket is in full, owner host-a, lease until {mode.iso(NOW + LEASE_TTL)} mode, "
        "not direct (make mode)"
    )


# Direct mode's state, written after an export.


def standby() -> Standby:
    return Standby(
        order={"us0001": 1, "us0002": 2},
        ids={
            "us0001": ("us0001", "2026-10-30T11:00:00Z"),
            "ak9": ("us0002", "2026-10-30T11:01:00Z"),
        },
        checkpoints={"catchup": "2026-10-30T11:30:00Z"},
    )


def sent() -> SentState:
    state = SentState()
    state.mark_sent("us0001", 1792137700000, 1792137600000)
    return state


@pytest.mark.req("REQ-MODE-019")
def test_the_standby_state_is_the_id_map_the_poller_state_and_the_checkpoints(s3):
    bucket = Bucket(s3, BUCKET)
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps({"seq": 41, "checkpoints": {}}))
    held = lock.Held(bucket, "run-1", "", NOW, timedelta(minutes=15), Clock())
    status = write_standby(bucket, held, standby(), sent(), run_id="run-1", now=NOW)
    stored = json.loads(s3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())
    assert stored == json.loads(json.dumps(status))
    assert stored["seq"] == 41  # direct mode's log goes on from where it was
    assert SentState.from_entries(stored["sent"]).entries() == sent().entries()
    assert stored["checkpoints"] == {"catchup": "2026-10-30T11:30:00Z"}
    assert IDS_KEY in objects(s3)


@pytest.mark.req("REQ-MODE-019")
def test_checkpoints_not_yet_safe_are_left_as_the_bucket_holds_them(s3):
    bucket = Bucket(s3, BUCKET)
    older = {"catchup": "2026-10-30T10:30:00Z"}
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=json.dumps({"seq": 0, "checkpoints": older}))
    held = lock.Held(bucket, "run-1", "", NOW, timedelta(minutes=15), Clock())
    write_standby(bucket, held, standby(), sent(), run_id="run-1", now=NOW, checkpoints=False)
    assert (
        json.loads(s3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())["checkpoints"]
        == older
    )


@pytest.mark.req("REQ-MODE-019")
def test_no_standby_state_over_an_unfinished_direct_round(s3):
    bucket = Bucket(s3, BUCKET)
    s3.put_object(Bucket=BUCKET, Key=PENDING_KEY, Body=b"{}")
    held = lock.Held(bucket, "run-1", "", NOW, timedelta(minutes=15), Clock())
    with pytest.raises(HandoverFailed):
        write_standby(bucket, held, standby(), sent(), run_id="run-1", now=NOW)
    assert IDS_KEY not in objects(s3) and STATE_KEY not in objects(s3)


# The exporting service.


class Fixture:
    """The service over the fake API's rows, with the cut as a test sets it."""

    def __init__(self, api, s3, tmp_path):
        self.api, self.s3 = api, s3
        self.clock = Clock()
        self.changed: datetime | None = NOW - timedelta(minutes=1)
        self.checkpoints = True
        self.extra: dict[str, int] = {}  # records in the cut the export does not have
        self.cuts = 0
        self.service = Service(
            Bucket(s3, BUCKET),
            machine="host-a",
            make_exporter=lambda: exporter(api, s3, self.clock, tmp_path=tmp_path),
            cut=self.cut,
            last_change=lambda: self.changed,
            interval=timedelta(minutes=5),
            clock=self.clock,
        )

    @contextmanager
    def cut(self):
        self.cuts += 1
        order = {row["key"]: row["id"] for row in self.api.rows} | self.extra
        ids = {key: (key, "2026-10-30T11:00:00Z") for key in order}
        yield Cut(
            Standby(order, ids, {"catchup": mode.iso(self.clock.now)}),
            sent(),
            self.checkpoints,
            self.changed,
        )

    def state(self) -> dict | None:
        found = objects(self.s3, STATE_KEY)
        if not found:
            return None
        return json.loads(self.s3.get_object(Bucket=BUCKET, Key=STATE_KEY)["Body"].read())


@pytest.fixture
def service(api, s3, tmp_path):
    return Fixture(api, s3, tmp_path)


@pytest.mark.req("REQ-MODE-019", "REQ-MODE-023")
def test_the_service_exports_then_writes_direct_modes_state_from_the_same_cut(service, s3):
    assert service.service.step() == "exported"
    state = service.state()
    assert state["checkpoints"] == {"catchup": mode.iso(NOW)}
    assert len(objects(s3, "earthquakes/")) == len(service.api.rows)
    assert marker(s3)["owner"] == "host-a"
    assert service.service.step() == "idle"  # nothing new, lease just renewed
    assert service.cuts == 1


@pytest.mark.req("REQ-MODE-023")
def test_the_service_exports_at_most_every_interval_and_renews_the_lease_between(service, s3):
    service.service.step()
    service.changed = NOW + timedelta(seconds=30)
    service.clock.now = NOW + timedelta(minutes=4)
    assert service.service.step() == "idle"  # changed, but not yet due
    service.clock.now = NOW + timedelta(minutes=5)
    assert service.service.step() == "exported"
    assert service.cuts == 2

    service.clock.now = NOW + timedelta(minutes=10)
    puts = len(objects(s3))
    assert service.service.step() == "renewed"
    assert marker(s3)["lease_expires_at"] == mode.iso(service.clock.now + LEASE_TTL)
    assert len(objects(s3)) == puts and service.cuts == 2  # only the marker rewritten


@pytest.mark.req("REQ-MODE-019")
def test_checkpoints_are_copied_only_when_catch_up_is_stored(service):
    service.service.step()
    service.checkpoints = False
    service.changed = NOW + timedelta(minutes=1)
    service.clock.now = NOW + timedelta(minutes=6)
    assert service.service.step() == "exported"
    assert service.state()["checkpoints"] == {"catchup": mode.iso(NOW)}  # the earlier ones


@pytest.mark.req("REQ-MODE-019")
def test_a_cut_whose_records_differ_from_the_export_writes_no_state(service):
    service.extra = {"us9999": 99}  # a record the export did not read
    assert service.service.step() == "failed"
    assert service.state() is None
    assert service.service.exported is None  # tried again once due


@pytest.mark.req("REQ-MODE-023")
@pytest.mark.parametrize("bucket_is", ["direct", "another host's"])
def test_the_service_stops_when_this_host_may_not_write(service, s3, bucket_is):
    if bucket_is == "direct":
        start_direct(s3)
    else:
        full_mode(s3, owner="host-b", lease_expires_at=mode.iso(NOW + LEASE_TTL))
    before = objects(s3)
    stop = threading.Event()
    assert service.service.run(stop, poll=timedelta(0)) == 0
    assert objects(s3) == before  # nothing written, not even the lease
    assert not stop.is_set()  # it stopped on its own


@pytest.mark.req("REQ-MODE-023")
def test_the_service_waits_while_another_run_holds_the_bucket(service, s3):
    lock.acquire(Bucket(s3, BUCKET), "export", "", NOW, timedelta(minutes=30))
    calls = []
    real = service.service.step

    def step():
        calls.append(1)
        if len(calls) == 2:
            lock.release(Bucket(s3, BUCKET), "export", timedelta(minutes=30))
        if len(calls) == 3:
            stop.set()
        return real()

    stop = threading.Event()
    service.service.step = step
    assert service.service.run(stop, poll=timedelta(0)) == 0
    assert len(calls) == 3 and service.state() is not None  # exported once it was free


@pytest.mark.req("REQ-MODE-023")
def test_the_service_without_r2_settings_exports_nothing(monkeypatch, capsys):
    from earth_hub.direct import standby as standby_module

    for name in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"):
        monkeypatch.setenv(name, "")
    monkeypatch.setenv("R2_ENDPOINT_URL", "")
    for name in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"):  # CI has no .env
        monkeypatch.setenv(name, "earth_hub")
    from earth_hub.config import get_settings

    get_settings.cache_clear()
    try:
        assert standby_module.main() == 0
    finally:
        get_settings.cache_clear()
    assert "no R2 settings, not exporting" in capsys.readouterr().out  # JSON log lines


class CountingBucket(Bucket):
    """R2's operation classes: writes and listings are Class A, reads Class B."""

    def __init__(self, client, name):
        super().__init__(client, name)
        self.class_a = self.class_b = 0

    def put(self, key, body, **kwargs):
        self.class_a += 1
        return super().put(key, body, **kwargs)

    def keys(self, prefix):
        self.class_a += 1
        yield from super().keys(prefix)

    def get(self, key):
        self.class_b += 1
        return super().get(key)


@pytest.mark.req("REQ-MODE-023", "REQ-MODE-007")
def test_the_service_and_the_skipped_rounds_stay_inside_the_budget(api, s3, tmp_path):
    """At most one export every 5 minutes, each after a change (a row updated, which rewrites
    the manifest, the snapshot and direct mode's state), and 24 direct rounds a day that skip
    (2 audit writes each), at three times the rate, inside REQ-MODE-007's limits."""
    from support.export import make_row
    from test_direct_budget import DAYS, FREE_CLASS_A, FREE_CLASS_B, LOAD_FACTOR

    bucket = CountingBucket(s3, BUCKET)
    clock = Clock()
    fixture = Fixture(api, s3, tmp_path)
    fixture.service.bucket = bucket
    fixture.service.make_exporter = lambda: Exporter(
        api.client(), bucket, api_url="", workers=2, clock=clock, local_dir=None, machine="host-a"
    )
    fixture.service.clock = clock
    fixture.service.step()  # the first export, of everything
    api.rows[0] = make_row(1, magnitude=4.4)
    fixture.changed = NOW + timedelta(minutes=1)
    clock.now = NOW + timedelta(minutes=5)
    bucket.class_a = bucket.class_b = 0
    assert fixture.service.step() == "exported"
    per_export = (bucket.class_a, bucket.class_b)

    exports_per_day = 24 * 60 // 5
    month_a = LOAD_FACTOR * DAYS * (exports_per_day * per_export[0] + 24 * 2)
    month_b = LOAD_FACTOR * DAYS * exports_per_day * per_export[1]
    print(
        f"per export: A {per_export[0]}, B {per_export[1]}; a month at {LOAD_FACTOR}x: "
        f"A {month_a:,}, B {month_b:,}"
    )
    assert month_a < FREE_CLASS_A / 2
    assert month_b < FREE_CLASS_B / 10


def test_the_service_keeps_no_local_copy_of_its_run_records(s3):
    # DEF-063: its container's working directory is not writable by the service's user.
    from types import SimpleNamespace

    from earth_hub.direct import standby

    settings = SimpleNamespace(export_workers=4, machine_id="host-a")
    exporter = standby.make_exporter(None, Bucket(s3, BUCKET), "http://api:8000/api/v1", settings)
    assert exporter.local_dir is None
    assert exporter.machine == "host-a"
