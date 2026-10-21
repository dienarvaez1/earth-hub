"""The R2 exporter against a fake API and an in-memory S3 (moto): no Docker, no network."""

import gzip
import json
from datetime import timedelta

import httpx
import pytest
from support.export import BUCKET, NOW, make_row

from earth_hub.config import Settings
from earth_hub.export import r2
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import (
    LOCK_KEY,
    MANIFEST_KEY,
    Exporter,
    LockHeld,
    encode,
    list_runs,
    object_row,
    published,
    row_key,
)


def name(n: int) -> str:
    """The object of test row n (support/export.py:make_row gives it the key us000n)."""
    return row_key(f"us{n:04d}")


def stored_rows(s3) -> dict[str, dict]:
    """key -> the row object's content."""
    objects = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="earthquakes/"):
        for item in page.get("Contents", []):
            body = s3.get_object(Bucket=BUCKET, Key=item["Key"])["Body"].read()
            objects[object_row(item["Key"])] = json.loads(body)
    return objects


def counts(record) -> dict:
    c = record.counts
    return {k: getattr(c, k) for k in ("read", "new", "updated", "unchanged", "deleted", "failed")}


@pytest.mark.req("REQ-EXP-002", "REQ-EXP-004")
def test_first_run_uploads_every_row_exactly(export, api, s3):
    record = export()
    assert record.status == "succeeded"
    assert counts(record) == {
        "read": 7,
        "new": 7,
        "updated": 0,
        "unchanged": 0,
        "deleted": 0,
        "failed": 0,
    }
    assert stored_rows(s3) == {row["key"]: published(row) for row in api.rows}
    body = s3.get_object(Bucket=BUCKET, Key=name(1))["Body"].read()
    assert body == encode(published(api.rows[0]))  # byte for byte the API row, less its id
    assert record.verified  # no manifest yet, so the bucket was listed
    assert record.verification == {
        "source": "bucket listing",
        "rows_read": 7,
        "objects": 7,
        "missing": [],
        "unexpected": [],
        "match": True,
    }


@pytest.mark.req("REQ-EXP-002", "REQ-EXP-003")
def test_a_second_run_uploads_nothing(export, bucket):
    export()
    bucket.puts.clear()
    record = export()
    assert record.status == "succeeded"
    assert (record.counts.unchanged, record.counts.new, record.counts.updated) == (7, 0, 0)
    assert record.counts.bytes_uploaded == 0
    assert not [k for k in bucket.puts if k.startswith("earthquakes/")]


@pytest.mark.req("REQ-EXP-002", "REQ-EXP-004")
def test_new_changed_and_removed_rows(export, api, s3):
    export()
    api.rows[1] = make_row(2, magnitude=2.41, revision_count=2)  # a revision
    api.rows[3] = make_row(4, review_status="deleted", magnitude=None)  # a USGS deletion
    del api.rows[4]  # row 5 merged into another event: no longer in the table
    api.rows.append(make_row(9))
    record = export()
    assert record.status == "succeeded"
    assert counts(record) == {
        "read": 7,
        "new": 1,
        "updated": 2,
        "unchanged": 4,
        "deleted": 1,
        "failed": 0,
    }
    assert stored_rows(s3) == {row["key"]: published(row) for row in api.rows}


@pytest.mark.req("REQ-EXP-008")
def test_an_object_removed_from_the_bucket_is_uploaded_again_by_a_verified_run(export, s3):
    export()
    s3.delete_object(Bucket=BUCKET, Key=name(3))
    quick = export()  # trusts the manifest: does not notice
    assert quick.status == "succeeded" and not quick.verified
    assert (quick.counts.new, quick.counts.unchanged) == (0, 7)
    assert quick.verification["source"] == "manifest"
    assert len(stored_rows(s3)) == 6

    record = export(verify=True)
    assert record.status == "succeeded" and record.verified
    assert (record.counts.new, record.counts.unchanged) == (1, 6)
    assert len(stored_rows(s3)) == 7


@pytest.mark.req("REQ-EXP-008")
def test_a_verified_run_deletes_objects_the_manifest_does_not_know(export, s3):
    export()
    s3.put_object(Bucket=BUCKET, Key=name(99), Body=b"{}")  # left by hand
    assert export().verification["match"]  # unverified: not seen
    record = export(verify=True)
    assert record.status == "succeeded" and record.counts.deleted == 1
    assert sorted(stored_rows(s3)) == [f"us{n:04d}" for n in range(1, 8)]


@pytest.mark.req("REQ-EXP-003")
def test_the_manifest_is_saved_only_when_uploads_changed_it(export, api, bucket):
    """Each save uploads the whole manifest (about 5 MB for 115k rows, ~7 s on R2)."""
    export(checkpoint_pages=1)
    bucket.puts.clear()
    export(checkpoint_pages=1, clock=lambda: NOW + timedelta(minutes=1))
    assert MANIFEST_KEY not in bucket.puts  # nothing changed: no save at all

    api.rows[0] = make_row(1, magnitude=3.0)  # on the first of three pages
    bucket.puts.clear()
    export(checkpoint_pages=1, clock=lambda: NOW + timedelta(minutes=2))
    assert bucket.puts.count(MANIFEST_KEY) == 1  # after page 1 only; the end adds nothing


def test_the_final_manifest_filter_is_computed_once(export, monkeypatch):
    """DEF-030: `k in read & stored` rebuilt the intersection for every row, about 190 s for
    115k rows. The rows to keep are now one set, built once and passed in."""
    calls = []
    real_kept = r2.kept

    def kept(known, ids):
        calls.append(ids)
        return real_kept(known, ids)

    monkeypatch.setattr(r2, "kept", kept)
    export()
    assert len(calls) == 1 and calls[0] == {f"us{n:04d}" for n in range(1, 8)}


@pytest.mark.req("REQ-EXP-008")
def test_only_a_verified_run_lists_the_bucket_and_only_once(export, api, monkeypatch):
    """Listing 100k+ keys takes about 100 s on R2; a day's changes take seconds."""
    export()
    listings = []
    real_keys = Bucket.keys

    def keys(self, prefix):
        listings.append(prefix)
        return real_keys(self, prefix)

    monkeypatch.setattr(Bucket, "keys", keys)
    api.rows[0] = make_row(1, magnitude=3.0)
    del api.rows[4]
    record = export(clock=lambda: NOW + timedelta(minutes=1))
    assert record.status == "succeeded" and record.verification["match"]
    assert (record.counts.updated, record.counts.deleted) == (1, 1)  # from the manifest
    assert listings == []
    record = export(verify=True, clock=lambda: NOW + timedelta(minutes=2))
    assert record.status == "succeeded" and record.verification["match"]
    assert listings == ["earthquakes/"]


@pytest.mark.req("REQ-EXP-008")
def test_a_run_without_a_manifest_is_verified(export, s3):
    s3.put_object(Bucket=BUCKET, Key=name(99), Body=b"{}")
    record = export()
    assert record.verified and record.counts.deleted == 1


@pytest.mark.req("REQ-EXP-003")
def test_a_failed_delete_is_retried_by_the_next_run(export, api, bucket, monkeypatch):
    export()
    del api.rows[4]
    real_delete = Bucket.delete

    def delete(self, keys):  # row deletes fail; the lock is still released
        keys = list(keys)
        rows = [k for k in keys if k.startswith("earthquakes/")]
        return rows + real_delete(self, [k for k in keys if k not in rows])

    monkeypatch.setattr(Bucket, "delete", delete)
    record = export(clock=lambda: NOW + timedelta(minutes=1))
    assert record.status == "failed" and record.counts.failed == 1
    monkeypatch.undo()
    record = export(clock=lambda: NOW + timedelta(minutes=2))
    assert record.status == "succeeded" and record.counts.deleted == 1


@pytest.mark.req("REQ-EXP-002")
def test_objects_outside_the_row_layout_are_left_alone(export, s3):
    s3.put_object(Bucket=BUCKET, Key="earthquakes/README.txt", Body=b"notes")
    record = export()
    assert record.status == "succeeded" and record.counts.deleted == 0
    assert s3.get_object(Bucket=BUCKET, Key="earthquakes/README.txt")["Body"].read() == b"notes"


@pytest.mark.req("REQ-EXP-003", "REQ-EXP-004", "REQ-EXP-005")
def test_failed_uploads_fail_the_run_and_are_retried_without_duplicates(export, bucket, api, s3):
    bucket.failing = {name(2), name(6)}
    record = export()
    assert record.status == "failed"
    assert (record.counts.new, record.counts.failed) == (5, 2)
    assert record.verification["missing"] == ["us0002", "us0006"]
    assert any("row us0002" in e for e in record.errors)

    record = export()
    assert record.status == "succeeded"
    assert (record.counts.new, record.counts.unchanged) == (2, 5)
    assert stored_rows(s3) == {row["key"]: published(row) for row in api.rows}


@pytest.mark.req("REQ-EXP-003", "REQ-EXP-005")
def test_an_interrupted_run_is_recorded_and_the_next_run_completes_it(export, api, s3, tmp_path):
    api.fail_after_pages = 1  # Ctrl-C while reading the second page
    with pytest.raises(KeyboardInterrupt):
        export()
    (local,) = (tmp_path / "exports").glob("*.json")
    interrupted = json.loads(local.read_text())
    assert interrupted["status"] == "interrupted"
    assert interrupted["counts"]["new"] == 3
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix=LOCK_KEY)["KeyCount"] == 0  # released

    api.fail_after_pages = None
    record = export(clock=lambda: NOW + timedelta(minutes=1))
    assert record.status == "succeeded"
    # The interrupted run saved its progress: the 3 rows it uploaded are not sent again.
    assert (record.counts.new, record.counts.updated, record.counts.unchanged) == (4, 0, 3)
    assert stored_rows(s3) == {row["key"]: published(row) for row in api.rows}


@pytest.mark.req("REQ-EXP-003")
def test_progress_is_saved_every_few_pages(export, bucket):
    export(checkpoint_pages=2)  # pages of 3, 3 and 1 rows
    assert bucket.puts.count(MANIFEST_KEY) == 2  # after page 2, and at the end


@pytest.mark.req("REQ-EXP-003")
def test_a_killed_run_loses_only_the_pages_since_the_last_checkpoint(export, api, s3, monkeypatch):
    """kill -9: no cleanup runs, so only the periodic checkpoints survive."""
    monkeypatch.setattr(Exporter, "_checkpoint", lambda self, known, record: None)
    api.fail_after_pages = 2
    with pytest.raises(KeyboardInterrupt):
        export()
    monkeypatch.undo()
    api.fail_after_pages = None
    record = export(clock=lambda: NOW + timedelta(minutes=1))
    assert record.status == "succeeded"
    assert (record.counts.unchanged, record.counts.new, record.counts.updated) == (6, 1, 0)
    assert stored_rows(s3) == {row["key"]: published(row) for row in api.rows}


@pytest.mark.req("REQ-EXP-005")
def test_every_run_is_recorded_in_the_bucket_and_locally(export, s3, tmp_path):
    first = export()
    second = export(clock=lambda: NOW + timedelta(hours=1))
    runs = list_runs(Bucket(s3, BUCKET), now=NOW + timedelta(hours=1))
    assert [r["run_id"] for r in runs] == [first.run_id, second.run_id]
    assert runs[0] | {"counts": None} == {
        "run_id": first.run_id,
        "started_at": "2026-10-18T09:00:00Z",
        "finished_at": "2026-10-18T09:00:00Z",
        "duration_s": 0.0,
        "api_url": "http://api.test/api/v1",
        "bucket": BUCKET,
        "git_commit": "abc1234",
        "dry_run": False,
        "verified": True,
        "status": "succeeded",
        "counts": None,
        "verification": first.verification,
        "snapshot": {"written": True, "rows": 7, "bytes": first.snapshot["bytes"]},
        "errors": [],
    }
    assert runs[1]["counts"]["unchanged"] == 7
    local = sorted(p.name for p in (tmp_path / "exports").glob("*.json"))
    assert local == [f"{first.run_id}.json", f"{second.run_id}.json"]


@pytest.mark.req("REQ-EXP-005")
def test_a_run_that_crashed_is_listed_as_interrupted(s3):
    """A killed process leaves its record `running` and its lock until the lock expires."""
    crashed = {"run_id": "2026-10-18T08-00-00Z-dead", "status": "running", "counts": {}}
    s3.put_object(Bucket=BUCKET, Key=f"runs/{crashed['run_id']}.json", Body=json.dumps(crashed))
    lock = {"run_id": crashed["run_id"], "started_at": "", "expires_at": "2026-10-18T10:00:00Z"}
    s3.put_object(Bucket=BUCKET, Key=LOCK_KEY, Body=json.dumps(lock))
    bucket = Bucket(s3, BUCKET)
    assert list_runs(bucket, now=NOW)[0]["status"] == "running"  # lock still live
    assert list_runs(bucket, now=NOW + timedelta(hours=2))[0]["status"] == "interrupted"


@pytest.mark.req("REQ-EXP-006")
def test_a_second_run_is_refused_while_one_holds_the_lock(export, s3):
    lock = {
        "run_id": "other",
        "started_at": "2026-10-18T08:30:00Z",
        "expires_at": "2026-10-18T10:30:00Z",
    }
    s3.put_object(Bucket=BUCKET, Key=LOCK_KEY, Body=json.dumps(lock))
    with pytest.raises(LockHeld, match="run other has been in progress"):
        export()
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="earthquakes/")["KeyCount"] == 0
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="runs/")["KeyCount"] == 0
    assert json.loads(s3.get_object(Bucket=BUCKET, Key=LOCK_KEY)["Body"].read()) == lock


@pytest.mark.req("REQ-EXP-006")
def test_an_expired_lock_is_taken_over(export, s3):
    lock = {
        "run_id": "crashed",
        "started_at": "2026-10-18T06:00:00Z",
        "expires_at": "2026-10-18T08:00:00Z",
    }
    s3.put_object(Bucket=BUCKET, Key=LOCK_KEY, Body=json.dumps(lock))
    record = export()
    assert record.status == "succeeded"
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix=LOCK_KEY)["KeyCount"] == 0


def advancing_pages(monkeypatch, minutes: int, at_page=None):
    """Each page the exporter fetches moves the clock `minutes` on; `at_page(n, now)` runs as
    page n is fetched, as if from another process."""
    state = {"now": NOW, "page": 0}
    real_page = Exporter._page

    def page(self, after_id, *, limit):
        state["page"] += 1
        state["now"] += timedelta(minutes=minutes)
        if at_page is not None:
            at_page(state["page"], state["now"])
        return real_page(self, after_id, limit=limit)

    monkeypatch.setattr(Exporter, "_page", page)
    return lambda: state["now"]


@pytest.mark.req("REQ-EXP-006")
def test_a_long_run_renews_its_lock_so_a_second_run_is_refused(export, s3, monkeypatch):
    """DEF-048: the lock was taken once and never renewed, so a run longer than its time left the
    bucket to the next export. Pages 25 minutes apart: by page 6 the run has lasted 2½ hours,
    past any lock time, and a second export must still be refused."""
    refused = []

    def second_export(n, now):
        if n == 6:
            with pytest.raises(LockHeld, match="has been in progress"):
                export(clock=lambda: now)
            refused.append(now)

    clock = advancing_pages(monkeypatch, 25, second_export)
    record = export(clock=clock, page_size=1)
    assert record.status == "succeeded" and refused == [NOW + timedelta(minutes=150)]
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix=LOCK_KEY)["KeyCount"] == 0  # released


@pytest.mark.req("REQ-EXP-006")
def test_a_run_whose_lock_was_taken_over_stops_writing(export, s3, monkeypatch):
    other = {"run_id": "other", "started_at": "", "expires_at": "2026-10-19T00:00:00Z"}

    def take_over(n, now):
        if n == 3:
            s3.put_object(Bucket=BUCKET, Key=LOCK_KEY, Body=json.dumps(other))

    clock = advancing_pages(monkeypatch, 10, take_over)
    record = export(clock=clock, page_size=1)
    assert record.status == "failed"
    assert any("LockLost" in e and "no longer holds" in e for e in record.errors)
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="earthquakes/")["KeyCount"] == 2  # pages 1, 2
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="analytics/")["KeyCount"] == 0
    assert json.loads(s3.get_object(Bucket=BUCKET, Key=LOCK_KEY)["Body"].read()) == other


@pytest.mark.req("REQ-EXP-006")
def test_the_lock_is_atomic(s3):
    """Two runs racing for the lock: the conditional write lets exactly one win."""
    bucket = Bucket(s3, BUCKET)
    exporters = [
        Exporter(None, bucket, api_url="", clock=lambda: NOW, local_dir=None) for _ in range(2)
    ]
    records = [r2.RunRecord(f"run-{i}", "", "", BUCKET, None) for i in range(2)]
    exporters[0]._acquire_lock(records[0], NOW)
    with pytest.raises(LockHeld):
        exporters[1]._acquire_lock(records[1], NOW)


@pytest.mark.req("REQ-EXP-007")
def test_a_dry_run_counts_changes_and_writes_nothing(export, api, s3, tmp_path):
    export()
    api.rows[0] = make_row(1, magnitude=3.0)
    api.rows.append(make_row(10))
    record = export(dry_run=True, clock=lambda: NOW + timedelta(minutes=5))
    assert record.status == "succeeded" and record.dry_run
    assert (record.counts.new, record.counts.updated, record.counts.unchanged) == (1, 1, 6)
    assert stored_rows(s3)["us0001"]["magnitude"] == 2.37  # unchanged in the bucket
    assert "us0010" not in stored_rows(s3)
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="runs/")["KeyCount"] == 1  # first run only
    assert len(list((tmp_path / "exports").glob("*.json"))) == 2  # recorded locally


@pytest.mark.req("REQ-EXP-003")
def test_an_unreadable_manifest_only_costs_a_reupload(export, s3, api):
    export()
    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=b"not gzip")
    record = export()
    assert record.status == "succeeded"
    # Without a manifest nothing is known to be stored, so every row counts as new.
    assert (record.counts.updated, record.counts.new) == (0, 7)
    assert len(stored_rows(s3)) == 7
    manifest = json.loads(
        gzip.decompress(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
    )
    assert sorted(manifest["rows"]) == [r["key"] for r in api.rows]


@pytest.mark.req("REQ-EXP-005")
def test_an_api_failure_fails_the_run_with_the_reason(bucket, tmp_path, s3):
    api = httpx.Client(
        base_url="http://api.test/api/v1/",
        transport=httpx.MockTransport(lambda r: httpx.Response(503, text="database unavailable")),
    )
    record = Exporter(api, bucket, api_url="http://api.test/api/v1", local_dir=None).run()
    assert record.status == "failed"
    assert "returned 503: database unavailable" in record.errors[0]
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix=LOCK_KEY)["KeyCount"] == 0


@pytest.mark.req("REQ-EXP-002")
def test_pages_are_read_once_each(export, api):
    record = export(page_size=2)
    assert record.counts.read == 7
    assert api.pages == 4


@pytest.mark.req("REQ-EXP-007")
def test_missing_settings_stop_the_command(monkeypatch, capsys):
    # _env_file=None: the developer's .env may hold real R2 settings (DEF-029).
    settings = Settings(_env_file=None, postgres_db="x", postgres_user="x", postgres_password="x")
    monkeypatch.setattr("earth_hub.config.get_settings", lambda: settings)
    assert r2.main([]) == 2
    assert "set R2_BUCKET, R2_ACCESS_KEY_ID" in capsys.readouterr().err


@pytest.mark.req("REQ-EXP-012")
def test_rows_are_named_by_key_and_published_without_id():
    assert row_key("us7000abcd") == "earthquakes/us7000abcd.json"
    assert object_row("earthquakes/us7000abcd.json") == "us7000abcd"
    assert object_row("earthquakes/README.txt") is None
    assert object_row("earthquakes/us/1.json") is None
    assert published({"id": 7, "key": "us1", "magnitude": 2.0}) == {"key": "us1", "magnitude": 2.0}


@pytest.mark.req("REQ-EXP-012")
def test_a_bucket_named_by_id_is_renamed_by_the_next_run(export, api, s3):
    # The layout before keys: objects earthquakes/<id>.json, a manifest keyed by id.
    for row in api.rows:
        s3.put_object(Bucket=BUCKET, Key=f"earthquakes/{row['id']}.json", Body=encode(row))
    old = {str(row["id"]): "0" * 64 for row in api.rows}
    s3.put_object(
        Bucket=BUCKET,
        Key=MANIFEST_KEY,
        Body=gzip.compress(json.dumps({"table": "earthquakes", "rows": old}).encode()),
    )
    record = export()
    assert record.status == "succeeded"
    assert (record.counts.new, record.counts.deleted) == (7, 7)  # every row moves to its key
    assert stored_rows(s3) == {row["key"]: published(row) for row in api.rows}
