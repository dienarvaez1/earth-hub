"""make reconcile-direct: each invariant of direct mode's bucket, intact and broken."""

import gzip
import json
from datetime import UTC, datetime, timedelta

import pytest
from support.export import BUCKET

from earth_hub.direct.reconcile import main, reconcile
from earth_hub.direct.runner import DirectRunner
from earth_hub.direct.store import IDS_KEY, PENDING_KEY
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import MANIFEST_KEY, row_key

NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)
UPDATED = 1792137700000


@pytest.fixture
def bucket(s3, tmp_path, make_envelope, make_feature):
    """A bucket after three rounds, with a merge."""
    runner = DirectRunner(Bucket(s3, BUCKET), workdir=tmp_path, workers=2)

    def f(event_id, ids=None, **props):
        return make_envelope(make_feature(event_id, ids, **{"updated": UPDATED, **props}))

    runner.process([f("ak1"), f("us2"), f("ci3")], now=NOW)
    runner.process([f("us2", ["us2", "ak1"], updated=UPDATED + 9)], now=NOW + timedelta(minutes=1))
    runner.process([f("nc4"), b"not json"], now=NOW + timedelta(minutes=2))
    return Bucket(s3, BUCKET)


def edit_gz(s3, key, change):
    body = json.loads(gzip.decompress(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()))
    change(body)
    s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(json.dumps(body).encode()))


@pytest.mark.req("REQ-MODE-006")
def test_an_intact_bucket_reconciles(bucket):
    report = reconcile(bucket, deep=True)
    assert report.ok, report.broken
    assert (report.rows, report.ids, report.last_seq, report.log_objects) == (3, 4, 6, 3)


def remove_row(s3):
    s3.delete_object(Bucket=BUCKET, Key=row_key("ci3"))


def stray_row(s3):
    s3.put_object(Bucket=BUCKET, Key=row_key("zz9"), Body=b"{}")


def log_gap(s3):
    s3.delete_object(Bucket=BUCKET, Key="log/2026-10-16/000000000004-000000000004.ndjson")


def id_to_nowhere(s3):
    edit_gz(s3, IDS_KEY, lambda b: b["ids"].update(xx1=["gone", "2026-10-16T09:00:00Z"]))


def key_moved(s3):
    edit_gz(s3, IDS_KEY, lambda b: b["ids"].update(ci3=["nc4", "2026-10-16T09:00:00Z"]))


def manifest_wrong(s3):
    edit_gz(s3, MANIFEST_KEY, lambda b: b["rows"].update(ci3="0" * 64))


def unfinished(s3):
    s3.put_object(Bucket=BUCKET, Key=PENDING_KEY, Body=b'{"run_id": "x"}')


@pytest.mark.req("REQ-MODE-006")
@pytest.mark.parametrize(
    ("damage", "broken"),
    [
        (remove_row, {"manifest rows without an object"}),
        (stray_row, {"row objects not in the manifest"}),
        (log_gap, {"gaps or overlaps in the log's sequence numbers"}),
        (id_to_nowhere, {"IDs pointing to no record"}),
        (
            key_moved,
            {
                "records whose key does not point to them",
                "records whose preferred ID does not point to them",
                "records with a revision ID that does not point to them",
            },
        ),
        (
            manifest_wrong,
            {
                "snapshot fingerprint differs from the manifest (analytics/meta.json)",
                "snapshot rows that differ from their manifest fingerprint",
            },
        ),
        (unfinished, {"unfinished round (_state/direct-pending.json)"}),
    ],
    ids=lambda v: v.__name__ if callable(v) else "",
)
def test_each_broken_invariant_is_reported(bucket, s3, damage, broken):
    damage(s3)
    report = reconcile(bucket)
    assert set(report.broken) == broken


@pytest.mark.req("REQ-MODE-006")
def test_only_a_deep_check_reads_the_row_objects(bucket, s3):
    s3.put_object(Bucket=BUCKET, Key=row_key("ci3"), Body=b'{"edited": "by hand"}')
    assert reconcile(bucket).ok
    assert set(reconcile(bucket, deep=True).broken) == {
        "row objects that differ from their manifest fingerprint"
    }


@pytest.mark.req("REQ-MODE-006")
def test_the_command_exits_1_when_broken(bucket, s3, monkeypatch, capsys):
    from earth_hub.config import Settings

    settings = Settings(
        _env_file=None,  # never the developer's .env (DEF-029)
        postgres_db="x",
        postgres_user="x",
        postgres_password="x",
        r2_bucket=BUCKET,
    )
    monkeypatch.setattr("earth_hub.config.get_settings", lambda: settings)
    monkeypatch.setattr("earth_hub.export.bucket.make_s3_client", lambda settings: s3)
    assert main([]) == 0
    assert capsys.readouterr().out.endswith("OK\n")
    remove_row(s3)
    assert main([]) == 1
    assert "BROKEN: manifest rows without an object: 1" in capsys.readouterr().out
