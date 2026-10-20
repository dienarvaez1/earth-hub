"""The read-only R2 browser (`make r2-ls`, `r2-count`, `r2-get`), on moto."""

import gzip
import json
from pathlib import Path

import pytest
from support.export import BUCKET

from earth_hub.export import browse

pytestmark = pytest.mark.req("REQ-EXP-011")


@pytest.fixture
def filled(s3):
    for i in (3, 1, 2):
        s3.put_object(Bucket=BUCKET, Key=f"earthquakes/{i}.json", Body=json.dumps({"id": i}))
    s3.put_object(Bucket=BUCKET, Key="_state/manifest.json.gz", Body=gzip.compress(b'{"1": "x"}'))
    s3.put_object(Bucket=BUCKET, Key="analytics/earthquakes.parquet", Body=b"PAR1\xff\x00")
    return s3


@pytest.fixture
def run(filled, monkeypatch, capsys):
    monkeypatch.setattr("earth_hub.export.bucket.make_s3_client", lambda settings: filled)
    monkeypatch.setattr(
        "earth_hub.config.get_settings", lambda: type("S", (), {"r2_bucket": BUCKET})()
    )

    def main(*argv):
        code = browse.main(list(argv))
        out, err = capsys.readouterr()
        return code, out, err

    return main


def test_ls_lists_keys_under_a_prefix_in_order_up_to_the_limit(run):
    code, out, _ = run("ls", "earthquakes/", "--limit", "2")
    assert code == 0
    assert [line.split()[-1] for line in out.splitlines()] == [
        "earthquakes/1.json",
        "earthquakes/2.json",
    ]
    assert len(run("ls", "--limit", "0")[1].splitlines()) == 5


def test_count_counts_objects_and_bytes(run):
    code, out, _ = run("count", "earthquakes/")
    assert code == 0 and out.startswith("3 objects, 27 bytes under earthquakes/")


def test_get_prints_an_object_gunzipping_gz(run):
    assert run("get", "earthquakes/2.json")[1] == '{"id": 2}'
    assert run("get", "_state/manifest.json.gz")[1] == '{"1": "x"}'


def test_get_saves_binary_objects_only_to_a_file(run, tmp_path):
    code, _, err = run("get", "analytics/earthquakes.parquet")
    assert code == 1 and "--out" in err
    out = tmp_path / "snapshot.parquet"
    assert run("get", "analytics/earthquakes.parquet", "--out", str(out))[0] == 0
    assert out.read_bytes() == b"PAR1\xff\x00"


def test_a_missing_key_is_an_error(run):
    code, _, err = run("get", "earthquakes/99.json")
    assert code == 1 and "no object" in err


def test_it_never_writes(filled, run):
    before = {o["Key"]: o["ETag"] for o in filled.list_objects_v2(Bucket=BUCKET)["Contents"]}
    for argv in (("ls",), ("count",), ("get", "earthquakes/1.json")):
        run(*argv)
    after = {o["Key"]: o["ETag"] for o in filled.list_objects_v2(Bucket=BUCKET)["Contents"]}
    assert after == before
    source = Path(browse.__file__).read_text()
    assert not any(call in source for call in ("put_object", "delete_object", "copy_object"))
