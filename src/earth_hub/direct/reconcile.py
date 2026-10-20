"""Direct mode's reconciliation: `python -m earth_hub.direct.reconcile [--deep]`
(make reconcile-direct [DEEP=1]).

What `make reconcile` checks in PostgreSQL, checked in the bucket. It reads the state objects
and the snapshot, and lists the row objects and the log (one request per 1,000 objects); it
never writes. Each check counts what breaks an invariant; all must be zero, or it exits 1:

- the row objects, the manifest, the snapshot and the ID map's records are the same keys, and
  the snapshot's rows give exactly the manifest's fingerprints (so they are the objects' rows);
- every record's key, preferred ID and revision IDs point to it in the ID map, and every ID
  points to a record;
- the log's sequence numbers run from 1 to the last one saved, without gaps or overlaps;
- no round is left unfinished.

With --deep it also reads every row object (one request each: 115k requests for a full
bucket) and checks it against its manifest fingerprint.
"""

import argparse
import re
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from earth_hub.direct.store import LOG_PREFIX, BucketState, record_of, row_body
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import TABLE_PREFIX, fingerprint, object_row, row_key
from earth_hub.export.snapshot import content_hash

_LOG_NAME = re.compile(r"/(\d+)-(\d+)\.ndjson$")


@dataclass
class Report:
    rows: int = 0
    ids: int = 0
    last_seq: int = 0
    log_objects: int = 0
    deep: bool = False
    broken: dict[str, int] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.broken

    def check(self, name: str, count: int) -> None:
        if count:
            self.broken[name] = count


def reconcile(bucket: Bucket, *, deep: bool = False, workers: int = 16) -> Report:
    with tempfile.TemporaryDirectory(prefix="earth-hub-reconcile-") as tmp:
        state = BucketState(bucket, Path(tmp))
        state.refresh()
        return _check(bucket, state, deep=deep, workers=workers)


def _check(bucket: Bucket, state: BucketState, *, deep: bool, workers: int) -> Report:
    report = Report(rows=len(state.manifest), ids=len(state.ids), deep=deep)
    report.last_seq = state.status["seq"]
    report.check("unfinished round (_state/direct-pending.json)", int(state.pending is not None))

    manifest = set(state.manifest)
    objects = {k for name in bucket.keys(TABLE_PREFIX) if (k := object_row(name)) is not None}
    report.check("row objects not in the manifest", len(objects - manifest))
    report.check("manifest rows without an object", len(manifest - objects))
    report.check("records in the ID map but not the manifest", len(set(state.order) - manifest))
    report.check("manifest rows not in the ID map", len(manifest - set(state.order)))

    rows = state.rows(manifest & set(state.order), strict=False)
    stale = bool(manifest or state.meta) and (state.meta or {}).get("content_sha256") != (
        content_hash(state.manifest)
    )
    report.check("manifest rows not in the snapshot", len(manifest - set(rows)))
    report.check(
        "snapshot fingerprint differs from the manifest (analytics/meta.json)",
        int(stale),
    )
    report.check(
        "snapshot rows that differ from their manifest fingerprint",
        sum(fingerprint(row_body(record_of(r))) != state.manifest[k] for k, r in rows.items()),
    )

    owner = {i: key for i, (key, _) in state.ids.items()}
    report.check("IDs pointing to no record", sum(key not in manifest for key in owner.values()))
    report.check("records whose key does not point to them", sum(owner.get(k) != k for k in rows))
    report.check(
        "records whose preferred ID does not point to them",
        sum(owner.get(r["preferred_event_id"]) != k for k, r in rows.items()),
    )
    report.check(
        "records with a revision ID that does not point to them",
        sum(any(owner.get(i) != k for i in r["revision_ids"]) for k, r in rows.items()),
    )

    ranges = sorted(
        (int(m.group(1)), int(m.group(2)))
        for name in bucket.keys(LOG_PREFIX)
        if (m := _LOG_NAME.search(name))
    )
    report.log_objects = len(ranges)
    expected, gaps = 1, 0
    for first, last in ranges:
        gaps += first != expected
        expected = last + 1
    gaps += expected != report.last_seq + 1
    report.check("gaps or overlaps in the log's sequence numbers", gaps)

    if deep:

        def differs(key: str) -> bool:
            found = bucket.get(row_key(key))
            return found is None or fingerprint(found[0]) != state.manifest[key]

        with ThreadPoolExecutor(workers) as pool:
            report.check(
                "row objects that differ from their manifest fingerprint",
                sum(pool.map(differs, sorted(manifest & objects))),
            )
    return report


def main(argv: list[str] | None = None) -> int:
    from earth_hub.config import get_settings
    from earth_hub.export.bucket import MissingSettings, make_s3_client
    from earth_hub.logging_config import configure_logging

    parser = argparse.ArgumentParser(description="Check direct mode's state in the bucket.")
    parser.add_argument("--deep", action="store_true", help="also read every row object")
    args = parser.parse_args(argv)
    configure_logging()
    settings = get_settings()
    try:
        bucket = Bucket(make_s3_client(settings), settings.r2_bucket)
    except MissingSettings as exc:
        print(f"reconcile-direct: {exc}", file=sys.stderr)
        return 2
    report = reconcile(bucket, deep=args.deep, workers=settings.export_workers)
    print(
        f"rows {report.rows}, IDs {report.ids}, log {report.log_objects} objects up to "
        f"sequence {report.last_seq}{', row objects read' if report.deep else ''}"
    )
    for name, count in report.broken.items():
        print(f"  BROKEN: {name}: {count}")
    print("OK" if report.ok else "FAILED")
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
