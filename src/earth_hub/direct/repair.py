"""Row objects written again from the snapshot: `python -m earth_hub.direct.repair [--dry-run]`
(make repair-rows [DRY_RUN=1]).

A writer paused inside a write past its lock's expiry can leave a row object older than the
manifest says (DEF-058: the writes in flight when it paused). Nothing in the pipeline reads row
objects back (rounds load records from the snapshot, exports compare the manifest), so only the
deep check finds one (`make reconcile-direct DEEP=1`, weekly in .github/workflows/direct.yml).

The snapshot holds every row exactly (REQ-MODE-008). Each row object that is missing or differs
from its manifest fingerprint is written again from it, and only when the row rebuilt from the
snapshot has that fingerprint. It holds the bucket's lock, in either mode, never during a
handover, and refuses a snapshot that does not describe the manifest (an export that failed).
"""

import argparse
import secrets
import sys
import tempfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from earth_hub.direct.store import BucketState, Inconsistent, iso, record_of, row_body
from earth_hub.export import lock, mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import fingerprint, row_key
from earth_hub.export.snapshot import content_hash

# Reading every row object takes minutes; the lock is renewed while it lasts (lock.Held).
LOCK_TTL = timedelta(minutes=15)


@dataclass
class Repair:
    rows: int = 0
    differing: list[str] = field(default_factory=list)  # missing or not their fingerprint
    repaired: int = 0
    # The snapshot's row does not give the manifest's fingerprint: not written.
    unrepairable: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.unrepairable


def repair(
    bucket: Bucket,
    *,
    dry_run: bool = False,
    workers: int = 16,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Repair:
    now = clock()
    run_id = f"repair-{now:%Y-%m-%dT%H-%M-%SZ}-{secrets.token_hex(2)}"
    held = None if dry_run else lock.Held(bucket, run_id, iso(now), now, LOCK_TTL, clock)
    try:
        current = mode.read(bucket)
        if current.state != mode.ACTIVE:
            raise mode.WrongMode(f"the bucket is {current.state}: a handover is in progress")
        with tempfile.TemporaryDirectory() as tmp:
            return _repair(bucket, BucketState(bucket, Path(tmp)), held, workers)
    finally:
        if held is not None:
            held.release()


def _repair(bucket: Bucket, state: BucketState, held: lock.Held | None, workers: int) -> Repair:
    state.refresh()
    report = Repair(rows=len(state.manifest))
    if not state.manifest:
        return report
    if (
        state.meta is None
        or not state.has_snapshot
        or state.meta.get("content_sha256") != content_hash(state.manifest)
    ):
        raise Inconsistent("the snapshot does not describe the manifest: run an export first")

    def differs(key: str) -> bool:
        found = bucket.get(row_key(key))
        return found is None or fingerprint(found[0]) != state.manifest[key]

    keys = sorted(state.manifest)
    with ThreadPoolExecutor(workers) as pool:
        report.differing = [k for k, bad in zip(keys, pool.map(differs, keys), strict=True) if bad]
    rows = state.rows(report.differing)
    for key in report.differing:
        body = row_body(record_of(rows[key]))
        if fingerprint(body) != state.manifest[key]:
            report.unrepairable.append(key)
            continue
        if held is not None:
            held.confirm()
            bucket.put(row_key(key), body)
        report.repaired += 1
    return report


def main(argv: list[str] | None = None) -> int:
    from earth_hub.config import get_settings
    from earth_hub.export.bucket import MissingSettings, make_s3_client
    from earth_hub.logging_config import configure_logging

    parser = argparse.ArgumentParser(description="Write row objects again from the snapshot.")
    parser.add_argument("--dry-run", action="store_true", help="list them, write nothing")
    args = parser.parse_args(argv)
    configure_logging()
    settings = get_settings()
    try:
        bucket = Bucket(make_s3_client(settings), settings.r2_bucket)
    except MissingSettings as exc:
        print(f"repair-rows: {exc}", file=sys.stderr)
        return 2
    try:
        report = repair(bucket, dry_run=args.dry_run, workers=settings.export_workers)
    except (lock.LockHeld, mode.WrongMode, Inconsistent) as exc:
        print(f"repair-rows: refused: {exc}", file=sys.stderr)
        return 1
    verb = "would write" if args.dry_run else "written"
    print(
        f"rows {report.rows}, differing {len(report.differing)}, {verb} {report.repaired}, "
        f"unrepairable {len(report.unrepairable)}"
    )
    for key in report.differing[:20]:
        print(f"  {key}{'  (unrepairable)' if key in report.unrepairable else ''}")
    print("OK" if report.ok else "FAILED")
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
