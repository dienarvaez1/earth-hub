"""Direct mode takes the bucket over from full mode's host (Milestone 9, step 5).

A direct execution (`python -m earth_hub.direct once|run`) calls `take_over` first. It changes
nothing unless the bucket is in full mode with an owner whose lease has expired: the host
stopped exporting, so it stopped renewing (mode.LEASE_TTL after its last renewal). Then,
holding the bucket's lock (so no export runs meanwhile, and a host that wakes finds the
bucket gone at its next export), it checks that direct mode's state describes the bucket and
switches the marker to direct mode with a conditional write: of two rounds racing, one takes
over, and the other sees direct mode. The round then runs as usual, catch-up included, from the
state the host's last export wrote (direct/standby.py), and fetches again from USGS what the
host stored after that export.

A bucket whose state does not describe it (the host died in the middle of an export: rows and
manifest newer than the snapshot and the state) is first put back to its last complete export
(decided on 2026-10-30): row objects written again from the snapshot or removed, and the
manifest from it. Catch-up then fetches again what the rollback took out. The execution fails
after its round, so GitHub e-mails (`TookOver.rolled_back`). Only a bucket the rollback cannot
make consistent is `Refused`, and every round then fails until a human acts.

`force=True` (/eh-host takeover) does not wait for the lease to expire: for a host known gone.
"""

import logging
import secrets
import tempfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from earth_hub.direct.store import BucketState, Inconsistent, iso, record_of, row_body
from earth_hub.export import lock, mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import MANIFEST_KEY, TABLE_PREFIX, fingerprint, manifest_body, object_row
from earth_hub.export.r2 import row_key as object_key
from earth_hub.export.snapshot import content_hash

logger = logging.getLogger(__name__)

LOCK_TTL = timedelta(minutes=15)


class Refused(Exception):
    """The bucket cannot be taken over as it is: nothing was written."""


@dataclass
class TookOver:
    previous_owner: str | None
    lease_expires_at: str | None
    rolled_back: int = 0  # row objects put back to the last complete export


def take_over(
    bucket: Bucket,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    force: bool = False,
    workers: int = 16,
) -> TookOver | None:
    """Take the bucket into direct mode if its owner's lease has expired (or `force`); None
    when there is nothing to take over. Raises LockHeld (an export or round in progress: next
    time) or Refused."""
    current = mode.read(bucket)
    if not _expired(current, clock(), force=force):
        return None
    now = clock()
    run_id = f"takeover-{now:%Y-%m-%dT%H-%M-%SZ}-{secrets.token_hex(2)}"
    held = lock.Held(bucket, run_id, iso(now), now, LOCK_TTL, clock)
    try:
        current = mode.read(bucket)  # again, now that no export can run
        if not _expired(current, clock(), force=force):
            return None
        with tempfile.TemporaryDirectory() as tmp:
            state = BucketState(bucket, Path(tmp), workers=workers)
            state.fence = held.confirm
            state.refresh()
            rolled_back = _check(bucket, state, held, workers=workers)
            held.confirm()
            mode.write(
                bucket,
                current,
                mode=mode.DIRECT,
                state=mode.ACTIVE,
                since=iso(now),
                checkpoint_seq=state.status["seq"],
                replay_from=None,
                owner=None,
                lease_expires_at=None,
            )
    finally:
        held.release()
    logger.warning(
        "took the bucket over from full mode's host",
        extra={
            "previous_owner": current.owner,
            "lease_expires_at": current.lease_expires_at,
            "rolled_back": rolled_back,
        },
    )
    return TookOver(current.owner, current.lease_expires_at, rolled_back)


def _expired(current: mode.Mode, now: datetime, *, force: bool) -> bool:
    if (current.mode, current.state) != (mode.FULL, mode.ACTIVE):
        return False
    if current.owner is None:
        # No host has kept a standby state here (a bucket from before Milestone 9): direct
        # mode would build on nothing. Only a handover (make mode-direct) switches it.
        return False
    return force or not current.lease_live(now)


def _check(bucket: Bucket, state: BucketState, held: lock.Held, *, workers: int) -> int:
    """Direct mode's state must describe the bucket; one left by an export that did not
    finish is first put back to the snapshot. Returns the row objects rewritten or removed."""
    if state.pending is not None:
        raise Refused("the bucket holds an unfinished direct round")
    try:
        state.check()
        return 0
    except Inconsistent as exc:
        logger.warning("an export did not finish (%s): rolling back to the last complete one", exc)
    changed = _roll_back(bucket, state, held, workers)
    state.refresh()
    try:
        state.check()
    except Inconsistent as exc:
        raise Refused(f"still inconsistent after the rollback: {exc}") from None
    return changed


def _roll_back(bucket: Bucket, state: BucketState, held: lock.Held, workers: int) -> int:
    """The bucket as the snapshot describes it: the row objects (written again from the
    snapshot where they differ, removed where it lacks them) and the manifest. The snapshot,
    its meta.json and the ID map are the last complete export's already."""
    if state.meta is None or not state.has_snapshot:
        raise Refused("no snapshot to roll back to")
    keys = sorted(state.order)
    rows = state.rows(keys, strict=False)
    if set(rows) != set(keys):
        raise Refused("the snapshot and the ID map list different records")
    manifest = {}
    bodies = {}
    for key, row in rows.items():
        body = row_body(record_of(row))
        bodies[key] = body
        manifest[key] = fingerprint(body)
    if state.meta.get("content_sha256") != content_hash(manifest):
        raise Refused("the snapshot does not match its meta.json")
    stored = {k for name in bucket.keys(TABLE_PREFIX) if (k := object_row(name)) is not None}

    def differs(key: str) -> bool:
        found = bucket.get(object_key(key))
        return found is None or fingerprint(found[0]) != manifest[key]

    with ThreadPoolExecutor(workers) as pool:
        rewrite = [k for k, bad in zip(keys, pool.map(differs, keys), strict=True) if bad]
    for key in rewrite:
        held.confirm()
        bucket.put(object_key(key), bodies[key])
    extra = sorted(stored - set(manifest))
    if extra:
        held.confirm()
        bucket.delete(object_key(key) for key in extra)
    held.confirm()
    bucket.put(
        MANIFEST_KEY,
        manifest_body(manifest, held.run_id, iso(held.clock())),
        content_type="application/gzip",
    )
    logger.warning(
        "rolled the bucket back to its last complete export",
        extra={"rewritten": len(rewrite), "removed": len(extra)},
    )
    return len(rewrite) + len(extra)
