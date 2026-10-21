"""The bucket's one lock, `_state/lock.json`: whoever holds it is the only writer. The full-mode
exporter takes it for a run, the direct runner for each round that writes (Milestone 8), so the
two never write at once.

Taken with a conditional write (If-None-Match), which R2 evaluates atomically. A lock left by a
process that crashed expires; it is taken over only if it is still that lock (If-Match). A run
longer than the lock's time renews it (`renew`, also If-Match), so a long export keeps the bucket
for as long as it runs (DEF-048).
"""

import json
import logging
from datetime import UTC, datetime, timedelta

from earth_hub.export.bucket import Bucket, PreconditionFailed

logger = logging.getLogger(__name__)

LOCK_KEY = "_state/lock.json"


class LockHeld(Exception):
    """Another run is in progress."""


class LockLost(Exception):
    """This run's lock was taken over or removed: it must write nothing more."""


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _body(run_id: str, started_at: str, expires: datetime) -> bytes:
    return json.dumps(
        {"run_id": run_id, "started_at": started_at, "expires_at": _iso(expires)}
    ).encode()


def acquire(bucket: Bucket, run_id: str, started_at: str, now: datetime, ttl: timedelta) -> str:
    """Take the lock for `run_id`, or raise LockHeld before anything is written. Returns the
    lock's ETag, for `renew`."""
    body = _body(run_id, started_at, now + ttl)
    try:
        return bucket.put(LOCK_KEY, body, if_none_match=True)
    except PreconditionFailed:
        pass
    current = bucket.get(LOCK_KEY)
    if current is None:  # released in between
        try:
            return bucket.put(LOCK_KEY, body, if_none_match=True)
        except PreconditionFailed:
            raise LockHeld("another run took the lock") from None
    holder = json.loads(current[0])
    if datetime.fromisoformat(holder["expires_at"]) > now:
        raise LockHeld(f"run {holder['run_id']} has been in progress since {holder['started_at']}")
    logger.warning("taking over the expired lock of run %s", holder["run_id"])
    try:
        return bucket.put(LOCK_KEY, body, if_match=current[1])  # only if still that lock
    except PreconditionFailed:
        raise LockHeld("another run took the expired lock") from None


def renew(
    bucket: Bucket, run_id: str, started_at: str, etag: str, now: datetime, ttl: timedelta
) -> str:
    """Move this run's lock to expire `ttl` from `now`, only if it is still the lock written with
    `etag`; raises LockLost if it is not. Returns the new ETag."""
    try:
        return bucket.put(LOCK_KEY, _body(run_id, started_at, now + ttl), if_match=etag)
    except PreconditionFailed:
        raise LockLost(f"run {run_id} no longer holds the bucket's lock") from None


def release(bucket: Bucket, run_id: str, ttl: timedelta) -> None:
    """Remove the lock if `run_id` still holds it; a failure only leaves it to expire."""
    try:
        current = bucket.get(LOCK_KEY)
        if current is not None and json.loads(current[0])["run_id"] == run_id:
            bucket.delete([LOCK_KEY])
    except Exception:
        logger.exception("could not release the lock; it expires after %s", ttl)


def live_holder(bucket: Bucket, now: datetime) -> str | None:
    """The run that holds an unexpired lock, if any."""
    lock = bucket.get(LOCK_KEY)
    if lock is None:
        return None
    holder = json.loads(lock[0])
    return holder["run_id"] if datetime.fromisoformat(holder["expires_at"]) > now else None
