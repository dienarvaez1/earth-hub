"""Which mode writes the bucket: `_state/mode.json` (Milestone 8).

The one setting that chooses the mode. It lives in the bucket rather than in each machine's
.env, so every process that could write the bucket reads the same answer, and two machines
can never each believe they are in charge. Full mode's exporter writes only while it says
`full`; the direct runner only while it says `direct`. The handovers (direct/handover.py)
change it, with conditional writes, through `handing-over`: then neither writes.

No marker at all means full mode, as every bucket was before direct mode existed.

In full mode it also names the host that writes (Milestone 9): `owner`, a machine ID, and
`lease_expires_at`, which the owner's exports renew (`hold`). Only the owner exports; a full-mode
export that finds no owner (a bucket from before) takes it. In direct mode there is no owner.
"""

import json
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta

from earth_hub.export.bucket import Bucket

MODE_KEY = "_state/mode.json"
FULL = "full"
DIRECT = "direct"
ACTIVE = "active"
HANDING_OVER = "handing-over"


# The owner renews its lease at most every LEASE_RENEW, so a host alive and exporting keeps it
# well inside LEASE_TTL; one that stopped loses it LEASE_TTL after its last renewal.
LEASE_TTL = timedelta(minutes=20)
LEASE_RENEW = timedelta(minutes=5)


class WrongMode(Exception):
    """The bucket is not in the mode this process writes in."""


class NotOwner(WrongMode):
    """Full mode, but another host owns the bucket."""


@dataclass(frozen=True)
class Mode:
    mode: str = FULL
    state: str = ACTIVE
    since: str | None = None
    # Direct mode's log sequence number when the mode last changed: in direct mode, the
    # entries after it are what full mode has not seen.
    checkpoint_seq: int = 0
    # The backfill topic's end offset (partition 0) when a replay to full mode began, so a
    # replay started again after a crash finds what it already sent.
    replay_from: int | None = None
    # Full mode's host and when its lease ends (ISO 8601, UTC); None in direct mode.
    owner: str | None = None
    lease_expires_at: str | None = None
    etag: str | None = None

    def lease_live(self, now: datetime) -> bool:
        return (
            self.lease_expires_at is not None
            and datetime.fromisoformat(self.lease_expires_at) > now
        )

    def describe(self) -> str:
        """`full`, `direct (handing-over)`, `full (owner a, lease until …)`: for messages."""
        text = self.mode if self.state == ACTIVE else f"{self.mode}, {self.state}"
        if self.owner is not None:
            text += f", owner {self.owner}, lease until {self.lease_expires_at}"
        return text

    def body(self) -> bytes:
        fields = {k: v for k, v in asdict(self).items() if k != "etag"}
        return json.dumps(fields, indent=1).encode()


def read(bucket: Bucket) -> Mode:
    found = bucket.get(MODE_KEY)
    if found is None:
        return Mode()
    fields = json.loads(found[0])
    return Mode(**{k: fields[k] for k in fields if k in Mode.__dataclass_fields__}, etag=found[1])


def write(bucket: Bucket, current: Mode, **changes) -> Mode:
    """Change the marker only if it is still `current` (raises PreconditionFailed if not)."""
    new = replace(current, **changes)
    if current.etag is None:
        etag = bucket.put(MODE_KEY, new.body(), if_none_match=True)
    else:
        etag = bucket.put(MODE_KEY, new.body(), if_match=current.etag)
    return replace(new, etag=etag)


def require(bucket: Bucket, wanted: str) -> Mode:
    current = read(bucket)
    if (current.mode, current.state) != (wanted, ACTIVE):
        raise WrongMode(f"the bucket is in {current.describe()} mode, not {wanted} (make mode)")
    return current


def iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def hold(bucket: Bucket, current: Mode, machine: str, now: datetime) -> Mode:
    """Full mode's write permission for `machine`, from the marker `current` (read under the
    bucket's lock): NotOwner if another host owns the bucket; otherwise the lease is taken (no
    owner yet) or renewed once LEASE_RENEW has passed since the last renewal."""
    if current.owner is not None and current.owner != machine:
        raise NotOwner(
            f"the bucket is owned by {current.owner} (lease until {current.lease_expires_at}),"
            f" not {machine}: make to-full on this host to take it over"
        )
    if current.owner == machine and current.lease_expires_at is not None:
        renewed = datetime.fromisoformat(current.lease_expires_at) - LEASE_TTL
        if now - renewed < LEASE_RENEW:
            return current
    return write(bucket, current, owner=machine, lease_expires_at=iso(now + LEASE_TTL))
