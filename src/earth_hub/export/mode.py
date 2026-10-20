"""Which mode writes the bucket: `_state/mode.json` (Milestone 8).

The one setting that chooses the mode. It lives in the bucket rather than in each machine's
.env, so every process that could write the bucket reads the same answer, and two machines
can never each believe they are in charge. Full mode's exporter writes only while it says
`full`; the direct runner only while it says `direct`. The handovers (direct/handover.py)
change it, with conditional writes, through `handing-over`: then neither writes.

No marker at all means full mode, as every bucket was before direct mode existed.
"""

import json
from dataclasses import asdict, dataclass, replace

from earth_hub.export.bucket import Bucket

MODE_KEY = "_state/mode.json"
FULL = "full"
DIRECT = "direct"
ACTIVE = "active"
HANDING_OVER = "handing-over"


class WrongMode(Exception):
    """The bucket is not in the mode this process writes in."""


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
    etag: str | None = None

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
        state = current.mode if current.state == ACTIVE else f"{current.mode}, {current.state}"
        raise WrongMode(f"the bucket is in {state} mode, not {wanted} (make mode)")
    return current
