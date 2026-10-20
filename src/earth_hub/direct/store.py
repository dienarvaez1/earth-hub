"""Direct mode's state in the bucket, and its EventStore (processing/ledger.py).

Direct mode keeps what full mode keeps in PostgreSQL and Kafka in the R2 bucket, beside what
the exporter writes (export/r2.py) and in the same formats, so a bucket is the same whichever
mode wrote it:

    earthquakes/<key>.json                 the rows, as the exporter writes them
    _state/earthquakes-manifest.json.gz    each row's fingerprint, as the exporter keeps it
    analytics/earthquakes.parquet          the snapshot, and analytics/meta.json, updated in
                                           every round that changes rows
    _state/ids.json.gz                     the ID map: every USGS ID ever seen -> its record's
                                           key and when the ID was first seen; and each
                                           record's creation number (full mode's id order,
                                           which decides the survivor of a merge)
    _state/direct.json                     the last sequence number, the poller's 26-hour
                                           state and the catch-up checkpoints
    _state/direct-pending.json             a round's writes, while they are being made
    log/<date>/<first>-<last>.ndjson       every message processed, in order: sequence,
                                           time, source, outcome and envelope (raw_events)

A round loads the records its messages touch from the snapshot, which holds every column of
every row and is current: it is updated in the same round as the rows. Loading costs no
request per record, and the snapshot is read only when another round or run changed it.

Writes are made in a fixed order, behind a redo record. The round's writes are first saved
whole to `_state/direct-pending.json` (only if no other is there); then the rows, the ID
map, the manifest, the snapshot, the log and the state are written; then the record is
removed. Each step can be repeated with the same result, so a round interrupted anywhere is
finished by the next one, which applies a pending record before anything else.
"""

import gzip
import hashlib
import json
import logging
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from earth_hub.api.schemas import EarthquakeRow
from earth_hub.export.bucket import Bucket, PreconditionFailed
from earth_hub.export.r2 import (
    MANIFEST_KEY,
    encode,
    fingerprint,
    manifest_body,
    parse_manifest,
    published,
    row_key,
)
from earth_hub.export.snapshot import (
    COLUMNS,
    META_KEY,
    PARQUET_TYPE,
    SNAPSHOT_KEY,
    content_hash,
    snapshot_meta,
    updated_snapshot,
)
from earth_hub.processing.ledger import Key, Ledger, NewKey, Record

logger = logging.getLogger(__name__)

STATE_KEY = "_state/direct.json"
IDS_KEY = "_state/ids.json.gz"
PENDING_KEY = "_state/direct-pending.json"
LOG_PREFIX = "log/"
NDJSON = "application/x-ndjson"

# The columns a revision sets (ValidEvent.fields), as in database/repository.py.
EVENT_COLUMNS = tuple(
    c
    for c in COLUMNS
    if c not in ("key", "preferred_event_id", "revision_count", "first_seen_at", "last_applied_at")
)


class Inconsistent(Exception):
    """The bucket's state cannot be used: direct mode refuses to write to it."""


def iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def log_key(first: int, last: int, when: datetime) -> str:
    """A round's log object: named by its sequence numbers, so a listing shows any gap."""
    return f"{LOG_PREFIX}{when:%Y-%m-%d}/{first:012d}-{last:012d}.ndjson"


def record_of(row: dict) -> Record:
    """A published row (from the snapshot) as the ledger's record."""
    return Record(
        key=row["key"],
        preferred_event_id=row["preferred_event_id"],
        source_updated_ms=row["source_updated_ms"],
        deleted=row["review_status"] == "deleted",
        revision_ids=tuple(row["revision_ids"]),
        revision_count=row["revision_count"],
        first_seen_at=row["first_seen_at"],
        last_applied_at=row["last_applied_at"],
        columns={c: row[c] for c in EVENT_COLUMNS},
    )


def row_body(record: Record) -> bytes:
    """The row object of a record, byte for byte what the exporter uploads for the same row:
    the API's response model makes the JSON, as it does for the export endpoint."""
    row = EarthquakeRow.model_validate(
        {
            "id": 0,  # not published
            "key": record.key,
            **record.columns,
            "preferred_event_id": record.preferred_event_id,
            "revision_count": record.revision_count,
            "first_seen_at": record.first_seen_at,
            "last_applied_at": record.last_applied_at,
        }
    ).model_dump(mode="json")
    return encode(published(row))


def empty_status() -> dict:
    return {"seq": 0, "run_id": None, "written_at": None, "sent": {}, "checkpoints": {}}


@dataclass
class Changes:
    """What a round writes, found by BucketStore.write."""

    rows: dict[str, bytes] = field(default_factory=dict)  # key -> row object
    removed: set[str] = field(default_factory=set)  # keys of records merged away
    ids: dict[str, list[str]] = field(default_factory=dict)  # USGS ID -> [key, first seen]
    order: dict[str, int] = field(default_factory=dict)  # new keys -> creation number
    next: int = 1


class BucketState:
    """The bucket's direct-mode state, cached between rounds. `refresh` reads only what
    changed since (conditional requests); `apply` makes a round's writes."""

    def __init__(self, bucket: Bucket, workdir: Path, *, workers: int = 16):
        self.bucket = bucket
        self.workers = workers
        self.snapshot_path = workdir / "earthquakes.parquet"
        self._etags: dict[str, str | None] = {}
        self.ids: dict[str, tuple[str, str]] = {}  # USGS ID -> (key, first seen)
        self.ids_by_key: dict[str, set[str]] = {}
        self.order: dict[str, int] = {}  # key -> creation number
        self.next = 1
        self.manifest: dict[str, str] = {}
        self.meta: dict | None = None
        self.status = empty_status()
        self.pending: dict | None = None
        self.has_snapshot = False

    # Reading.

    def refresh(self) -> None:
        """Bring the cache up to date with the bucket. Reads only: `check` says whether the
        state can be used, and a pending round is the caller's to finish."""
        if (found := self._fetch(STATE_KEY)) is not None:
            self.status = json.loads(found) if found else empty_status()
        if (found := self._fetch(IDS_KEY)) is not None:
            self._set_ids(json.loads(gzip.decompress(found)) if found else None)
        if (found := self._fetch(MANIFEST_KEY)) is not None:
            self.manifest = parse_manifest(found) if found else {}
        if (found := self._fetch(META_KEY)) is not None:
            self.meta = json.loads(found) if found else None
        if (found := self._fetch(SNAPSHOT_KEY)) is not None:
            self.has_snapshot = bool(found)
            if found:
                self.snapshot_path.write_bytes(found)
        if (found := self._fetch(PENDING_KEY)) is not None:
            self.pending = json.loads(found) if found else None

    def check(self) -> None:
        """Refuse a bucket whose parts disagree: direct mode would build on wrong records."""
        if not self.manifest and self.meta is None and not self.order:
            return  # an empty bucket
        if self.meta is None or not self.has_snapshot:
            raise Inconsistent("no snapshot: run a full export first")
        if self.meta.get("content_sha256") != content_hash(self.manifest):
            raise Inconsistent(
                "the snapshot does not describe the rows in the manifest (did the last export "
                "fail?): run a full export first"
            )
        digest = hashlib.sha256(self.snapshot_path.read_bytes()).hexdigest()
        if self.meta.get("sha256") != digest:
            raise Inconsistent("analytics/meta.json does not describe the snapshot file")
        if set(self.order) != set(self.manifest):
            raise Inconsistent("the ID map and the manifest list different records")

    def rows(self, keys: Iterable[str], *, strict: bool = True) -> dict[str, dict]:
        """Rows by key, every column, from the snapshot. `strict`: a key it lacks is an
        Inconsistent bucket; otherwise it is left out."""
        keys = sorted(set(keys))
        if not keys or not self.has_snapshot:
            return {}
        con = duckdb.connect()
        try:
            con.execute("SET TimeZone = 'UTC'")
            path = str(self.snapshot_path).replace("'", "''")
            result = con.execute(
                f"SELECT * FROM read_parquet('{path}') WHERE list_contains(?, key)", [keys]
            )
            columns = [d[0] for d in result.description]
            found = {r[0]: dict(zip(columns, r, strict=True)) for r in result.fetchall()}
        finally:
            con.close()
        if strict and (missing := set(keys) - set(found)):
            raise Inconsistent(f"records missing from the snapshot: {sorted(missing)[:5]}")
        return found

    def _fetch(self, key: str) -> bytes | None:
        """The object's body if it changed since the last read (b"" if it is gone), None if
        not."""
        seen, etag = key in self._etags, self._etags.get(key)
        found = self.bucket.get_if_changed(key, etag)
        if isinstance(found, str):
            return None
        if found is None:
            self._etags[key] = None
            return None if seen and etag is None else b""  # still missing, or now gone
        self._etags[key] = found[1]
        return found[0]

    def _set_ids(self, body: dict | None) -> None:
        body = body or {"next": 1, "records": {}, "ids": {}}
        self.next = body["next"]
        self.order = dict(body["records"])
        self.ids = {i: (k, seen) for i, (k, seen) in body["ids"].items()}
        self.ids_by_key = {}
        for event_id, (key, _) in self.ids.items():
            self.ids_by_key.setdefault(key, set()).add(event_id)

    # Writing: the caller holds the lock.

    def begin(self, pending: dict) -> None:
        """Save a round's writes before making them; refused if another round's are there."""
        body = json.dumps(pending, separators=(",", ":")).encode()
        self._etags[PENDING_KEY] = self.bucket.put(PENDING_KEY, body, if_none_match=True)
        self.pending = pending

    def apply(self, pending: dict) -> None:
        """Make a round's writes, in order; repeating them gives the same bucket."""
        rows = {key: body.encode() for key, body in pending["rows"].items()}
        removed = set(pending["removed"])
        with ThreadPoolExecutor(self.workers) as pool:
            list(pool.map(lambda item: self.bucket.put(row_key(item[0]), item[1]), rows.items()))
        if removed:
            failed = self.bucket.delete(row_key(key) for key in sorted(removed))
            if failed:
                raise OSError(f"could not delete {failed[:5]}")

        self._apply_ids(pending)
        manifest = dict(self.manifest)
        manifest.update({key: fingerprint(body) for key, body in rows.items()})
        for key in removed:
            manifest.pop(key, None)
        if manifest != self.manifest or MANIFEST_KEY not in self._etags:
            self._put(
                MANIFEST_KEY,
                manifest_body(manifest, pending["run_id"], pending["written_at"]),
                content_type="application/gzip",
            )
            self.manifest = manifest
        self._apply_snapshot(pending, list(rows.values()), removed)
        if pending["log"]:
            try:
                self.bucket.put(
                    pending["log_key"],
                    pending["log"].encode(),
                    content_type=NDJSON,
                    if_none_match=True,
                )
            except PreconditionFailed:
                pass  # written before an interruption
        self._put(STATE_KEY, json.dumps(pending["status"], indent=1).encode())
        self.status = pending["status"]
        self.bucket.delete([PENDING_KEY])
        self._etags[PENDING_KEY] = None
        self.pending = None

    def save_status(self, status: dict) -> None:
        """Write the state object alone (a checkpoint)."""
        self._put(STATE_KEY, json.dumps(status, indent=1).encode())
        self.status = status

    def _apply_ids(self, pending: dict) -> None:
        for event_id, (key, seen) in pending["ids"].items():
            previous = self.ids.get(event_id)
            if previous is not None:
                self.ids_by_key.get(previous[0], set()).discard(event_id)
            self.ids[event_id] = (key, seen)
            self.ids_by_key.setdefault(key, set()).add(event_id)
        for key in pending["removed"]:
            self.order.pop(key, None)
            if not self.ids_by_key.get(key):
                self.ids_by_key.pop(key, None)
        self.order.update(pending["order"])
        self.next = max(self.next, pending["next"])
        if not (pending["ids"] or pending["order"] or pending["removed"]):
            return  # no ID or record added or merged
        body = {
            "next": self.next,
            "records": dict(sorted(self.order.items())),
            "ids": {i: list(self.ids[i]) for i in sorted(self.ids)},
        }
        self._put(
            IDS_KEY,
            gzip.compress(json.dumps(body, separators=(",", ":")).encode(), mtime=0),
            content_type="application/gzip",
        )

    def _apply_snapshot(self, pending: dict, bodies: list[bytes], removed: set[str]) -> None:
        content = content_hash(self.manifest)
        if self.meta and self.meta.get("content_sha256") == content and self.has_snapshot:
            return  # already holds these rows
        body = updated_snapshot(self.snapshot_path if self.has_snapshot else None, bodies, removed)
        self._put(SNAPSHOT_KEY, body, content_type=PARQUET_TYPE)
        self.snapshot_path.write_bytes(body)
        self.has_snapshot = True
        meta = snapshot_meta(
            pending["run_id"], pending["written_at"], len(self.manifest), body, content
        )
        self._put(META_KEY, meta)  # after the file, as the exporter does
        self.meta = json.loads(meta)

    def _put(self, key: str, body: bytes, **kwargs) -> None:
        """A write that fails if someone else changed the object since this cache read it."""
        etag = self._etags.get(key)
        if etag is None:
            self._etags[key] = self.bucket.put(key, body, if_none_match=True, **kwargs)
        else:
            self._etags[key] = self.bucket.put(key, body, if_match=etag, **kwargs)


class BucketStore:
    """The EventStore of direct mode, over a BucketState. Store identities are records'
    creation numbers, so records sort in creation order as full mode's ids do."""

    def __init__(self, state: BucketState):
        self.state = state
        self.changes = Changes()

    def load(self, ids) -> Ledger:
        state = self.state
        keys = {i: state.ids[i][0] for i in set(ids) if i in state.ids}
        rows = state.rows(keys.values())
        records = {state.order[key]: record_of(row) for key, row in rows.items()}
        return Ledger(records, {i: state.order[key] for i, key in keys.items()})

    def write(self, ledger: Ledger, now: datetime) -> dict[Key, int]:
        """Find a ledger's writes (in self.changes); returns each record's creation number."""
        state = self.state
        numbers: dict[Key, int] = {k: k for k in ledger.records if not isinstance(k, NewKey)}
        for merge in ledger.merges:
            numbers.update({k: k for k in merge.others if not isinstance(k, NewKey)})
        wanted = set(numbers.values())
        key_of = {n: key for key, n in state.order.items() if n in wanted}
        changes = Changes(next=state.next)
        for k in ledger.created:
            numbers[k] = changes.next
            key_of[changes.next] = ledger.initial[k].key
            changes.order[ledger.initial[k].key] = changes.next
            changes.next += 1

        for merge in ledger.merges:
            survivor = key_of[numbers[merge.survivor]]
            gone = [key_of[numbers[k]] for k in merge.others]
            for key in gone:
                for event_id in state.ids_by_key.get(key, ()):
                    changes.ids[event_id] = [survivor, state.ids[event_id][1]]
                for event_id, (owner, seen) in list(changes.ids.items()):
                    if owner == key:
                        changes.ids[event_id] = [survivor, seen]
                changes.removed.add(key)
                changes.order.pop(key, None)
            logger.info(
                "merged event records",
                extra={
                    "event_id": merge.preferred_event_id,
                    "survivor": survivor,
                    "merged": gone,
                    "preferred_event_id": merge.preferred_event_id,
                },
            )
        for k, record in ledger.records.items():
            if k in ledger.changed or isinstance(k, NewKey):
                changes.rows[record.key] = row_body(record)
        for event_id, k in ledger.new_ids.items():
            changes.ids[event_id] = [key_of[numbers[ledger.resolve(k)]], iso(now)]
        self.changes = changes
        return numbers
