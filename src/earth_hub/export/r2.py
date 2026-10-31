"""On-demand export of the `earthquakes` table to Cloudflare R2, read through the API.

    python -m earth_hub.export.r2 [--dry-run] [--verify]   # make export-r2 [DRY_RUN=1] [VERIFY=1]
    python -m earth_hub.export.r2 runs          # make export-r2-runs

Bucket layout:
    earthquakes/<key>.json                 one object per row: every column but `id`, as stored
    _state/earthquakes-manifest.json.gz    SHA-256 of each row as last uploaded
    _state/lock.json                       held while a run is in progress
    _state/mode.json                       which mode writes the bucket; runs only in full
    runs/<run_id>.json                     one record per run (also kept in reports/exports/)
    analytics/earthquakes.parquet          the whole table for the dashboard (see snapshot.py)
    analytics/meta.json                    what the snapshot holds

A run reads every row from GET /api/v1/export/earthquakes (paged by id), then:
- uploads a row the manifest does not list (new) or lists with another fingerprint
  (updated: a revision, a deletion, a new place);
- skips a row the manifest lists with the same fingerprint (unchanged);
- deletes objects for rows no longer in the table (merged into another event).
By default the manifest alone says what the bucket holds: listing 100k+ keys takes about
100 s, while finding and uploading a day's changes takes seconds. A verified run (--verify,
and any run without a readable manifest) lists the bucket once, after the uploads, and
checks it: it also deletes objects the manifest does not know and re-uploads rows whose
object was removed outside the exporter. Changes made to the bucket by hand are therefore
found only by the next verified run.
Each row is one object named by its key (the USGS ID the record was created with), so a rerun
or an interrupted run can only overwrite, never duplicate. Rows are published without the
database's `id`, which differs between stores: the bucket is the same whichever store
processed the events (Milestone 8). A run succeeds only when the bucket then holds exactly the rows
read. The manifest lists only uploads that succeeded, so anything that failed is retried
next time. It is saved every CHECKPOINT_PAGES pages and when a run stops, so the next run
skips what an interrupted one already uploaded. A run that succeeded then rewrites the
analytics snapshot if the rows changed.
"""

import argparse
import gzip
import hashlib
import json
import logging
import re
import secrets
import subprocess
import sys
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from pathlib import Path

import httpx

from earth_hub.export import lock, mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.lock import LOCK_KEY, LockHeld  # noqa: F401  (LOCK_KEY: for callers)
from earth_hub.export.snapshot import (
    META_KEY,
    PARQUET_TYPE,
    SNAPSHOT_KEY,
    SnapshotFile,
    content_hash,
    snapshot_meta,
)

logger = logging.getLogger(__spec__.name)  # the module name, also under `python -m`

TABLE_PREFIX = "earthquakes/"
MANIFEST_KEY = "_state/earthquakes-manifest.json.gz"
RUNS_PREFIX = "runs/"
EXPORT_PATH = "export/earthquakes"
PAGE_SIZE = 1000
# The manifest is saved every this many pages, so an interrupted run loses at most that much
# progress: the next run skips everything already confirmed uploaded.
CHECKPOINT_PAGES = 10
# A lock older than this belongs to a run that crashed; a full first export takes minutes.
LOCK_TTL = timedelta(minutes=30)  # renewed while a run lasts: a crashed run frees it soon
LOCK_RENEW = timedelta(minutes=5)
MAX_ERRORS = 20
LOCAL_RUNS_DIR = Path("reports/exports")
_ROW_KEY = re.compile(rf"^{TABLE_PREFIX}([^/]+)\.json$")


def row_key(key: str) -> str:
    """The object name of a row."""
    return f"{TABLE_PREFIX}{key}.json"


def object_row(name: str) -> str | None:
    """The row key an object name holds, if it is a row object."""
    match = _ROW_KEY.match(name)
    return match.group(1) if match else None


def published(row: dict) -> dict:
    """A row as the bucket holds it: every column but the database's own `id`."""
    return {column: value for column, value in row.items() if column != "id"}


def encode(row: dict) -> bytes:
    """The object body: the row as the API returned it, columns in table order. JSON floats
    round-trip exactly, and timestamps keep their microseconds."""
    return json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()


def fingerprint(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def kept(known: dict[str, str], keys: set[str]) -> dict[str, str]:
    """The manifest entries for `keys`."""
    return {k: v for k, v in known.items() if k in keys}


def manifest_body(rows: dict[str, str], run_id: str, written_at: str) -> bytes:
    """The manifest as stored: each row's fingerprint, by key, gzipped."""
    body = {
        "table": "earthquakes",
        "run_id": run_id,
        "written_at": written_at,
        "rows": {str(k): rows[k] for k in sorted(rows)},
    }
    return gzip.compress(json.dumps(body).encode(), mtime=0)  # same rows, same bytes


def parse_manifest(body: bytes) -> dict[str, str]:
    """A stored manifest's rows; an unreadable one is empty, so every row counts as changed."""
    try:
        rows = json.loads(gzip.decompress(body))["rows"]
        # Keys are strings. A manifest from before keys lists ids: none matches a key, so
        # every row is uploaded under its key and the objects named by id are deleted.
        return {str(k): v for k, v in rows.items()}
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        logger.warning("manifest unreadable; every row will be uploaded again")
        return {}


class ApiUnavailable(Exception):
    pass


@dataclass
class Counts:
    read: int = 0
    new: int = 0
    updated: int = 0
    unchanged: int = 0
    deleted: int = 0
    failed: int = 0
    bytes_uploaded: int = 0


@dataclass
class RunRecord:
    run_id: str
    started_at: str
    api_url: str
    bucket: str
    git_commit: str | None
    dry_run: bool = False
    verified: bool = False  # the bucket was listed and checked, not just the manifest
    status: str = "running"  # running, succeeded, failed, interrupted
    finished_at: str | None = None
    duration_s: float | None = None
    counts: Counts = field(default_factory=Counts)
    verification: dict | None = None
    snapshot: dict | None = None
    errors: list[str] = field(default_factory=list)

    def error(self, message: str) -> None:
        logger.error(message)
        if len(self.errors) < MAX_ERRORS:
            self.errors.append(message)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), indent=2).encode()


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


class Exporter:
    def __init__(
        self,
        api: httpx.Client,
        bucket: Bucket,
        *,
        api_url: str,
        workers: int = 16,
        page_size: int = PAGE_SIZE,
        checkpoint_pages: int = CHECKPOINT_PAGES,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        lock_ttl: timedelta = LOCK_TTL,
        git_commit: str | None = None,
        local_dir: Path | None = LOCAL_RUNS_DIR,
    ):
        self.api = api
        self.bucket = bucket
        self.api_url = api_url
        self.workers = workers
        self.page_size = page_size
        self.checkpoint_pages = checkpoint_pages
        self.clock = clock
        self.lock_ttl = lock_ttl
        self.git_commit = git_commit
        self.local_dir = local_dir
        self._saved: dict[str, str] = {}
        self._ids: dict[str, int] = {}  # each row read: key -> the API's id, for paging
        self._held: lock.Held | None = None  # this run's lock (None on a dry run)

    def run(self, *, dry_run: bool = False, verify: bool = False) -> RunRecord:
        started = self.clock()
        record = RunRecord(
            run_id=f"{started:%Y-%m-%dT%H-%M-%SZ}-{secrets.token_hex(2)}",
            started_at=_iso(started),
            api_url=self.api_url,
            bucket=self.bucket.name,
            git_commit=self.git_commit,
            dry_run=dry_run,
        )
        locked = False
        if not dry_run:
            self._acquire_lock(record, started)  # raises LockHeld before anything is written
            locked = True
            self._save(record)
        try:
            self._export(record, dry_run=dry_run, verify=verify)
            record.status = "failed" if record.errors else "succeeded"
        except KeyboardInterrupt:
            record.status = "interrupted"
            raise
        except Exception as exc:
            record.status = "failed"
            record.error(f"{type(exc).__name__}: {exc}")
        finally:
            finished = self.clock()
            record.finished_at = _iso(finished)
            record.duration_s = round((finished - started).total_seconds(), 3)
            self._save(record)
            if locked:
                self._release_lock()
        return record

    # The export itself.

    def _export(self, record: RunRecord, *, dry_run: bool, verify: bool) -> None:
        manifest = self._load_manifest()
        self._saved = manifest  # the manifest as last written to the bucket
        # Without a manifest nothing says what the bucket holds, so the bucket is listed.
        record.verified = verify = verify or not manifest
        logger.info("manifest lists %d rows; verify %s", len(manifest), verify)
        read: set[str] = set()
        failed_keys: set[str] = set()
        # What each object in the bucket holds, as far as this run knows: the last manifest,
        # updated as uploads are confirmed. A failed upload leaves the object, and its entry,
        # as they were.
        known = dict(manifest)
        snapshot = None if dry_run else SnapshotFile()
        try:
            self._sync(record, manifest, known, read, failed_keys, snapshot, verify=verify)
        finally:
            if snapshot is not None:
                snapshot.close()

    def _sync(self, record, manifest, known, read, failed_keys, snapshot, *, verify) -> None:
        """Upload, delete, save the manifest, check, then write the snapshot."""
        counts = record.counts
        dry_run = snapshot is None
        try:
            self._upload_changes(record, manifest, known, read, failed_keys, snapshot)
        except lock.LockLost:
            raise  # another run writes the bucket now: not even a checkpoint
        except BaseException:
            if not dry_run and known != self._saved:
                self._checkpoint(known, record)
            raise

        # What the bucket holds: listed, or as the manifest and this run's uploads say.
        stored = self._stored_keys() if verify else set(known)
        gone = sorted(stored - read)
        if dry_run:
            counts.deleted = len(gone)
            return
        failed = set(self._delete(row_key(key) for key in gone))
        counts.deleted = len(gone) - len(failed)
        for key in sorted(failed):
            counts.failed += 1
            record.error(f"delete of {key} failed")
        stored -= {key for key in gone if row_key(key) not in failed}
        if verify:
            # Skipped because the manifest listed them, yet not in the bucket (removed by hand).
            stored |= self._repair(record, read - stored - failed_keys, known)
        # Only rows in the bucket: an object whose delete failed stays listed, so the next run
        # deletes it again.
        rows = kept(known, stored)
        if rows != self._saved:  # each save uploads the whole manifest: ~5 MB, seconds on R2
            self._save_manifest(rows, record)
        record.verification = self._verify(read, stored, listed=verify)
        if not record.verification["match"]:
            record.error("the bucket does not hold exactly the rows read; see verification")
        if not record.errors:  # only a bucket that holds exactly the rows read
            self._write_snapshot(record, rows, snapshot)

    def _write_snapshot(self, record: RunRecord, rows: dict[str, str], snapshot) -> None:
        content = content_hash(rows)
        meta = self._load_meta()
        if meta and meta.get("content_sha256") == content:
            record.snapshot = {"written": False, "rows": meta.get("rows"), "reason": "unchanged"}
            return
        if snapshot.rows != len(rows):
            record.error(f"snapshot has {snapshot.rows} rows, the bucket {len(rows)}; not written")
            return
        try:
            body = snapshot.build()
            self._put(SNAPSHOT_KEY, body, content_type=PARQUET_TYPE)
            meta = snapshot_meta(record.run_id, _iso(self.clock()), len(rows), body, content)
            # After the file: meta.json never describes a snapshot that is not there.
            self._put(META_KEY, meta)
        except lock.LockLost:
            raise
        except Exception as exc:
            record.error(f"snapshot not written: {type(exc).__name__}: {exc}")
            return
        record.snapshot = {"written": True, "rows": len(rows), "bytes": len(body)}
        logger.info("snapshot written: %d rows, %d bytes", len(rows), len(body))

    def _load_meta(self) -> dict | None:
        found = self.bucket.get(META_KEY)
        if found is None:
            return None
        try:
            return json.loads(found[0])
        except ValueError:
            return None  # unreadable: the snapshot is rewritten

    def _repair(self, record: RunRecord, keys: set[str], known: dict[str, str]) -> set[str]:
        """Fetch and upload rows the manifest wrongly listed as stored; returns those uploaded."""
        counts = record.counts
        repaired: set[str] = set()
        for key in sorted(keys):
            row_id = self._ids[key]
            rows = self._page(row_id - 1, limit=1)["rows"]
            if not rows or rows[0]["key"] != key:
                continue  # gone from the table since it was read; the verification reports it
            body = encode(published(rows[0]))
            try:
                self._put(row_key(key), body)
            except lock.LockLost:
                raise
            except Exception as exc:
                counts.failed += 1
                record.error(f"upload of row {key} failed: {exc}")
                continue
            counts.unchanged -= 1
            counts.new += 1
            counts.bytes_uploaded += len(body)
            known[key] = fingerprint(body)
            repaired.add(key)
        if repaired:
            logger.warning("%d rows listed in the manifest were missing; uploaded", len(repaired))
        return repaired

    def _upload_changes(self, record, manifest, known, read, failed_keys, snapshot) -> None:  # noqa: C901 (to refactor: docs/progress/plan.md)
        """Upload what changed. `snapshot` collects every row read; None on a dry run."""
        counts = record.counts
        dry_run = snapshot is None
        unsaved = 0  # uploads confirmed since the manifest was last saved
        with ThreadPoolExecutor(self.workers) as pool:
            for number, page in enumerate(self._pages(), start=1):
                self._confirm()  # also while nothing changes, so a long run keeps it (DEF-048)
                pending = []
                for row in page:
                    key = row["key"]
                    self._ids[key] = row["id"]
                    body = encode(published(row))
                    digest = fingerprint(body)
                    read.add(key)
                    counts.read += 1
                    if snapshot is not None:
                        snapshot.add(body)
                    if manifest.get(key) == digest:
                        counts.unchanged += 1
                    else:
                        kind = "updated" if key in manifest else "new"
                        pending.append((key, body, digest, kind))
                if dry_run:
                    for *_, kind in pending:
                        setattr(counts, kind, getattr(counts, kind) + 1)
                    continue
                futures = {
                    pool.submit(self._put, row_key(key), body): (
                        key,
                        digest,
                        kind,
                        len(body),
                    )
                    for key, body, digest, kind in pending
                }
                for future in as_completed(futures):
                    key, digest, kind, size = futures[future]
                    try:
                        future.result()
                    except lock.LockLost:
                        raise
                    except Exception as exc:
                        counts.failed += 1
                        failed_keys.add(key)
                        record.error(f"upload of row {key} failed: {exc}")
                        continue
                    setattr(counts, kind, getattr(counts, kind) + 1)
                    counts.bytes_uploaded += size
                    known[key] = digest
                    unsaved += 1
                if number % self.checkpoint_pages == 0:
                    logger.info(
                        "read %d rows; new %d, updated %d, unchanged %d",
                        counts.read,
                        counts.new,
                        counts.updated,
                        counts.unchanged,
                    )
                    if unsaved:
                        self._save_manifest(known, record)
                        unsaved = 0

    def _checkpoint(self, known: dict[str, str], record: RunRecord) -> None:
        """Save progress on the way out of an interrupted or failed run, if possible."""
        try:
            self._save_manifest(known, record)
        except lock.LockLost:
            logger.warning("the lock was taken over; no checkpoint")
        except Exception:
            logger.exception("could not save the manifest; the next run uploads more again")

    def _pages(self) -> Iterator[list[dict]]:
        after_id = 0
        while True:
            body = self._page(after_id, limit=self.page_size)
            yield body["rows"]
            if body["next_after_id"] is None:
                return
            after_id = body["next_after_id"]

    def _page(self, after_id: int, *, limit: int) -> dict:
        try:
            response = self.api.get(EXPORT_PATH, params={"after_id": after_id, "limit": limit})
        except httpx.HTTPError as exc:
            raise ApiUnavailable(f"{self.api_url}/{EXPORT_PATH}: {exc}") from None
        if response.status_code != HTTPStatus.OK:
            raise ApiUnavailable(
                f"{self.api_url}/{EXPORT_PATH} returned {response.status_code}: "
                f"{response.text[:200]}"
            )
        return response.json()

    def _stored_keys(self) -> set[str]:
        return {k for name in self.bucket.keys(TABLE_PREFIX) if (k := object_row(name)) is not None}

    @staticmethod
    def _verify(read: set[str], stored: set[str], *, listed: bool) -> dict:
        """Compare the rows read with the bucket: the run's listing (or, unverified, the
        manifest and this run's uploads), less confirmed deletes, plus confirmed repairs."""
        return {
            "source": "bucket listing" if listed else "manifest",
            "rows_read": len(read),
            "objects": len(stored),
            "missing": sorted(read - stored)[:MAX_ERRORS],
            "unexpected": sorted(stored - read)[:MAX_ERRORS],
            "match": stored == read,
        }

    def _load_manifest(self) -> dict[str, str]:
        found = self.bucket.get(MANIFEST_KEY)
        return {} if found is None else parse_manifest(found[0])

    def _save_manifest(self, rows: dict[str, str], record: RunRecord) -> None:
        self._put(
            MANIFEST_KEY,
            manifest_body(rows, record.run_id, _iso(self.clock())),
            content_type="application/gzip",
        )
        self._saved = dict(rows)

    # Writes, each after confirming the lock (DEF-058).

    def _confirm(self) -> None:
        if self._held is not None:
            self._held.confirm()

    def _put(self, key: str, body: bytes, **kwargs) -> str:
        self._confirm()
        return self.bucket.put(key, body, **kwargs)

    def _delete(self, keys) -> list[str]:
        self._confirm()
        return self.bucket.delete(keys)

    # Run records and the lock. A run's record is its own object, written without the lock.

    def _save(self, record: RunRecord) -> None:
        body = record.to_json()
        if self.local_dir is not None:
            self.local_dir.mkdir(parents=True, exist_ok=True)
            (self.local_dir / f"{record.run_id}.json").write_bytes(body)
        if not record.dry_run:
            try:
                self.bucket.put(f"{RUNS_PREFIX}{record.run_id}.json", body)
            except Exception:
                logger.exception("could not save run record %s to the bucket", record.run_id)

    def _acquire_lock(self, record: RunRecord, now: datetime) -> None:
        """The lock, and only in full mode: in direct mode the bucket is direct mode's."""
        self._held = lock.Held(
            self.bucket,
            record.run_id,
            record.started_at,
            now,
            self.lock_ttl,
            self.clock,
            renew_after=LOCK_RENEW,
        )
        try:
            mode.require(self.bucket, mode.FULL)
        except BaseException:
            self._release_lock()
            raise

    def _release_lock(self) -> None:
        self._held.release()
        self._held = None


def list_runs(bucket: Bucket, *, limit: int = 20, now: datetime | None = None) -> list[dict]:
    """The latest run records, oldest first. A record still `running` without a live lock
    belongs to a run that never finished, and is reported as `interrupted`."""
    live = lock.live_holder(bucket, now or datetime.now(UTC))
    runs = []
    for key in sorted(bucket.keys(RUNS_PREFIX))[-limit:]:
        found = bucket.get(key)
        if found is None:
            continue
        run = json.loads(found[0])
        if run["status"] == "running" and run["run_id"] != live:
            run["status"] = "interrupted"
        runs.append(run)
    return runs


def format_record(record: dict) -> str:
    c = record["counts"]
    return (
        f"{record['run_id']}  {record['status']:<11}"
        f"{' (dry run)' if record.get('dry_run') else ''}"
        f"{' (verified)' if record.get('verified') else ''}  "
        f"read {c['read']}  new {c['new']}  updated {c['updated']}  "
        f"unchanged {c['unchanged']}  deleted {c['deleted']}  failed {c['failed']}  "
        f"{record.get('duration_s') or 0:.1f} s"
    )


def _git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True, check=False
        )
    except OSError:
        return None
    return result.stdout.strip() or None


def main(argv: list[str] | None = None) -> int:
    from earth_hub.config import get_settings
    from earth_hub.export.bucket import MissingSettings, make_s3_client
    from earth_hub.logging_config import configure_logging

    parser = argparse.ArgumentParser(description="Export the earthquakes table to R2.")
    parser.add_argument("command", nargs="?", choices=("export", "runs"), default="export")
    parser.add_argument("--dry-run", action="store_true", help="count changes, write nothing")
    parser.add_argument(
        "--verify", action="store_true", help="list the bucket and check it (about 100 s)"
    )
    parser.add_argument("--limit", type=int, default=20, help="runs: how many to show")
    args = parser.parse_args(argv)

    configure_logging()
    settings = get_settings()
    try:
        bucket = Bucket(make_s3_client(settings), settings.r2_bucket)
    except MissingSettings as exc:
        print(f"export-r2: {exc}", file=sys.stderr)
        return 2

    if args.command == "runs":
        for run in list_runs(bucket, limit=args.limit):
            print(format_record(run))
        return 0

    api_url = str(settings.export_api_url).rstrip("/")
    with httpx.Client(
        base_url=api_url + "/", timeout=60, transport=httpx.HTTPTransport(retries=3)
    ) as api:
        exporter = Exporter(
            api,
            bucket,
            api_url=api_url,
            workers=settings.export_workers,
            git_commit=_git_commit(),
        )
        try:
            record = exporter.run(dry_run=args.dry_run, verify=args.verify)
        except LockHeld as exc:
            print(f"export-r2: {exc}; try again later", file=sys.stderr)
            return 3
        except mode.WrongMode as exc:
            print(f"export-r2: {exc}; nothing written", file=sys.stderr)
            return 3
    print(format_record(asdict(record)))
    for message in record.errors:
        print(f"  error: {message}", file=sys.stderr)
    return 0 if record.status == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
