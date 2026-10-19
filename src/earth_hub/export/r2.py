"""On-demand export of the `earthquakes` table to Cloudflare R2, read through the API.

    python -m earth_hub.export.r2 [--dry-run] [--verify]   # make export-r2 [DRY_RUN=1] [VERIFY=1]
    python -m earth_hub.export.r2 runs          # make export-r2-runs

Bucket layout:
    earthquakes/<id>.json                  one object per row: every column, exactly as stored
    _state/earthquakes-manifest.json.gz    SHA-256 of each row as last uploaded
    _state/lock.json                       held while a run is in progress
    runs/<run_id>.json                     one record per run (also kept in reports/exports/)

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
Each row is one object named by its primary key, so a rerun or an interrupted run can only
overwrite, never duplicate. A run succeeds only when the bucket then holds exactly the rows
read. The manifest lists only uploads that succeeded, so anything that failed is retried
next time. It is saved every CHECKPOINT_PAGES pages and when a run stops, so the next run
skips what an interrupted one already uploaded.
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
from pathlib import Path

import httpx

from earth_hub.export.bucket import Bucket, PreconditionFailed

logger = logging.getLogger(__name__)

TABLE_PREFIX = "earthquakes/"
MANIFEST_KEY = "_state/earthquakes-manifest.json.gz"
LOCK_KEY = "_state/lock.json"
RUNS_PREFIX = "runs/"
EXPORT_PATH = "export/earthquakes"
PAGE_SIZE = 1000
# The manifest is saved every this many pages, so an interrupted run loses at most that much
# progress: the next run skips everything already confirmed uploaded.
CHECKPOINT_PAGES = 10
# A lock older than this belongs to a run that crashed; a full first export takes minutes.
LOCK_TTL = timedelta(hours=2)
MAX_ERRORS = 20
LOCAL_RUNS_DIR = Path("reports/exports")
_ROW_KEY = re.compile(rf"^{TABLE_PREFIX}(\d+)\.json$")


def row_key(row_id: int) -> str:
    return f"{TABLE_PREFIX}{row_id}.json"


def key_row_id(key: str) -> int | None:
    match = _ROW_KEY.match(key)
    return int(match.group(1)) if match else None


def encode(row: dict) -> bytes:
    """The object body: the row as the API returned it, columns in table order. JSON floats
    round-trip exactly, and timestamps keep their microseconds."""
    return json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode()


def fingerprint(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def kept(known: dict[int, str], ids: set[int]) -> dict[int, str]:
    """The manifest entries for `ids`."""
    return {k: v for k, v in known.items() if k in ids}


class LockHeld(Exception):
    """Another run is in progress."""


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
        self._saved: dict[int, str] = {}

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
                self._release_lock(record)
        return record

    # The export itself.

    def _export(self, record: RunRecord, *, dry_run: bool, verify: bool) -> None:
        counts = record.counts
        manifest = self._load_manifest()
        self._saved = manifest  # the manifest as last written to the bucket
        # Without a manifest nothing says what the bucket holds, so the bucket is listed.
        record.verified = verify = verify or not manifest
        logger.info("manifest lists %d rows; verify %s", len(manifest), verify)
        read: set[int] = set()
        failed_ids: set[int] = set()
        # What each object in the bucket holds, as far as this run knows: the last manifest,
        # updated as uploads are confirmed. A failed upload leaves the object, and its entry,
        # as they were.
        known = dict(manifest)
        try:
            self._upload_changes(record, manifest, known, read, failed_ids, dry_run=dry_run)
        except BaseException:
            if not dry_run and known != self._saved:
                self._checkpoint(known, record)
            raise

        # What the bucket holds: listed, or as the manifest and this run's uploads say.
        stored = self._stored_ids() if verify else set(known)
        gone = sorted(stored - read)
        if dry_run:
            counts.deleted = len(gone)
            return
        failed = set(self.bucket.delete(row_key(row_id) for row_id in gone))
        counts.deleted = len(gone) - len(failed)
        for key in sorted(failed):
            counts.failed += 1
            record.error(f"delete of {key} failed")
        stored -= {row_id for row_id in gone if row_key(row_id) not in failed}
        if verify:
            # Skipped because the manifest listed them, yet not in the bucket (removed by hand).
            stored |= self._repair(record, read - stored - failed_ids, known)
        # Only rows in the bucket: an object whose delete failed stays listed, so the next run
        # deletes it again.
        rows = kept(known, stored)
        if rows != self._saved:  # each save uploads the whole manifest: ~5 MB, seconds on R2
            self._save_manifest(rows, record)
        record.verification = self._verify(read, stored, listed=verify)
        if not record.verification["match"]:
            record.error("the bucket does not hold exactly the rows read; see verification")

    def _repair(self, record: RunRecord, ids: set[int], known: dict[int, str]) -> set[int]:
        """Fetch and upload rows the manifest wrongly listed as stored; returns those uploaded."""
        counts = record.counts
        repaired: set[int] = set()
        for row_id in sorted(ids):
            rows = self._page(row_id - 1, limit=1)["rows"]
            if not rows or rows[0]["id"] != row_id:
                continue  # gone from the table since it was read; the verification reports it
            body = encode(rows[0])
            try:
                self.bucket.put(row_key(row_id), body)
            except Exception as exc:
                counts.failed += 1
                record.error(f"upload of row {row_id} failed: {exc}")
                continue
            counts.unchanged -= 1
            counts.new += 1
            counts.bytes_uploaded += len(body)
            known[row_id] = fingerprint(body)
            repaired.add(row_id)
        if repaired:
            logger.warning("%d rows listed in the manifest were missing; uploaded", len(repaired))
        return repaired

    def _upload_changes(self, record, manifest, known, read, failed_ids, *, dry_run: bool) -> None:
        counts = record.counts
        unsaved = 0  # uploads confirmed since the manifest was last saved
        with ThreadPoolExecutor(self.workers) as pool:
            for number, page in enumerate(self._pages(), start=1):
                pending = []
                for row in page:
                    row_id = row["id"]
                    body = encode(row)
                    digest = fingerprint(body)
                    read.add(row_id)
                    counts.read += 1
                    if manifest.get(row_id) == digest:
                        counts.unchanged += 1
                    else:
                        kind = "updated" if row_id in manifest else "new"
                        pending.append((row_id, body, digest, kind))
                if dry_run:
                    for *_, kind in pending:
                        setattr(counts, kind, getattr(counts, kind) + 1)
                    continue
                futures = {
                    pool.submit(self.bucket.put, row_key(row_id), body): (
                        row_id,
                        digest,
                        kind,
                        len(body),
                    )
                    for row_id, body, digest, kind in pending
                }
                for future in as_completed(futures):
                    row_id, digest, kind, size = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        counts.failed += 1
                        failed_ids.add(row_id)
                        record.error(f"upload of row {row_id} failed: {exc}")
                        continue
                    setattr(counts, kind, getattr(counts, kind) + 1)
                    counts.bytes_uploaded += size
                    known[row_id] = digest
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

    def _checkpoint(self, known: dict[int, str], record: RunRecord) -> None:
        """Save progress on the way out of an interrupted or failed run, if possible."""
        try:
            self._save_manifest(known, record)
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
        if response.status_code != 200:
            raise ApiUnavailable(
                f"{self.api_url}/{EXPORT_PATH} returned {response.status_code}: "
                f"{response.text[:200]}"
            )
        return response.json()

    def _stored_ids(self) -> set[int]:
        return {i for key in self.bucket.keys(TABLE_PREFIX) if (i := key_row_id(key)) is not None}

    @staticmethod
    def _verify(read: set[int], stored: set[int], *, listed: bool) -> dict:
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

    def _load_manifest(self) -> dict[int, str]:
        found = self.bucket.get(MANIFEST_KEY)
        if found is None:
            return {}
        try:
            rows = json.loads(gzip.decompress(found[0]))["rows"]
            return {int(k): v for k, v in rows.items()}
        except (OSError, ValueError, KeyError, TypeError):
            # Unreadable: every stored row counts as changed and is uploaded again.
            logger.warning("manifest unreadable; every row will be uploaded again")
            return {}

    def _save_manifest(self, rows: dict[int, str], record: RunRecord) -> None:
        body = {
            "table": "earthquakes",
            "run_id": record.run_id,
            "written_at": _iso(self.clock()),
            "rows": {str(k): rows[k] for k in sorted(rows)},
        }
        self.bucket.put(
            MANIFEST_KEY, gzip.compress(json.dumps(body).encode()), content_type="application/gzip"
        )
        self._saved = dict(rows)

    # Run records and the lock.

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
        body = json.dumps(
            {
                "run_id": record.run_id,
                "started_at": record.started_at,
                "expires_at": _iso(now + self.lock_ttl),
            }
        ).encode()
        try:
            self.bucket.put(LOCK_KEY, body, if_none_match=True)
            return
        except PreconditionFailed:
            pass
        current = self.bucket.get(LOCK_KEY)
        if current is None:  # released in between
            try:
                self.bucket.put(LOCK_KEY, body, if_none_match=True)
                return
            except PreconditionFailed:
                raise LockHeld("another run took the lock") from None
        holder = json.loads(current[0])
        if datetime.fromisoformat(holder["expires_at"]) > now:
            raise LockHeld(
                f"run {holder['run_id']} has been in progress since {holder['started_at']}"
            )
        logger.warning("taking over the expired lock of run %s", holder["run_id"])
        try:
            self.bucket.put(LOCK_KEY, body, if_match=current[1])  # only if still that lock
        except PreconditionFailed:
            raise LockHeld("another run took the expired lock") from None

    def _release_lock(self, record: RunRecord) -> None:
        try:
            current = self.bucket.get(LOCK_KEY)
            if current is not None and json.loads(current[0])["run_id"] == record.run_id:
                self.bucket.delete([LOCK_KEY])
        except Exception:
            logger.exception("could not release the lock; it expires after %s", self.lock_ttl)


def list_runs(bucket: Bucket, *, limit: int = 20, now: datetime | None = None) -> list[dict]:
    """The latest run records, oldest first. A record still `running` without a live lock
    belongs to a run that never finished, and is reported as `interrupted`."""
    now = now or datetime.now(UTC)
    lock = bucket.get(LOCK_KEY)
    live = None
    if lock is not None:
        holder = json.loads(lock[0])
        if datetime.fromisoformat(holder["expires_at"]) > now:
            live = holder["run_id"]
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
    print(format_record(asdict(record)))
    for message in record.errors:
        print(f"  error: {message}", file=sys.stderr)
    return 0 if record.status == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
