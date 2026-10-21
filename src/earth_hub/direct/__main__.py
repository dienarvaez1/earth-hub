"""Run direct mode: `python -m earth_hub.direct once|run` (make direct-once / direct-run).

The real poller and catch-up, publishing to the bucket instead of Kafka: no Docker, Kafka or
PostgreSQL, only R2 credentials. Runs only while the bucket's marker says `direct`
(make mode-direct hands over from full mode).

- `once`: one poller round, then catch-up's month-feed pass and deleted-events check if
  they are due by their checkpoints in the bucket; then exit. Suited to a schedule. With
  `--all`, both run whether due or not (make catchup-now, and the end of make mode-direct).
- `run`: the poller, waiting for each new feed version, and catch-up's loop beside it,
  until stopped (Ctrl-C or SIGTERM), or until a handover (make mode-full) freezes the
  bucket: its next round then finds the bucket no longer in direct mode, and it stops.

`--skip-unless-direct` (the schedule in .github/workflows/direct.yml): a bucket not in direct
mode, before or during the run, is a skip (exit 0) rather than an error, so the schedule stays
quiet while full mode or a handover has the bucket. Missing settings still fail.
"""

import argparse
import logging
import signal
import sys
import threading
from datetime import UTC, datetime, timedelta

from earth_hub.direct.runner import BucketProgress, DirectPublisher, DirectRunner
from earth_hub.export import mode

logger = logging.getLogger(__spec__.name)  # the module name, also under `python -m`


def main(argv: list[str] | None = None) -> int:
    from earth_hub.config import get_settings
    from earth_hub.export.bucket import Bucket, MissingSettings, make_s3_client
    from earth_hub.ingestion.catchup import CatchUp
    from earth_hub.ingestion.fdsn import FdsnClient
    from earth_hub.ingestion.poller import Poller
    from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client
    from earth_hub.logging_config import configure_logging

    parser = argparse.ArgumentParser(description="Run direct mode: USGS straight to R2.")
    parser.add_argument("command", choices=("once", "run"))
    parser.add_argument(
        "--skip-unless-direct",
        action="store_true",
        help="exit 0 without writing when the bucket is not in direct mode (for a schedule)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="once: run catch-up and the deleted-events check even if they are not due",
    )
    args = parser.parse_args(argv)
    configure_logging()
    settings = get_settings()
    try:
        bucket = Bucket(make_s3_client(settings), settings.r2_bucket)
        mode.require(bucket, mode.DIRECT)
    except mode.WrongMode as exc:
        if args.skip_unless_direct:
            print(f"direct: skipped: {exc}")
            return 0
        print(f"direct: {exc}", file=sys.stderr)
        return 2
    except MissingSettings as exc:
        print(f"direct: {exc}", file=sys.stderr)
        return 2

    stopping = threading.Event()
    runner = DirectRunner(bucket, workers=settings.export_workers)
    state = runner.sent_state()
    progress = BucketProgress(runner)
    with (
        make_http_client(settings.usgs_contact) as feed_http,
        make_http_client(settings.usgs_contact, timeout_s=60.0) as month_http,
        make_http_client(settings.usgs_contact, timeout_s=60.0) as fdsn_http,
    ):
        poller = Poller(
            UsgsClient(str(settings.usgs_feed_url), feed_http),
            DirectPublisher(runner, source="feed", state=state, on_wrong_mode=stopping.set),
            state,
            fallback_wait_s=settings.poll_interval_seconds,
        )
        catchup = CatchUp(
            UsgsClient(str(settings.usgs_month_feed_url), month_http),
            FdsnClient(str(settings.fdsn_url), fdsn_http, pause_s=settings.fdsn_pause_seconds),
            DirectPublisher(runner, source="catchup", on_wrong_mode=stopping.set),
            progress,
            catchup_interval_s=settings.catchup_interval_seconds,
            deleted_interval_s=settings.deleted_check_interval_seconds,
        )
        if args.command == "once":
            result = poller.run_round()
            print(f"direct: poller {result.status}, {result.published} events processed")
            if stopping.is_set():  # a handover froze the bucket during the round
                if args.skip_unless_direct:
                    print("direct: skipped: the bucket left direct mode during the run")
                    return 0
                return 1
            now = datetime.now(UTC)
            for name, interval, job in (
                ("catchup", settings.catchup_interval_seconds, catchup.catch_up),
                ("deleted", settings.deleted_check_interval_seconds, catchup.check_deleted),
            ):
                last = progress.checkpoint(name)
                if args.all or last is None or now - last >= timedelta(seconds=interval):
                    print(f"direct: {name}: {job()} events processed")
            return 0 if result.status != "failed" else 1

        def stop(*_):
            poller.stop_event.set()
            catchup.stop_event.set()

        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, stop)
        side = threading.Thread(target=catchup.run_forever, name="catchup", daemon=True)
        side.start()
        # A handover to full mode freezes the bucket: then stop, as Ctrl-C would.
        threading.Thread(target=lambda: (stopping.wait(), stop()), daemon=True).start()
        poller.run_forever()
        side.join(timeout=30)
    return 0


if __name__ == "__main__":
    sys.exit(main())
