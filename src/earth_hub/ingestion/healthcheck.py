"""Container health check: healthy while the poller's heartbeat is recent.

`python -m earth_hub.ingestion.healthcheck` exits 0 if a successful round happened within
three poll intervals, 1 otherwise.
"""

import sys
import time

from earth_hub.config import get_settings


def is_healthy(heartbeat_mtime: float | None, now: float, poll_interval_s: float) -> bool:
    return heartbeat_mtime is not None and now - heartbeat_mtime <= 3 * max(poll_interval_s, 60)


def main() -> int:
    settings = get_settings()
    try:
        mtime = settings.poller_heartbeat_path.stat().st_mtime
    except FileNotFoundError:
        mtime = None
    return 0 if is_healthy(mtime, time.time(), settings.poll_interval_seconds) else 1


if __name__ == "__main__":
    sys.exit(main())
