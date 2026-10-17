"""Container health check: healthy while a service's heartbeat file is recent.

`python -m earth_hub.healthcheck poller|consumer|catchup` exits 0 if healthy, 1 otherwise.
The poller beats after each successful round; the consumer after each message, or every
loop while idle as long as PostgreSQL answers.
"""

import sys
import time

from earth_hub.config import Settings, get_settings

CONSUMER_MAX_AGE_S = 90.0
CATCHUP_MAX_AGE_S = 300.0  # it beats every minute while its passes succeed
SERVICES = ("poller", "consumer", "catchup")


def max_age_s(service: str, settings: Settings) -> float:
    if service == "poller":
        return 3 * max(settings.poll_interval_seconds, 60)
    if service == "catchup":
        return CATCHUP_MAX_AGE_S
    return CONSUMER_MAX_AGE_S


def is_healthy(heartbeat_mtime: float | None, now: float, max_age: float) -> bool:
    return heartbeat_mtime is not None and now - heartbeat_mtime <= max_age


def main(argv: list[str]) -> int:
    if len(argv) != 1 or argv[0] not in SERVICES:
        print(f"usage: python -m earth_hub.healthcheck {'|'.join(SERVICES)}", file=sys.stderr)
        return 2
    service = argv[0]
    settings = get_settings()
    path = {
        "poller": settings.poller_heartbeat_path,
        "consumer": settings.consumer_heartbeat_path,
        "catchup": settings.catchup_heartbeat_path,
    }[service]
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        mtime = None
    return 0 if is_healthy(mtime, time.time(), max_age_s(service, settings)) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
