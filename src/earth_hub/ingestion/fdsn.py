"""The USGS FDSN event web service, used for backfill and the deleted-events check.

Usage rules this client enforces, because the service is not CDN-cached:
- one request at a time, with a pause between requests (FDSN_PAUSE_SECONDS);
- bounded retries with backoff on 429, 500, 502, 503 and connection errors, honouring
  Retry-After; a 504 or a timeout is not retried but raised as FdsnTooSlow, because the
  gateway caches the 504: the caller asks for a smaller window instead;
- no `limit` parameter: a capped result would silently drop events, so callers compare the
  query result with /count instead;
- times sent as whole seconds in UTC. The service ignores milliseconds in starttime and
  endtime, so a window that ends at 23:59:59.999 really ends at 23:59:59 and loses events in
  that last second. Windows must end exactly where the next begins.
"""

import json
import logging
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

logger = logging.getLogger(__name__)

MAX_RESULTS = 20_000  # the service rejects queries matching more events
RETRYABLE_STATUS = frozenset({429, 500, 502, 503})
TOO_SLOW_STATUS = 504
MAX_RETRY_AFTER_S = 120.0
EMPTY_COLLECTION = b'{"type":"FeatureCollection","features":[]}'


class FdsnUnavailable(Exception):
    """The service could not answer; the window stays unfinished and can be retried."""


class FdsnTooSlow(FdsnUnavailable):
    """The service timed out on this request (504, or no answer in time). Asking again is
    pointless: the gateway caches the 504. Ask for a smaller window instead."""


def fdsn_time(value: datetime) -> str:
    """UTC, whole seconds. Refuses fractions instead of letting the service drop them."""
    if value.tzinfo is None:
        raise ValueError("FDSN times must be timezone-aware")
    utc = value.astimezone(UTC)
    if utc.microsecond:
        raise ValueError(f"FDSN ignores sub-second times; window edge {utc} is not whole seconds")
    return utc.strftime("%Y-%m-%dT%H:%M:%S")


def retry_after_seconds(header: str | None, now: datetime) -> float | None:
    if not header:
        return None
    try:
        return max(0.0, float(header))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(header)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(0.0, (when - now).total_seconds())


class FdsnClient:
    def __init__(
        self,
        base_url: str,
        http: httpx.Client,
        *,
        pause_s: float = 1.0,
        max_attempts: int = 3,
        backoff_s: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        rng: Callable[[], float] = random.random,
    ):
        self.base_url = base_url.rstrip("/")
        self._http = http
        self._pause_s = pause_s
        self._max_attempts = max_attempts
        self._backoff_s = backoff_s
        self._sleep = sleep
        self._monotonic = monotonic
        self._clock = clock
        self._rng = rng
        self._last_request: float | None = None
        self.requests = 0

    def count(
        self,
        start: datetime,
        end: datetime,
        *,
        include_deleted: str = "true",
        updated_after: datetime | None = None,
    ) -> int:
        body = self._get("count", self._params(start, end, include_deleted, updated_after))
        return int(json.loads(body)["count"])

    def query(
        self,
        start: datetime,
        end: datetime,
        *,
        include_deleted: str = "true",
        updated_after: datetime | None = None,
    ) -> bytes:
        """The window's events as a GeoJSON FeatureCollection, oldest first."""
        params = self._params(start, end, include_deleted, updated_after) | {"orderby": "time-asc"}
        return self._get("query", params) or EMPTY_COLLECTION

    @staticmethod
    def _params(start, end, include_deleted, updated_after) -> dict[str, str]:
        if end <= start:
            raise ValueError(f"empty window {start} .. {end}")
        params = {
            "format": "geojson",
            "starttime": fdsn_time(start),
            "endtime": fdsn_time(end),
            "includedeleted": include_deleted,
        }
        if updated_after is not None:
            params["updatedafter"] = fdsn_time(updated_after)
        return params

    def _get(self, method: str, params: dict[str, str]) -> bytes:
        url = f"{self.base_url}/{method}"
        last_error = "no attempts made"
        for attempt in range(1, self._max_attempts + 1):
            self._wait_turn()
            retry_after = None
            try:
                response = self._http.get(url, params=params)
            except httpx.TimeoutException as exc:
                raise FdsnTooSlow(f"{url} {params} -> {type(exc).__name__}") from exc
            except httpx.TransportError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code == 204:  # no events match
                    return b""
                if response.status_code == 200:
                    return response.content
                if response.status_code == TOO_SLOW_STATUS:
                    raise FdsnTooSlow(f"{url} {params} -> HTTP 504 Gateway Time-out")
                last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                if response.status_code not in RETRYABLE_STATUS:
                    raise FdsnUnavailable(f"{url} {params} -> {last_error} (not retried)")
                retry_after = retry_after_seconds(
                    response.headers.get("Retry-After"), self._clock()
                )
            if attempt < self._max_attempts:
                delay = self._backoff_s * 2 ** (attempt - 1) * (1 + self._rng())
                if retry_after is not None:
                    delay = max(delay, min(retry_after, MAX_RETRY_AFTER_S))
                logger.warning(
                    "FDSN request failed, retrying",
                    extra={"attempt": attempt, "error": last_error, "retry_in_s": round(delay, 1)},
                )
                self._sleep(delay)
        raise FdsnUnavailable(f"{url} failed after {self._max_attempts} attempts: {last_error}")

    def _wait_turn(self) -> None:
        """Keep at least pause_s between the start of consecutive requests."""
        if self._last_request is not None:
            remaining = self._pause_s - (self._monotonic() - self._last_request)
            if remaining > 0:
                self._sleep(remaining)
        self._last_request = self._monotonic()
        self.requests += 1
