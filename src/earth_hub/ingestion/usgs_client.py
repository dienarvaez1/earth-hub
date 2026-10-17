"""HTTP access to the USGS GeoJSON summary feed: conditional requests and bounded retries."""

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from earth_hub import __version__

logger = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
USER_AGENT = f"earth-hub/{__version__} (local-first earthquake data platform)"


class FeedUnavailable(Exception):
    """The feed could not be fetched; the round should be skipped."""


@dataclass(frozen=True)
class FeedResponse:
    not_modified: bool
    body: bytes | None
    last_modified: str | None
    expires: datetime | None
    fetched_at: datetime


def parse_http_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def user_agent(contact: str = "") -> str:
    """Identify Earth-Hub to USGS, with an optional contact so they can reach the operator."""
    if not contact:
        return USER_AGENT
    return f"earth-hub/{__version__} (local-first earthquake data platform; +{contact})"


def make_http_client(contact: str = "", timeout_s: float = 15.0, **kwargs) -> httpx.Client:
    return httpx.Client(
        timeout=httpx.Timeout(timeout_s, connect=5.0),
        headers={"User-Agent": user_agent(contact)},
        follow_redirects=True,
        **kwargs,
    )


class UsgsClient:
    def __init__(
        self,
        url: str,
        http: httpx.Client,
        *,
        max_attempts: int = 3,
        backoff_seconds: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
        rng: Callable[[], float] = random.random,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.url = url
        self._http = http
        self._max_attempts = max_attempts
        self._backoff_seconds = backoff_seconds
        self._sleep = sleep
        self._rng = rng
        self._clock = clock

    def fetch(self, if_modified_since: str | None = None) -> FeedResponse:
        headers = {"If-Modified-Since": if_modified_since} if if_modified_since else {}
        last_error = "no attempts made"
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._http.get(self.url, headers=headers)
            except httpx.TransportError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            else:
                if response.status_code in (200, 304):
                    return self._to_feed_response(response)
                last_error = f"HTTP {response.status_code}"
                if response.status_code not in RETRYABLE_STATUS:
                    raise FeedUnavailable(f"{self.url} returned {last_error} (not retried)")

            if attempt < self._max_attempts:
                delay = self._backoff_seconds * 2 ** (attempt - 1) * (1 + self._rng())
                logger.warning(
                    "feed request failed, retrying",
                    extra={"attempt": attempt, "error": last_error, "retry_in_s": round(delay, 2)},
                )
                self._sleep(delay)
        raise FeedUnavailable(
            f"{self.url} failed after {self._max_attempts} attempts: {last_error}"
        )

    def _to_feed_response(self, response: httpx.Response) -> FeedResponse:
        not_modified = response.status_code == 304
        return FeedResponse(
            not_modified=not_modified,
            body=None if not_modified else response.content,
            last_modified=response.headers.get("Last-Modified"),
            expires=parse_http_date(response.headers.get("Expires")),
            fetched_at=self._clock(),
        )
