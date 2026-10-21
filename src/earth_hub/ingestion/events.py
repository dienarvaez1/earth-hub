"""The Kafka event contract: an envelope around the unchanged USGS GeoJSON feature.

The poller only rejects features it cannot route (no id, updated or time). Data-quality
rules such as coordinate ranges belong to the consumer, so every other record is published
as-is and leaves an audit trail downstream.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

SCHEMA_VERSION = 1


class InvalidFeed(ValueError):
    """The response body is not a usable GeoJSON FeatureCollection."""


class InvalidFeature(ValueError):
    """A feature lacks a field needed to key or order it."""


@dataclass(frozen=True)
class EventMessage:
    event_id: str
    ids: tuple[str, ...]
    source_updated_ms: int
    event_time_ms: int
    source: str
    feed_generated_ms: int | None
    fetched_at: datetime
    payload: dict[str, Any] = field(repr=False)

    @property
    def key(self) -> bytes:
        return self.event_id.encode()

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "event_id": self.event_id,
                "ids": list(self.ids),
                "source_updated_ms": self.source_updated_ms,
                "event_time_ms": self.event_time_ms,
                "source": self.source,
                "feed_generated_ms": self.feed_generated_ms,
                "fetched_at": self.fetched_at.isoformat().replace("+00:00", "Z"),
                "payload": self.payload,
            },
            separators=(",", ":"),
        ).encode()


@dataclass(frozen=True)
class RejectedFeature:
    index: int
    event_id: str | None
    reason: str


def log_rejected(logger: logging.Logger, rejected: list[RejectedFeature], **context) -> None:
    """One `feature rejected` line per feature dropped before Kafka, wherever a feed is read
    (the poller, catch-up, backfill), so every one can be found: they reach no table."""
    for feature in rejected:
        logger.warning(
            "feature rejected",
            extra={
                "stage": "parse",
                "index": feature.index,
                "event_id": feature.event_id,
                "reason": feature.reason,
                **context,
            },
        )


@dataclass(frozen=True)
class ParsedFeed:
    messages: list[EventMessage]
    rejected: list[RejectedFeature]
    generated_ms: int | None


def _require_int(props: dict[str, Any], name: str) -> int:
    value = props.get(name)
    # bool is a subclass of int; a true/false timestamp is still invalid.
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidFeature(f"properties.{name} must be an integer, got {value!r}")
    return value


def parse_ids(raw: Any, event_id: str) -> tuple[str, ...]:
    """USGS lists associated IDs as ",id1,id2,". Always include the preferred ID."""
    ids = [part for part in raw.split(",") if part] if isinstance(raw, str) else []
    if event_id not in ids:
        ids.insert(0, event_id)
    return tuple(dict.fromkeys(ids))


def build_message(
    feature: Any,
    *,
    fetched_at: datetime,
    feed_generated_ms: int | None,
    source: str = "feed",
) -> EventMessage:
    if not isinstance(feature, dict):
        raise InvalidFeature("feature is not an object")
    event_id = feature.get("id")
    if not isinstance(event_id, str) or not event_id.strip():
        raise InvalidFeature(f"id must be a non-empty string, got {event_id!r}")
    props = feature.get("properties")
    if not isinstance(props, dict):
        raise InvalidFeature("properties must be an object")
    return EventMessage(
        event_id=event_id,
        ids=parse_ids(props.get("ids"), event_id),
        source_updated_ms=_require_int(props, "updated"),
        event_time_ms=_require_int(props, "time"),
        source=source,
        feed_generated_ms=feed_generated_ms,
        fetched_at=fetched_at,
        payload=feature,
    )


def parse_feed(body: bytes, *, fetched_at: datetime, source: str = "feed") -> ParsedFeed:
    try:
        document = json.loads(body)
    except ValueError as exc:
        raise InvalidFeed(f"body is not valid JSON: {exc}") from exc
    if not isinstance(document, dict) or not isinstance(document.get("features"), list):
        raise InvalidFeed("body is not a GeoJSON FeatureCollection with a features list")

    metadata = document.get("metadata")
    generated = metadata.get("generated") if isinstance(metadata, dict) else None
    generated_ms = (
        generated if isinstance(generated, int) and not isinstance(generated, bool) else None
    )

    messages, rejected = [], []
    for index, feature in enumerate(document["features"]):
        try:
            messages.append(
                build_message(
                    feature, fetched_at=fetched_at, feed_generated_ms=generated_ms, source=source
                )
            )
        except InvalidFeature as exc:
            raw_id = feature.get("id") if isinstance(feature, dict) else None
            rejected.append(
                RejectedFeature(index, raw_id if isinstance(raw_id, str) else None, str(exc))
            )
    return ParsedFeed(messages, rejected, generated_ms)
