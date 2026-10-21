"""Validation of earthquakes.raw messages and conversion to normalized fields.

Every rule has a stable ID, documented in docs/technical/data-quality-rules.md, which is stored with
each rejected message so rejections can be counted and traced. Envelope rules stop
validation early (nothing else can be checked); payload rules are all evaluated, so a
rejection lists every problem at once.
"""

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from earth_hub.ingestion.events import SCHEMA_VERSION
from earth_hub.validation.places import parse_place

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
EARLIEST_EVENT_TIME = datetime(1900, 1, 1, tzinfo=UTC)
MAX_FUTURE = timedelta(hours=1)
LATITUDE_RANGE = (-90.0, 90.0)
LONGITUDE_RANGE = (-180.0, 180.0)
DEPTH_RANGE_KM = (-10.0, 1000.0)
MAGNITUDE_RANGE = (-2.0, 10.5)
REVIEW_STATUSES = frozenset({"automatic", "reviewed", "deleted"})


def review_status(props: dict) -> Any:
    """A feature's `status`, in lower case if it is a string. The Pacific Tsunami Warning
    Center's features say `REVIEWED` (DEF-049), so every check of the status goes through
    this."""
    status = props.get("status")
    return status.lower() if isinstance(status, str) else status


@dataclass(frozen=True)
class Violation:
    rule: str
    message: str


class Rejected(Exception):
    def __init__(self, violations: list[Violation]):
        self.violations = violations
        super().__init__(self.reason)

    @property
    def rule(self) -> str:
        return self.violations[0].rule

    @property
    def reason(self) -> str:
        return "; ".join(f"{v.rule}: {v.message}" for v in self.violations)


@dataclass(frozen=True)
class ValidEvent:
    event_id: str
    ids: tuple[str, ...]
    source_updated_ms: int
    event_time_ms: int
    # Normalized values for the earthquakes table, keyed by column name.
    fields: dict[str, Any] = field(repr=False)

    @property
    def deleted(self) -> bool:
        return self.fields["review_status"] == "deleted"


def ms_to_datetime(ms: int) -> datetime:
    # timedelta arithmetic is exact; datetime.fromtimestamp(ms / 1000) can be off by 1 µs.
    return EPOCH + timedelta(milliseconds=ms)


def _safe_datetime(ms: int) -> datetime | None:
    """None for values beyond what datetime (and PostgreSQL bigint) can hold."""
    if not -(2**63) <= ms < 2**63:
        return None
    try:
        return ms_to_datetime(ms)
    except OverflowError:
        return None


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return (_is_int(value) or isinstance(value, float)) and math.isfinite(value)


def _in_range(value: float, bounds: tuple[float, float]) -> bool:
    return bounds[0] <= value <= bounds[1]


def _validate_envelope(envelope: Any) -> None:
    if not isinstance(envelope, dict):
        raise Rejected([Violation("ENV-001", "message is not a JSON object")])
    if envelope.get("schema_version") != SCHEMA_VERSION:
        raise Rejected(
            [Violation("ENV-002", f"unsupported schema_version {envelope.get('schema_version')!r}")]
        )
    problems = []
    if not isinstance(envelope.get("event_id"), str) or not envelope["event_id"]:
        problems.append("event_id must be a non-empty string")
    ids = envelope.get("ids")
    if not isinstance(ids, list) or not ids or not all(isinstance(i, str) and i for i in ids):
        problems.append("ids must be a non-empty list of strings")
    for name in ("source_updated_ms", "event_time_ms"):
        if not _is_int(envelope.get(name)):
            problems.append(f"{name} must be an integer")
    if not isinstance(envelope.get("payload"), dict):
        problems.append("payload must be an object")
    if problems:
        raise Rejected([Violation("ENV-003", p) for p in problems])

    payload = envelope["payload"]
    if payload.get("id") != envelope["event_id"]:
        raise Rejected([Violation("ENV-004", f"payload id {payload.get('id')!r} != event_id")])
    props = payload.get("properties")
    if not isinstance(props, dict):
        raise Rejected([Violation("ENV-003", "payload.properties must be an object")])
    if (props.get("updated"), props.get("time")) != (
        envelope["source_updated_ms"],
        envelope["event_time_ms"],
    ):
        raise Rejected(
            [Violation("ENV-005", "envelope timestamps disagree with payload properties")]
        )


def validate(envelope: Any, *, now: datetime) -> ValidEvent:
    """Return the normalized event, or raise Rejected listing every violated rule."""
    _validate_envelope(envelope)
    payload = envelope["payload"]
    props = payload["properties"]
    violations: list[Violation] = []

    # Geometry: GeoJSON order is [longitude, latitude, depth].
    geometry = payload.get("geometry")
    coords = geometry.get("coordinates") if isinstance(geometry, dict) else None
    if (
        not isinstance(geometry, dict)
        or geometry.get("type") != "Point"
        or not isinstance(coords, list)
        or len(coords) != 3
        or not all(_is_number(c) for c in coords)
    ):
        violations.append(Violation("GEO-001", "geometry must be a Point with 3 finite numbers"))
        longitude = latitude = depth = None
    else:
        longitude, latitude, depth = (float(c) for c in coords)
        if not _in_range(latitude, LATITUDE_RANGE):
            violations.append(Violation("GEO-002", f"latitude {latitude} outside -90..90"))
        if not _in_range(longitude, LONGITUDE_RANGE):
            violations.append(Violation("GEO-003", f"longitude {longitude} outside -180..180"))
        if not _in_range(depth, DEPTH_RANGE_KM):
            violations.append(Violation("GEO-004", f"depth {depth} km outside -10..1000"))

    event_time = _safe_datetime(envelope["event_time_ms"])
    if event_time is None or event_time < EARLIEST_EVENT_TIME:
        violations.append(
            Violation("TIME-001", f"event time {envelope['event_time_ms']} ms before 1900")
        )
    elif event_time > now + MAX_FUTURE:
        violations.append(
            Violation(
                "TIME-002", f"event time {event_time.isoformat()} more than 1 h in the future"
            )
        )
    if _safe_datetime(envelope["source_updated_ms"]) is None:
        violations.append(Violation("TIME-003", "updated is not a representable time"))
    elif envelope["source_updated_ms"] < envelope["event_time_ms"]:
        violations.append(Violation("TIME-003", "updated is earlier than the event time"))

    # A deletion only has to identify and order the event. USGS fills some deletions with
    # placeholders (magnitude -5 on uw deletions), which must not block the deletion: such
    # values are stored as unknown instead (DEF-027).
    status = review_status(props)
    deletion = status == "deleted"
    cleared: set[str] = set()

    magnitude = props.get("mag")
    if magnitude is not None and (
        not _is_number(magnitude) or not _in_range(magnitude, MAGNITUDE_RANGE)
    ):
        if deletion:
            cleared.add("mag")
        else:
            violations.append(Violation("MAG-001", f"magnitude {magnitude!r} outside -2..10.5"))

    if not isinstance(props.get("type"), str) or not props["type"]:
        violations.append(Violation("PROP-001", "type must be a non-empty string"))
    if status not in REVIEW_STATUSES:
        violations.append(Violation("PROP-002", f"status {props.get('status')!r} is not allowed"))

    # Optional fields still go into typed columns, so their types are checked.
    optional_types = {
        "felt": _is_int,
        "sig": _is_int,
        "cdi": _is_number,
        "mmi": _is_number,
        "magType": lambda v: isinstance(v, str),
        "place": lambda v: isinstance(v, str),
        "alert": lambda v: isinstance(v, str),
        "net": lambda v: isinstance(v, str),
        "url": lambda v: isinstance(v, str),
        "title": lambda v: isinstance(v, str),
        "tsunami": lambda v: v in (0, 1) and not isinstance(v, float),
    }
    for name, check in optional_types.items():
        value = props.get(name)
        if value is not None and not check(value):
            if deletion:
                cleared.add(name)
            else:
                violations.append(Violation("PROP-003", f"{name} has an invalid value {value!r}"))

    if violations:
        raise Rejected(violations)

    ids = tuple(dict.fromkeys([envelope["event_id"], *envelope["ids"]]))
    props = {k: v for k, v in props.items() if k not in cleared}
    magnitude = props.get("mag")
    location = parse_place(props.get("place"), longitude)
    return ValidEvent(
        event_id=envelope["event_id"],
        ids=ids,
        source_updated_ms=envelope["source_updated_ms"],
        event_time_ms=envelope["event_time_ms"],
        fields={
            "event_time": event_time,
            "event_time_ms": envelope["event_time_ms"],
            "source_updated": ms_to_datetime(envelope["source_updated_ms"]),
            "source_updated_ms": envelope["source_updated_ms"],
            "magnitude": float(magnitude) if magnitude is not None else None,
            "mag_type": props.get("magType"),
            "place": props.get("place"),
            "city": location.city,
            "region": location.region,
            "country": location.country,
            "longitude": longitude,
            "latitude": latitude,
            "depth_km": depth,
            "event_type": props["type"],
            "review_status": status,
            "tsunami": bool(props.get("tsunami") or 0),
            "felt": props.get("felt"),
            "cdi": float(props["cdi"]) if props.get("cdi") is not None else None,
            "mmi": float(props["mmi"]) if props.get("mmi") is not None else None,
            "alert": props.get("alert"),
            "sig": props.get("sig"),
            "net": props.get("net"),
            "url": props.get("url"),
            "title": props.get("title"),
            "revision_ids": list(ids),
        },
    )
