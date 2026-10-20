"""The fixed API test dataset: 22 messages that become 20 events (two are merged)."""

import copy
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from earth_hub.database.repository import IncomingMessage, process_message
from earth_hub.ingestion.events import build_message

T0 = datetime(2026, 10, 10, tzinfo=UTC)
PROCESSED_AT = T0 + timedelta(days=3)
# The API's "now": 23:30 on day one, so the default hourly window is exactly T0..T0+24h.
API_NOW = T0 + timedelta(hours=23, minutes=30)


@dataclass(frozen=True)
class Spec:
    event_id: str
    mag: float | None
    mag_type: str | None
    place: str | None
    hours: float  # event time, in hours after T0
    depth: float = 10.0
    event_type: str = "earthquake"
    status: str = "reviewed"
    lon: float = 0.0
    lat: float = 0.0


SPECS = [
    Spec("us01", 4.4, "mb", "100 km S of Tokyo, Japan", 0.25, depth=35, lon=139.7, lat=34.7),
    Spec("us17", 4.4, "mb", "Fiji", 0.25, depth=500, lon=178.0, lat=-17.8),  # ties with us01
    Spec("us02", 6.1, "mww", "Tonga Islands", 1.5, depth=120),
    Spec("ak03", 1.5, "ml", "10 km N of Anchorage, Alaska", 1.75, depth=20, status="automatic"),
    Spec("ak04", 2.0, "ml", "50 km W of Anchorage, Alaska", 3.0, depth=15, status="automatic"),
    Spec("ci05", -0.5, "ml", "Central California", 3.5, depth=5),
    Spec("ci06", 0.2, "ml", "Central California", 5.0, depth=3, status="automatic"),
    Spec("ci07", 0.9, "md", "Northern California", 5.0, depth=2),  # same time as ci06
    Spec("nc08", 1.0, "md", "The Geysers, CA", 7.2, depth=-0.5),
    Spec("uw09", 1.8, "md", "Mount St. Helens", 8.0, depth=-0.36, event_type="explosion"),
    Spec("uw10", 2.5, "md", "5 km E of Quarry_Town", 9.0, depth=0.1, event_type="quarry blast"),
    Spec("hv11", 3.3, "ml", "Hawaii 50% zone", 10.0, depth=1.0, status="automatic"),
    Spec("us12", 5.0, "mb", "Chile", 12.0, depth=33),
    Spec("us13", 6.99, "mww", "Kamchatka", 14.0, depth=40),
    Spec("us14", 7.0, "mww", "Papua New Guinea", 15.0, depth=60),
    Spec("us15", 8.2, "mww", "Peru", 20.0, depth=100),
    Spec("us16", None, None, None, 22.0, status="automatic"),
    Spec("us18", 3.0, "ml", "Deleted event", 23.0, status="deleted"),
    Spec("nn19", 2.2, "ml", "Nevada", 25.5, event_type="explosion", status="automatic"),
    # Two reports of one quake, merged by a third message (below).
    Spec("ak20", 3.1, "ml", "Alaska Peninsula", 26.0, status="automatic"),
    Spec("us20", 3.2, "mb", "Alaska Peninsula", 26.0),
]
MERGED_US20 = {"mag": 3.4, "mag_type": "mww"}

# The events as stored: ak20 is folded into us20, which takes the merging message's data.
EVENTS = [
    replace(s, **MERGED_US20) if s.event_id == "us20" else s for s in SPECS if s.event_id != "ak20"
]
VISIBLE = [s for s in EVENTS if s.status != "deleted"]  # us18 is hidden by default
EVENT_COUNT = len(EVENTS)
VISIBLE_COUNT = len(VISIBLE)


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def feature(template: dict, spec: Spec, ids: list[str], updated_offset_s: int = 60) -> dict:
    f = copy.deepcopy(template)
    time = T0 + timedelta(hours=spec.hours)
    f["id"] = spec.event_id
    f["geometry"]["coordinates"] = [spec.lon, spec.lat, spec.depth]
    f["properties"].update(
        mag=spec.mag,
        magType=spec.mag_type,
        place=spec.place,
        type=spec.event_type,
        status=spec.status,
        time=ms(time),
        updated=ms(time + timedelta(seconds=updated_offset_s)),
        ids="," + ",".join(ids) + ",",
        net=spec.event_id[:2],
        code=spec.event_id[2:],
        title=f"M {spec.mag} - {spec.place}",
        felt=None,
        tsunami=0,
    )
    return f


def seed(engine, template: dict) -> None:
    features = [feature(template, s, [s.event_id]) for s in SPECS]
    us20 = next(s for s in SPECS if s.event_id == "us20")
    merged = feature(template, us20, ["us20", "ak20"], updated_offset_s=600)
    merged["properties"].update(mag=MERGED_US20["mag"], magType=MERGED_US20["mag_type"])
    features.append(merged)

    with engine.begin() as conn:
        for offset, f in enumerate(features):
            message = build_message(f, fetched_at=PROCESSED_AT, feed_generated_ms=None)
            result = process_message(
                conn,
                IncomingMessage("earthquakes.raw", 0, offset, message.key, message.to_json()),
                now=PROCESSED_AT,
            )
            assert result.outcome == "applied", (f["id"], result)


# For the documented examples only (the `documented_engine` fixture), after the messages above:
# a redelivery under a new offset, an older revision, and an event outside the latitude range.
# (spec, seconds from event time to `updated`, the outcome expected)
_BY_ID = {s.event_id: s for s in SPECS}
NOT_APPLIED = [
    (_BY_ID["us02"], 60, "duplicate"),
    (_BY_ID["us12"], 30, "stale"),
    (Spec("us99", 4.0, "mb", "Nowhere", 2.0, lat=95.0), 60, "rejected"),
]
