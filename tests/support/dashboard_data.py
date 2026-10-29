"""A fixed dataset for the dashboard's tests, with answers worked by hand in
tests/unit/test_dashboard_queries.py, and a snapshot built from it by the exporter's code."""

from datetime import UTC, datetime
from pathlib import Path

from earth_hub.export.r2 import encode
from earth_hub.export.snapshot import SnapshotFile
from support.export import make_row


def event(row_id, when, magnitude, country, region, **changes):
    moment = datetime.fromisoformat(when).replace(tzinfo=UTC)
    return make_row(
        row_id,
        event_time=moment.isoformat().replace("+00:00", "Z"),
        event_time_ms=int(moment.timestamp() * 1000),
        magnitude=magnitude,
        country=country,
        region=region,
        place=f"place {row_id}",
        **changes,
    )


# 29 February and the midnights around it, a Sunday at the end of an ISO week, bin edges,
# a negative magnitude, missing values, a deletion and another event type.
EVENTS = [
    event(1, "2024-02-28T23:59:59.999", 2.49, "United States", "Alaska"),
    event(2, "2024-02-29T00:00:00", 2.5, "United States", "Alaska"),
    event(3, "2024-02-29T12:00:00", 4.5, "Mexico", "Baja California"),
    event(4, "2024-03-01T00:00:00", None, None, None),
    event(5, "2024-03-04T06:00:00", -0.43, "United States", "California"),
    event(6, "2024-03-10T23:00:00", 5.2, "Japan", None),  # a Sunday
    event(7, "2024-03-10T08:00:00", 3.0, "United States", "Alaska", review_status="deleted"),
    event(8, "2024-04-02T10:00:00", 1.0, "United States", "Alaska", event_type="quarry blast"),
]


def build(directory: Path, rows: list[dict]) -> Path:
    snapshot = SnapshotFile()
    try:
        for row in rows:
            snapshot.add(encode(row))
        path = directory / "earthquakes.parquet"
        path.write_bytes(snapshot.build())
    finally:
        snapshot.close()
    return path


def synthetic(directory: Path, rows: int) -> Path:
    """A snapshot of `rows` made-up events over 2025, deterministic, in the snapshot's own
    column types, gathered around hotspots as real ones are: for measuring the page at sizes
    beyond today's table."""
    import duckdb

    from earth_hub.export.snapshot import COLUMNS

    places = [
        ("United States", "Alaska"),
        ("United States", "California"),
        ("United States", "Hawaii"),
        ("Japan", None),
        ("Indonesia", None),
        ("Chile", None),
        ("Mexico", "Baja California"),
        ("Philippines", None),
        (None, None),
        ("Tonga", None),
        ("United States", "Texas"),
        ("Peru", None),
    ]
    countries = "[" + ", ".join("NULL" if c is None else f"'{c}'" for c, _ in places) + "]"
    regions = "[" + ", ".join("NULL" if r is None else f"'{r}'" for _, r in places) + "]"
    step_ms = 365 * 86_400_000 // rows
    values = {
        "key": "'syn' || lpad(CAST(i AS VARCHAR), 7, '0')",
        "preferred_event_id": "'syn' || i",
        "event_time": "to_timestamp((1735689600000 + i * $step) / 1000.0)",
        "event_time_ms": "1735689600000 + i * $step",
        "source_updated": "to_timestamp((1735689600000 + i * $step) / 1000.0 + 300)",
        "source_updated_ms": "1735689600000 + i * $step + 300000",
        # Each unit of magnitude rarer than the last, as Gutenberg and Richter found: about 1 in
        # 450 is 6 or more (in USGS's real feed, about 1 in 1,100).
        "magnitude": "CASE WHEN h % 97 = 0 THEN NULL ELSE $magnitude END",
        "mag_type": "'ml'",
        "place": "'synthetic place ' || (h % 1000)",
        "city": "'Town ' || (h % 1000)",
        "region": f"{regions}[(h % 12) + 1]",
        "country": f"{countries}[(h % 12) + 1]",
        # Around 40 hotspots, 4 degrees across, as earthquakes gather along plate boundaries:
        # spread evenly over the globe, nearly every event would have a map cell of its own.
        "longitude": "least(180, greatest(-180, (h % 40) * 47 % 360 - 180"
        " + ((h // 7) % 400) / 100.0 - 2))",
        "latitude": "(h % 40) * 29 % 140 - 70 + ((h // 11) % 400) / 100.0 - 2",
        "depth_km": "((h // 13) % 70000) / 100.0",
        "event_type": "CASE WHEN h % 50 = 0 THEN 'quarry blast' ELSE 'earthquake' END",
        "review_status": "CASE WHEN h % 200 = 0 THEN 'deleted' ELSE 'reviewed' END",
        "tsunami": "h % 1000 = 0",
        "felt": "NULL",
        "cdi": "NULL",
        "mmi": "NULL",
        "alert": "NULL",
        "sig": "(h % 900)",
        "net": "'us'",
        "url": "'https://earthquake.usgs.gov/earthquakes/eventpage/syn' || i",
        "title": "'M ' || $magnitude || ' - synthetic'",
        "revision_ids": "['syn' || i]",
        "revision_count": "1",
        "first_seen_at": "to_timestamp((1735689600000 + i * $step) / 1000.0 + 60)",
        "last_applied_at": "to_timestamp((1735689600000 + i * $step) / 1000.0 + 360)",
    }
    assert list(values) == list(COLUMNS)
    magnitude = "least(9.0, round(0.5 - 0.9 * ln(1 - (h % 100000) / 100000.0), 2))"
    select = ", ".join(
        f"CAST({values[c].replace('$magnitude', magnitude)} AS {kind}) AS {c}"
        for c, kind in COLUMNS.items()
    )
    path = directory / f"synthetic-{rows}.parquet"
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(
        f"COPY (SELECT {select} FROM ("
        "SELECT i, CAST(hash(i) % 1000000007 AS BIGINT) AS h FROM range(1, $rows + 1) t(i)"
        f")) TO '{path}' (FORMAT parquet, COMPRESSION zstd)",
        {"rows": rows, "step": step_ms},
    )
    con.close()
    return path
