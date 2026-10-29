"""Load-test the API with Locust: `make load` (headless, a report) or `make load-ui` (the web UI).

    uv run python scripts/load_test.py [--ui] [--users 50] [--spawn-rate 10] [--run-time 60s]
                                       [--events 140000]

1. Creates a temporary database, migrates it, and seeds it with synthetic events through the
   real `process_batch`: a year of history like the live table (about 140,000 events, real
   magnitudes and places spread out, some deleted, some with a second ID). No Kafka, never the main
   database.
2. Serves the API from it with uvicorn on a free port of 127.0.0.1.
3. Runs Locust (tests/load/locustfile.py) against it: headless, writing
   reports/load-report.html and reports/load_*.csv, or with the web UI on
   http://127.0.0.1:8089 until stopped.
4. Stops the API and drops the database, whatever happened.

Locust runs as a separate process: it patches Python's networking (gevent) when imported,
which must not leak into anything else. Needs `make up` (PostgreSQL).
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from pathlib import Path

import httpx
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
LOCUSTFILE = ROOT / "tests" / "load" / "locustfile.py"
REPORTS = ROOT / "reports"
EVENTS = 140_000
BATCH = 2000
MOMENT_FROM = 4  # magnitudes from here get a moment magnitude type (mww), as at USGS
PLACES = [
    ("86 km E of Ugashik, Alaska", -155.6, 57.4),
    ("10 km NW of The Geysers, CA", -122.8, 38.8),
    ("5 km S of Volcano, Hawaii", -155.2, 19.4),
    ("Fiji region", 178.1, -17.9),
    ("45 km SW of Coquimbo, Chile", -71.6, -30.2),
    ("12 km NNE of Ridgecrest, CA", -117.6, 35.7),
    ("Kepulauan Talaud, Indonesia", 126.9, 4.3),
    ("20 km E of Anchorage, Alaska", -149.6, 61.2),
]


def event_id(i: int) -> str:
    """The synthetic events' IDs, which the locustfile asks for by number."""
    return f"ld{i:07d}"


def feature(i: int, now: datetime, count: int) -> dict:
    """Event i of a year ending now: magnitudes mostly small, as in the real feed."""
    when = now - timedelta(days=365) + timedelta(seconds=i * (365 * 86400 / count))
    place, lon, lat = PLACES[i % len(PLACES)]
    t = int(when.timestamp() * 1000)
    magnitude = round(((i * 7919) % 1000) / 1000 * 3.5 + (2.5 if i % 25 == 0 else 0) - 0.5, 2)
    ids = [event_id(i)] + ([f"dup{i:07d}"] if i % 97 == 0 else [])
    return {
        "type": "Feature",
        "id": event_id(i),
        "geometry": {
            "type": "Point",
            "coordinates": [lon + (i % 50) / 100, lat - (i % 30) / 100, 5.0 + i % 40],
        },
        "properties": {
            "mag": magnitude,
            "magType": "ml" if magnitude < MOMENT_FROM else "mww",
            "place": place,
            "time": t,
            "updated": t + 600_000,
            "status": "deleted" if i % 211 == 0 else ("reviewed" if i % 3 else "automatic"),
            "type": "earthquake" if i % 40 else "quarry blast",
            "tsunami": 0,
            "sig": int(max(magnitude, 0) * 60),
            "net": "ak" if "Alaska" in place else "us",
            "ids": "," + ",".join(ids) + ",",
            "url": f"https://earthquake.usgs.gov/earthquakes/eventpage/{event_id(i)}",
            "title": f"M {magnitude} - {place}",
        },
    }


def seed(engine, count: int, now: datetime) -> None:
    from earth_hub.database.repository import IncomingMessage, process_batch
    from earth_hub.ingestion.events import build_message

    for start in range(0, count, BATCH):
        messages = [
            IncomingMessage(
                "load",
                0,
                i,
                event_id(i).encode(),
                build_message(
                    feature(i, now, count), fetched_at=now, feed_generated_ms=None
                ).to_json(),
            )
            for i in range(start, min(start + BATCH, count))
        ]
        with engine.begin() as conn:
            process_batch(conn, messages, now=now)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run(  # noqa: PLR0915 (to refactor: docs/progress/plan.md)
    *, ui: bool, users: int, spawn_rate: float, run_time: str, events: int, prefix: str = "load"
) -> int:
    """Seed, serve and load-test; returns Locust's exit code (1 if any request failed)."""
    from alembic import command
    from alembic.config import Config

    from earth_hub.config import get_settings
    from earth_hub.database.engine import make_engine

    settings = get_settings()
    admin = make_engine(settings)
    name = f"earth_hub_load_{uuid.uuid4().hex[:8]}"
    server = None
    with admin.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        config = Config(str(ROOT / "alembic.ini"))
        config.attributes["database_url"] = settings.database_url(name)
        command.upgrade(config, "head")
        engine = make_engine(settings, name)
        now = datetime.now(UTC)
        started = time.monotonic()
        seed(engine, events, now)
        engine.dispose()
        print(f"load: seeded {events:,} events in {time.monotonic() - started:.0f} s", flush=True)

        port = free_port()
        server = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "earth_hub.api.main:app"]
            + ["--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
            env={**os.environ, "POSTGRES_DB": name},
        )
        base = f"http://127.0.0.1:{port}/api/v1"
        deadline = time.monotonic() + 30
        while True:
            try:
                if httpx.get(f"{base}/health", timeout=2).status_code == HTTPStatus.OK:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline or server.poll() is not None:
                raise RuntimeError("the API did not start")
            time.sleep(0.3)

        REPORTS.mkdir(exist_ok=True)
        locust = [sys.executable, "-m", "locust", "-f", str(LOCUSTFILE), "--host", base]
        env = {**os.environ, "LOAD_EVENTS": str(events), "LOAD_NOW": now.isoformat()}
        if ui:
            print("load: Locust's web UI on http://127.0.0.1:8089 (Ctrl-C to stop)", flush=True)
            locust += ["--web-host", "127.0.0.1", "--web-port", "8089"]
            locust += ["--users", str(users), "--spawn-rate", str(spawn_rate)]
        else:
            locust += ["--headless", "--users", str(users), "--spawn-rate", str(spawn_rate)]
            locust += ["--run-time", run_time, "--only-summary", "--stop-timeout", "10"]
            locust += ["--html", str(REPORTS / f"{prefix}-report.html")]
            locust += ["--csv", str(REPORTS / prefix)]
        try:
            return subprocess.run(locust, env=env, check=False).returncode
        except KeyboardInterrupt:
            return 0
    finally:
        if server is not None:
            server.terminate()
            server.wait(timeout=30)
        with admin.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--ui", action="store_true", help="Locust's web UI instead of a report")
    parser.add_argument("--users", type=int, default=50)
    parser.add_argument("--spawn-rate", type=float, default=10)
    parser.add_argument("--run-time", default="60s")
    parser.add_argument("--events", type=int, default=EVENTS)
    args = parser.parse_args(argv)
    code = run(
        ui=args.ui,
        users=args.users,
        spawn_rate=args.spawn_rate,
        run_time=args.run_time,
        events=args.events,
    )
    if not args.ui:
        print(json.dumps({"report": str(REPORTS / "load-report.html"), "exit": code}))
    return code


if __name__ == "__main__":
    sys.exit(main())
