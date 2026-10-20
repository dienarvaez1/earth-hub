"""Locust users for `make load` / `make load-ui` (scripts/load_test.py seeds the database).

Two kinds of users, as the API is used:
- browsers (3 in 4): the latest events, strong ones, one event by ID, a day's summaries;
- analysts (1 in 4): wider statistics, place searches, deep pages and the export, which are
  the heavy queries.

Each request is named after its endpoint and tagged with it (`--tags stats`), so the report
groups requests by endpoint rather than by URL. A response that is not 200 is a failure.

Run by scripts/load_test.py, which sets LOAD_EVENTS (how many events it seeded) and LOAD_NOW
(when): times are chosen relative to it, so every query finds data.
"""

import os
import random
from datetime import datetime, timedelta

from locust import HttpUser, between, tag, task

EVENTS = int(os.environ.get("LOAD_EVENTS", "140000"))
NOW = datetime.fromisoformat(os.environ.get("LOAD_NOW", datetime.now().astimezone().isoformat()))


def ago(**delta) -> str:
    return (NOW - timedelta(**delta)).isoformat()


class ApiUser(HttpUser):
    abstract = True

    def get(self, name: str, path: str, **params):
        with self.client.get(path, params=params, name=name, catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"{response.status_code}: {response.text[:200]}")


class Browser(ApiUser):
    weight = 3
    wait_time = between(1, 3)

    @tag("list")
    @task(5)
    def latest(self):
        self.get("/earthquakes", "/earthquakes", limit=20)

    @tag("list")
    @task(3)
    def strong(self):
        self.get("/earthquakes?min_magnitude", "/earthquakes", min_magnitude=4, limit=20)

    @tag("lookup")
    @task(3)
    def one_event(self):
        self.get("/earthquakes/{event_id}", f"/earthquakes/ld{random.randrange(EVENTS):07d}")

    @tag("summary")
    @task(1)
    def last_day(self):
        self.get("/earthquakes/summary/hourly", "/earthquakes/summary/hourly", end_time=ago())

    @tag("health")
    @task(1)
    def health(self):
        self.get("/health", "/health")


class Analyst(ApiUser):
    weight = 1
    wait_time = between(2, 5)

    @tag("stats")
    @task(2)
    def month_stats(self):
        self.get("/earthquakes/stats", "/earthquakes/stats", start_time=ago(days=30))

    @tag("stats")
    @task(1)
    def year_stats(self):
        self.get("/earthquakes/stats", "/earthquakes/stats")

    @tag("summary")
    @task(2)
    def magnitudes(self):
        self.get(
            "/earthquakes/summary/magnitude",
            "/earthquakes/summary/magnitude",
            start_time=ago(days=90),
        )

    @tag("summary")
    @task(1)
    def week_by_hour(self):
        self.get(
            "/earthquakes/summary/hourly",
            "/earthquakes/summary/hourly",
            start_time=ago(days=7),
            end_time=ago(),
        )

    @tag("list")
    @task(2)
    def place_search(self):
        place = random.choice(["Alaska", "CA", "Chile", "Fiji", "Hawaii"])
        self.get("/earthquakes?place", "/earthquakes", place=place, limit=100)

    @tag("list")
    @task(1)
    def deep_page(self):
        offset = random.randrange(0, EVENTS // 2, 100)
        self.get("/earthquakes?offset", "/earthquakes", limit=100, offset=offset)

    @tag("export")
    @task(1)
    def export_page(self):
        self.get(
            "/export/earthquakes",
            "/export/earthquakes",
            after_id=random.randrange(EVENTS),
            limit=1000,
        )
