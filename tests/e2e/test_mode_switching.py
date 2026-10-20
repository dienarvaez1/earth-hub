"""Switching modes back and forth while the feed keeps changing loses no event and no
revision (REQ-MODE-013).

A mock USGS feed changes in every phase, including while each handover runs, when neither
mode is polling. After every switch, the first round of the mode taking over must pick up what
changed during the handover, and whatever serves the data must hold every event at its latest
version: the API in full mode, the bucket in direct mode. Two full cycles, full -> direct ->
full -> direct -> full, on real Kafka and PostgreSQL, with the bucket on moto.
"""

import json
import time
from datetime import UTC, datetime

import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from support.feed import revised

from earth_hub.api.main import create_app
from earth_hub.direct.handover import ReplayTopic, to_direct, to_full
from earth_hub.direct.reconcile import reconcile as reconcile_direct
from earth_hub.direct.runner import DirectPublisher, DirectRunner
from earth_hub.export import mode
from earth_hub.export.bucket import Bucket
from earth_hub.export.r2 import Exporter
from earth_hub.ingestion.poller import Poller
from earth_hub.ingestion.state import STATE_WINDOW, read_topic_since, state_from_messages
from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client

pytestmark = pytest.mark.e2e

BUCKET = "earth-hub-test"
DAY_FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


class Live:
    """The feed as USGS shows it, and what every reader should therefore see."""

    def __init__(self, feed, event):
        self.feed, self.event = feed, event
        self.expected: dict[str, dict] = {}  # preferred ID -> its latest version

    def new(self, event_id: str, **kwargs) -> dict:
        feature = self.event(event_id, **kwargs)
        self._show(feature)
        return feature

    def revise(self, event_id: str, **properties) -> None:
        self._show(revised(self.expected[event_id], by_s=60, **properties))

    def merge(self, survivor: str, absorbed: str) -> None:
        """USGS finds two reports are one event: the survivor lists both IDs, and the other
        leaves the feed."""
        feature = revised(self.expected[survivor], by_s=120)
        feature["properties"]["ids"] = f",{survivor},{absorbed},"
        self.feed.remove(absorbed)
        del self.expected[absorbed]
        self._show(feature)

    def _show(self, feature: dict) -> None:
        self.feed.show(feature)
        self.expected[feature["id"]] = feature

    def versions(self) -> dict[str, tuple]:
        return {
            event_id: (f["properties"]["updated"], f["properties"]["mag"])
            for event_id, f in self.expected.items()
        }


class Switching:
    def __init__(self, pipeline, live, s3, tmp_path):
        self.pipeline, self.live, self.s3, self.tmp = pipeline, live, s3, tmp_path
        self.bucket = Bucket(s3, BUCKET)
        self.api = TestClient(create_app(pipeline.db), base_url="http://testserver/api/v1/")
        self.direct_poller: Poller | None = None
        self.handovers: list[tuple[str, float]] = []
        self.runs = 0

    # Each mode's live round, as its long-running process does it.

    def full_round(self) -> None:
        assert self.pipeline.poll().status in ("published", "not_modified")
        self.pipeline.sync()

    def direct_round(self) -> None:
        assert self.direct_poller.run_round().status in ("published", "not_modified")

    # The handovers, as make mode-direct and make mode-full run them.

    def problems(self) -> list[str]:
        reports = (self.pipeline.reconcile(), self.pipeline.reconcile(backfill=True))
        return [f"{r.topic}: {r.lost} {r.broken_invariants}" for r in reports if not r.ok]

    def to_direct(self) -> None:
        started = time.monotonic()
        values = read_topic_since(
            self.pipeline.settings.kafka_bootstrap_servers,
            self.pipeline.topic,
            datetime.now(UTC) - STATE_WINDOW,
        )
        to_direct(
            self.bucket,
            self.pipeline.db,
            api=self.api,
            api_url="http://testserver/api/v1",
            sent=state_from_messages(values),
            problems=self.problems,
            local_dir=self.tmp / "runs",
        )
        self.handovers.append(("to direct", time.monotonic() - started))
        # make direct-run starts: a new process, its state from the bucket.
        self.runs += 1
        workdir = self.tmp / f"direct{self.runs}"
        workdir.mkdir()
        runner = DirectRunner(self.bucket, workdir=workdir, workers=4)
        state = runner.sent_state()
        self.direct_poller = Poller(
            UsgsClient(
                DAY_FEED, make_http_client(transport=self.live.feed.transport), sleep=lambda s: 0
            ),
            DirectPublisher(runner, source="feed", state=state),
            state,
            fallback_wait_s=60,
        )

    def to_full(self) -> None:
        started = time.monotonic()
        to_full(
            self.bucket,
            self.pipeline.db,
            api=self.api,
            api_url="http://testserver/api/v1",
            topic=ReplayTopic(
                self.pipeline.settings.kafka_bootstrap_servers, self.pipeline.backfill_topic
            ),
            wait=self.pipeline.sync,
            problems=self.problems,
            local_dir=self.tmp / "runs",
        )
        self.handovers.append(("to full", time.monotonic() - started))
        self.direct_poller = None
        self.pipeline.restart_poller()  # make ingest: the poller rebuilds its state

    # What each mode serves.

    def served_by_api(self) -> dict[str, tuple]:
        return {
            event_id: (_ms(item["source_updated"]), item["magnitude"])
            for event_id, item in self.pipeline.events().items()
            if item["review_status"] != "deleted"
        }

    def served_by_bucket(self) -> dict[str, tuple]:
        found = {}
        for name in self.bucket.keys("earthquakes/"):
            row = json.loads(self.bucket.get(name)[0])
            found[row["preferred_event_id"]] = (row["source_updated_ms"], row["magnitude"])
        return found


def _ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


@pytest.mark.req("REQ-MODE-013")
def test_switching_modes_back_and_forth_loses_no_event_or_revision(
    pipeline, feed, event, s3, tmp_path
):
    live = Live(feed, event)
    switch = Switching(pipeline, live, s3, tmp_path)

    for cycle in range(2):
        c = f"c{cycle}"
        # Full mode, live.
        live.new(f"{c}a", minutes_ago=50, mag=2.0)
        live.new(f"{c}b", minutes_ago=45, mag=3.1)
        switch.full_round()
        live.revise(f"{c}a", mag=2.2)
        switch.full_round()
        assert switch.served_by_api() == live.versions()

        # USGS keeps publishing while the handover runs; no one polls until it ends.
        live.new(f"{c}during1", minutes_ago=30)
        live.revise(f"{c}b", mag=3.3)
        switch.to_direct()
        assert mode.read(switch.bucket).mode == mode.DIRECT
        switch.direct_round()  # its first round catches up on the handover
        assert switch.served_by_bucket() == live.versions()

        # Direct mode, live: new events, revisions, a merge, and an unchanged feed.
        live.new(f"{c}c", minutes_ago=20, mag=1.4)
        live.revise(f"{c}during1", mag=4.0)
        switch.direct_round()
        live.merge(f"{c}c", f"{c}a")
        switch.direct_round()
        switch.direct_round()  # nothing new: a 304
        assert switch.served_by_bucket() == live.versions()
        assert reconcile_direct(switch.bucket).ok

        # Back to full mode, the feed changing during the handover again.
        live.new(f"{c}during2", minutes_ago=5)
        live.revise(f"{c}c", mag=1.9)
        switch.to_full()
        assert mode.read(switch.bucket).mode == mode.FULL
        switch.full_round()
        assert switch.served_by_api() == live.versions()

    # At the end: nothing lost anywhere, and everything consistent.
    assert len(live.expected) == 8  # 5 events a cycle, one merged into another
    assert switch.problems() == []
    record = Exporter(switch.api, switch.bucket, api_url="x", local_dir=tmp_path / "runs").run()
    assert record.status == "succeeded", record.errors
    assert switch.served_by_bucket() == live.versions()
    print("handovers:", ", ".join(f"{name} {seconds:.1f} s" for name, seconds in switch.handovers))
