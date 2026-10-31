"""Milestone 9 end to end, on real Kafka and PostgreSQL with the bucket on moto: a full-mode host
that stops, direct mode taking over, and another host claiming the bucket with its database
rebuilt from it; and a planned move between hosts. The reference database is the one writer
that never stopped (test_mode_handover.py's harness).

What must hold: no event and no revision lost, no record twice, the new host's database
exactly the bucket (the handover's export dry run), and the old host unable to write again."""

from contextlib import contextmanager
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text

from e2e.test_mode_handover import START, RoundTrip, reference, s3, tables  # noqa: F401 (fixtures)
from earth_hub.api.main import create_app
from earth_hub.database.models import Earthquake
from earth_hub.database.repository import export_cut
from earth_hub.direct.handover import ReplayTopic, read_standby, to_full
from earth_hub.direct.standby import Cut, Service
from earth_hub.direct.takeover import take_over
from earth_hub.export import mode
from earth_hub.export.mode import LEASE_TTL, NotOwner
from earth_hub.export.r2 import Exporter
from earth_hub.ingestion.state import SentState
from earth_hub.reconcile import INVARIANTS

pytestmark = pytest.mark.e2e

API = "http://testserver/api/v1"


class Hosts(RoundTrip):
    """RoundTrip, with the exporting service of a host over a database."""

    def exporter(self, engine, machine: str | None, at) -> Exporter:
        return Exporter(
            TestClient(create_app(engine), base_url=API + "/"),
            self.bucket,
            api_url=API,
            workers=4,
            clock=lambda: at,
            local_dir=self.tmp / "runs",
            machine=machine,
        )

    def service_export(self, engine, machine: str, at) -> None:
        @contextmanager
        def cut():
            with export_cut(engine) as conn:
                changed = conn.execute(select(func.max(Earthquake.last_applied_at))).scalar()
                yield Cut(read_standby(conn), SentState(), True, changed)

        service = Service(
            self.bucket,
            machine=machine,
            make_exporter=lambda: self.exporter(engine, machine, at),
            cut=cut,
            last_change=lambda: at,
            interval=timedelta(0),
            clock=lambda: at,
        )
        assert service.step() == "exported"

    def claim(self, engine, machine: str, topic: str, *, rebuild: bool):
        def problems() -> list[str]:
            with engine.connect() as conn:
                return [n for n, q in INVARIANTS.items() if conn.execute(text(q)).scalar()]

        return to_full(
            self.bucket,
            engine,
            api=TestClient(create_app(engine), base_url=API + "/"),
            api_url=API,
            topic=ReplayTopic(self.pipeline.settings.kafka_bootstrap_servers, topic),
            wait=lambda: None,
            problems=problems,
            local_dir=self.tmp / "runs",
            machine=machine,
            rebuild=rebuild,
        )


@pytest.fixture
def hosts(pipeline, reference, s3, tmp_path, make_envelope, make_feature):  # noqa: F811
    return Hosts(pipeline, reference, s3, tmp_path, make_envelope, make_feature)


@pytest.fixture
def new_host(make_database, make_alembic_config, settings):
    """Another host's database: empty, migrated."""
    from alembic import command

    from earth_hub.database.engine import make_engine

    name = make_database()
    command.upgrade(make_alembic_config(settings.database_url(name)), "head")
    engine = make_engine(settings, name)
    yield engine
    engine.dispose()


def versions(engine) -> set[tuple]:
    """Each event's latest version: what must not be lost or doubled, whatever the times."""
    with engine.connect() as conn:
        rows = conn.execute(
            select(
                Earthquake.preferred_event_id,
                Earthquake.source_updated_ms,
                Earthquake.review_status,
                Earthquake.magnitude,
                Earthquake.revision_ids,
            )
        ).all()
    found = [(r[0], r[1], r[2], r[3], tuple(r[4])) for r in rows]
    assert len(found) == len(set(found))  # no record twice
    return set(found)


@pytest.mark.req("REQ-MODE-020", "REQ-MODE-021", "REQ-MODE-022")
def test_a_host_that_stops_is_replaced_and_a_new_host_rebuilds_from_the_bucket(
    hosts, new_host, make_temp_topic
):
    h = hosts
    h.full([h.feature("ak1"), h.feature("us2", place="Fiji region")])
    h.full([h.feature("ak1", mag=3.0, updated=1792137760000), h.feature("ci3")])
    exported_at = START + timedelta(hours=1)
    h.service_export(h.pipeline.db, "host-a", exported_at)
    # Stored by host-a after its last export, then it stops: lost with it, unless refetched.
    tail = [h.feature("us5"), h.feature("ci3", mag=1.9, updated=1792137790000)]
    h.full(tail)

    took = take_over(h.bucket, clock=lambda: exported_at + LEASE_TTL + timedelta(seconds=1))
    assert took.previous_owner == "host-a" and took.rolled_back == 0
    runner = h.runner()
    # Catch-up fetches the tail again from USGS (here: the same messages, later).
    runner.process(tail, now=h.now())
    h.direct(runner, [h.feature("nc4"), h.feature("us2b", ["us2b", "us2"], updated=1792137800000)])
    h.direct(runner, [h.feature("ak1", status="deleted", updated=1792137900000)])

    # The returning host-a may not write any more.
    with pytest.raises(mode.WrongMode):
        h.exporter(h.pipeline.db, "host-a", exported_at + timedelta(hours=2)).run()

    result = h.claim(new_host, "host-b", make_temp_topic(), rebuild=True)
    assert result.replayed == 0
    marker = mode.read(h.bucket)
    assert (marker.mode, marker.owner) == (mode.FULL, "host-b")
    assert versions(new_host) == versions(h.reference)  # nothing lost, nothing doubled

    # host-b goes on in full mode; host-a's exports are refused, even with full mode back.
    h.service_export(new_host, "host-b", exported_at + timedelta(hours=3))
    with pytest.raises(NotOwner):
        h.exporter(h.pipeline.db, "host-a", exported_at + timedelta(hours=3)).run()


@pytest.mark.req("REQ-MODE-024")
def test_a_planned_move_gives_the_new_host_exactly_the_one_writers_data(
    hosts, new_host, make_temp_topic
):
    """host-a releases (make to-direct), direct mode writes for a while, host-b claims with
    an empty database: it holds what one writer that never moved holds, every column and
    processing time included."""
    h = hosts
    h.full([h.feature("ak1"), h.feature("us2", place="Fiji region")])
    h.full([h.feature("ak1", mag=3.0, updated=1792137760000), h.feature("ci3")])
    h.to_direct()  # host-a's release
    assert mode.read(h.bucket).owner is None
    runner = h.runner()
    h.direct(runner, [h.feature("ci3", mag=1.2, updated=1792137701000), h.feature("nc4")])
    h.direct(runner, [h.feature("nc4", ["nc4", "ak1"], updated=1792137790000), b"not json"])

    h.claim(new_host, "host-b", make_temp_topic(), rebuild=True)
    assert mode.read(h.bucket).owner == "host-b"
    mine, theirs = tables(new_host), tables(h.reference)
    assert mine["earthquakes"] == theirs["earthquakes"]
    assert mine["ids"] == theirs["ids"]
