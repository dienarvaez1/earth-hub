"""GET /api/v1/export/earthquakes over the fixed dataset, compared with the table itself."""

from datetime import datetime

import pytest
from sqlalchemy import select

from earth_hub.database.models import Earthquake

from .dataset import SPECS

pytestmark = [pytest.mark.integration, pytest.mark.req("REQ-EXP-001")]

COLUMNS = [c.name for c in Earthquake.__table__.c]
EVENTS = len({s.event_id for s in SPECS}) - 1  # two of the messages are merged into one


def all_pages(client, limit):
    rows, after, pages = [], 0, 0
    while after is not None:
        response = client.get(
            "/api/v1/export/earthquakes", params={"after_id": after, "limit": limit}
        )
        assert response.status_code == 200
        body = response.json()
        rows += body["rows"]
        after = body["next_after_id"]
        pages += 1
    return rows, pages


def table_rows(engine):
    with engine.connect() as conn:
        return [
            dict(r) for r in conn.execute(select(Earthquake.__table__).order_by("id")).mappings()
        ]


def as_stored(value, like):
    """A JSON value converted back to the type of the stored one."""
    return datetime.fromisoformat(value) if isinstance(like, datetime) else value


def test_every_row_and_column_is_returned_exactly(client, seeded_engine):
    exported, _ = all_pages(client, limit=1000)
    stored = table_rows(seeded_engine)
    assert len(exported) == len(stored) == EVENTS
    for got, row in zip(exported, stored, strict=True):
        assert list(got) == COLUMNS  # every column, in table order
        for column, value in row.items():
            converted = as_stored(got[column], value)
            assert converted == value, column
            assert type(converted) is type(value), column


def test_deleted_events_are_included(client):
    exported, _ = all_pages(client, limit=1000)
    assert any(r["review_status"] == "deleted" for r in exported)


@pytest.mark.parametrize("limit", [1, 3, 7])
def test_paging_by_id_returns_each_row_once(client, limit):
    exported, pages = all_pages(client, limit)
    ids = [r["id"] for r in exported]
    assert ids == sorted(set(ids)) and len(ids) == EVENTS
    assert pages == len(ids) // limit + 1


def test_after_the_last_row_the_page_is_empty(client):
    body = client.get("/api/v1/export/earthquakes", params={"after_id": 10**12}).json()
    assert body == {"rows": [], "next_after_id": None}


@pytest.mark.parametrize(
    "params",
    [{"limit": 0}, {"limit": 5001}, {"after_id": -1}, {"after_id": 2**63}, {"limit": "x"}],
)
def test_invalid_paging_is_rejected(client, params):
    response = client.get("/api/v1/export/earthquakes", params=params)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_parameters"


def test_an_empty_table_exports_nothing(empty_client):
    body = empty_client.get("/api/v1/export/earthquakes").json()
    assert body == {"rows": [], "next_after_id": None}
