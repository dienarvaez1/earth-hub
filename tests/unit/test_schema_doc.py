"""docs/schema.md against the models: every table, every column in order, with its type and
nullability."""

import re
from pathlib import Path

import pytest
from sqlalchemy.dialects import postgresql

from earth_hub.database.models import Base

DOC = Path(__file__).resolve().parents[2] / "docs" / "schema.md"
# The names the page uses for PostgreSQL's types.
TYPES = {"TIMESTAMP WITH TIME ZONE": "timestamptz", "TEXT[]": "text[]"}
ROW = re.compile(r"^\| `(\w+)` \| ([^|]+) \| (yes|no) \|", re.M)


def documented() -> dict[str, list[tuple[str, str, bool]]]:
    """{table: [(column, type, nullable)]}, from each '### table' section."""
    sections = re.split(r"^### (\w+)\n", DOC.read_text(), flags=re.M)[1:]
    return {
        name: [(c, t.strip(), null == "yes") for c, t, null in ROW.findall(body)]
        for name, body in zip(sections[::2], sections[1::2], strict=True)
    }


def defined() -> dict[str, list[tuple[str, str, bool]]]:
    dialect = postgresql.dialect()
    tables = {}
    for table in Base.metadata.sorted_tables:
        tables[table.name] = [
            (c.name, TYPES.get(t := c.type.compile(dialect=dialect), t.lower()), c.nullable)
            for c in table.columns
        ]
    return tables


@pytest.mark.req("REQ-DOC-004")
def test_every_table_is_documented():
    assert set(documented()) == set(defined())


@pytest.mark.req("REQ-DOC-004")
@pytest.mark.parametrize("table", sorted(defined()))
def test_every_column_is_documented_in_order_with_its_type(table):
    assert documented()[table] == defined()[table]
