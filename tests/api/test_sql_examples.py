"""The PostgreSQL queries in docs/sql-examples.md, run on the documented dataset and compared
with the results the document shows."""

import pytest
from sqlalchemy import text
from support.doc_examples import cell, sql_examples

pytestmark = pytest.mark.integration

EXAMPLES = [e for e in sql_examples() if e.engine == "postgres"]


def test_the_document_has_postgres_examples():
    assert len(EXAMPLES) >= 5


@pytest.mark.req("REQ-DOC-003")
@pytest.mark.parametrize("example", EXAMPLES, ids=lambda e: f"line {e.line}")
def test_each_query_gives_the_documented_result(documented_engine, example):
    with documented_engine.connect() as conn:
        conn.execute(text("SET TIME ZONE 'UTC'"))
        result = conn.execute(text(example.sql))
        columns = list(result.keys())
        rows = [[cell(v) for v in row] for row in result]
    assert columns == example.columns
    assert rows == example.rows
