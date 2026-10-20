"""The dashboard report examples in docs/technical/sql-examples.md, run on the dashboard's
fixed dataset with DuckDB, as the page runs them, and compared with the results the
document shows."""

import pytest
from support.analytics import QUERIES, Dashboard
from support.dashboard_data import EVENTS, build
from support.doc_examples import cell, sql_examples

EXAMPLES = [e for e in sql_examples() if e.engine == "dashboard"]


@pytest.fixture(scope="module")
def dash(tmp_path_factory):
    dashboard = Dashboard(build(tmp_path_factory.mktemp("snapshot"), EVENTS))
    yield dashboard
    dashboard.close()


def test_the_document_has_an_example_of_every_filtered_report():
    filtered = {p.stem for p in QUERIES.glob("*.sql") if "FROM filtered" in p.read_text()}
    assert filtered and filtered <= {e.report for e in EXAMPLES}


@pytest.mark.req("REQ-DOC-003")
@pytest.mark.parametrize("example", EXAMPLES, ids=lambda e: f"line {e.line}")
def test_each_report_gives_the_documented_result(dash, example):
    dash.set_filter(**example.filters)
    rows = dash.query(example.report)
    columns = list(rows[0]) if rows else example.columns
    assert columns == example.columns
    assert [[cell(v) for v in row.values()] for row in rows] == example.rows
