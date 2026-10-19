"""Runs the dashboard's SQL (dashboard/queries/) the way the page does, with DuckDB."""

from pathlib import Path

import duckdb

QUERIES = Path(__file__).resolve().parents[2] / "dashboard" / "queries"
FILTER_ORDER = (
    "start_time",
    "end_time",
    "min_magnitude",
    "max_magnitude",
    "country",
    "region",
    "event_type",
    "include_deleted",
    "time_bucket",
    "top_n",
    "significant_magnitude",
)
DEFAULT_FILTER = {
    "start_time": None,
    "end_time": None,
    "min_magnitude": None,
    "max_magnitude": None,
    "country": None,
    "region": None,
    "event_type": None,
    "include_deleted": False,
    "time_bucket": "month",
    "top_n": 10,
    "significant_magnitude": 4.5,
}


class Dashboard:
    def __init__(self, parquet: Path):
        self.con = duckdb.connect()
        self.con.execute(f"CREATE VIEW snapshot AS SELECT * FROM read_parquet('{parquet}')")
        self.con.execute(sql("setup"))

    def set_filter(self, **values) -> None:
        unknown = set(values) - set(FILTER_ORDER)
        assert not unknown, unknown
        merged = DEFAULT_FILTER | values
        self.con.execute(sql("set_filter"), [merged[name] for name in FILTER_ORDER])

    def query(self, name: str) -> list[dict]:
        relation = self.con.execute(sql(name))
        columns = [d[0] for d in relation.description]
        return [dict(zip(columns, row, strict=True)) for row in relation.fetchall()]

    def close(self) -> None:
        self.con.close()


def sql(name: str) -> str:
    return (QUERIES / f"{name}.sql").read_text()
