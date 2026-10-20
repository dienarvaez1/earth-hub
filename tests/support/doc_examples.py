"""The examples in the docs that tests replay, so a document cannot drift from the code:
API requests with their responses (docs/api.md), and SQL with its results
(docs/sql-examples.md).

An API example is a request block, its status, and the response body:

    ```http
    GET /api/v1/earthquakes/us02
    ```

    Response: `200 OK`

    ```json
    {...}
    ```

Headers to check may follow the status on its line: Response: `405 ...`, with `Allow: GET`.

A SQL example is a marker comment, an optional `sql` block, and the result as a table:

    <!-- replay: postgres -->             the sql block below, run on PostgreSQL
    <!-- replay: dashboard summary include_deleted=true -->
                                          dashboard/queries/summary.sql, with that filter

Values in a table are written by `cell`. A block or marker the parser cannot read fails the
test, rather than being skipped.
"""

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

DOCS = Path(__file__).resolve().parents[2] / "docs"
API_DOC = DOCS / "api.md"
SQL_DOC = DOCS / "sql-examples.md"

API_EXAMPLE = re.compile(
    r"```http\n(?P<method>[A-Z]+) (?P<path>\S+)\n```\n\n"
    r"Response: `(?P<status>\d{3})[^`\n]*`(?P<headers>[^\n]*)\n\n"
    r"```json\n(?P<body>.*?)\n```",
    re.S,
)
HEADER = re.compile(r"`([A-Za-z-]+): ([^`]+)`")
MARKER = re.compile(r"^<!-- replay: (?P<args>[^\n]*?) -->$", re.M)
SQL_BLOCK = re.compile(r"\A\s*```sql\n(?P<sql>.*?)\n```", re.S)
TABLE = re.compile(r"\A\s*((?:\|[^\n]*\|\n?)+)")


@dataclass(frozen=True)
class ApiExample:
    method: str
    path: str
    status: int
    headers: dict[str, str]
    body: object
    line: int


@dataclass
class SqlExample:
    engine: str  # "postgres" or "dashboard"
    sql: str | None  # the documented SQL (postgres), or None
    report: str | None  # the dashboard query file's name (dashboard), or None
    filters: dict = field(default_factory=dict)
    columns: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    line: int = 0


def _line(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def api_examples(path: Path = API_DOC) -> list[ApiExample]:
    text = path.read_text()
    found = list(API_EXAMPLE.finditer(text))
    blocks = [m.start() for m in re.finditer(r"^```http$", text, re.M)]
    unread = sorted(set(blocks) - {m.start() for m in found})
    if unread:
        lines = ", ".join(str(_line(text, offset)) for offset in unread)
        raise ValueError(f"{path.name}: http blocks not followed by a response, lines {lines}")
    return [
        ApiExample(
            m["method"],
            m["path"],
            int(m["status"]),
            dict(HEADER.findall(m["headers"])),
            json.loads(m["body"]),
            _line(text, m.start()),
        )
        for m in found
    ]


def _filters(words: list[str]) -> dict:
    filters = {}
    for word in words:
        name, sep, value = word.partition("=")
        if not sep:
            raise ValueError(f"filter {word!r} is not name=value")
        filters[name] = json.loads(value)
    return filters


def _cells(row: str) -> list[str]:
    return [cell.strip() for cell in row.strip().strip("|").split("|")]


def sql_examples(path: Path = SQL_DOC) -> list[SqlExample]:
    text = path.read_text()
    examples = []
    for marker in MARKER.finditer(text):
        line = _line(text, marker.start())
        engine, *words = marker["args"].split()
        rest = text[marker.end() :]
        if engine == "postgres":
            block = SQL_BLOCK.match(rest)
            if not block:
                raise ValueError(f"{path.name}:{line}: a postgres example needs a sql block")
            example = SqlExample("postgres", block["sql"], None, line=line)
            rest = rest[block.end() :]
        elif engine == "dashboard" and words:
            example = SqlExample("dashboard", None, words[0], _filters(words[1:]), line=line)
        else:
            raise ValueError(f"{path.name}:{line}: unknown replay marker {marker['args']!r}")
        table = TABLE.match(rest)
        if not table:
            raise ValueError(f"{path.name}:{line}: the example needs a result table")
        header, separator, *rows = table[1].strip().splitlines()
        if set(separator.replace("|", "").replace(" ", "")) - {"-", ":"}:
            raise ValueError(f"{path.name}:{line}: the table's second row must be the separator")
        example.columns = _cells(header)
        example.rows = [_cells(row) for row in rows]
        examples.append(example)
    return examples


def cell(value) -> str:
    """A value as the docs write it: NULL, true/false, times in UTC to the millisecond
    ("+00" when the type has a time zone, as psql prints it), lists in braces."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        utc = value.astimezone(UTC) if value.tzinfo else value
        text = utc.strftime("%Y-%m-%d %H:%M:%S")
        if utc.microsecond:
            text += f".{utc.microsecond // 1000:03d}"
        return text + ("+00" if value.tzinfo else "")
    if isinstance(value, list | tuple):
        return "{" + ",".join(cell(v) for v in value) + "}"
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    text = str(value)
    if "|" in text or "\n" in text:
        raise ValueError(f"{text!r} cannot be written in a table cell")
    return text


def table(columns: list[str], rows: list[tuple]) -> str:
    """Markdown for a result, as the docs hold it (used to write the docs)."""
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |"]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)
