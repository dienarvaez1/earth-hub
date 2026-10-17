# DEF-016: A NUL character in a parameter caused a `500`

| | |
| --- | --- |
| **Severity** | Medium (a crafted request causes a server error; no data exposed) |
| **Priority** | P2 |
| **Status** | Fixed in `4d2748f` (Milestone 4) |
| **Component** | API parameter handling (`src/earth_hub/api/routes.py`) |
| **Found by** | Schemathesis, property-based contract testing (`not_a_server_error` check) |

## Environment
FastAPI 0.143, psycopg 3.3.6, PostgreSQL 18.6.

## Steps to reproduce
```bash
curl -s 'http://127.0.0.1:8000/api/v1/earthquakes?place=a%00b'
```

The same happens with `event_type`, and with `%00` inside the event ID in `/earthquakes/{event_id}`.

## Expected result
`422 invalid_parameters`. A NUL character can't appear in any place name, type or USGS ID.

## Actual result
```json
{"error":{"code":"internal_error","message":"internal server error","details":[]}}
```

The server log showed that psycopg refused the value: PostgreSQL `text` cannot contain NUL (`0x00`) bytes.

## Root cause
Parameter validation checked length but not content. The value went to PostgreSQL as a bound parameter, which is correctly safe from SQL injection, but PostgreSQL refused the NUL byte as a data error. That error isn't a connection problem, so it surfaced as a `500`.

The error handler worked as designed: the client got a generic `500` with no SQL or stack trace.

## Fix
- A `Text` parameter type (a Pydantic `AfterValidator`) rejects NUL with "must not contain NUL characters". It's used for `place`, `event_type` and the event ID.
- `CLAUDE.md` requires it for every string parameter that reaches SQL.

## Regression test
`tests/unit/test_api_validation.py::test_nul_characters_are_rejected_not_a_500`, for all three parameters, with no database needed. Schemathesis has run clean since: 10 soak runs and one deep run of about 6,000 requests.

## Prevention
- Generated inputs find classes of values that hand-written tests rarely include: NUL, the year 1, numbers past bigint. Contract tests run on every regression, and `CONTRACT_EXAMPLES=1000` gives a deeper run.
- The consumer had the same class of defect with `jsonb` (DEF-009), so NUL handling is now explicit on both the write and read paths.
