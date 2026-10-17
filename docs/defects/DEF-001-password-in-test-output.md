# DEF-001: PostgreSQL password printed in pytest tracebacks

| | |
| --- | --- |
| **Severity** | High (secret exposure) |
| **Priority** | P1 |
| **Status** | Fixed in `afc9250` (Milestone 1) |
| **Component** | Test infrastructure (`tests/integration/conftest.py`, pytest configuration) |
| **Found by** | Negative testing: running the integration tests with PostgreSQL stopped |

## Environment
macOS 27 (arm64), Python 3.13.2, pytest 9.1.1, psycopg 3.3.6, PostgreSQL 18.6 in Docker.

## Steps to reproduce
1. `make up`, then `docker compose stop postgres`.
2. `uv run pytest -m integration tests/integration/test_postgres_smoke.py`.
3. Search the output for the value of `POSTGRES_PASSWORD`.

## Expected result
The test fails with a message that PostgreSQL is unreachable. The password appears nowhere.

## Actual result
pytest's default long traceback printed the arguments of every frame, including psycopg's connection call:

```
conninfo = 'host=localhost dbname=earth_hub user=earth_hub_app password=change_me_locally port=5433 ...'
kwargs = {'host': 'localhost', 'dbname': 'earth_hub', 'user': 'earth_hub_app', 'password': 'change_me_locally', ...}
```

In CI (Milestone 7) this output becomes a build log, often readable by everyone with repository access.

## Root cause
Two things combined:
- pytest's `--tb=long` (the default) shows function arguments.
- The connection error came out of a fixture as an ordinary exception, so pytest printed its whole traceback.

The project's own code never logged the password; the test runner did.

## Fix
- `pyproject.toml` sets `addopts = "--tb=short"`, which prints source lines but not arguments.
- The database fixture catches `OperationalError` and calls `pytest.fail(..., pytrace=False)` with a message built from `str(URL)`, which SQLAlchemy masks as `***`.

## Regression test
- Rerunning the steps above showed **0 occurrences** of the password.
- `tests/unit/test_config.py::test_password_never_appears_in_text_output` checks the settings' `repr`, `str` and JSON, and the database URL.
- `tests/unit/test_api_validation.py::test_no_internals_or_credentials_leak_in_errors` checks API error bodies.

## Prevention
- Passwords are a `SecretStr` in the settings.
- The database URL is built in code, so it's never written into a config file.
- "Run with the dependency stopped and read the whole output" is now a standard negative test (see the [test strategy](../test-strategy.md)).
