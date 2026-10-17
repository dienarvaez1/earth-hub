# DEF-006: Alembic's logging setup silenced the application's loggers

| | |
| --- | --- |
| **Severity** | Medium (lost observability; tests gave false results) |
| **Priority** | P2 |
| **Status** | Fixed in `a71d7c1` (Milestone 3) |
| **Component** | `migrations/env.py` |
| **Found by** | Running the unit and integration suites together in one process |

## Environment
Python 3.13.2, Alembic 1.20, pytest 9.1.1 with `caplog`.

## Steps to reproduce
1. `uv run pytest` (unit and integration tests in one process; the integration tests sort first and run migrations).
2. Observe `tests/unit/test_poller.py::test_rejected_features_are_counted_and_logged` and `tests/unit/test_state.py::test_rebuild_skips_unreadable_messages`.

## Expected result
All tests pass, as they did when the unit and integration suites ran separately (211 and 44 passed).

## Actual result
```
AssertionError: assert 'feature rejected' in ''
 +  where '' = <_pytest.logging.LogCaptureFixture object>.text
2 failed, 253 passed
```

## Root cause
`migrations/env.py` called `logging.config.fileConfig(alembic.ini)`. Its default `disable_existing_loggers=True` disables **every logger created before the call**. The integration tests run migrations in-process, after the `earth_hub.*` loggers already exist, so from then on those loggers emitted nothing.

In production the effect would have been worse and harder to notice: a service that ran migrations on startup and then did its work would have **lost all its logs**. That isn't how Earth-Hub runs today, because migrations have their own container.

## Fix
`fileConfig(config.config_file_name, disable_existing_loggers=False)`, with a comment explaining why.

## Regression test
- `make test-all` runs every non-disruptive suite in one process, and passed **255/255 twice** after the fix.
- `make regression` runs the layers as separate processes but accumulates coverage across them, and the unit tests that check log output catch the problem if it comes back.

## Prevention
- `make test-all` exists specifically because running suites separately hides interactions between them.
- The [test strategy](../test-strategy.md) lists test isolation as a risk and requires a combined run.
