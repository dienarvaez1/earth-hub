---
name: eh-test
description: Run Earth-Hub's tests by layer — lint, unit, integration, API, contract, end-to-end, dashboard, disruptive, performance — or the whole regression, the flake check, traceability, coverage, or the tests matching a keyword or naming a requirement. Use for /eh-test, "run the unit tests", "run the regression", "test the API", "run the tests for REQ-API-005", "is it flaky".
argument-hint: "[lint | unit | integration | api | contract | e2e | dashboard | disruptive | performance | regression | flake | traceability | coverage | REQ-… | <keyword>]"
---

# Tests

Run from the repository root. Report real results; never call something passing without running it.

| Argument | Command | Needs | Time |
| --- | --- | --- | --- |
| `lint` | `make lint` | — | < 1 s |
| `unit` (default) | `make test` | nothing: no Docker, no network | ~15 s |
| `integration` | `uv run pytest tests/integration -m integration` | `make up` | ~1 min |
| `api` | `uv run pytest tests/api -m integration --deselect tests/api/test_contract.py` | `make up` | ~5 s |
| `contract` | `uv run pytest tests/api/test_contract.py -m integration` (`CONTRACT_EXAMPLES=1000` for a deeper run) | `make up` | ~10 s |
| `e2e` | `make test-e2e` | `make up` | ~40 s |
| `dashboard` | `make test-dashboard` | `make dashboard-install` | 3–6 min |
| `disruptive` | `make test-disruptive` | `make up`; **stops and starts the shared Kafka and PostgreSQL** | ~1 min |
| `performance` | `make test-performance` | `make up` | ~4 min |
| `regression` | `make regression`: every layer in order, reports in `reports/`, the traceability matrix | all of the above | ~15 min |
| `flake` | `make flake-check`: the end-to-end suite 5 times | `make up` | ~4 min |
| `traceability` | `make traceability` (from the last regression's results) | — | < 1 s |
| `coverage` | `make coverage` | — | ~15 s |
| `REQ-…` | `grep -rln '"<REQ-ID>"' tests` finds the files whose tests name it (the marker's ID, as in `docs/traceability-matrix.md`); run those files with their layer's options, e.g. `uv run pytest tests/api/test_lookup.py -m integration` | the layer's | |
| a keyword | `uv run pytest -k "<keyword>" -m "not disruptive and not performance"` (add `--run-disruptive` only if asked) | `make up` for non-unit tests | |

- `disruptive` restarts containers the live pipeline uses (`make ingest`): warn the user if the pipeline is
  running, and run it only on request.
- Run long layers (`dashboard`, `performance`, `regression`, `flake`) in the background and report when done.
- For a regression, report from `reports/regression-summary.md`: the result, each layer's counts and time,
  coverage, traceability.
- A failing test: show its name and the assertion, read the code it tests, and say what broke. Don't change a
  test to make it pass without saying so; record real defects in `docs/defects/README.md` (CLAUDE.md).

## Report

Passed / failed counts per layer, time, and the failing tests with their first error line.
