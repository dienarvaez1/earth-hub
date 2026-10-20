# Test strategy

How Earth-Hub is tested, and why. The [test plan](test-plan.md) applies this strategy to the current release; the [requirements](requirements.md) and the [traceability matrix](traceability-matrix.md) show what is covered.

## Objectives

1. **Data integrity:** no event lost, duplicated or wrongly overwritten, from the USGS feed through Kafka and PostgreSQL to the API.
2. **Recoverability:** outages and restarts of the database, the broker, the poller or the consumer end in a correct state without manual repair.
3. **Correct, predictable API:** documented results, one error format, no internals exposed.
4. **Trustworthy evidence:** results are repeatable, never depend on live data, and every requirement traces to a passing test.

## Approach: risk-based

Testing effort follows what would hurt most if it failed.

| Risk | Impact | How it is tested |
| --- | --- | --- |
| Events lost between the feed and the database | Critical | Delivery confirmed before an event counts as sent; database committed before the Kafka offset; reconciliation of topic against database; crash simulations; real outages |
| Duplicates, or an older revision overwriting a newer one | High | Redelivery and replay tests; newer-only update rules; concurrency test with advisory locks |
| USGS changing an event's ID, or merging two reports | High | Synthetic ID changes and two- and three-way merges, checked in the repository, the pipeline and through the API |
| Bad source data stored or served | High | 16 validation rules with boundary tests, plus database `CHECK` constraints as a second line of defense |
| API inputs crashing the service | Medium | Validation tests and property-based contract testing (Schemathesis) |
| Tests that pass for the wrong reason | High | Mutation spot checks; repeated and combined runs; tests of the test tooling |
| Secrets leaking into logs or output | High | Negative tests that read whole outputs with dependencies stopped |

## Test levels

| Level | Location | Needs | What it proves |
| --- | --- | --- | --- |
| Unit | `tests/unit/` | Nothing: no Docker, **no network** | Logic: parsing, validation rules, retries, timing, state, consumer control flow, API validation and error format |
| Integration | `tests/integration/` | `make up` | Real Kafka and PostgreSQL: migrations, repository behavior (merges, concurrency, constraints), the poller and consumer against real infrastructure |
| API | `tests/api/` | `make up` | Every endpoint over a fixed dataset of 20 events stored through the real consumer code; aggregates checked against separate SQL |
| Contract | `tests/api/test_contract.py` | `make up` | Responses match the OpenAPI schema for generated inputs, with no server errors |
| End-to-end | `tests/e2e/` | `make up` | The whole pipeline: mock feed → real poller → Kafka → real consumer → PostgreSQL → API, checked only through the API |
| Dashboard | `tests/dashboard/` | `make dashboard-install` | The built page and Worker under `wrangler dev` over a simulated R2, in Chromium, offline: the Worker's responses, the reports' answers against Python's, the page's controls |
| Disruptive | `tests/disruptive/` | `make up`, plus `--run-disruptive` | Real outages: PostgreSQL stopped while events arrive, a Kafka restart, data surviving a restart of both |
| Performance | `tests/performance/` | `make up`, plus `--run-performance` | A year of synthetic history through the pipeline, and the dashboard at up to 1,000,000 rows, within time budgets |
| Live checks (manual) | `make ingest`, `make reconcile` | Internet | The real feed end to end; never part of the automated suite |

Each layer uses throwaway resources: a temporary Kafka topic, a temporary database migrated to the latest schema, and a unique consumer group per test. Nothing writes to `earthquakes.raw` or the main database, so the suites can run while the live pipeline is collecting data.

## Techniques

- **Equivalence partitioning and boundary values.** Every range rule is tested just inside, at and just outside its limits. Examples: latitude 90 versus 90.0001, magnitude −2 and 10.5, the event time exactly 1 h ahead, `limit` 500 versus 501, an exclusive `end_time` at exactly an event's time.
- **Negative testing.** Invalid input at every layer: malformed GeoJSON, unroutable features, values `jsonb` can't hold, invalid API parameters, and SQL-injection strings.
- **Fault injection.** Mocked HTTP failures (timeouts, `429`, `5xx`), fake Kafka delivery errors, simulated crashes between commits, a database on a closed port, and real container outages.
- **Property-based contract testing.** Schemathesis generates requests from the OpenAPI schema. 60 examples per endpoint by default; `CONTRACT_EXAMPLES=1000` for a deeper run. It found 6 of the 37 recorded defects.
- **Independent oracles.** Aggregates are checked against SQL written separately in the test, or against values computed from the test's own input. Never against the code under test.
- **Mutation spot checks.** Before trusting a new test suite, key safeguards are deliberately broken one at a time to confirm the tests fail. So far 19 deliberate breaks across Milestones 2–4 (5, 6 and 8), and every one was caught.
- **Repetition and combination.** Intermittent failures are treated as defects. `make flake-check` runs the end-to-end suite 5 times, and `make test-all` runs suites together to expose interactions between them (DEF-006).
- **Reconciliation.** Every end-to-end test ends by checking that every message in the topic has a database row with an outcome, with lost and pending messages kept apart.

## Test data

- **Never the live feed in automated tests.** A guard in `tests/conftest.py` blocks network access in unit tests, and `tests/unit/test_network_guard.py` proves it (REQ-QA-001).
- **Fixtures:**
  - `tests/fixtures/usgs/all_day_sample.geojson` holds 6 real events picked for edge cases: two IDs, negative magnitude, negative depth, an explosion, felt reports.
  - Synthetic events are built from that sample by `tests/support/feed.py` and `tests/api/dataset.py`, with times relative to the real clock where validation requires it.
- **Producer-contract reuse.** Consumer and API tests build messages with the poller's own `build_message`, so a contract change breaks the tests.
- **Determinism.** Fixed datasets, injected clocks and fixed random values in unit tests. End-to-end assertions depend only on the inputs each test creates (REQ-QA-002).

## Environments

| Environment | Used for |
| --- | --- |
| Developer machine with Docker Compose | All automated layers; services bound to `127.0.0.1` |
| Same machine with internet | Live checks of the real feed (`make ingest`, `make reconcile`) |
| CI: GitHub Actions ([ci.md](ci.md)) | Every layer but performance, on every pull request and push to `main`, from the example settings and with no secrets |

## Entry and exit criteria

**A milestone may start when:**
- the previous milestone is merged and its regression run passed
- its proposal, test strategy and acceptance criteria are approved

**A milestone is done when:**
- `make regression` passes every layer
- `make traceability` reports every requirement covered by a passing test
- `make flake-check` passes 5 runs out of 5
- new and changed behavior has negative tests
- defects found are fixed with a regression test, or recorded in the [defect log](defects/README.md) with a reason
- the documentation and known limitations are updated

## Defect management

- Every defect gets an ID in the [defect log](defects/README.md), with severity, how it was found and the commit that fixed it.
- Instructive ones can get a full report from the template. Reports of fixed defects are kept in the git history, linked from the log.
- A fix needs a regression test that fails without it, wherever a test can express it.

## Tooling

pytest, pytest-cov, Schemathesis (with Hypothesis), httpx `MockTransport`, FastAPI `TestClient`, real Kafka and PostgreSQL through Docker Compose, Ruff, and the project's own `scripts/regression.py` and `scripts/traceability.py`.

## AI-assisted development

The code and tests were written with Claude Code, under a fixed working method:
- a proposal is approved before each milestone
- tests are defined before or alongside the code
- the real suite is run and its output reviewed
- every diff is reviewed before commit

The safeguards in this strategy apply to AI-written tests as much as to AI-written code: mutation spot checks, repeated runs, independent oracles and tests of the test tooling. Five of the recorded defects were in the test tooling itself (DEF-007, DEF-008, DEF-025, DEF-028 and DEF-029), found because results were checked rather than trusted. The documentation is held to the same standard: its API and SQL examples are replayed by tests, and its links and schema are checked.
