# Test plan: Milestones 1–5

This plan applies the [test strategy](test-strategy.md) to the release made up of Milestones 1 to 5.

## Scope

### In scope
| Feature | Milestone | Requirements |
| --- | --- | --- |
| Infrastructure: Kafka (KRaft), PostgreSQL 18, configuration, migrations | 1 | REQ-OPS-001 to 003, REQ-PER-012 |
| Ingestion: the poller and Kafka producer | 2 | REQ-ING-001 to 010, REQ-OPS-004, REQ-OPS-005 |
| Persistence: the consumer, validation, PostgreSQL, reconciliation | 3 | REQ-PER-001 to 014, REQ-DQ-* |
| REST API | 4 | REQ-API-001 to 013 |
| End-to-end behavior and recovery | 5 | REQ-E2E-001 to 013, REQ-QA-001, REQ-QA-002 |

### Out of scope
| Item | Why |
| --- | --- |
| Performance, load and query plans | Milestone 6 |
| CI pipeline | Milestone 7 |
| Catch-up through the USGS FDSN API, and deleted events arriving from the source | Not built yet (deferred) |
| Authentication and multi-user security | Out of the product's scope: everything is local and bound to `127.0.0.1` |
| Kafka UI | A third-party read-only viewer; only its read-only setting was checked, by hand |
| Behavior of the live USGS feed itself | Outside our control. Covered by recorded fixtures and the manual live checks |

## Test items and how to run them

| Suite | Command | Tests | Typical time |
| --- | --- | --- | --- |
| Lint | `make lint` | — | under 1 s |
| Unit | `make test` | 271 | about 3 s |
| Integration | `uv run pytest tests/integration -m integration` | 44 | about 13 s |
| API | `uv run pytest tests/api -m integration` | 70 | about 4 s |
| Contract | `uv run pytest tests/api/test_contract.py -m integration` | 1 test, 6 endpoints | about 8 s |
| End-to-end | `make test-e2e` | 12 | about 18 s |
| Disruptive | `make test-disruptive` | 3 | about 60 s |
| **Everything, with reports** | **`make regression`** | **407** | **about 2 min** |
| Repeatability | `make flake-check` | 5 × 12 | about 90 s |
| Traceability | `make traceability` (also run by `make regression`) | 74 requirements | under 1 s |

`make test-integration` selects by marker, so it runs the integration, API and contract suites together. Every suite except lint and unit needs `make up`. **The disruptive tests stop and start the shared Kafka and PostgreSQL containers.** Anything else using them, the live pipeline included, sees the outage and must recover, which the tests also check.

## Reports

`make regression` writes to `reports/`, which git ignores:

| File | Contents |
| --- | --- |
| `regression-summary.md` | Result, environment (commit, versions), per-layer counts and times, failures, coverage |
| `junit-<layer>.xml` | JUnit results for each layer, for CI and other tools |
| `coverage-html/index.html` and `coverage.xml` | Line coverage accumulated across all layers |
| `trace-<layer>.json` | Requirement-to-test outcomes; `make traceability` turns them into [traceability-matrix.md](traceability-matrix.md) |

## Schedule and responsibilities

| Milestone | Testing done |
| --- | --- |
| 1 | Smoke tests, configuration tests, restart persistence |
| 2 | Poller unit tests, Kafka integration, live feed checks |
| 3 | Validation rules, repository, pipeline, reconciliation, outage check |
| 4 | API, validation, contract tests |
| 5 | End-to-end scenarios, disruptive tests, the regression command, traceability, these documents |

The developer, working with Claude Code, writes and runs the tests. The project owner approves each milestone's proposal and reviews each pull request before merging.

## Risks to testing

| Risk | Mitigation |
| --- | --- |
| Intermittent failures from timing or infrastructure | Wait on conditions instead of fixed delays; retry Kafka metadata lookups; `make flake-check`; intermittent failures treated as defects |
| Tests affecting each other | Throwaway topics, databases and consumer groups; `make test-all` runs suites together |
| Disruptive tests affecting other work on the machine | Opt-in only (`--run-disruptive`); a fixture restores and waits for the services after each test |
| Tests passing for the wrong reason | Mutation spot checks; independent oracles; checks of the test tooling's own clean-up |
| Generated-input tests finding new edge cases later | Each finding becomes a regression test; deeper runs with `CONTRACT_EXAMPLES` |

## Deliverables

- the automated suites and `make regression`
- [requirements](requirements.md) and the generated [traceability matrix](traceability-matrix.md)
- this test plan and the [test strategy](test-strategy.md)
- the [defect log](defects/README.md), with 6 full defect reports
- the [data-quality rules](data-quality-rules.md)

## Exit criteria for this release
These come from Milestone 5 in the [plan](IMPLEMENTATION.md):

1. Every critical end-to-end path passes reproducibly: 5 runs out of 5.
2. Results never depend on live earthquakes.
3. `make regression` passes every layer and produces the reports.
4. All 74 requirements are covered by a passing test.
5. The QA documents are complete.
