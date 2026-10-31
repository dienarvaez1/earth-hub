# Earth-Hub — Complete Project Implementation Plan

Project identity · Architecture · Milestones · QA strategy · Environment setup

Earth-Hub is a local-first, open-source data platform that ingests earthquake data from the USGS, processes events through Apache Kafka, validates and stores them in PostgreSQL, and exposes the resulting data through its own REST API.

The project is designed to showcase your experience in software quality assurance, test automation, database testing, ETL validation, API testing, and AI-assisted engineering with Claude Code.

## Status

As of 2026-10-30. The plan below is the original, with Milestone 7 revised; the work as built is described in the [architecture](../technical/architecture.md), and what each step found in the [defect log](defects.md).

| Milestone | Status | Merged in |
| --- | --- | --- |
| Setup: plan and project rules | Done | #1 |
| 1. Foundation and infrastructure | Done | #2 |
| 2. USGS ingestion and Kafka producer | Done | #3 |
| 3. Consumer, validation and PostgreSQL | Done | #4 |
| 4. REST API (and the read-only Kafka UI) | Done | `4d2748f`, `620b8c7` |
| 5. End-to-end QA and regression automation | Done | #6 |
| Backfill and catch-up, between 5 and 6 | Done | #7, #8; place-derived location #9; an end after now cut to now (DEF-053) #45 |
| R2 export, before 6 | Done | #10, #11 |
| 6. Analytics dashboard from R2, observability, performance | Done | #12, #13, #14, #15 |
| 7.1 CI, third-party licenses, preview marking | Done | #16 |
| 7.2 Documentation and demo | Done | #18; skills and cheat sheet #19; defect reports to history #20; docs reorganized #21 |
| 7.3 Release, prepared: audit, MIT license, `USGS_CONTACT` setting, preview label removed, the release script | Done | #37, #38 |
| 7.3 Release, the rest: public repository, v1.0.0, branch protection on `main`, `USGS_CONTACT`, R2 secrets in the `r2` environment only (the script's merge and deploy steps removed: DEF-055) | Done, 2026-10-29 (`make release`, run by the author) | #51 |
| 8.1 Persistence decisions in memory (`processing/ledger.py`), `EventStore`, `PostgresStore`; the record key | Done | #22, #23 |
| 8.2 `BucketStore`, the direct runner, parity tests | Done | #24 |
| 8.3 The mode switch, handovers with log replay | Done | #25 |
| Direct mode on GitHub Actions' hourly schedule, after 8.3 | Done | #27; skills and docs for the modes #28 |
| Rounds started by the dashboard Worker's Cloudflare cron trigger (GitHub's schedule skipped runs, DEF-052); `make trigger-token`; an e-mailed reminder before the token expires | Done | #40, #41 |
| Direct mode's audit trail: a record per execution in the bucket, `make direct-runs` | Done | #42 |
| Dashboard: opens on This week per hour; menus that recount; top countries and regions as shares by magnitude, with a note on network density; US- places; events over time as columns, areas, lines or trends | Done | #39 |
| Load test of the API with Locust, after 8 | Done | #26 |
| Reconciliation with USGS, catching up after each switch | Done | #32 |
| One command each to switch modes and to backfill in full mode (`make to-full`, `make to-direct`, `make backfill-full`) | Done | #44 |
| Dashboard map above 25,000 events: cells counted in SQL, the visible area's events once zoomed in (history back to 2024 loaded) | Done | #46 |
| Dashboard: popups with the time to the second and the UTC offset, tsunami flag, closing on a click or resize; significant events under the map, 10, 25 or 50, each opening on the map; zoom 1 to 15; the Countries card naming the chosen country; a Tsunami filter; ranking notes on what a bar is | Done | #47 |
| CI's regression in parallel parts (the dashboard layer in two shards, merged into the required check); the snapshot compressed smaller (zstd level 19: 24.8 MB to 18.1 MB); the popup's depth to one decimal | Done | #48 |
| The check against USGS measures each feed by when USGS generated it, not when a round read it or ended (DEF-054) | Done | #49 |
| Dashboard: a link to the project on GitHub in the footer | Done | #50 |
| No schedule in `direct.yml`: the Worker starts the daily reconcile too, and a watchdog workflow holds the only schedule (DEF-056) | Done | #52 |
| `make trigger-check`: whether the Worker's deployed token works, asked on Cloudflare, starting nothing | Done | #54 |
| The USGS credit and a disclaimer (research purposes, not affiliated with or endorsed by USGS, data that may be late or inaccurate when a part fails, no warranty, not for emergencies) in the dashboard's footer and the README | Done | #57 |
| A privacy notice (`/privacy`, linked from the footer): no cookies, storage, tracking or location; the Worker's invocation logs, which recorded visitors' IP addresses and IP-derived locations, switched off | Done | #58 |
| Code quality: Ruff's complexity (`C90`), security (`S`), simplification (`SIM`) and Pylint (`PL`) rules, with their first findings fixed or marked; CodeQL code scanning | Done | #59 |
| [9. Standby: direct mode takes over from one full-mode machine](history.md#milestone-9--standby-direct-mode-takes-over-from-one-full-mode-machine) | Done, 2026-10-30: steps 1 to 4 (lock fencing, DEF-058; owner and lease; the exporting service and its standby state), steps 5 to 8 (automatic takeover, rebuild from the bucket, `/eh-host`, failover end to end) | #61, #62 |

The current counts of requirements, defects and tests are in the [README](../../README.md#quality-at-a-glance), and the last regression's results in the [traceability matrix](traceability-matrix.md).

**To refactor** (#59): functions over Ruff's complexity limits when the rules were added, each marked `# noqa: … (to refactor: docs/progress/plan.md)`. New code is held to the limits. Each one is a separate change, with the full regression as its check.

| Function | Over |
| --- | --- |
| `validation/earthquake_rules.py`: `validate`, `_validate_envelope` | complexity, branches, statements |
| `usgs_reconcile.py`: `compare`, `main` | complexity, branches, statements |
| `direct/__main__.py`: `_execute` | complexity, returns |
| `direct/handover.py`: `main`; `direct/store.py`: `BucketStore.write` | complexity |
| `export/r2.py`: `_upload_changes`; `ingestion/fdsn.py`: `_get`; `export/browse.py`: `main` | complexity or returns |
| `scripts/load_test.py`: `main`; `tests/support/streams.py`: `random_stream`; `tests/unit/test_release.py`: `Fake.read` | statements, complexity or returns |

## 1. Project vision

**Earth-Hub**
Earthquake Data & Quality Intelligence Platform
*Ingest. Validate. Persist. Analyze. Expose.*

### Mission

Build a reliable, testable data platform that transforms public earthquake data into validated, queryable information through a documented API, while making data quality and software testing first-class engineering concerns.

### Core objectives

- Ingest public earthquake data from the USGS GeoJSON feed.
- Use Kafka to transport events between ingestion and processing services.
- Validate event quality and preserve source traceability.
- Store normalized data and processing records in PostgreSQL.
- Develop a versioned REST API backed by PostgreSQL.
- Automate unit, integration, API, regression, and end-to-end tests.
- Demonstrate observability, analytics, and performance testing.
- Use Claude Code to assist development and QA while reviewing its work critically.
- Run the complete system locally without paid cloud infrastructure.

## 2.–5. Architecture, setup and milestones, as planned

The target architecture as first planned, the setup prerequisites, the roadmap of the nine milestones and the repository structure are kept as written, in the [plan history](history.md). How Earth-Hub works now is in the [architecture](../technical/architecture.md).

## 6. Definition of done for each milestone

Use this checklist before moving to the next phase.

**Earth-Hub milestone gate**

- [ ] Implementation is reviewed against acceptance criteria.
- [ ] Relevant automated tests pass, including negative cases.
- [ ] Regression checks show no unintended changes.
- [ ] Data integrity and recovery behavior are verified where applicable.
- [ ] Linting and configured static checks pass.
- [ ] Documentation and known limitations are updated.
- [ ] Changes are reviewed and committed in Git.

## 7. Claude Code development workflow

Use Claude Code as an engineering assistant with explicit quality gates, not as an unrestricted code generator.

For every milestone:

1. **Plan:** Inspect the repository and propose a small, reviewable implementation.
2. **Define tests:** Specify acceptance criteria and negative scenarios before coding.
3. **Implement:** Make focused changes without unnecessarily refactoring unrelated modules.
4. **Verify:** Run the actual tests and review their output.
5. **Inspect:** Review the diff, error handling, security, data integrity, and test quality.
6. **Document:** Record decisions, evidence, and known limitations.
7. **Commit:** Save the reviewed result before proceeding.

Add a `CLAUDE.md` that defines project conventions, commands, architecture, and a strict rule against using the live USGS feed in unit tests.

The initial prompt given to Claude Code is in the [plan history](history.md#initial-prompt-for-claude-code).

## 8. Final success criteria

Earth-Hub is portfolio-ready when it has:

- A working USGS → Kafka → PostgreSQL → REST API flow.
- Reliable data-quality rules, duplicate handling, and revision management.
- A documented API with automated contract and integration tests.
- Reproducible end-to-end tests and source-to-database reconciliation.
- Performance and observability evidence based on actual runs.
- A CI workflow and clear setup instructions.
- Documentation showing how Claude Code assisted implementation while you retained responsibility for QA decisions and validation.

**Recommended first action:** Create the earth-hub repository, complete the prerequisite checks, and give Claude Code the initialization prompt above. Start with infrastructure and a working smoke test; expand only after that milestone is verified.
