# Defect log

Every defect found while building Earth-Hub, from Milestone 1 to Milestone 7. Each was fixed in the milestone where it was found and covered by a regression test where a test could catch it. All but four were found before their code was merged: DEF-034 and DEF-036 in the live logs, DEF-035 in the public dashboard, and DEF-037 in a script already merged.

Six are written up in full, as examples of the [defect report template](#template), because they show the most instructive failure modes.

## Severity and priority

| Severity | Meaning |
| --- | --- |
| Critical | Data loss or corruption, or a security exposure, with no workaround |
| High | A core function fails, results become unreliable, or secrets could leak |
| Medium | A function misbehaves in specific conditions; a workaround exists |
| Low | Cosmetic, a standards deviation, or a minor inconvenience |

Priority is how urgently it had to be fixed: **P1** before anything else, **P2** within the milestone, **P3** when convenient. All 37 were fixed within their milestone, or in a follow-up straight after.

## Log

| ID | Milestone | Severity | Found by | Summary | Fixed in |
| --- | --- | --- | --- | --- | --- |
| [DEF-001](DEF-001-password-in-test-output.md) | 1 | High | Negative test (database stopped) | The PostgreSQL password was printed in pytest tracebacks | `afc9250` |
| DEF-002 | 1 | Medium | Smoke run | `docker compose up --wait` returned before `kafka-init` had created the topic, so tests could start too early | `afc9250` |
| DEF-003 | 1 | Low | Configuration review | The Kafka `CLUSTER_ID` was not a valid base64 UUID | `afc9250` |
| [DEF-004](DEF-004-ipv6-flaky-offsets.md) | 2 | High | Repeated test runs | Kafka offset lookups failed intermittently because `localhost` resolved to IPv6 `::1`, which isn't published | `e76c9df` |
| DEF-005 | 2 | Critical | Design review, then a unit test | Recording `Last-Modified` after a failed delivery would have made the next request a `304`, losing the failed events for good | `e76c9df` |
| [DEF-006](DEF-006-alembic-silences-logging.md) | 3 | Medium | Running all suites in one process | Alembic's `fileConfig()` disabled every existing logger, silencing the application after in-process migrations | `a71d7c1` |
| DEF-007 | 3 | Medium | Log review of the outage check | The outage script exported `POSTGRES_DB`, so Compose recreated the Postgres container with the wrong configuration | `a71d7c1` |
| [DEF-008](DEF-008-outage-check-false-pass.md) | 3 | High | Process check after the run | The outage check left a consumer running and still reported PASS | `a71d7c1` |
| DEF-009 | 3 | Medium | Code review before testing | JSON `null`, `NaN` and NUL could not be stored in `jsonb`, and huge timestamps overflowed, crashing processing instead of rejecting the message | `a71d7c1` |
| DEF-010 | 4 | Medium | Code review | FastAPI resolves dependencies before validating parameters, so an invalid request returned `503` instead of `422` whenever the database was down | `4d2748f` |
| DEF-011 | 4 | High | First API test run | `array_agg` used `WITHIN GROUP`, which PostgreSQL rejects, so the event list returned `500` | `4d2748f` |
| DEF-012 | 4 | High | API tests | The lookup's ID subquery was auto-correlated away by SQLAlchemy and lost its `FROM`, so lookups returned `500` | `4d2748f` |
| DEF-013 | 4 | Low | Schemathesis | Numbers such as `start_time=0.5` were accepted as Unix timestamps, against the documented contract | `4d2748f` |
| DEF-014 | 4 | Low | Schemathesis | `405` responses lacked the `Allow` header that HTTP requires | `4d2748f` |
| [DEF-015](DEF-015-hourly-window-overflow.md) | 4 | Medium | Schemathesis | A lone old `start_time` made the hourly window run to "now", past the 31-day limit, so a valid request was refused | `4d2748f` |
| [DEF-016](DEF-016-nul-in-parameters.md) | 4 | Medium | Schemathesis | A NUL character in a text parameter reached PostgreSQL and caused a `500` | `4d2748f` |
| DEF-017 | 4 | Medium | Schemathesis (intermittent) | `end_time=0001-01-01` overflowed `datetime` when the window start was computed, causing a `500` | `4d2748f` |
| DEF-018 | 4 | Medium | Schemathesis (intermittent) | An `offset` beyond PostgreSQL's bigint caused a `500` | `4d2748f` |
| DEF-019 | 5 | Medium | End-to-end tests (intermittent) | Reading offsets of a brand-new topic failed with `NOT_LEADER_FOR_PARTITION`, affecting reconciliation and the poller's state rebuild | `b39991b` |
| DEF-020 | 5 | Low | Disruptive tests | The test suite's admin database engine kept connections that a PostgreSQL restart had killed, so cleanup failed | `b39991b` |
| DEF-021 | Backfill | High | Live backfill against USGS | Asking FDSN for deleted and non-deleted events together (`includedeleted=true`) was 20 to 100 times slower, timing out on busy weeks, and returned some real, reviewed earthquakes as stubs with no time or location, which were dropped. 3 events of the test week were missing until the two kinds were queried separately | `d9b2bfa` |
| DEF-022 | Backfill | Medium | Unit test (simulated revision) | An event moving out of a window between `/count` and `/query` aborted the whole backfill; a short window is now fetched again, up to 3 times | `d9b2bfa` |
| DEF-023 | Backfill | Medium | Live backfill | A USGS outage ended the backfill command with a Python traceback instead of a "run again to resume" message | `d9b2bfa` |
| DEF-024 | Backfill | Medium | End-to-end test | The deleted-events check crashed when its stored checkpoint had sub-second precision; it is now rounded down to the second | `d9b2bfa` |
| DEF-025 | Backfill | Medium | Mutation check | The concurrent-batches deadlock test could not detect a wrong lock order once locking became a single statement; a deterministic test now checks the lock order through `pg_locks` | `d9b2bfa` |
| DEF-026 | Backfill | High | Reconciling the 2026 backfill with USGS, window by window | The deletion of a duplicate hid the event it had been merged into: USGS deleted `aka2026jntkwa` on its own after merging it into `aka2026jntiye`, and since both IDs point to one stored event, the newer deletion replaced it. Deletions that don't list the stored event's current ID no longer apply to it, whichever arrives first | `a3a8392` |
| DEF-027 | Backfill | Medium | Reviewing the 19 messages the consumer rejected during the 2026 backfill | Deletions were held to the content rules, so `uw` deletions carrying magnitude −5 as a placeholder were rejected by `MAG-001`. Had the event been stored, it would have stayed visible. Placeholder values in deletions are now stored as unknown | `a3a8392` |
| DEF-028 | R2 export | Low | The regression run on Sunday 18 October | The year-backfill performance test backfills from 1 January to *now* but expected exactly 42 weekly windows, so it failed as soon as a new week began. The expected count now comes from `weekly_windows`, the function the backfill uses | `06700d5` |
| DEF-029 | R2 export | Low | A test run after R2 settings were added to `.env` | The missing-settings test built `Settings()`, which reads the developer's `.env`, so it passed only while R2 was not configured. It now passes `_env_file=None` | `06700d5` |
| DEF-030 | R2 export | Medium | Timing each step of a real `make export-r2` that took 388 s for 2 changed rows | Building the final manifest tested `k in read & stored` for every row, rebuilding the intersection of two 115k-row sets each time (about 190 s). The run also re-uploaded the 5 MB manifest at every checkpoint even when nothing had changed (12 × ~7 s). The intersection is now built once, and the manifest is saved only when uploads changed it | `06700d5` |
| DEF-031 | 6 | Medium | Worker tests against a simulated R2 | A `Range` starting past the end of the snapshot was answered `206 Partial Content` with the whole 7.5 MB file, because the storage ignores such a range and returns the whole object. The Worker now checks the start against the size and answers `416` with `Content-Range: bytes */<size>` | `f004426` |
| DEF-032 | 6 | High | The first run of the page in a browser | With DuckDB-WASM 1.32.0, the newest release not marked dev, every query failed with `function signature mismatch` in current Chromium, so the page never loaded. It now uses 1.33.1-dev65.0 (npm's "next" tag, DuckDB 1.5.6, the same engine as the Python tests), and the browser tests compare its answers with Python's on every report | `f004426` |
| DEF-033 | 6 | Medium | The first run of the page in a browser | The map stayed empty with "Worker failed to load": MapLibre 6 runs its tile work in a separate worker file that the page did not point to. The page now sets it with `setWorkerUrl`, and the browser tests fail on any console error | `f004426` |
| DEF-034 | 6 | Low | Reading the live logs after `make ingest` | Lines from the modules run with `python -m` had no `stage`: there `__name__` is `__main__`, so catch-up and backfill logged as `__main__`, and the poller's entry point named its logger `earth_hub.ingestion` by hand, which the stage defaults did not match (`state rebuilt from topic`). Modules that run as scripts now name their logger `__spec__.name`, and a test checks every module's logger name and that each pipeline module that logs has a default stage | #15 |
| DEF-035 | 7 | Medium | License review while choosing the project's license | The public page bundles MapLibre (BSD-3-Clause), Observable Plot and d3 (ISC), Apache Arrow (Apache-2.0) and others, whose licenses require their notices in copies, but the build stripped them all, and DuckDB-WASM's package ships no license text. The build now writes `third-party-licenses.txt` from the bundled packages, plus DuckDB's for the Parquet extension the page serves, linked from the footer; a package without a text fails the build | #16 |
| DEF-036 | 7 | Medium | Writing the demo walkthrough against the live logs | The live consumer processed `earthquakes.raw` in batches of 500, because `CONSUMER_BATCH_SIZE` in `.env` applied to both consumers, so routine outcomes were logged only at debug level and a live event could not be followed from the feed to the database as documented. The end-to-end tests ran the live consumer one message at a time, so they never saw the production setting. `docker-compose.yml` now gives the live consumer a batch size of 1, and a test checks it | #18 |
| DEF-037 | 7 | Low | Code review while writing the documentation tests | `traceability.py --not-run` found requirement IDs with a two-part pattern, missing three-part ones such as `REQ-DQ-GEO-002`, so such a requirement covered only by a layer left out would have been reported as a gap instead of not run. No run was affected: none is covered only by the performance tests | #18 |

## By how they were found

| Technique | Defects |
| --- | --- |
| Property-based contract testing (Schemathesis) | 6: DEF-013 to DEF-018 |
| Code and design review | 6: DEF-003, DEF-005, DEF-009, DEF-010, DEF-035, DEF-037 |
| Running against the live service | 7: DEF-021, DEF-023, DEF-026, DEF-027, DEF-030, DEF-034, DEF-036 |
| Ordinary test runs | 10: DEF-002, DEF-011, DEF-012, DEF-022, DEF-024, DEF-028, DEF-029, DEF-031, DEF-032, DEF-033 |
| Running tests repeatedly, or all together | 3: DEF-004, DEF-006, DEF-019 |
| Fault injection (stopping services) | 2: DEF-001, DEF-020 |
| Checking the test tooling itself | 3: DEF-007, DEF-008, DEF-025 |

Three of the 37 only ever showed up **intermittently** (DEF-004, DEF-017, DEF-019), and five were **defects in test tooling** that made results untrustworthy (DEF-007, DEF-008, DEF-025, DEF-028, DEF-029). Both groups are why the regression suite includes `make flake-check` and why test scripts are themselves checked.

## Template

Each full report has these sections:

1. **Summary** with ID, severity, priority, status, component, the milestone and commit, and how it was found
2. **Environment**
3. **Steps to reproduce**
4. **Expected result** and **actual result**, with evidence
5. **Root cause**
6. **Fix**
7. **Regression test**
8. **Prevention:** what changed so the class of defect is less likely
