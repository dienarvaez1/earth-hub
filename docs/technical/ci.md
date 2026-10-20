# Continuous integration

GitHub Actions runs `.github/workflows/ci.yml` on every pull request, on every push to `main`, and on demand (**Actions → CI → Run workflow**). It runs the same commands as a developer does locally, starting from a clean machine and the example settings.

A second workflow, `.github/workflows/direct.yml`, is not a check: it runs [direct mode on a schedule](#direct-mode-on-a-schedule).

```mermaid
%%{init: {"flowchart": {"rankSpacing": 30, "nodeSpacing": 50}}}%%
flowchart TB
    pr["pull request,<br/>push to main"] --> ci["ci.yml<br/>lint, unit, regression<br/>(no secrets)"]
    cron["every hour at :17,<br/>daily at 05:41 UTC,<br/>or by hand"] --> direct["direct.yml<br/>round / reconcile<br/>(R2 secrets)"]
    direct --> check{"_state/mode.json<br/>direct (active)?"}
    check -- yes --> r2[("R2 bucket:<br/>rows, snapshot, log")]
    check -- "no: full or<br/>handing-over" --> skip["skip, succeed"]
```

## The jobs

| Job | Runs | Takes about | For |
| --- | --- | --- | --- |
| **Lint and unit tests** (`checks`) | `make install`, `make lint`, `make test` | 1 min | quick feedback; no Docker |
| **Regression** (`regression`) | the quick start (`cp .env.example .env`, `make install`, the dashboard and its test browser, `make up`), then `scripts/regression.py --skip performance` | usually 9–15 min | every layer but performance: lint, unit, integration, api, contract, e2e, dashboard, disruptive; then the traceability check |

The regression job uses `scripts/regression.py`, the script behind `make regression`, so CI and a local run cannot drift apart. It stops at the first failing layer, like locally.

**Performance is left out.** Shared runners vary too much for the ±15% repeatability check, so those tests run on a developer machine (`make regression`). In CI the layer shows as `skipped (--skip)`, and the two requirements covered only by it (REQ-BF-011, REQ-DASH-008) as *not run*, not as gaps. Every other requirement still needs a passing test, or the run fails.

## Reading a run

- **Run page:** the regression summary (layers, counts, coverage, traceability) is shown at the bottom of the run's **Summary**, even when the run fails.
- **Artifact `regression-reports`**, kept 30 days: JUnit XML per layer, the coverage report (`coverage-html/index.html`), `regression-summary.md` and the run's traceability matrix.
- **When a run fails:** the summary's *Failed tests* names them; the last 200 log lines of Kafka and PostgreSQL are printed in the last step.

To reproduce a CI run locally: `uv run python scripts/regression.py --skip performance` (with `make up` first, like `make regression`). With Claude Code, `/eh-test regression` runs the full local version, performance included.

## Security

- **No secrets in CI.** Tests use moto instead of R2, `wrangler dev --local` instead of Cloudflare, and a fake USGS feed; `ci.yml` references no `secrets`. A pull request from a fork can run it safely. Only the scheduled direct-mode workflow holds secrets ([below](#direct-mode-on-a-schedule)).
- **Read-only token** (`permissions: contents: read`), and checkout does not keep it (`persist-credentials: false`).
- **Actions pinned to commit SHAs**, with the release in a comment, chosen at least two weeks after their release. To update one, look up the new release's commit (`gh api repos/<owner>/<action>/commits/<tag> -q .sha`) and change the SHA and the comment together.
- **Deploying stays manual** (`make dashboard-deploy`); CI never holds Cloudflare credentials.

`tests/unit/test_ci.py` checks all of this in the workflow file (REQ-CI-001 to REQ-CI-004), and `tests/unit/test_direct_workflow.py` the scheduled workflow (REQ-CI-005).

## Cost and limits

$0. A private repository gets 2,000 free Actions minutes a month; a public one has no limit. The [schedule](#direct-mode-on-a-schedule) uses about half while the repository is private, leaving some 60 runs of both CI jobs. Concurrent runs for the same branch are cancelled when a newer commit arrives.

Until the repository is public, GitHub's free plan cannot require these checks before merging: a red run is visible on the pull request but does not block the merge button. Requiring them (branch protection on `main`) is part of the release.

## Direct mode on a schedule

`.github/workflows/direct.yml` keeps the bucket and the dashboard current with no machine of ours running: [direct mode](architecture.md#direct-mode) needs only Python and R2 credentials.

| Job | When | Runs |
| --- | --- | --- |
| **Direct round** (`round`) | every hour, at :17 | `python -m earth_hub.direct once --skip-unless-direct`: one poller round, and catch-up if due (`make direct-once`) |
| **Direct reconcile** (`reconcile`) | every day, at 05:41 UTC | `python -m earth_hub.direct.reconcile --skip-unless-direct` (`make reconcile-direct`) |

Both also run by hand: **Actions → Direct mode → Run workflow**, or `gh workflow run direct.yml -f job=round` (`job=reconcile`); `gh run list --workflow direct.yml` lists the runs. Each run's page shows the command's summary lines.

- **The mode marker switches it.** While the bucket is not in direct mode (full mode, or a handover running), both jobs skip and succeed: `make mode-full` pauses the schedule, `make mode-direct` resumes it. A handover that freezes the bucket during a round makes it skip too, without catch-up. Missing R2 settings still fail the run.
- **One direct process.** The schedule and a `make direct-run` elsewhere would both poll and take turns writing; stop `make direct-run` while the schedule is on. Runs never overlap (`concurrency`, never cancelled), and a run stops after 20 minutes; a round cut off is finished by the next, and its lock expires after 15.
- **Catch-up every hour.** GitHub starts scheduled runs a few minutes late, unevenly, so the workflow sets `CATCHUP_INTERVAL_SECONDS=3000` and `DELETED_CHECK_INTERVAL_SECONDS=82800`: an hourly run never finds catch-up 59 minutes old and skips it.
- **Secrets.** R2 credentials (`R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`) are repository secrets, given only to the step that runs the command; `USGS_CONTACT` is an optional repository variable. Use an R2 token for this workflow only (Object Read & Write on the bucket), so it can be revoked alone. Set them with `gh secret set <NAME>`, which reads the value from the terminal. The workflow has no pull request or push trigger, and pull requests from forks never get secrets; but anyone with write access could run a workflow from a branch with them, so keep write access to yourself. A GitHub environment limited to `main` would close that, but GitHub Free offers environments only to public repositories: a step for the release.
- **Freshness.** New events reach the bucket up to about an hour after USGS publishes them (a minute with `make direct-run`); none are lost, as the day feed lists the last 24 hours. GitHub may delay or drop scheduled runs when busy.
- **Cost.** About 1–1½ minutes a run with uv's cache: some 750–1,100 of the private repository's 2,000 free minutes a month (no limit once public). R2 operations are fewer than with `make direct-run`: 24 feed rounds a day instead of about 437 (`tests/unit/test_direct_budget.py`).
- **Failures** e-mail whoever last changed the schedule, as GitHub does for scheduled workflows.

**With Claude Code:** `/eh-mode schedule` runs a round now, `/eh-mode runs` lists the recent runs, and `/eh-mode` says whether the schedule writes (direct mode) or skips (full mode).
