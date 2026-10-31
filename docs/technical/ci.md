# Continuous integration

GitHub Actions runs `.github/workflows/ci.yml` on every pull request, on every push to `main`, and on demand (**Actions → CI → Run workflow**). It runs the same commands as a developer does locally, starting from a clean machine and the example settings.

A second workflow, `.github/workflows/direct.yml`, is not a check: it runs [direct mode on a schedule](#direct-mode-on-a-schedule).

```mermaid
%%{init: {"flowchart": {"rankSpacing": 30, "nodeSpacing": 50}}}%%
flowchart TB
    pr["pull request,<br/>push to main"] --> ci["ci.yml<br/>lint, unit, regression<br/>(no secrets)"]
    trigger["dashboard Worker's<br/>cron triggers"] -- "round, every 4 h;<br/>reconcile, 00:10 UTC" --> direct["direct.yml<br/>round / reconcile<br/>(r2 environment)"]
    hand["by hand"] --> direct
    watchdog["watchdog.yml,<br/>hourly"] -. "a run in<br/>the last 5 h?" .-> direct
    direct --> check{"_state/mode.json<br/>direct (active)?"}
    check -- yes --> r2[("R2 bucket:<br/>rows, snapshot, log")]
    check -- "no: full or<br/>handing-over" --> skip["skip, succeed"]
```

## The jobs

| Job | Runs | Takes about | For |
| --- | --- | --- | --- |
| **Lint and unit tests** (`checks`) | `make install`, `make lint`, `make test` | 1 min | quick feedback; no Docker |
| **Regression layers** (`layers`) | the quick start (`cp .env.example .env`, `make install`, `make up`), then `scripts/regression.py --skip performance dashboard` | 6 min | lint, unit, integration, api, contract, e2e, disruptive |
| **Regression layers, dashboard** (`dashboard`, shards 1 and 2) | `cp .env.example .env`, `make install`, the dashboard and its test browser, then `scripts/regression.py --only dashboard --shard K/2` | 6 and 10 min | each half of the dashboard layer; no Docker |
| **Regression** (`regression`) | after the three above: their reports merged with `scripts/regression.py --skip performance --combine parts/*/` | 1 min | one summary, coverage and traceability check over every part; the check a merge requires |

The jobs use `scripts/regression.py`, the script behind `make regression`, so CI and a local run cannot drift apart. Each part stops at its first failing layer, like locally.

**Why in parts** (REQ-CI-008). The dashboard layer took 15 of the 21 minutes of a single regression job (about 12 s per browser test on a shared runner), so it runs beside the other layers, split in two by `pytest --shard K/N` (`tests/support/shard.py`: the Kth, (K+N)th, ... test collected, so the shards run each test once). A part runs no traceability check, since it sees only some tests; `--combine` does it over all of them. **Regression** is the check a merge requires (and `make release`'s ruleset names), so it runs unless the run was cancelled, even when a part failed: a required check that was skipped would count as passed. A part, or a shard, that failed or wrote no summary fails it. A run takes about 10 minutes instead of 21. Shard 2 is the longer, as the tests over the large synthetic datasets fell to it. The price is more runner time, about 26 minutes per run instead of 22, as each job installs its own tools; with `shard: [1]` in the matrix (and `--shard "$SHARD/1"`) the dashboard runs in one job again.

**Performance is left out.** Shared runners vary too much for the ±15% repeatability check, so those tests run on a developer machine (`make regression`). In CI the layer shows as `skipped (--skip)`, and the requirements covered only by it (REQ-BF-011, REQ-DASH-008, REQ-API-014) as *not run*, not as gaps. Every other requirement still needs a passing test, or the run fails.

## Reading a run

- **Run page:** the regression summary (layers, counts, coverage, traceability) is shown at the bottom of the run's **Summary**, even when the run fails.
- **Artifact `regression-reports`**, kept 30 days: JUnit XML per layer (`junit-dashboard-1of2.xml` and `-2of2` for the shards), the coverage report of every part combined (`coverage-html/index.html`), `regression-summary.md` and the run's traceability matrix. Each part's own reports are kept too, as `part-layers`, `part-dashboard-1` and `part-dashboard-2`.
- **When a run fails:** the summary's *Failed tests* names them, and a part that never reported shows as *not run*; the last 200 log lines of Kafka and PostgreSQL are printed in the last step of **Regression layers**.

To reproduce a CI run locally: `uv run python scripts/regression.py --skip performance` (with `make up` first, like `make regression`), all in one go. With Claude Code, `/eh-test regression` runs the full local version, performance included.

## Security

- **No secrets in CI.** Tests use moto instead of R2, `wrangler dev --local` instead of Cloudflare, and a fake USGS feed; `ci.yml` references no `secrets`. A pull request from a fork can run it safely. Only the scheduled direct-mode workflow holds secrets ([below](#direct-mode-on-a-schedule)).
- **Read-only token** (`permissions: contents: read`), and checkout does not keep it (`persist-credentials: false`).
- **Actions pinned to commit SHAs**, with the release in a comment, chosen at least two weeks after their release. To update one, look up the new release's commit (`gh api repos/<owner>/<action>/commits/<tag> -q .sha`) and change the SHA and the comment together.
- **Deploying stays manual** (`make dashboard-deploy`); CI never holds Cloudflare credentials.
- **Code scanning** is GitHub's CodeQL, in its default setup (no workflow file): Python, JavaScript/TypeScript and the workflows, on every pull request and push to `main` and weekly, beside CI. Findings are in **Security → Code scanning**; they don't block a merge. Dependabot alerts and secret scanning with push protection are on too.

`tests/unit/test_ci.py` checks all of this in the workflow file (REQ-CI-001 to REQ-CI-004), and `tests/unit/test_direct_workflow.py` the scheduled workflow (REQ-CI-005).

## Cost and limits

$0. The repository is public, and GitHub-hosted runners are free for public repositories, with no limit on minutes. Concurrent runs for the same branch are cancelled when a newer commit arrives.

A ruleset on `main` requires both checks before a merge, and blocks force pushes and deleting the branch; the owner can bypass it in an emergency. It was created by the release (`make release`).

## Direct mode on a schedule

`.github/workflows/direct.yml` keeps the bucket and the dashboard current with no machine of ours running: [direct mode](architecture.md#direct-mode) needs only Python and R2 credentials.

| Job | When | Runs |
| --- | --- | --- |
| **Direct round** (`round`) | every 4 hours at :05 (00:05, 04:05, … UTC), started by the dashboard Worker's [cron trigger](#the-trigger) | `python -m earth_hub.direct once --skip-unless-direct`: one poller round, and catch-up if due (`make direct-once`) |
| **Trigger token** (`token`) | once a day, when the trigger passes the days its token has left | Fails within 14 days of the expiry, so GitHub e-mails the token's owner ([the reminder](#the-trigger)); no secrets |
| **Direct reconcile** (`reconcile`) | every day at 00:10 UTC, started by the Worker's other cron trigger | `python -m earth_hub.direct.reconcile --skip-unless-direct` (`make reconcile-direct`; on Sundays `--deep`, every row object read, DEF-058), then `python -m earth_hub.usgs_reconcile --skip-unless-direct --max-age-hours 5` ([against USGS](architecture.md#reconciliation-with-usgs)); either failing fails the run |

Both also run by hand: **Actions → Direct mode → Run workflow**, or `gh workflow run direct.yml -f job=round` (`job=reconcile`); `gh run list --workflow direct.yml` lists the runs. Each run's page shows the command's summary lines.

- **The mode marker switches it.** While the bucket is not in direct mode (full mode, or a handover running), both jobs skip and succeed: `make mode-full` pauses the schedule, `make mode-direct` resumes it. Missing R2 settings still fail the run.
- **One direct process.** The schedule and a `make direct-run` elsewhere would take turns writing; stop `make direct-run` while the schedule is on. Runs never overlap (`concurrency`, never cancelled), and a run stops after 20 minutes; a round cut off is finished by the next, and its lock expires after 15.
- **Catch-up on every round.** The workflow sets `CATCHUP_INTERVAL_SECONDS` and `DELETED_CHECK_INTERVAL_SECONDS` a little under the rounds' spacing and a day, because runs start a little late, unevenly.
- **Secrets.** The four `R2_*` settings are secrets of the `r2` environment, which only `main` can use (`gh secret set <NAME> --env r2` reads the value from the terminal), given only to the step that runs the command; `USGS_CONTACT` is a repository variable. Use an R2 token for this workflow only (Object Read & Write on the bucket), so it can be revoked alone. The workflow has no pull request or push trigger, and pull requests from forks never get secrets.
- **Freshness and cost.** How late an event can be in direct mode is in the [limitations](../overview/limitations.md#direct-mode); none are lost, as the day feed lists the last 24 hours. Each run takes about 1–1½ minutes, free for a public repository; the Cloudflare trigger is free too. R2's operations stay within budget (`tests/unit/test_direct_budget.py`).
- **Failures** e-mail whoever started the run: for the rounds and the reconcile, the trigger token's owner. A trigger GitHub refused shows in the Worker's logs on Cloudflare, and the watchdog fails at its next run.
- **Why Cloudflare starts the rounds** (DEF-052): GitHub's own schedule ran about one slot in five while the repository was private, and once none for 8 hours; Cloudflare's cron triggers fire on time.
- **No schedule in `direct.yml`** (DEF-056): GitHub disables a public repository's scheduled workflows after 60 days without commits, and a disabled workflow cannot be dispatched either. `direct.yml` has only `workflow_dispatch`, and [the watchdog](#the-watchdog) holds the repository's one schedule.

### The trigger

The dashboard's Worker (`dashboard/src/worker.ts`) has two cron triggers in `dashboard/wrangler.template.jsonc`. `5 */4 * * *`: every 4 hours it asks GitHub's API to run `direct.yml`'s round on `main` (`POST /repos/dienarvaez1/earth-hub/actions/workflows/direct.yml/dispatches`, `{"ref": "main", "inputs": {"job": "round", "started_by": "cloudflare-trigger"}}`). `10 0 * * *`: once a day, the same with `"job": "reconcile"`. Its scheduled handler cannot be reached over HTTP, so the page still exposes only its two files. Without a token it starts nothing and says so in its log; a refusal fails the trigger. `tests/dashboard/test_dashboard_trigger.py` runs it under `wrangler dev --test-scheduled` against a local stand-in for GitHub (REQ-CI-006).

To set it up, once, then again before the token expires: run `make trigger-token` in a terminal (`scripts/trigger_token.py`), then `make dashboard-deploy` the first time.

**To check the stored token now** (REQ-CI-010): `make trigger-check`. A Worker secret cannot be read back, so it asks the Worker: `scripts/trigger_check.py` runs it on Cloudflare with the deployed Worker's secrets (`wrangler dev --remote --test-scheduled`, a preview that leaves the deployed Worker alone) and fires its scheduled handler with the "cron" `check`, which no deployed trigger has. The handler reads `direct.yml` with the token, as the daily expiry check does, starts nothing, and logs `trigger check: OK, GitHub accepts the token for direct.yml (N days left)`; a refused or missing token fails it. The token never leaves Cloudflare. It needs `npx wrangler login`.

1. It opens GitHub's form for a fine-grained token with its name, description and **Actions: Read and write** filled in; you pick repository access (only `earth-hub`) and the expiry, and note the date: rounds stop when it passes.
2. It reads the token without showing it and checks it can see `direct.yml` (a read, which starts nothing): a token GitHub refuses, or one without access to the repository, is not stored.
3. It stores it as the Worker's `GITHUB_TOKEN` secret through `wrangler secret put`'s standard input: never printed, written to a file or put on a command line.
4. It prints the expiry GitHub states for the token.

By hand, the same: create the token in **Settings → Developer settings → Fine-grained tokens**, then `cd dashboard && npx wrangler secret put GITHUB_TOKEN`, which reads it from the terminal.

To run every 15 minutes once the repository is public, change the cron to `2,17,32,47 * * * *` and deploy.

**The reminder to renew it** (REQ-CI-007). GitHub states a token's expiry on every reply to it (`GitHub-Authentication-Token-Expiration`). Once a day, the 08:05 UTC trigger reads it (a read of the workflow, which starts nothing) and passes the days left with the round, as the input `token_days_left`. The workflow's **Trigger token** job, which has no secrets, fails when 14 days or fewer are left, with *"The dashboard trigger's GitHub token expires in N days: run make trigger-token"*; GitHub e-mails a failed run to whoever started it, the token's owner (check **Settings → Notifications → Actions**: failed workflows, e-mail). The round runs as usual, and the Worker logs the same warning. After `make trigger-token`, the next day's check passes. Until then it adds a minute of Actions time a day; the job is skipped, at no cost, on the other runs.

### The watchdog

`.github/workflows/watchdog.yml` runs hourly (`47 * * * *`), and by hand. It reads when the newest `direct.yml` run started by `workflow_dispatch` began (`gh run list`, with the job's own token, which can only read), and `scripts/watchdog.py` fails if that was more than 5 hours ago: the trigger stopped, or its token expired. The failure says *"The dashboard Worker's trigger has started no direct.yml run since …: check its cron trigger and its GitHub token (make trigger-token)"*, and GitHub e-mails whoever last changed the workflow's schedule. Rounds run, and skip, in either mode, so it holds in full mode too. No secrets (REQ-CI-009).

**GitHub starts it late, or not at all** (DEF-057). Its first 3-hourly slots, the day it was added, were skipped (18:47 UTC) and run 72 minutes late (21:47); GitHub's schedule for the old daily reconcile, at 05:41, started 4 to 9 hours late every day that week. So it runs hourly, off the busy top of the hour: one skipped or late run is covered by the next, at about 10 seconds a run, free for a public repository. A stopped trigger is still caught, within about 5 hours plus GitHub's delay.

It is the repository's only schedule. After 60 days without commits GitHub disables it, and only it: the rounds go on. **Actions → Watchdog → Enable workflow**, or any push, turns it back on.

**With Claude Code:** `/eh-mode schedule` runs a round now, `/eh-mode runs` lists the recent runs, and `/eh-mode` says whether the schedule writes (direct mode) or skips (full mode).
