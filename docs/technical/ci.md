# Continuous integration

GitHub Actions runs `.github/workflows/ci.yml` on every pull request, on every push to `main`, and on demand (**Actions → CI → Run workflow**). It runs the same commands as a developer does locally, starting from a clean machine and the example settings.

## The jobs

| Job | Runs | Takes about | For |
| --- | --- | --- | --- |
| **Lint and unit tests** (`checks`) | `make install`, `make lint`, `make test` | 1 min | quick feedback; no Docker |
| **Regression** (`regression`) | the quick start (`cp .env.example .env`, `make install`, the dashboard and its test browser, `make up`), then `scripts/regression.py --skip performance` | usually 9–15 min | every layer but performance: lint, unit, integration, api, contract, e2e, dashboard, disruptive; then the traceability check |

The regression job uses `scripts/regression.py`, the script behind `make regression`, so CI and a local run cannot drift apart. It stops at the first failing layer, like locally.

**Performance is left out.** Shared runners vary too much for the ±15% repeatability check, so those tests run on a developer machine (`make regression`). In CI the layer shows as `skipped (--skip)`, and the three requirements covered only by it (REQ-BF-011, REQ-DASH-008, REQ-API-014) as *not run*, not as gaps. Every other requirement still needs a passing test, or the run fails.

## Reading a run

- **Run page:** the regression summary (layers, counts, coverage, traceability) is shown at the bottom of the run's **Summary**, even when the run fails.
- **Artifact `regression-reports`**, kept 30 days: JUnit XML per layer, the coverage report (`coverage-html/index.html`), `regression-summary.md` and the run's traceability matrix.
- **When a run fails:** the summary's *Failed tests* names them; the last 200 log lines of Kafka and PostgreSQL are printed in the last step.

To reproduce a CI run locally: `uv run python scripts/regression.py --skip performance` (with `make up` first, like `make regression`). With Claude Code, `/eh-test regression` runs the full local version, performance included.

## Security

- **No secrets.** Tests use moto instead of R2, `wrangler dev --local` instead of Cloudflare, and a fake USGS feed; the workflow references no `secrets`. A pull request from a fork can run it safely.
- **Read-only token** (`permissions: contents: read`), and checkout does not keep it (`persist-credentials: false`).
- **Actions pinned to commit SHAs**, with the release in a comment, chosen at least two weeks after their release. To update one, look up the new release's commit (`gh api repos/<owner>/<action>/commits/<tag> -q .sha`) and change the SHA and the comment together.
- **Deploying stays manual** (`make dashboard-deploy`); CI never holds Cloudflare credentials.

`tests/unit/test_ci.py` checks all of this in the workflow file (REQ-CI-001 to REQ-CI-004).

## Cost and limits

$0. A private repository gets 2,000 free Actions minutes a month, about 120 runs of both jobs; a public one has no limit. Concurrent runs for the same branch are cancelled when a newer commit arrives.

Until the repository is public, GitHub's free plan cannot require these checks before merging: a red run is visible on the pull request but does not block the merge button. Requiring them (branch protection on `main`) is part of the release.
