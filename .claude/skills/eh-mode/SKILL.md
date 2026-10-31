---
name: eh-mode
description: Earth-Hub's two modes and who writes the R2 bucket — show the mode, hand over from full mode (Docker, Kafka, PostgreSQL) to direct mode (R2 only) and back, run direct mode locally, check direct mode's bucket state, or run a round on GitHub Actions and list direct mode's executions (its audit trail, the 4-hourly rounds a Cloudflare cron trigger starts, which keep the dashboard current with no machine on). Use for /eh-mode, "which mode are we in", "switch to direct mode", "go back to full mode", "can I close my laptop", "is the schedule running", "run a direct round".
argument-hint: "[status | direct | full | usgs [DAYS] | catchup | once | run | reconcile [deep] | schedule | runs]  (default: status)"
---

# Modes

Run from the repository root. How the modes, the handovers and the rounds work: docs/technical/architecture.md#switching-modes
and docs/technical/ci.md#direct-mode-on-a-schedule.

| Argument | Command | Writes |
| --- | --- | --- |
| `status` (default) | `make -s mode`, `make -s direct-runs LIMIT=5` | no |
| `direct` | `make to-direct` ⚠, in the background (a few minutes) | bucket, stops services |
| `full` | `make to-full` ⚠, in the background | bucket, Kafka, PostgreSQL |
| `usgs [DAYS]` | `make reconcile-usgs [DAYS=3]` (any range: `/eh-reconcile`) | no |
| `catchup` | `make catchup-now` ⚠ | bucket, or Kafka in full mode |
| `once` | `make direct-once` ⚠ | bucket |
| `run` | `make direct-run` ⚠, in the background, until stopped | bucket |
| `reconcile` | `make reconcile-direct` (`deep`: `DEEP=1`, one read per row) | no |
| `repair` | `make repair-rows DRY_RUN=1`; then, if it lists rows and the user asked for the repair, `make repair-rows` ⚠ | bucket (not the dry run) |
| `schedule` | `gh workflow run direct.yml -f job=round` ⚠, then `gh run list --workflow direct.yml --limit 1` | bucket |
| `runs` | `make -s direct-runs LIMIT=10`, then `gh run list --workflow direct.yml --limit 5` | no |

- **Everything but `status`, `usgs`, `reconcile`, `runs` and `repair`'s dry run writes the real bucket or stops
  services:** run it only when the user asked for that (the argument counts). Never edit `_state/mode.json` by hand.
- **`make to-direct` / `make to-full` print each step.** Exit 2: refused before starting, nothing changed. Exit 1:
  the named step failed, the steps before it stand, and rerunning is safe. After a switch they catch up and run the
  USGS check; an unexplained difference fails the command, but the switch stands: report the check's table.
- **One direct process.** Before `once`, `run` or `schedule`, check `pgrep -fl 'earth_hub.direct run'`: a local
  `direct-run` and the schedule would take turns writing. Say so rather than stopping one silently.
- **In direct mode, don't start full mode's pipeline** (`make ingest`, `make catchup-once`, `make export-r2`):
  the marker doesn't stop them, and events stored meanwhile make the handover back fail its checks.
- **A failed run:** its audit record (`make -s direct-runs`, then `make -s r2-get KEY=audit/direct/<day>/<run_id>.json`),
  and `gh run view <id> --log-failed`. Empty R2 values mean the `r2` environment's secrets are empty: the user sets
  them (`gh secret set <NAME> --env r2`).
- **A failed `Watchdog` run:** the trigger stopped starting rounds; `make -s trigger-check 2>&1 | grep -v '@' | tail -1`
  says whether its token works. A failed `Trigger token` run: the token expires soon, and the user runs
  `make trigger-token` in a terminal (never run it for them).
- Filter lines with `@` from `gh` output before showing it.

## Report

The mode and since when, whether the rounds write (direct mode) or skip (full mode), and the latest executions
(status, who started them, what they processed). After a handover: the command's last lines and `make -s mode`. After
a round: `make -s r2-get KEY=_state/direct.json | jq -c '{seq, written_at}'` and the dashboard's run (`/eh-dashboard`).
In direct mode, say the computer can be closed; in full mode, that the dashboard updates only while this machine runs
the pipeline.
