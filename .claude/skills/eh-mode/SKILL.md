---
name: eh-mode
description: Earth-Hub's two modes and who writes the R2 bucket — show the mode, hand over from full mode (Docker, Kafka, PostgreSQL) to direct mode (R2 only) and back, run direct mode locally, check direct mode's bucket state, or run and list the hourly GitHub Actions schedule that keeps the dashboard current with no machine on. Use for /eh-mode, "which mode are we in", "switch to direct mode", "go back to full mode", "can I close my laptop", "is the schedule running", "run a direct round".
argument-hint: "[status | direct | full | usgs [DAYS] | catchup | once | run | reconcile [deep] | schedule | runs]  (default: status)"
---

# Modes

Run from the repository root. One marker in the bucket, `_state/mode.json`, says which mode writes it
(docs/technical/architecture.md#switching-modes). Full mode: poller → Kafka → consumer → PostgreSQL → API →
`make export-r2`. Direct mode: poller → validate → R2, with the 15-minute workflow `.github/workflows/direct.yml`
(docs/technical/ci.md#direct-mode-on-a-schedule) keeping the dashboard current while no machine runs.

| Argument | Command | Writes |
| --- | --- | --- |
| `status` (default) | `make -s mode`, `gh run list --workflow direct.yml --limit 5` | no |
| `direct` | `make mode-direct` ⚠ (stops poller and catch-up, exports, hands over; ~1 min) | bucket, stops services |
| `full` | `make mode-full` ⚠ (replays direct mode's log into Kafka, checks, switches back; pauses the schedule) | bucket, Kafka, PostgreSQL |
| `usgs [DAYS]` | `make reconcile-usgs [DAYS=3]`: the mode writing the bucket against USGS, every difference pending (explained) or not | no |
| `catchup` | `make catchup-now` ⚠ (in either mode: day feed, month feed and deletions, whether due or not) | bucket, or Kafka in full mode |
| `once` | `make direct-once` ⚠ (one poller round, and catch-up if due) | bucket |
| `run` | `make direct-run` ⚠ (until stopped; run it in the background) | bucket |
| `reconcile` | `make reconcile-direct` (`deep`: `make reconcile-direct DEEP=1`, one read per row) | no |
| `schedule` | `gh workflow run direct.yml -f job=round` ⚠, then wait for `gh run list --workflow direct.yml --limit 1` | bucket |
| `runs` | `gh run list --workflow direct.yml --limit 10` | no |

- **After a switch, `direct` and `full` catch up and run the USGS check by themselves**; a check that finds an
  unexplained difference fails the command, but the switch stands. Report the check's table.
- **Everything but `status`, `usgs`, `reconcile` and `runs` writes the real bucket or stops services:** run it only
  when the user asked for that (the argument counts). Never edit `_state/mode.json` by hand.
- **One direct process.** Before `once`, `run` or `schedule`, check `pgrep -fl 'earth_hub.direct run'`; a local
  `direct-run` and the schedule would take turns writing. Say so rather than stopping one silently.
- **In direct mode, don't start full mode's pipeline** (`make ingest`, `make catchup-once`, `make export-r2`):
  the marker doesn't stop them, and events stored meanwhile make the handover back fail its checks.
- `mode-full` needs the consumers and the API (it starts them); `mode-direct` needs full mode idle and
  `make reconcile` clean. A failed `mode-direct` resumes full mode; a failed `mode-full` leaves the bucket in
  direct mode, and running it again is safe (it sends only the log entries not yet replayed).
- A failed scheduled run: `gh run view <id> --log-failed`. Empty R2 values in its `env:` block mean the
  `r2` environment's secrets are empty: the user sets them (`gh secret set -f <file> --env r2` with the four `R2_*` lines).
- Filter lines with `@` from `gh` output before showing it.

## Report

The mode and since when, whether the schedule is active (direct mode) or paused (full mode), and the last
scheduled run's result. After a handover: the command's last lines and `make -s mode`. After a round: the
new sequence number and time, `make -s r2-get KEY=_state/direct.json | jq -c '{seq, written_at}'`, today's newest log files,
`make -s r2-ls PREFIX=log/$(date -u +%F)/ LIMIT=0 | tail -3` (the previous day's folder if empty), and the dashboard's run
(`/eh-dashboard`). In direct mode, say the computer can be closed; in full mode, that the dashboard updates
only while this machine runs the pipeline and exports.
