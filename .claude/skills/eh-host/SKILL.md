---
name: eh-host
description: Move Earth-Hub's full mode from one host to another, or see who runs it — which host owns the R2 bucket and whether its lease is live, release it on the old host, claim it on a new one (its database rebuilt from the bucket when needed), rebuild this host's database, or have direct mode take over now from a host that is gone. Use for /eh-host, "move to another machine", "which host owns the bucket", "take over from the other host", "the other machine died", "set up this machine in full mode".
argument-hint: "[status | cutover-a | cutover-b | release | claim | rebuild | takeover]  (default: status)"
---

# Hosts

Run from the repository root. Only one host runs full mode. In full mode the bucket's marker,
`_state/mode.json`, names the **owner** (`EARTH_HUB_MACHINE_ID`, the hostname when empty) and its **lease**
(20 minutes, renewed by the host's `exporter` service). Direct mode's hourly rounds are the standby: once the
lease has expired they take the bucket over by themselves (docs/technical/architecture.md#the-standby).

| Argument | Command | Writes |
| --- | --- | --- |
| `status` (default) | `make -s host-status` | no |
| `cutover-a` | `make cutover-a DRY_RUN=1` first and report it; then the user runs `make cutover-a` in a terminal (it asks `Proceed? [y/N]`) ⚠ | bucket (if this machine owns it), stops containers |
| `cutover-b` | `make cutover-b DRY_RUN=1` first and report it; then the user runs `make cutover-b` in a terminal ⚠ | bucket, Kafka, PostgreSQL |
| `release` | `make host-release` ⚠ (in the background: only on the owner; `make to-direct`, a few minutes) | bucket, stops services |
| `claim` | `make host-claim` ⚠ (in the background; see below) | bucket, Kafka, PostgreSQL |
| `rebuild` | the user runs `make host-rebuild` in a terminal ⚠ | bucket, wipes Kafka and PostgreSQL |
| `takeover` | `make host-takeover` ⚠ | bucket |

- **A cutover in one command per machine:** `cutover-a` on the machine giving up full mode, then `cutover-b` on the
  one taking it. Each runs its pre-checks (every one reported: R2, `.env`, the USGS feed, Docker, the mode), then a dry
  run of its steps, then asks before changing anything, then checks the result. Their `DRY_RUN=1` is safe to run any
  time; never answer the prompt for the user.
- **A planned move:** `release` on the old host, then `claim` on the new one, whenever convenient. In between
  direct mode writes the bucket and the dashboard stays current.
- **A host that died:** wait for the next hourly round (its lease ends 20 minutes after its last export), or
  `takeover`; then `claim` on the host that should run full mode. A takeover over an export left unfinished
  rolls the bucket back to its last complete export, and its run fails so GitHub e-mails: report it.
- **`claim` rebuilds when it has to.** An empty database is loaded from the bucket. One that differs (a host
  back after a takeover) is checked, backed up and wiped first, and that asks for the machine ID to be typed:
  if `claim` stops at that prompt, tell the user to run `make host-claim` (or `make host-rebuild`) in a terminal.
  Never type the confirmation for them and never pass `--yes`.
- **Refusals change nothing** (exit 2): this host already the owner, another host's live lease (release it
  there, or `takeover` if it is gone), full mode with no owner (`make to-direct` on the host that runs it),
  a handover in progress, Docker stopped, a local `make direct-run`. `rebuild check` refusing means this database
  holds events the bucket lacks: say which, and that nothing was wiped.
- **Everything but `status` writes the real bucket or this host's data:** run it only when the user asked for
  that (the argument counts). `DRY_RUN=1` lists the steps of `release`, `claim` and `rebuild`.

## Report

The owner, its lease (live or expired), this host's ID and whether it is the owner, what its database holds
against the bucket, and the latest direct executions (a `took over from …` line names a takeover). After
`release`: direct mode writes. After `claim`: `make -s mode` and that this host now exports every 5 minutes after
a change. Backups are in `reports/backup/<time>/`.
