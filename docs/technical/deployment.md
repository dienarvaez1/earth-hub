# Deployment

Where each part of Earth-Hub runs, how to set up a machine for full mode, and how to move full mode from one machine to another. How the parts work is in the [architecture](architecture.md); the commands are in the [cheat sheet](../overview/cheatsheet.md), and the `/eh-host` skill runs those of this page.

## Where each part runs

| Part | Runs on | Started by |
| --- | --- | --- |
| Full mode: poller, consumers, catch-up, API, the `exporter` service, Kafka, PostgreSQL | One machine of yours, the bucket's **owner** | `make host-claim` (or `make cutover-b`); `make ingest` restarts the pipeline |
| Direct mode's rounds, the **standby** | GitHub Actions (`.github/workflows/direct.yml`) | The dashboard Worker's cron trigger, every hour ([CI](ci.md#direct-mode-on-a-schedule)) |
| The dashboard and its cron trigger | A Cloudflare Worker | `make dashboard-deploy` |
| The data the dashboard reads | The Cloudflare R2 bucket | Written by the owner's `exporter` in full mode, by the hourly rounds otherwise |

Only one machine runs full mode at a time. The bucket's marker, `_state/mode.json`, names it (`EARTH_HUB_MACHINE_ID`) and holds its lease, which its exports renew. While the lease is live, the hourly rounds write nothing; once it expires, about 20 minutes after the owner's last export, the next round takes the bucket over and keeps the dashboard current with no machine running ([the standby](architecture.md#the-standby)). Any other machine needs nothing running at all: it is a machine that can claim the bucket later.

On the owner, the poller reads the USGS feed about every minute, and the `exporter` writes R2 at most every `EXPORT_INTERVAL_SECONDS` after something was stored: 300 by default, up to about 6.6 GB of uploads a day, since each export with changes uploads the manifest and the snapshot, about 23 MB. The lowest, 60, updates R2 every minute, for up to about 33 GB a day.

## Setting up a machine

Once per machine, before it can take full mode:

1. Install git, [uv](https://docs.astral.sh/uv/) (it installs Python 3.13, pinned in `.python-version`) and a container runtime: Docker Desktop, or Colima with the Docker CLI. Node is needed only to work on the dashboard.
2. Get the code: `git clone https://github.com/dienarvaez1/earth-hub.git`, `cd earth-hub`, `make install`.
3. Copy `.env.example` to `.env` and fill it in:
   - `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET` and `USGS_CONTACT`: the same values on every machine. Copy them through a password manager or `scp`, never through git, an issue or a chat. `.env` is never committed.
   - `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`: anything; they are local to the machine.
   - `EARTH_HUB_MACHINE_ID`: a name no other machine uses, such as `host-b`. Empty means the hostname.
   - `EXPORT_INTERVAL_SECONDS`: see above.
4. Start the container runtime (`colima start`, or open Docker Desktop).

`make mode` then prints the bucket's mode and `this host: <its ID>`: the machine reaches the bucket.

## Cutover from A to B

A runs full mode, or no machine does (the hourly rounds write the bucket); B is to run full mode from now on. One command on each machine does it: `make cutover-a` on A, then `make cutover-b` on B (`scripts/cutover.py`). Each runs in four parts:

1. **Pre-checks**, read-only. Every check is printed as `ok`, `warn` or `FAIL`; one that fails stops the command with nothing changed, and says what to fix.
2. **A dry run** of the steps it would take.
3. **`Proceed with the cutover? [y/N]`**. Nothing changes unless the answer is yes. With `DRY_RUN=1` the command stops before the question: it is safe to run any time.
4. **The steps**, then checks that they did what they should.

Run both in a terminal: they ask for confirmation, and `make cutover-b` may ask for B's machine ID to be typed (below).

### On A

1. `git pull`, so A runs the same code as B.
2. `make cutover-a DRY_RUN=1`. The pre-checks:

   | Check | Fails when |
   | --- | --- |
   | R2 reachable and the mode readable | the R2 settings are wrong, or the bucket is unreachable |
   | No `make direct-run` on this machine | one is running: stop it (Ctrl-C) |
   | Direct mode's hourly rounds ran within 2 hours | a warning only: the standby looks stopped (`make direct-runs`, `gh run list --workflow watchdog.yml`) |
   | Docker | a warning only: without it there are no containers to stop |
   | The bucket's state | full mode with no owner (from before Milestone 9: run `make to-direct` on the machine that runs it), or a handover in progress |

   If A owns the bucket, the dry run lists `make host-release`'s steps: wait for the consumers, `make reconcile`, `make mode-direct`, `make down`. Otherwise there is nothing to release, and A only stops its containers.
3. `make cutover-a`, and answer `y`. A releases the bucket if it owns it, then `make down` stops its containers. Their data and Docker or Colima itself stay, so A can claim the bucket again later. From here the hourly rounds write the bucket until B claims it.

### On B

1. Set B up as above, if not done yet, and `git pull`.
2. `make cutover-b DRY_RUN=1`. The pre-checks:

   | Check | Fails when |
   | --- | --- |
   | uv, Docker running | either is missing, or Docker is stopped |
   | `.env` present, its settings filled | a setting is empty: it lists which |
   | `EARTH_HUB_MACHINE_ID` set | a warning only: the hostname is used |
   | R2 reachable and the mode readable | as on A |
   | The USGS feed answers | no network, or USGS is down |
   | No `make direct-run` on this machine | as on A |
   | The bucket can be claimed | B owns it already; another machine owns it with a live lease (run `make cutover-a` there first); full mode with no owner |

   The dry run is `make host-claim DRY_RUN=1`'s: start Kafka and PostgreSQL, hand over to full mode, wait for the consumers, `make reconcile`. It takes a minute or two, as comparing B's database with the bucket downloads the snapshot.
3. `make cutover-b`, and answer `y`. It runs `make host-claim`, a few minutes:
   - Kafka and PostgreSQL start and are migrated;
   - the bucket is frozen (the hourly rounds pause);
   - B's database is loaded from the bucket when it is empty, about 115,000 records. If it holds data that differs from the bucket, as on a machine that ran full mode before, it is rebuilt instead: `rebuild check` must find nothing the bucket lacks, a backup goes to `reports/backup/<time>/` and is checked to restore, and you type B's machine ID before Kafka and PostgreSQL are wiped;
   - `make reconcile` must pass, and an export dry run must find B's database and the bucket the same;
   - the bucket switches to full mode, owned by B with a live lease; the poller, catch-up and the `exporter` start; it catches up with USGS and checks against it.

   Then `make cutover-b` checks that B owns the bucket with a live lease and that the poller, both consumers, catch-up, the API and the `exporter` run. If a step fails, the bucket goes back to direct mode with nothing lost: fix the cause and run it again.

### Checking the result

- `make host-status` on B: owner B, the lease live, `(the owner)` beside this host.
- `make direct-runs LIMIT=3`, after the next hour: a skipped round, its reason naming B as the owner.
- `make -s export-r2-runs | tail -1` on B: a recent export. After a new earthquake, the dashboard updates within `EXPORT_INTERVAL_SECONDS`.

### Afterwards

- **A runs nothing.** Don't start `make ingest`, `make export-r2` or `make direct-run` there: an export would be refused (the bucket is B's), and a direct process beside the hourly rounds would be a second writer.
- **If B stops** (asleep, offline, crashed) for more than about 20 minutes, the next hourly round takes the bucket over and catches up from USGS: nothing is lost, and the dashboard is at most about 80 minutes behind. B's `exporter` stops once it finds the bucket is no longer B's. To put B back in charge, run `make cutover-b` on B again; its database differs from the bucket by then, so it is rebuilt, with the backup and the typed machine ID. Keeping B awake avoids this.
- **To move back, or on to another machine**, run the same two commands with the roles swapped: `make cutover-a` on B, then `make cutover-b` on the machine taking over.
- **If B is gone for good**, `make host-takeover` on any machine has direct mode take the bucket over at once, without waiting for the lease; then `make cutover-b` on the next machine.

The steps one by one, without the wrappers, are `make host-release` on A and `make host-claim` on B (`scripts/modes.py`, `/eh-host release|claim`).
