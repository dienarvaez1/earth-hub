"""Switch modes, or backfill in full mode, in one command each, with every check around them.

    uv run python scripts/modes.py to-full                # make to-full
    uv run python scripts/modes.py backfill START [END]   # make backfill-full START=... [END=...]
    uv run python scripts/modes.py to-direct              # make to-direct
    ... --dry-run                                         # list the steps, run nothing

Each runs the existing make targets (the handovers stay in earth_hub.direct.handover), with the
checks a person would otherwise make by hand: Docker running, the bucket in the expected mode,
no `make direct-run` left running, the consumers caught up (lag 0) before a switch and after a
backfill, and `make reconcile` clean. It prints each step, and stops at the first that fails
with what failed; every step can be run again.

- to-full:   make up, make mode-full (replays direct mode's log, switches, restarts the poller and
             catch-up, catches up, checks against USGS), waits for the consumers, make reconcile.
- backfill:  full mode only: make ingest (the consumers), make backfill, waits for
             consumer-backfill, make reconcile, then make export-r2 so R2 and the dashboard
             have the history.
- to-direct: waits for the consumers, make reconcile, make mode-direct (stops the poller and
             catch-up, exports, switches, catches up, checks against USGS), then make down: direct
             mode needs no Docker (the data is kept; to-full starts it again).

All three write the real bucket, Kafka or PostgreSQL: run them only when meant (the /eh-mode and
/eh-backfill skills run them when asked).

Moving full mode between hosts (Milestone 9; the /eh-host skill):

    uv run python scripts/modes.py host-status     # make host-status: read-only
    uv run python scripts/modes.py host-release    # make host-release: on the owner
    uv run python scripts/modes.py host-claim      # make host-claim: on the new host
    uv run python scripts/modes.py host-rebuild    # make host-rebuild
    uv run python scripts/modes.py host-takeover   # make host-takeover: for a host known gone

- host-status:   who owns the bucket and its lease, this host's ID, whether this host's database
                 is empty or holds anything the bucket lacks, direct mode's latest executions.
- host-release:  only on the owner: to-direct. Direct mode's hourly rounds write from then on.
- host-claim:    on any host: the bucket in direct mode (a host whose lease expired is taken over
                 first), make up, then make mode-full, which makes this host the owner; when its
                 database is empty, or differs from the bucket, it is rebuilt from it (below).
                 Only the handover's own comparison counts as differing (reports/handover.json):
                 any other failure of make mode-full stops it with nothing wiped (DEF-062).
- host-rebuild:  what wiping this host's database would lose must be nothing (`rebuild check`),
                 a backup of PostgreSQL and the topics in reports/backup/ that restores, and the
                 machine ID typed; then make reset, make up, make migrate, make mode-full
                 REBUILD=1. Run in a terminal: it asks for the confirmation.
- host-takeover: direct mode takes the bucket over now, without waiting for the lease.
"""

import argparse
import json
import re
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OUTCOME = REPO / "reports" / "handover.json"  # earth_hub.direct.handover.OUTCOME, from REPO
GROUPS = ("earth-hub-consumer", "earth-hub-consumer-backfill")
# kafka-consumer-groups --describe: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
LAG = 5
DIRECT, FULL = "direct (active)", "full (active)"


class Refused(Exception):
    """The starting state is not one this command runs from; nothing was changed."""


class Failed(Exception):
    """A step failed; what ran before it stands."""


@dataclass
class Shell:
    """Runs commands from the repository's root. `read` changes nothing; `run` may change
    something and streams its output (only listed in a dry run)."""

    dry_run: bool = False
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    ran: list[list[str]] = field(default_factory=list)

    def read(self, *cmd: str) -> tuple[int, str]:
        result = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, check=False)
        return result.returncode, result.stdout + result.stderr

    def run(self, *cmd: str) -> None:
        self.ran.append(list(cmd))
        shown = " ".join(cmd)
        if self.dry_run:
            print(f"    would run: {shown}")
            return
        print(f"    $ {shown}", flush=True)
        if subprocess.run(cmd, cwd=REPO, check=False).returncode != 0:
            raise Failed(f"`{shown}` failed (above)")


class Steps:
    def __init__(self, shell: Shell, total: int):
        self.shell, self.total, self.n = shell, total, 0

    def __call__(self, title: str) -> None:
        self.n += 1
        print(f"[{self.n}/{self.total}] {title}", flush=True)


# The checks.


def require_docker(shell: Shell) -> None:
    code, _ = shell.read("docker", "info")
    if code != 0:
        raise Refused("Docker is not running: start Docker Desktop, then run this again")


def mode(shell: Shell) -> str:
    code, out = shell.read("uv", "run", "python", "-m", "earth_hub.direct.handover", "status")
    if code != 0:
        raise Failed(f"could not read the mode: {out.strip()[-300:]}")
    # The mode's line ("full (active) since …"); log lines and the owner's come around it.
    lines = [line for line in out.splitlines() if line.startswith(("full (", "direct ("))]
    return lines[-1] if lines else ""


@dataclass
class Marker:
    line: str  # "full (active) since …"
    owner: str | None
    lease_live: bool
    this_host: str


def marker(shell: Shell) -> Marker:
    code, out = shell.read("uv", "run", "python", "-m", "earth_hub.direct.handover", "status")
    if code != 0:
        raise Failed(f"could not read the mode: {out.strip()[-300:]}")
    owner = re.search(r"^owner (\S+), lease until \S+ \((live|expired)\)", out, re.M)
    host = re.search(r"^this host: (\S+)", out, re.M)
    return Marker(
        line=mode(shell),
        owner=owner.group(1) if owner else None,
        lease_live=bool(owner) and owner.group(2) == "live",
        this_host=host.group(1) if host else "",
    )


def require_no_direct_run(shell: Shell) -> None:
    code, out = shell.read("pgrep", "-fl", "earth_hub.direct run")
    if code == 0 and out.strip():
        raise Refused(
            "a `make direct-run` is running on this machine: stop it (Ctrl-C) first, or the two "
            "modes would write in turn"
        )


def lag(shell: Shell, group: str) -> int | None:
    """Messages left to read by `group`, from the broker; None when it cannot say."""
    code, out = shell.read(
        "docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "localhost:9092", "--describe", "--group", group,
    )  # fmt: skip
    if code != 0:
        return None
    total, rows = 0, 0
    for line in out.splitlines():
        cols = line.split()
        if len(cols) > LAG and cols[0] == group and re.fullmatch(r"\d+|-", cols[LAG]):
            rows += 1
            total += 0 if cols[LAG] == "-" else int(cols[LAG])
    return total if rows else None


def wait_for_consumers(shell: Shell, groups, timeout_s: float, every_s: float = 10) -> None:
    if shell.dry_run:
        print(f"    would wait for lag 0 in {', '.join(groups)}")
        return
    deadline = shell.clock() + timeout_s
    while True:
        left = {g: lag(shell, g) for g in groups}
        if all(v == 0 for v in left.values()):
            print("    lag 0: " + ", ".join(groups), flush=True)
            return
        shown = ", ".join(f"{g} {'?' if v is None else v}" for g, v in left.items())
        if shell.clock() >= deadline:
            minutes = f"{timeout_s / 60:.0f} min"
            raise Failed(
                f"the consumers did not catch up within {minutes} ({shown}); "
                "check /eh-status, then run this again"
            )
        print(f"    waiting: {shown}", flush=True)
        shell.sleep(every_s)


# The commands.


def to_full(shell: Shell, *, wait_s: float = 600) -> None:
    step = Steps(shell, 6)
    step("Docker is running")
    require_docker(shell)
    step("Kafka and PostgreSQL up")
    shell.run("make", "up")
    step("The bucket is in direct mode, and no `make direct-run` runs here")
    current = mode(shell)
    if current.startswith(FULL):
        raise Refused(f"already in full mode: {current}")
    if not current.startswith(DIRECT):
        raise Refused(f"the bucket is mid-handover or unreadable ({current}): see /eh-mode")
    require_no_direct_run(shell)
    step("Hand over to full mode (make mode-full)")
    shell.run("make", "mode-full")
    step("The consumers caught up")
    wait_for_consumers(shell, GROUPS, wait_s)
    step("Kafka against PostgreSQL (make reconcile)")
    shell.run("make", "reconcile")
    if shell.dry_run:
        print("Dry run: nothing was changed.")
        return
    print(f"Full mode: {mode(shell)}. The poller and catch-up run; "
          "the dashboard updates when you run make export-r2.")  # fmt: skip


def backfill(shell: Shell, start: date, end: date | None, *, wait_s: float = 3600) -> None:
    if end is not None and end <= start:
        raise Refused(f"END ({end}) must be after START ({start})")
    step = Steps(shell, 7)
    step("Docker is running")
    require_docker(shell)
    step("Kafka and PostgreSQL up, the bucket in full mode")
    shell.run("make", "up")
    current = mode(shell)
    if not current.startswith(FULL):
        raise Refused(f"backfill runs in full mode only, and the bucket is in {current}: "
                      "run make to-full (/eh-mode full) first")  # fmt: skip
    step("The pipeline running (make ingest: the consumers store what the backfill sends)")
    shell.run("make", "ingest")
    step(f"Backfill {start} to {end or 'now'} from USGS FDSN (make backfill)")
    shell.run("make", "backfill", f"START={start}", *([f"END={end}"] if end else []))
    step("consumer-backfill caught up")
    wait_for_consumers(shell, ("earth-hub-consumer-backfill",), wait_s)
    step("Kafka against PostgreSQL (make reconcile)")
    shell.run("make", "reconcile")
    step("R2 and the dashboard updated (make export-r2)")
    shell.run("make", "export-r2")
    if shell.dry_run:
        print("Dry run: nothing was changed.")
        return
    print("Backfill done: stored, reconciled and exported. `make -s export-r2-runs | tail -1` "
          "shows the export's counts.")  # fmt: skip


def to_direct(shell: Shell, *, wait_s: float = 600) -> None:
    step = Steps(shell, 7)
    step("Docker is running")
    require_docker(shell)
    step("The bucket is in full mode")
    current = mode(shell)
    if current.startswith(DIRECT):
        raise Refused(f"already in direct mode: {current}")
    if not current.startswith(FULL):
        raise Refused(f"the bucket is mid-handover or unreadable ({current}): see /eh-mode")
    step("The consumers caught up")
    wait_for_consumers(shell, GROUPS, wait_s)
    step("Kafka against PostgreSQL (make reconcile; the handover needs it clean)")
    shell.run("make", "reconcile")
    step("Hand over to direct mode (make mode-direct)")
    shell.run("make", "mode-direct")
    step("Docker stopped (make down; the data is kept, make to-full starts it again)")
    shell.run("make", "down")
    step("Direct mode's latest executions")
    shell.run("make", "-s", "direct-runs", "LIMIT=3")
    if shell.dry_run:
        print("Dry run: nothing was changed.")
        return
    print("Direct mode: the hourly rounds write the bucket; this computer can be closed.")


def host_status(shell: Shell) -> None:
    """Read-only."""
    current = marker(shell)
    print(f"The bucket: {current.line}")
    if current.owner:
        lease = "live" if current.lease_live else "expired: the next direct round takes over"
        print(f"Owner: {current.owner}, lease {lease}")
    print(
        f"This host: {current.this_host}"
        + ("  (the owner)" if current.owner == current.this_host else "")
    )
    code, _ = shell.read("docker", "info")
    if code != 0:
        print("This host's database: Docker is not running")
    else:
        code, out = shell.read("uv", "run", "python", "-m", "earth_hub.direct.rebuild", "check")
        print(f"This host's database: {out.strip().splitlines()[-1] if out.strip() else '?'}")
    _, runs = shell.read("make", "-s", "direct-runs", "LIMIT=5")
    print("Direct mode's latest executions:\n" + runs.rstrip())


def host_release(shell: Shell) -> None:
    current = marker(shell)
    if not current.line.startswith(FULL):
        raise Refused(f"the bucket is not in full mode ({current.line}): nothing to release")
    if current.owner != current.this_host:
        raise Refused(
            f"this host ({current.this_host}) is not the owner ({current.owner}): release it on "
            "the owner, or claim it here with make host-claim"
        )
    to_direct(shell)
    print("Released: claim it on the new host with make host-claim (/eh-host claim).")


def host_takeover(shell: Shell) -> None:
    current = marker(shell)
    if not current.line.startswith(FULL) or current.owner is None:
        raise Refused(
            f"no host owns the bucket in full mode ({current.line}): nothing to take over"
        )
    print(f"Direct mode takes the bucket over from {current.owner} now.")
    shell.run("uv", "run", "python", "-m", "earth_hub.direct", "once", "--take-over")


def _ready_to_claim(shell: Shell) -> Marker:
    current = marker(shell)
    if current.line.startswith(FULL):
        if current.owner == current.this_host:
            raise Refused(
                f"this host ({current.this_host}) owns the bucket already; if another host uses "
                "this ID too, give each its own EARTH_HUB_MACHINE_ID"
            )
        if current.owner is None:
            raise Refused(
                "full mode with no owner (a bucket from before): on the host that runs it, "
                "make to-direct, then claim it here"
            )
        if current.lease_live:
            raise Refused(
                f"{current.owner} owns the bucket and its lease is live: release it there "
                "(make host-release), or, if that host is gone, make host-takeover first"
            )
        print(f"    {current.owner}'s lease has expired: direct mode takes the bucket over first")
        shell.run("uv", "run", "python", "-m", "earth_hub.direct", "once")
    elif not current.line.startswith(DIRECT):
        raise Refused(f"the bucket is mid-handover or unreadable ({current.line}): see /eh-mode")
    return current


def _require_differs(shell: Shell) -> None:
    """After `make mode-full` failed: go on to the rebuild only when the handover itself found
    PostgreSQL and the bucket different, and the bucket is back in direct mode. Any other
    failure (a build, Compose, the check against USGS after the switch) wipes nothing (DEF-062)."""
    try:
        outcome = json.loads(OUTCOME.read_text())
    except (OSError, ValueError):
        outcome = {}
    now = marker(shell)
    if outcome.get("command") == "full" and outcome.get("outcome") == "switched":
        why = f"the handover switched, then a later step failed; the bucket: {now.line}"
    elif outcome.get("command") != "full" or outcome.get("outcome") != "differs":
        why = outcome.get("reason") or "it stopped before the handover's comparison"
    elif not now.line.startswith(DIRECT):
        why = f"the bucket is not back in direct mode: {now.line}"
    else:
        return
    raise Failed(f"`make mode-full` failed, not over a difference ({why}); nothing was wiped")


def _rebuild_database(shell: Shell, machine: str, confirm: Callable[[str], bool]) -> None:
    """Check, back up, confirm, wipe, migrate: this host's database ready for a load."""
    shell.run("make", "ingest-stop")
    code, out = shell.read("uv", "run", "python", "-m", "earth_hub.direct.rebuild", "check")
    if code == 1:
        raise Refused(f"wiping this host's database would lose data:\n{out.strip()}")
    if code != 0:
        raise Failed(f"could not compare the database with the bucket: {out.strip()[-300:]}")
    directory = f"reports/backup/{time.strftime('%Y-%m-%dT%H-%M-%S')}"
    shell.run("uv", "run", "python", "-m", "earth_hub.direct.rebuild", "backup", directory)
    if shell.dry_run:
        print(f"    would ask for this host's machine ID ({machine}) before wiping")
    elif not confirm(machine):
        raise Refused("not confirmed: nothing was wiped")
    shell.run("make", "reset")
    shell.run("make", "up")
    shell.run("make", "migrate")


def host_claim(
    shell: Shell, *, confirm: Callable[[str], bool], rebuild: bool = False, wait_s: float = 600
) -> None:
    step = Steps(shell, 6)
    step("Docker is running")
    require_docker(shell)
    step("The bucket can be claimed (direct mode, or a host whose lease expired)")
    current = _ready_to_claim(shell)
    require_no_direct_run(shell)
    step("Kafka and PostgreSQL up")
    shell.run("make", "up")
    shell.run("make", "migrate")
    code, out = shell.read("uv", "run", "python", "-m", "earth_hub.direct.rebuild", "status")
    # `rebuild status` exits 1 when the database was read and wiping it would lose data, as
    # `check` does: read, not empty, so the handover is tried first (DEF-060).
    empty = code == 0 and "empty" in out
    if code == 1:
        code = 0
    step("Hand over to full mode, this host the owner (make mode-full)")
    if code != 0 and shell.dry_run:  # PostgreSQL not started in a dry run
        print("    would run: make mode-full REBUILD=1 if this host's database is empty; else make "
              "mode-full, and if it differs from the bucket, a rebuild (check, backup, the machine "
              "ID typed, make reset)")  # fmt: skip
    elif code != 0:
        raise Failed(f"could not read this host's database: {out.strip()[-300:]}")
    elif empty:
        shell.run("make", "mode-full", "REBUILD=1")
    elif rebuild:
        _rebuild_database(shell, current.this_host, confirm)
        shell.run("make", "mode-full", "REBUILD=1")
    else:
        OUTCOME.unlink(missing_ok=True)
        try:
            shell.run("make", "mode-full")
        except Failed:
            _require_differs(shell)
            print("    this host's database differs from the bucket: rebuilding it from the bucket")
            _rebuild_database(shell, current.this_host, confirm)
            shell.run("make", "mode-full", "REBUILD=1")
    step("The consumers caught up")
    wait_for_consumers(shell, GROUPS, wait_s)
    step("Kafka against PostgreSQL (make reconcile)")
    shell.run("make", "reconcile")
    if shell.dry_run:
        print("Dry run: nothing was changed.")
        return
    print(f"Claimed: {mode(shell)}. This host exports every 5 minutes after a change.")


def ask(machine: str) -> bool:
    try:
        answer = input(
            f"Type this host's machine ID ({machine}) to wipe its Kafka and PostgreSQL "
            "(the backup is above): "
        )
    except EOFError:  # no input to read (DEF-059): not typed, so nothing is wiped
        print("\nNo answer: run it in a terminal.")
        return False
    return answer.strip() == machine


def parse_date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a date (YYYY-MM-DD): {text}") from None


def main(argv: list[str] | None = None, shell: Shell | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="list the steps, run nothing")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("to-full", help="direct mode to full mode")
    commands.add_parser("to-direct", help="full mode to direct mode")
    fill = commands.add_parser("backfill", help="backfill a range in full mode, then export")
    fill.add_argument("start", type=parse_date)
    fill.add_argument("end", type=parse_date, nargs="?")
    commands.add_parser("host-status", help="who owns the bucket, this host (read-only)")
    commands.add_parser("host-release", help="on the owner: hand the bucket to direct mode")
    commands.add_parser("host-takeover", help="direct mode takes the bucket over now")
    for name in ("host-claim", "host-rebuild"):
        command = commands.add_parser(name, help="make this host the bucket's owner")
        command.add_argument("--yes", action="store_true", help=argparse.SUPPRESS)  # tests only
    args = parser.parse_args(argv)
    shell = shell or Shell()
    shell.dry_run = shell.dry_run or args.dry_run
    try:
        confirm = (lambda _: True) if getattr(args, "yes", False) else ask
        commands = {
            "to-full": lambda: to_full(shell),
            "to-direct": lambda: to_direct(shell),
            "backfill": lambda: backfill(shell, args.start, args.end),
            "host-status": lambda: host_status(shell),
            "host-release": lambda: host_release(shell),
            "host-takeover": lambda: host_takeover(shell),
            "host-claim": lambda: host_claim(shell, confirm=confirm),
            "host-rebuild": lambda: host_claim(shell, confirm=confirm, rebuild=True),
        }
        commands[args.command]()
    except Refused as exc:
        print(f"Not started: {exc}.")
        return 2
    except Failed as exc:
        print(f"Stopped: {exc}. The steps before it stand; run this again once it is fixed.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
