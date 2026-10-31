"""Cut full mode over from one machine to another, one command on each.

    uv run python scripts/cutover.py a [--dry-run]   # make cutover-a: on A, giving up full mode
    uv run python scripts/cutover.py b [--dry-run]   # make cutover-b: on B, taking it

Each runs in four parts, and stops at the first that fails, having changed nothing before
part 3:

1. Pre-checks, read-only: every one is run and reported, then any failure stops it.
2. A dry run: the steps it would take (scripts/modes.py's own dry run for the claim).
3. `Proceed with the cutover? [y/N]` (`--dry-run` stops before it; `--yes` is for the tests).
4. The steps, then checks that they did what they should.

A (cutover-a): R2 reachable and the mode readable; no local `make direct-run`; direct mode's
hourly rounds ran within 2 hours (the standby that writes until B claims). If A owns the bucket in
full mode, it releases it (make host-release); then it stops A's containers (make down, data
kept). Docker or Colima itself stays up, so A can claim again later.

B (cutover-b): uv and Docker; `.env` with the R2 settings, USGS_CONTACT and the PostgreSQL
settings, and EARTH_HUB_MACHINE_ID (the hostname is used when empty: a warning); R2 reachable,
the USGS feed answering; the bucket claimable (direct mode, or a lease expired; not this host's
already, not another host's live lease); no local `make direct-run`. Then make host-claim, which
asks for the machine ID to be typed if this host's database must be wiped. After: this host owns
the bucket with a live lease, and the pipeline's services and the exporter run.

Both write the real bucket or stop services: run them when meant (the /eh-host skill runs them
when asked).
"""

import argparse
import re
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import modes

REPO = Path(__file__).resolve().parents[1]
FEED = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
ROUNDS_WITHIN = timedelta(hours=2)
RUN_ID = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)-")
SERVICES = ("poller", "consumer", "consumer-backfill", "catchup", "api", "exporter")
REQUIRED = ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET", "USGS_CONTACT",
            "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")  # fmt: skip


class Checks:
    """Pre-checks: each is run and printed; `done` stops on any that failed."""

    def __init__(self):
        self.failed: list[str] = []

    def __call__(self, name: str, ok: bool, detail: str = "", *, warn: bool = False) -> bool:
        mark = "ok  " if ok else ("warn" if warn else "FAIL")
        print(f"  [{mark}] {name}" + (f": {detail}" if detail else ""), flush=True)
        if not ok and not warn:
            self.failed.append(name)
        return ok

    def done(self) -> None:
        if self.failed:
            raise modes.Refused(f"{len(self.failed)} pre-check(s) failed: {', '.join(self.failed)}")


def env_file(path: Path) -> dict[str, str]:
    values = {}
    if path.exists():
        for line in path.read_text().splitlines():
            name, sep, value = line.partition("=")
            if sep and not name.lstrip().startswith("#"):
                values[name.strip()] = value.strip().strip('"').strip("'")
    return values


def read_marker(shell: modes.Shell, check: Checks) -> modes.Marker | None:
    try:
        current = modes.marker(shell)
    except modes.Failed as exc:
        check("R2 reachable and the mode readable", False, str(exc))
        return None
    check("R2 reachable and the mode readable", bool(current.line), current.line or "no mode line")
    return current if current.line else None


def no_direct_run(shell: modes.Shell, check: Checks) -> None:
    try:
        modes.require_no_direct_run(shell)
        check("no `make direct-run` on this machine", True)
    except modes.Refused as exc:
        check("no `make direct-run` on this machine", False, str(exc))


def latest_round(shell: modes.Shell) -> datetime | None:
    code, out = shell.read("make", "-s", "direct-runs", "LIMIT=1")
    if code != 0:
        return None
    for line in out.splitlines():
        if match := RUN_ID.match(line.strip()):
            return datetime.strptime(match.group(1), "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=UTC)
    return None


def proceed(shell: modes.Shell, ask: Callable[[str], str]) -> bool:
    if shell.dry_run:
        print("Dry run: stopping here; nothing was changed.")
        return False
    if ask("Proceed with the cutover? [y/N] ").strip().lower() not in ("y", "yes"):
        print("Not confirmed: nothing was changed.")
        return False
    return True


# A: the machine giving up full mode.


def _owns(current: modes.Marker, docker: bool, check: Checks) -> bool:
    """Whether this machine owns the bucket in full mode, and so releases it."""
    if current.line.startswith(modes.FULL) and current.owner == current.this_host:
        return check(
            "this machine owns the bucket: it releases it",
            docker,
            "" if docker else "Docker is needed for make host-release",
        )
    if current.line.startswith(modes.FULL) and current.owner:
        check("the bucket is owned elsewhere", True, f"{current.owner}: nothing to release here")
    elif current.line.startswith(modes.FULL):
        check(
            "full mode with no owner", False, "release it on the host that runs it (make to-direct)"
        )
    elif current.line.startswith(modes.DIRECT):
        check("the bucket is in direct mode", True, "nothing to release")
    else:
        check("the bucket is not mid-handover", False, current.line)
    return False


def cutover_a(shell: modes.Shell, ask, now: Callable[[], datetime]) -> None:
    print("1. Pre-checks (read-only)")
    check = Checks()
    current = read_marker(shell, check)
    no_direct_run(shell, check)
    last = latest_round(shell)
    check(
        "direct mode's hourly rounds ran within 2 hours",
        last is not None and now() - last <= ROUNDS_WITHIN,
        f"last {last:%Y-%m-%d %H:%M} UTC" if last else "none found (make direct-runs)",
        warn=True,  # the standby; not needed for the cutover itself
    )
    docker = shell.read("docker", "info")[0] == 0
    check(
        "Docker", docker, "running" if docker else "not running: no containers to stop", warn=True
    )
    release = current is not None and _owns(current, docker, check)
    check.done()

    print("2. Dry run")
    if release:
        code, out = shell.read("make", "host-release", "DRY_RUN=1")
        print("\n".join(f"    {line}" for line in out.rstrip().splitlines()))
        if code != 0:
            raise modes.Refused("the release's dry run refused (above)")
    print(f"    then: {'make down (data kept)' if docker else 'nothing to stop'}")
    if not proceed(shell, ask):
        return

    print("3. Cutover")
    if release:
        shell.run("make", "host-release")
    if docker:
        shell.run("make", "down")
    print("4. After")
    after = modes.marker(shell)
    if after.line.startswith(modes.FULL) and after.owner == after.this_host:
        raise modes.Failed(f"this machine still owns the bucket: {after.line}")
    print(f"  [ok  ] the bucket: {after.line}" + (f", owner {after.owner}" if after.owner else ""))
    print("Done on A. Now run `make cutover-b` on B.")


# B: the machine taking full mode.


def cutover_b(shell: modes.Shell, ask, env: dict[str, str]) -> None:
    print("1. Pre-checks (read-only)")
    check = Checks()
    docker = shell.read("docker", "info")[0] == 0
    check("uv", shell.read("uv", "--version")[0] == 0)
    check("Docker running", docker, "" if docker else "start Docker or Colima")
    check(".env present", bool(env), "" if env else "copy .env.example to .env and fill it in")
    missing = [name for name in REQUIRED if not env.get(name)]
    if not (env.get("R2_ACCOUNT_ID") or env.get("R2_ENDPOINT_URL")):
        missing.insert(0, "R2_ACCOUNT_ID")
    check("settings in .env", not missing, f"empty: {', '.join(missing)}" if missing else "")
    check(
        "EARTH_HUB_MACHINE_ID set",
        bool(env.get("EARTH_HUB_MACHINE_ID")),
        env.get("EARTH_HUB_MACHINE_ID") or "empty: the hostname is used",
        warn=True,
    )
    current = read_marker(shell, check)
    code, out = shell.read("curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}", "-m", "15", FEED)
    check("the USGS feed answers", code == 0 and out.strip() in ("200", "304"), out.strip()[-80:])
    no_direct_run(shell, check)
    if current is not None:
        if current.line.startswith(modes.FULL) and current.owner == current.this_host:
            check("not the owner already", False, f"{current.this_host} owns the bucket")
        elif current.line.startswith(modes.FULL) and current.owner and current.lease_live:
            check("no other host's live lease", False,
                  f"{current.owner} owns it: run make cutover-a there first")  # fmt: skip
        elif current.line.startswith(modes.FULL) and not current.owner:
            check("full mode has an owner", False, "release it on the host that runs it")
        else:
            check("the bucket can be claimed", True, current.line)
    check.done()

    print("2. Dry run (make host-claim DRY_RUN=1)")
    code, out = shell.read("make", "host-claim", "DRY_RUN=1")
    print("\n".join(f"    {line}" for line in out.rstrip().splitlines()))
    if code != 0:
        raise modes.Refused("the claim's dry run refused (above)")
    if not proceed(shell, ask):
        return

    print("3. Cutover (make host-claim)")
    shell.run("make", "host-claim")

    print("4. After")
    after = modes.marker(shell)
    owned = after.line.startswith(modes.FULL) and after.owner == after.this_host
    if not (owned and after.lease_live):
        raise modes.Failed(f"this machine does not own the bucket with a live lease: {after.line}")
    print(f"  [ok  ] {after.this_host} owns the bucket, lease live")
    code, out = shell.read("docker", "compose", "ps", "--status", "running", "--services")
    down = [s for s in SERVICES if s not in out.split()]
    if code != 0 or down:
        raise modes.Failed(f"not running: {', '.join(down) or 'docker compose ps failed'}")
    print(f"  [ok  ] running: {', '.join(SERVICES)}")
    print("Done on B: full mode here, the hourly rounds standing by. `make host-status` any time.")


def main(argv: list[str] | None = None, shell=None, ask=input, env=None, now=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("machine", choices=("a", "b"))
    parser.add_argument("--dry-run", action="store_true", help="pre-checks and the dry run only")
    parser.add_argument("--yes", action="store_true", help=argparse.SUPPRESS)  # tests only
    args = parser.parse_args(argv)
    shell = shell or modes.Shell()
    shell.dry_run = shell.dry_run or args.dry_run
    answer = (lambda _: "y") if args.yes else ask
    try:
        if args.machine == "a":
            cutover_a(shell, answer, now or (lambda: datetime.now(UTC)))
        else:
            cutover_b(shell, answer, env_file(REPO / ".env") if env is None else env)
    except modes.Refused as exc:
        print(f"Not started: {exc}.")
        return 2
    except modes.Failed as exc:
        print(f"Stopped: {exc}. Run `make host-status`, then this again once it is fixed.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
