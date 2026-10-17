# DEF-008: The outage check left a consumer running and still reported PASS

| | |
| --- | --- |
| **Severity** | High (a test reported success it hadn't verified) |
| **Priority** | P1 |
| **Status** | Fixed in `a71d7c1` (Milestone 3); the shell check was replaced by a pytest test in Milestone 5 |
| **Component** | `scripts/check_db_outage.sh` (since replaced by `tests/disruptive/test_outages.py`) |
| **Found by** | Checking the machine's state after the test, prompted by an unexpected `Terminated: 15` line |

## Environment
macOS 27, bash 3.2 (the macOS default), uv 0.13.1.

## Steps to reproduce
1. In the script, start the consumer through a shell function that sets extra environment variables, and run it in the background: `app uv run python -m earth_hub.consumers >"$LOG" 2>&1 &`.
2. Later, stop it with `kill -TERM "$!"`.
3. Once the script finishes, run `pgrep -fl earth_hub.consumers`.

## Expected result
The consumer receives `SIGTERM`, logs "consumer stopped" and exits. Nothing is left running.

## Actual result
The script printed `PASS`, but:

```
91702 uv run python -m earth_hub.consumers
91705 .venv/bin/python -m earth_hub.consumers
```

The consumer was still running, now pointed at a test database the script had already dropped, and retrying forever.

## Root cause
Running a **shell function** in the background (`app ... &`) starts a subshell, so `$!` was the subshell's PID, not the consumer's. `kill` stopped the subshell and left its child running. The script never checked that the consumer had actually stopped, so it reported PASS based on database counts alone.

## Fix
- The consumer is started with `env "${APP_ENV[@]}" uv run ...`, where `env` replaces itself with the command, so `$!` is the real process.
- The check now **fails** unless the consumer logs a clean "consumer stopped" and no consumer process is left within 10 s.
- Milestone 5 replaced the script with `tests/disruptive/test_outages.py`. That test runs the consumer in a thread and asserts it stopped (`assert not thread.is_alive()`).

## Regression test
`tests/disruptive/test_outages.py::test_database_outage_loses_and_duplicates_nothing` (REQ-E2E-005). It passed in every `make regression` run, and the live pipeline's containers stayed healthy through the real outages.

## Prevention
- Test tooling is code and gets tested too: a check must verify its own clean-up, not just the result it's looking for.
- In the Compose stack, the same script also recreated the Postgres container by exporting a setting (DEF-007). Both are why outage tests now live in pytest, with explicit fixtures that put the services back.
