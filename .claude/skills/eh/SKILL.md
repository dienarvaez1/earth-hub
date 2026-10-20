---
name: eh
description: Earth-Hub's cheat sheet and skill index. Show the commands for an area, in the pipeline's order (start, USGS, poller, Kafka, PostgreSQL, API, export, R2, modes, dashboard, logs, tests, stop) or say which eh-* skill does what. Use when the user types /eh, asks "how do I … in Earth-Hub", "what commands are there", or wants the cheat sheet.
argument-hint: "[area or section number, e.g. kafka | db | api | r2 | modes | logs | tests | 9]"
---

# Earth-Hub cheat sheet

Read-only: this skill only shows commands.

1. Read `docs/overview/cheatsheet.md` (the Earth-Hub cheat sheet).
2. With no argument: print its **Skills** table (each `/eh-*` skill, what it does, the command behind it).
3. With an area (`$ARGUMENTS`, e.g. `kafka`, `db`, `api`, `r2`, `modes`, `usgs`, `docker`, `tests`) or a section
   number: print only that section's tables, as they are in the file. The sections are numbered in the order data
   flows (1 start … 13 stop); match an area to a title: `start` → 1, `db` → 5, `docker` or `logs` → 11, `stop` → 13. Don't paraphrase commands; copy them, so they stay exact.
4. If the user asks how to do something the cheat sheet doesn't cover, say so, answer from `Makefile`,
   `README.md` and `docs/`, and suggest adding it to the cheat sheet.

End with one line naming the skill that would do it for them, e.g. "or run `/eh-kafka read earthquakes.raw 0 42`".
