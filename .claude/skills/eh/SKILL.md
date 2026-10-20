---
name: eh
description: Earth-Hub's cheat sheet and skill index. Show the commands for an area (services, USGS, poller, Kafka, PostgreSQL, API, R2, export, dashboard, tests) or say which eh-* skill does what. Use when the user types /eh, asks "how do I … in Earth-Hub", "what commands are there", or wants the cheat sheet.
argument-hint: "[area, e.g. kafka | db | api | r2 | tests]"
---

# Earth-Hub cheat sheet

Read-only: this skill only shows commands.

1. Read `docs/overview/cheatsheet.md` (the Earth-Hub cheat sheet).
2. With no argument: print its **Skills** table (each `/eh-*` skill, what it does, the command behind it).
3. With an area (`$ARGUMENTS`, e.g. `kafka`, `db`, `api`, `r2`, `usgs`, `docker`, `tests`): print only that
   section's table, as it is in the file. Don't paraphrase commands; copy them, so they stay exact.
4. If the user asks how to do something the cheat sheet doesn't cover, say so, answer from `Makefile`,
   `README.md` and `docs/`, and suggest adding it to the cheat sheet.

End with one line naming the skill that would do it for them, e.g. "or run `/eh-kafka read earthquakes.raw 0 42`".
