---
name: eh-reconcile
description: Earth-Hub against USGS for any range of UTC days, as a side-by-side table — USGS's event count next to Earth-Hub's per day, month or year, with the differences pending, explained or unexplained, a total row, and every difference listed with its reason. Read-only. Use for /eh-reconcile, "is the data up to date for last month", "reconcile 2026 by month", "compare with USGS from START to END", "side-by-side table against USGS".
argument-hint: "START_DATE [END_DATE] [day|month|year]"
---

# Earth-Hub against USGS, side by side

Read-only: it reads the mode writing the bucket (PostgreSQL in full mode, the bucket in direct mode) and
USGS's FDSN service, and writes nothing. Run from the repository root.

## Arguments

- `START_DATE`: first UTC day, `YYYY-MM-DD`. Required (without it, use the last 31 days:
  `DAYS=31` instead of `START`/`END`).
- `END_DATE`: last UTC day, included; defaults to today, and is cut at today.
- `day` (default), `month` or `year`: what each row counts.

A month or year given as `2026-10` or `2026` means its first to last day (`2026-10-01` to `2026-10-31`).

## Run

```
make reconcile-usgs START=<START_DATE> [END=<END_DATE>] [BY=day|month|year]
```

It makes two FDSN requests per day, a second apart: about 2 s a day, so a month takes about a minute and
a year about 12–15 minutes. Use a 10-minute timeout, and run it in the background for more than about
250 days. Leave out the JSON lines of the HTTP log (`grep -v '"logger": "httpx"'`).

The output has one line per period, `<period>: USGS N, Earth-Hub M; <n> <status>, …`, then a `total:` line,
then one line per difference, `<status> <day> <kind> <event_id>: <reason>`, then `RESULT: OK` or
`RESULT: <n> unexplained` (exit 1). It ends with `make: *** … Error 1` when a difference is unexplained:
that is the result, not a failure to run. Exit 2 means it could not run (no R2 settings, or a handover
running): say so.

## Report

Lead with the answer in one sentence: up to date or not, for which range.

Then the side-by-side table, one row per period and a bold **Total** row, numbers with thousands
separators:

| Day (UTC) | USGS | Earth-Hub | Pending | Unexplained |
|---|---|---|---|---|

Name the first column after the grouping (`Day (UTC)`, `Month (UTC)`, `Year (UTC)`). Add a column for
every status the output prints (for example `Explained`), in its order. Keep every row, including those
with no difference.

Under it, the differences grouped by kind with their event IDs and reasons in plain words: **pending**
(USGS changed it after Earth-Hub last read what brings it: the day feed, the month feed, the history pass for events older than 30 days, or the audit for a deletion USGS kept no time for; the next poll or catch-up brings
it), **explained** if printed (by design and never cleared: `rejected here`, Earth-Hub's data-quality rules reject it, with the rule; `split at USGS`, a merge Earth-Hub keeps; `no origin at FDSN`, USGS's services disagree), and **unexplained** first and flagged if any (suggest `/eh-usgs event <ID>`
and `/eh-db event <ID>`, or `make catchup-now`). With more than about 20, give the counts per kind and the
first few IDs. Give the "last read" line's feed times when a difference is pending, and end with the
`RESULT` line.
