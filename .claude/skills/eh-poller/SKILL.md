---
name: eh-poller
description: Operate and inspect Earth-Hub's USGS poller — its last rounds (published, unchanged, rejected), one poll of the live feed from the host, restarting the container, or its health. Use for /eh-poller, "is the poller working", "poll once", "restart the poller", "what did the last round publish".
argument-hint: "[rounds | once | restart | health]  (default: rounds)"
---

# Poller

Run from the repository root.

| Argument | Command |
| --- | --- |
| `rounds` (default) | `docker compose logs --no-log-prefix --since 30m poller \| jq -cR 'fromjson? \| select(.message == "round complete") \| {ts, published, unchanged, rejected, failed_delivery, tracked_events}' \| tail -10` and the events it published: `... \| jq -cR 'fromjson? \| select(.message == "event published") \| {ts, event_id}' \| tail -10` |
| `once` | `make ingest-once`: one round from the host against the **live** feed, publishing to `earthquakes.raw`, then exit. Only on request; while the poller container runs, it sends nothing new (it rebuilds what was sent from the topic first) |
| `restart` | `docker compose --profile ingest restart poller`, then `docker compose ps poller` until healthy, then the first `state rebuilt from topic` and `round complete` lines |
| `health` | `docker compose ps poller` and `docker compose exec -T poller python -m earth_hub.healthcheck poller; echo $?` (0 = a successful round within 3 poll intervals) |

(In the table `\|` is a pipe; run it as `|`.)

If the poller isn't running, check `make -s mode` first. In full mode, `make ingest` (or `/eh-start pipeline`)
starts it. In direct mode it is stopped on purpose: the direct runner polls instead (`/eh-mode`), so don't start
it or run `once`; events it stored would make the handover back fail. To catch up there, `make catchup-now`
(`/eh-mode catchup`).

How it works, in one line each, if asked: it sends `If-Modified-Since`, waits for the feed's `Expires` plus
2–5 s of jitter (10 s – 5 min), publishes only new events or newer `updated` times, and counts an event as sent
only once Kafka confirms it (docs/technical/architecture.md, Ingestion).

## Report

The rounds as a short table, then one line: healthy or not, and anything odd (failed deliveries, rejections,
`published` staying 0 for long with a stale `Last-Modified`).
