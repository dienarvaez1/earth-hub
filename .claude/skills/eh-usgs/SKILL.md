---
name: eh-usgs
description: Query the live USGS earthquake services directly with curl — a GeoJSON summary feed, one event by ID, an FDSN search or count by time, magnitude or place, the feed's caching headers — and compare with what Earth-Hub stored. Use for /eh-usgs, "what does USGS say about us7000abcd", "how many M4+ yesterday", "check the feed".
argument-hint: "[feed <name> | event <ID> | count <params> | query <params> | headers]"
---

# USGS directly

These call the **live** USGS services; never from tests. Be gentle: one request at a time, and always send a
User-Agent. Use `UA="earth-hub (manual check)"` and `curl -s -A "$UA"`. Summarize with `jq`; don't dump whole feeds.

| Argument | Request |
| --- | --- |
| `feed [name]` | `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/<name>.geojson`, name one of `all_hour`, `all_day` (the poller's), `all_week`, `all_month` (catch-up's), `significant_week`, `4.5_day`, … Summarize: `jq -c '{generated: .metadata.generated, count: .metadata.count}'` and the 5 strongest: `jq -c '[.features[] \| {id, mag: .properties.mag, place: .properties.place, time: (.properties.time/1000 \| todate)}] \| sort_by(-.mag)[:5][]'` |
| `event <ID>` | `https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventid=<ID>`: `jq -c '{id, mag: .properties.mag, place: .properties.place, status: .properties.status, updated: (.properties.updated/1000 \| todate), ids: .properties.ids}'`. A deleted event answers `404` unless `&includedeleted=true` is added |
| `count <params>` | `https://earthquake.usgs.gov/fdsnws/event/1/count?<params>`, e.g. `starttime=2026-10-19&endtime=2026-10-20&minmagnitude=4` |
| `query <params>` | `https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&<params>` (add `&limit=20&orderby=magnitude` unless the user gave a limit). Params: `starttime`, `endtime` (UTC; FDSN ignores milliseconds), `minmagnitude`, `maxmagnitude`, `latitude`/`longitude`/`maxradiuskm`, `eventtype`, `includedeleted` |
| `headers` | `curl -sI -A "$UA" .../summary/all_day.geojson \| grep -iE '^(last-modified\|expires\|cache-control)'`: what the poller times its next round from |

(In the table `\|` is a pipe; run it as `|`.)

**Compare with Earth-Hub** when it helps: for `event`, also
`curl -s http://127.0.0.1:8000/api/v1/earthquakes/<ID>` (if the API runs), or `/eh-db event <ID>`. Differences
in `updated` mean a revision Earth-Hub hasn't applied yet (the live feed covers a day, catch-up 30 days).

## Report

The answer in a line or a small table, with the URL used, and any difference from what Earth-Hub stored.
