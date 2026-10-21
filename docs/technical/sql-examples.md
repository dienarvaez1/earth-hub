# SQL examples

Two sets of queries, each run by a test on a fixed dataset and compared with the results shown here, so this page cannot drift from the code:

- [**Inspecting PostgreSQL**](#inspecting-postgresql): queries for looking at what the pipeline stored, and why. Replayed by `tests/api/test_sql_examples.py`.
- [**The dashboard's reports**](#dashboard-reports) (`dashboard/queries/*.sql`), which the page runs in the browser with DuckDB-WASM over the Parquet snapshot. Replayed by `tests/unit/test_report_examples.py`.

In the tables, `NULL` is a missing value, and times are UTC (`+00` when the column has a time zone).

## Inspecting PostgreSQL

Read-only queries for looking at stored data and its history, against the main database: `docker compose exec postgres psql -U earth_hub_app earth_hub` (the user and database are `POSTGRES_USER` and `POSTGRES_DB` in `.env`). The tables and columns are in [the schema](schema.md). With Claude Code, `/eh-db` runs them, or any read-only SQL: `/eh-db outcomes`, `/eh-db rejected`, `/eh-db event <ID>`, `/eh-db merged`, `/eh-db strongest`.

The examples run on the documented dataset of [the API examples](api.md#examples) (20 events, two of them merged), plus three messages added after it: `us02` delivered again under a new offset, an older revision of `us12`, and an event at latitude 95. The session's time zone is UTC (`SET TIME ZONE 'UTC';`).

### Every message and what happened to it

Each Kafka message gets one `raw_events` row with its outcome. Here: the 22 messages of the dataset were applied, and the three added for these examples were not.

<!-- replay: postgres -->

```sql
SELECT outcome, count(*) AS messages
FROM raw_events
GROUP BY outcome
ORDER BY messages DESC, outcome;
```

| outcome | messages |
| --- | --- |
| applied | 22 |
| duplicate | 1 |
| rejected | 1 |
| stale | 1 |

### Rejected messages and the rule they broke

The rule IDs are those of [the data-quality rules](data-quality-rules.md). A rejected message is kept whole in `envelope`, so it can be read back and replayed once fixed.

<!-- replay: postgres -->

```sql
SELECT kafka_topic, kafka_partition, kafka_offset, event_id, rule, reason
FROM raw_events
WHERE outcome IN ('rejected', 'error')
ORDER BY received_at DESC
LIMIT 20;
```

| kafka_topic | kafka_partition | kafka_offset | event_id | rule | reason |
| --- | --- | --- | --- | --- | --- |
| earthquakes.raw | 0 | 24 | us99 | GEO-002 | GEO-002: latitude 95.0 outside -90..90 |

### Events that have had more than one ID

When USGS finds that two reports are one quake, the revision lists both IDs, and the two records are merged. Every ID stays in `earthquake_ids`.

<!-- replay: postgres -->

```sql
SELECT e.preferred_event_id, array_agg(i.event_id ORDER BY i.event_id) AS ids,
       e.revision_count
FROM earthquakes AS e
JOIN earthquake_ids AS i ON i.earthquake_id = e.id
GROUP BY e.id
HAVING count(*) > 1
ORDER BY e.preferred_event_id;
```

| preferred_event_id | ids | revision_count |
| --- | --- | --- |
| us20 | {ak20,us20} | 3 |

### The history of one event

Every message that touched an event, found through any of its IDs. `ak20` and `us20` arrived as two events, and the third message, listing both IDs, merged them.

<!-- replay: postgres -->

```sql
SELECT r.kafka_offset, r.event_id, to_timestamp(r.source_updated_ms / 1000.0) AS source_updated,
       r.outcome
FROM raw_events AS r
JOIN earthquake_ids AS i ON i.earthquake_id = r.earthquake_id
WHERE i.event_id = 'ak20'
ORDER BY r.kafka_topic, r.kafka_partition, r.kafka_offset;
```

| kafka_offset | event_id | source_updated | outcome |
| --- | --- | --- | --- |
| 19 | ak20 | 2026-10-11 02:01:00+00 | applied |
| 20 | us20 | 2026-10-11 02:01:00+00 | applied |
| 21 | us20 | 2026-10-11 02:10:00+00 | applied |

### Messages that changed nothing

A redelivered revision (same `updated` time) is a `duplicate`; an older one is `stale`. Neither touches the stored event, as `revision_count` shows.

<!-- replay: postgres -->

```sql
SELECT r.kafka_offset, r.event_id, r.outcome, e.revision_count
FROM raw_events AS r
JOIN earthquakes AS e ON e.id = r.earthquake_id
WHERE r.outcome IN ('duplicate', 'stale')
ORDER BY r.kafka_topic, r.kafka_partition, r.kafka_offset;
```

| kafka_offset | event_id | outcome | revision_count |
| --- | --- | --- | --- |
| 22 | us02 | duplicate | 1 |
| 23 | us12 | stale | 1 |

### The strongest events

Deleted events are kept with `review_status = 'deleted'`; leave them out as the API does.

<!-- replay: postgres -->

```sql
SELECT preferred_event_id, event_time, magnitude, mag_type, place, depth_km
FROM earthquakes
WHERE review_status <> 'deleted'
ORDER BY magnitude DESC NULLS LAST, event_time DESC
LIMIT 5;
```

| preferred_event_id | event_time | magnitude | mag_type | place | depth_km |
| --- | --- | --- | --- | --- | --- |
| us15 | 2026-10-10 20:00:00+00 | 8.2 | mww | Peru | 100.0 |
| us14 | 2026-10-10 15:00:00+00 | 7.0 | mww | Papua New Guinea | 60.0 |
| us13 | 2026-10-10 14:00:00+00 | 6.99 | mww | Kamchatka | 40.0 |
| us02 | 2026-10-10 01:30:00+00 | 6.1 | mww | Tonga Islands | 120.0 |
| us12 | 2026-10-10 12:00:00+00 | 5.0 | mb | Chile | 33.0 |

### Events per hour

Unlike the API's hourly summary, a plain `GROUP BY` lists only the hours that have events (here, not 02:00 and 04:00).

<!-- replay: postgres -->

```sql
SELECT date_trunc('hour', event_time) AS hour, count(*) AS events,
       max(magnitude) AS max_magnitude
FROM earthquakes
WHERE review_status <> 'deleted'
  AND event_time >= '2026-10-10 00:00:00+00' AND event_time < '2026-10-10 06:00:00+00'
GROUP BY hour
ORDER BY hour;
```

| hour | events | max_magnitude |
| --- | --- | --- |
| 2026-10-10 00:00:00+00 | 2 | 4.4 |
| 2026-10-10 01:00:00+00 | 2 | 6.1 |
| 2026-10-10 03:00:00+00 | 2 | 2.0 |
| 2026-10-10 05:00:00+00 | 2 | 0.9 |

### Where an event is

`city`, `region` and `country` are derived from USGS's `place` text when the consumer stores the event (`validation/places.py`). Only text of the form "<distance> of <city>, <area>" names them, so `Central California` or `Peru` alone, as in most of this dataset, leaves them empty; `place` itself is always kept unchanged.

<!-- replay: postgres -->

```sql
SELECT place, city, region, country
FROM earthquakes
WHERE country IS NOT NULL
ORDER BY preferred_event_id;
```

| place | city | region | country |
| --- | --- | --- | --- |
| 10 km N of Anchorage, Alaska | Anchorage | Alaska | United States |
| 50 km W of Anchorage, Alaska | Anchorage | Alaska | United States |
| 100 km S of Tokyo, Japan | Tokyo | NULL | Japan |

## Dashboard reports

The page runs every report unchanged. All filters live in one `filter` table, set through `set_filter.sql`, so no report takes parameters; `setup.sql` creates it, and the `events` and `filtered` views every report reads. Times are bucketed from `event_time_ms` as a plain UTC `TIMESTAMP`, so results never depend on the browser's time zone.

| File | Answers | Filtered |
| --- | --- | --- |
| `setup.sql` | Creates the `filter` table and the `events` and `filtered` views | |
| `set_filter.sql` | Sets every filter at once (11 parameters, in order) | |
| `summary.sql` | The headline numbers: events, strongest, first and last, countries, tsunami flags | yes |
| `over_time.sql` | Events per day, ISO week or month, empty periods included, split by magnitude level | yes |
| `magnitude_bins.sql` | Events per half-unit of magnitude, empty bins included | yes |
| `map_points.sql` | One point per event, for the map and the depth chart | yes |
| `top_countries.sql`, `top_regions.sql` | The `top_n` countries and regions by events | yes |
| `significant.sql`, `largest.sql` | The latest and the strongest `top_n` events at or above `significant_magnitude` | yes |
| `countries.sql`, `regions.sql`, `event_types.sql` | The options of the filter's menus | no (regions: by the chosen country) |
| `date_range.sql` | The first and last event, the custom calendar's limits | no |

To run them on the real snapshot: after `make export-r2`, start `make dashboard-dev`, then from the repository's root (with Claude Code, `/eh-r2 snapshot` saves the file from the bucket directly, without the local server):

```bash
curl -s -o /tmp/earthquakes.parquet http://127.0.0.1:8787/data/earthquakes.parquet
uv run python - <<'PY'
import duckdb
con = duckdb.connect()
con.execute("CREATE VIEW snapshot AS SELECT * FROM read_parquet('/tmp/earthquakes.parquet')")
con.execute(open("dashboard/queries/setup.sql").read())
con.sql(open("dashboard/queries/summary.sql").read()).show()
PY
```

The examples run on the 8 events of `tests/support/dashboard_data.py`, chosen for edges: 29 February and the midnights around it, a Sunday at the end of an ISO week, magnitudes at band edges, a negative magnitude, an event without magnitude or country, a deletion and a quarry blast.

| key | time (UTC) | magnitude | country | region | note |
| --- | --- | --- | --- | --- | --- |
| us0001 | 2024-02-28 23:59:59.999 | 2.49 | United States | Alaska | |
| us0002 | 2024-02-29 00:00:00 | 2.5 | United States | Alaska | |
| us0003 | 2024-02-29 12:00:00 | 4.5 | Mexico | Baja California | |
| us0004 | 2024-03-01 00:00:00 | NULL | NULL | NULL | |
| us0005 | 2024-03-04 06:00:00 | -0.43 | United States | California | |
| us0006 | 2024-03-10 23:00:00 | 5.2 | Japan | NULL | a Sunday |
| us0007 | 2024-03-10 08:00:00 | 3.0 | United States | Alaska | deleted |
| us0008 | 2024-04-02 10:00:00 | 1.0 | United States | Alaska | quarry blast |

The filter is the default (no limits, deleted events left out, per month, `top_n` 10, `significant_magnitude` 4.5) except where an example says otherwise.

### The headline numbers

`summary.sql`, with the default filter: deleted events are left out, so 7 of the 8 rows count, and `countries` includes `Unknown`.

<!-- replay: dashboard summary -->

| events | with_magnitude | max_magnitude | max_magnitude_place | first_event | last_event | countries | tsunami_flagged | deleted |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 7 | 6 | 5.2 | place 6 | 2024-02-28 23:59:59.999 | 2024-04-02 10:00:00 | 4 | 0 | 0 |

With `include_deleted = true`, the deleted event is counted, and `deleted` says how many:

<!-- replay: dashboard summary include_deleted=true -->

| events | with_magnitude | max_magnitude | max_magnitude_place | first_event | last_event | countries | tsunami_flagged | deleted |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | 7 | 5.2 | place 6 | 2024-02-28 23:59:59.999 | 2024-04-02 10:00:00 | 4 | 0 | 1 |

### Events over time

`over_time.sql` per month. Each period is also split into the map legend's magnitude levels, each including its lower edge, so 2.5 is in `from_2_5` and 4.5 in `from_4_5`; the levels add up to `events`.

<!-- replay: dashboard over_time time_bucket="month" -->

| period | events | max_magnitude | below_2_5 | from_2_5 | from_4_5 | from_6 | no_magnitude |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2024-02-01 00:00:00 | 3 | 4.5 | 1 | 1 | 1 | 0 | 0 |
| 2024-03-01 00:00:00 | 3 | 5.2 | 1 | 0 | 1 | 0 | 1 |
| 2024-04-01 00:00:00 | 1 | 1.0 | 1 | 0 | 0 | 0 | 0 |

Per ISO week, from Monday. Weeks without events are listed with 0, so a chart's gaps are real. The event at 23:59:59.999 on Wednesday 28 February and the one at midnight on Friday 1 March are in the same week; Sunday 10 March belongs to the week of 4 March.

<!-- replay: dashboard over_time time_bucket="week" -->

| period | events | max_magnitude | below_2_5 | from_2_5 | from_4_5 | from_6 | no_magnitude |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2024-02-26 00:00:00 | 4 | 4.5 | 1 | 1 | 1 | 0 | 1 |
| 2024-03-04 00:00:00 | 2 | 5.2 | 1 | 0 | 1 | 0 | 0 |
| 2024-03-11 00:00:00 | 0 | NULL | 0 | 0 | 0 | 0 | 0 |
| 2024-03-18 00:00:00 | 0 | NULL | 0 | 0 | 0 | 0 | 0 |
| 2024-03-25 00:00:00 | 0 | NULL | 0 | 0 | 0 | 0 | 0 |
| 2024-04-01 00:00:00 | 1 | 1.0 | 1 | 0 | 0 | 0 | 0 |

### Magnitudes

`magnitude_bins.sql`: bins of half a unit, `bin_start <= magnitude < bin_end`, from the lowest bin with an event to the highest. 2.49 is in the 2.0 bin and 2.5 starts its own; the event without a magnitude is not in any bin (`summary.sql` counts it).

<!-- replay: dashboard magnitude_bins -->

| bin_start | bin_end | events |
| --- | --- | --- |
| -0.5 | 0.0 | 1 |
| 0.0 | 0.5 | 0 |
| 0.5 | 1.0 | 0 |
| 1.0 | 1.5 | 1 |
| 1.5 | 2.0 | 0 |
| 2.0 | 2.5 | 1 |
| 2.5 | 3.0 | 1 |
| 3.0 | 3.5 | 0 |
| 3.5 | 4.0 | 0 |
| 4.0 | 4.5 | 0 |
| 4.5 | 5.0 | 1 |
| 5.0 | 5.5 | 1 |

### Where

`top_countries.sql`: events without a country are grouped as `Unknown`, and ties are in alphabetical order.

<!-- replay: dashboard top_countries -->

| country | events | max_magnitude |
| --- | --- | --- |
| United States | 4 | 2.5 |
| Japan | 1 | 5.2 |
| Mexico | 1 | 4.5 |
| Unknown | 1 | NULL |

`top_regions.sql`, here with `top_n = 3`. A region is shown with its country, since names repeat across countries.

<!-- replay: dashboard top_regions top_n=3 -->

| region | country | events | max_magnitude |
| --- | --- | --- | --- |
| Alaska | United States | 3 | 2.5 |
| Baja California | Mexico | 1 | 4.5 |
| California | United States | 1 | -0.43 |

### The events to look at

`significant.sql`: the latest events at or above `significant_magnitude` (4.5 by default, included), newest first.

<!-- replay: dashboard significant -->

| key | event_ts | magnitude | place | country | depth_km | url |
| --- | --- | --- | --- | --- | --- | --- |
| us0006 | 2024-03-10 23:00:00 | 5.2 | place 6 | Japan | 10.0 | https://earthquake.usgs.gov/earthquakes/eventpage/ak0001 |
| us0003 | 2024-02-29 12:00:00 | 4.5 | place 3 | Mexico | 10.0 | https://earthquake.usgs.gov/earthquakes/eventpage/ak0001 |

`largest.sql`: the strongest, here at 1.0 and above with `top_n = 3`; equal magnitudes would be newest first.

<!-- replay: dashboard largest significant_magnitude=1.0 top_n=3 -->

| key | event_ts | magnitude | place | country | depth_km | url |
| --- | --- | --- | --- | --- | --- | --- |
| us0006 | 2024-03-10 23:00:00 | 5.2 | place 6 | Japan | 10.0 | https://earthquake.usgs.gov/earthquakes/eventpage/ak0001 |
| us0003 | 2024-02-29 12:00:00 | 4.5 | place 3 | Mexico | 10.0 | https://earthquake.usgs.gov/earthquakes/eventpage/ak0001 |
| us0002 | 2024-02-29 00:00:00 | 2.5 | place 2 | United States | 10.0 | https://earthquake.usgs.gov/earthquakes/eventpage/ak0001 |

### A filtered map

`map_points.sql` for 29 February 2024: `start_time` is inclusive and `end_time` exclusive, in UTC, so the events at 23:59:59.999 the day before and at midnight the day after are out.

<!-- replay: dashboard map_points start_time="2024-02-29" end_time="2024-03-01" -->

| key | longitude | latitude | depth_km | magnitude | event_time_ms | place | title | url | review_status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| us0002 | -155.6123 | 57.4401 | 10.0 | 2.5 | 1709164800000 | place 2 | M 2.4 - 86 km E of Ugashik, Alaska | https://earthquake.usgs.gov/earthquakes/eventpage/ak0001 | reviewed |
| us0003 | -155.6123 | 57.4401 | 10.0 | 4.5 | 1709208000000 | place 3 | M 2.4 - 86 km E of Ugashik, Alaska | https://earthquake.usgs.gov/earthquakes/eventpage/ak0001 | reviewed |

### The filter's options

`countries.sql` lists every country for the country filter. It ignores the filter, and counts the deleted event too: these are options, not results.

<!-- replay: dashboard countries -->

| country | events |
| --- | --- |
| Japan | 1 |
| Mexico | 1 |
| United States | 5 |
| Unknown | 1 |

`date_range.sql` gives the custom calendar's limits.

<!-- replay: dashboard date_range -->

| first_event | last_event |
| --- | --- |
| 2024-02-28 23:59:59.999 | 2024-04-02 10:00:00 |
