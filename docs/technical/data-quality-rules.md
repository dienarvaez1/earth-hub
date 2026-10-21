# Data-quality rules

The consumers validate every message, from `earthquakes.raw` and `earthquakes.backfill`, before storing it (`src/earth_hub/validation/earthquake_rules.py`). A message that breaks any rule is **rejected**: it is not stored in `earthquakes`, but its row in `raw_events` keeps the full message, `outcome = 'rejected'`, the first rule broken in `rule`, and every violation in `reason`.

The poller does not apply these rules. It publishes every feature it can route (one with an `id`, `updated` and `time`), so a bad record leaves an audit trail instead of disappearing upstream.

## How rules are applied

- **Envelope rules** (`ENV-*`) are checked first. If one fails, nothing else can be checked, so validation stops there.
- **Payload rules** (`GEO-*`, `TIME-*`, `MAG-*`, `PROP-*`) are all evaluated, so one rejection lists every problem.
- Ranges are **inclusive**: a latitude of exactly 90 is valid.
- Numbers must be finite. `NaN`, `Infinity` and booleans never count as numbers.
- The `earthquakes` table enforces the range rules again with `CHECK` constraints, so invalid data can't be written even by code that bypasses validation.

## Rules

| Rule | Checks | Rejected example |
| --- | --- | --- |
| `ENV-001` | The message is a JSON object | `not json`, `null`, a JSON list, `{"a": NaN}` |
| `ENV-002` | `schema_version` is `1` | `"schema_version": 2` |
| `ENV-003` | Envelope fields are present and typed: `event_id` (non-empty string), `ids` (non-empty list of strings), `source_updated_ms` and `event_time_ms` (integers), `payload` and `payload.properties` (objects) | `"ids": []` |
| `ENV-004` | `payload.id` equals `event_id` | the envelope says `us1`, the payload says `us2` |
| `ENV-005` | The envelope timestamps equal the payload's `updated` and `time` | the envelope was edited after it was built |
| `GEO-001` | `geometry` is a `Point` with exactly 3 finite numbers | `"coordinates": [1.0, 2.0]` |
| `GEO-002` | Latitude (second coordinate) is between −90 and 90 | `95.0` |
| `GEO-003` | Longitude (first coordinate) is between −180 and 180 | `200.0` |
| `GEO-004` | Depth (third coordinate) is between −10 and 1000 km | `1200.0` |
| `TIME-001` | The event time is on or after 1900-01-01 UTC and representable | `-2208988800001` ms |
| `TIME-002` | The event time is no more than 1 hour after the time of processing | a timestamp 2 hours ahead |
| `TIME-003` | `updated` is representable and not earlier than the event time | `updated` 1 ms before `time` |
| `MAG-001` | If present, magnitude is a number between −2 and 10.5 | `11.0`, `"4.5"` |
| `PROP-001` | `type` is a non-empty string | missing `type` |
| `PROP-002` | `status` is `automatic`, `reviewed` or `deleted` | `"manual"` |
| `PROP-003` | Optional fields have the right type when present: `felt` and `sig` integers; `cdi` and `mmi` finite numbers; `tsunami` 0 or 1; `magType`, `place`, `alert`, `net`, `url` and `title` strings | `"felt": "3"` |

## Deliberately accepted

These values occur in real USGS data and are valid:

| Value | Why it's valid |
| --- | --- |
| Negative magnitude (e.g. −0.34) | Very small events on local magnitude scales |
| Negative depth (e.g. −0.36 km) | Events above sea level, measured from the geoid |
| `type` other than `earthquake` (e.g. `explosion`, `quarry blast`) | Kept with `event_type`; the API filters them |
| `status = deleted` | Stored as a status; the row is kept |
| A deletion whose magnitude breaks `MAG-001`, or whose optional fields break `PROP-003` | A deletion only has to identify and order the event. USGS fills some deletions with placeholders (magnitude −5 on `uw` deletions), so those values are stored as unknown (`NULL`) instead of rejecting the deletion (DEF-027). Geometry, time, `type` and `status` rules still apply |
| Missing magnitude, place, felt reports, intensity or alert level | Optional in the USGS format |
| Different magnitude types (`ml`, `md`, `mb`, `mww`, …) | Kept in `mag_type`. Statistics that mix them should say so |

**Dropped before Kafka, deliberately.** The publishers (poller, catch-up, backfill) drop only features they cannot route: without a usable `id`, `updated` or `time`. In real data these are USGS deletions with an empty `id`, nearly all with no `ids`, `code` or `net` either: notices that name no event (607 of 146,891 features in the 2025 backfill; in the busiest week, 9–16 November, all 50 came from the deleted-events query). Each is logged as `feature rejected` with its reason, and counted in `backfill_windows.rejected` and catch-up's summaries (REQ-BF-014). A rare one lists its event in `ids` (`,tx2025wbgwbq,`) and is dropped too; it changes nothing unless that event was stored ([limitations](../overview/limitations.md#data)).

## Range choices

| Limit | Value | Reasoning |
| --- | --- | --- |
| Depth | −10 to 1000 km | The deepest recorded earthquakes are about 700 km down; events above sea level are a few km at most |
| Magnitude | −2 to 10.5 | The largest recorded earthquake is 9.5; small local events go slightly below 0 |
| Event time | 1900 to now + 1 h | Instrumental records start around 1900; 1 hour allows for clock differences without accepting nonsense |

## What happens after validation

| Outcome | Meaning |
| --- | --- |
| `applied` | New event, or a revision newer than the stored one, or the event that absorbed a duplicate whose deletion is stored |
| `duplicate` | Same `updated` as the stored event: nothing changes |
| `stale` | Older than the stored event, or the deletion of a duplicate merged into it (with a reason): nothing changes, but any new IDs in it are still recorded |
| `rejected` | Broke a rule above |
| `error` | Failed 3 times for an unexpected reason (a bug), stored with the exception so the partition can move on |

**Duplicates' deletions (DEF-026).** When USGS finds that two IDs are one event, the surviving event lists both IDs and the duplicate is deleted on its own, listing only its own ID. Both IDs point to one stored event, so a deletion that does not list the stored event's current ID is the duplicate's, and does not hide the event. Each event keeps the IDs of its current revision in `revision_ids` to tell the two cases apart. The rule is in `database/revisions.py`, shared by the consumer and by backfill and catch-up when they decide what to publish.

**City, region and country.** Each stored revision also gets `city`, `region` and `country`, derived from `place` by `validation/places.py`; they never cause a rejection, and `place` is kept exactly as USGS sent it.

| `place` | `city` | `region` | `country` |
| --- | --- | --- | --- |
| 86 km E of Ugashik, Alaska | Ugashik | Alaska | United States |
| 10 km N of Cobb, CA | Cobb | California | United States |
| 10 km NE of Mexicali, B.C., MX | Mexicali | Baja California | Mexico |
| 8 km SSE of Guarumal, Panama | Guarumal | | Panama |
| 12 km SSE of Bak’uriani, Georgia (longitude 43.6) | Bak’uriani | | Georgia |
| south of the Fiji Islands | | the Fiji Islands | |
| Fiji region (no " of ") | | | |

The text after the first " of " is split at commas: the first part is the city and the last the area. A US state, by name or postal code, becomes the region with country United States; otherwise the area is the country and any middle part the region. "Georgia" is the US state west of Greenwich and the country east of it. Without a comma the text names an area, not a city. US territories (Puerto Rico, Guam, U.S. Virgin Islands, Northern Mariana Islands) are kept as USGS names them.

A redelivered Kafka message (same topic, partition and offset) is recognized and skipped without a new row. Messages from backfill and catch-up (`source` is `backfill`, `sweep`, `catchup` or `deleted`) go through exactly the same rules, from the `earthquakes.backfill` topic. `make reconcile` checks that every message in the topic has exactly one `raw_events` row.
