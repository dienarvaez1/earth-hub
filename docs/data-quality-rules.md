# Data-quality rules

The consumer validates every `earthquakes.raw` message before storing it (`src/earth_hub/validation/earthquake_rules.py`). A message that breaks any rule is **rejected**: it is not stored in `earthquakes`, but its row in `raw_events` keeps the full message, `outcome = 'rejected'`, the first rule broken in `rule`, and every violation in `reason`.

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
| Missing magnitude, place, felt reports, intensity or alert level | Optional in the USGS format |
| Different magnitude types (`ml`, `md`, `mb`, `mww`, …) | Kept in `mag_type`. Statistics that mix them should say so |

## Range choices

| Limit | Value | Reasoning |
| --- | --- | --- |
| Depth | −10 to 1000 km | The deepest recorded earthquakes are about 700 km down; events above sea level are a few km at most |
| Magnitude | −2 to 10.5 | The largest recorded earthquake is 9.5; small local events go slightly below 0 |
| Event time | 1900 to now + 1 h | Instrumental records start around 1900; 1 hour allows for clock differences without accepting nonsense |

## What happens after validation

| Outcome | Meaning |
| --- | --- |
| `applied` | New event, or a revision newer than the stored one |
| `duplicate` | Same `updated` as the stored event: nothing changes |
| `stale` | Older than the stored event: nothing changes, but any new IDs in it are still recorded |
| `rejected` | Broke a rule above |
| `error` | Failed 3 times for an unexpected reason (a bug), stored with the exception so the partition can move on |

A redelivered Kafka message (same topic, partition and offset) is recognized and skipped without a new row. Messages from backfill and catch-up (`source` is `backfill`, `sweep`, `catchup` or `deleted`) go through exactly the same rules, from the `earthquakes.backfill` topic. `make reconcile` checks that every message in the topic has exactly one `raw_events` row.
