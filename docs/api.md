# REST API

A read-only FastAPI app (`src/earth_hub/api/`) over PostgreSQL. It never reads from Kafka or USGS. Run it with `make api` (a container) or `make api-dev` (on your machine, with reload); interactive docs are at http://127.0.0.1:8000/docs and the OpenAPI schema at `/openapi.json`.

The response models in `src/earth_hub/api/schemas.py` are the contract: Schemathesis checks every endpoint against the schema they generate (`tests/api/test_contract.py`), and the examples below are replayed by a test.

## Endpoints

| Endpoint | Returns |
| --- | --- |
| `GET /api/v1/health` | Database status, newest event time, version. `503` when the database is down |
| `GET /api/v1/earthquakes` | A page of events: `{items, total, limit, offset}` |
| `GET /api/v1/earthquakes/{event_id}` | One event. Former USGS IDs work too, and return the event under its current ID |
| `GET /api/v1/earthquakes/stats` | Count, min/max/average magnitude, counts by event type and by magnitude type |
| `GET /api/v1/earthquakes/summary/hourly` | Counts per UTC hour, empty hours included (24 h by default, 31 days at most) |
| `GET /api/v1/earthquakes/summary/magnitude` | Counts for `<0`, `0-1`, … `6-7`, `>=7` and `unknown`, empty ranges included |
| `GET /api/v1/export/earthquakes` | Every column of every row, deleted events included, paged by `id`: what `make export-r2` reads |

## Parameters

**Filters** (list, stats and summaries):
- `min_magnitude` and `max_magnitude` (−2 to 10.5)
- `start_time` (inclusive) and `end_time` (exclusive): ISO 8601 date-times (a date alone is refused), UTC assumed without a timezone, years 1900–2099
- `place`: a case-insensitive substring, where `%` and `_` match literally
- `event_type`, `review_status`, and `include_deleted` (deleted events are hidden by default)

**List only:**
- `limit`: 1–500, default 20
- `offset`: 0–10,000,000
- `sort`: `event_time`, `magnitude` or `depth`
- `order`: `asc` or `desc` (default `desc`)

Missing magnitudes sort last, and ties break by storage order, so pages are stable.

**Export only:** `after_id` (0 to start) and `limit` (1–5,000, default 1,000).

## Error format

Errors always look like `{"error": {"code", "message", "details"}}`:

| Code | Status |
| --- | --- |
| `invalid_parameters` | `422`, listing every problem |
| `not_found` | `404` |
| `method_not_allowed` | `405`, with an `Allow` header |
| `database_unavailable` | `503` |
| `internal_error` | `500`, with no internals |

An invalid request is answered `422` even while the database is down: routes connect to it only after the parameters are validated (DEF-010).

## Examples

Replayed by `tests/api/test_api_examples.py` against the documented dataset: the 20 events of the API tests (`tests/api/dataset.py`), stored through the real consumer code, with the API's clock at 2026-10-10 23:30 UTC. Each request must return exactly the status, headers and body shown, so this page cannot drift from the API. Run against your own data (`make api`), the same requests return your events.

### Health

The database status and the newest stored event. When PostgreSQL is down, the same body says `unavailable`, with status `503`.

```http
GET /api/v1/health
```

Response: `200 OK`

```json
{
  "status": "ok",
  "database": "ok",
  "latest_event_time": "2026-10-11T02:00:00Z",
  "version": "0.1.0"
}
```

### The strongest events

Sorted by magnitude, strongest first; `total` counts every match across pages. `ids` lists every USGS ID the event has had.

```http
GET /api/v1/earthquakes?min_magnitude=7&sort=magnitude&limit=1
```

Response: `200 OK`

```json
{
  "items": [
    {
      "event_id": "us15",
      "ids": [
        "us15"
      ],
      "event_time": "2026-10-10T20:00:00Z",
      "source_updated": "2026-10-10T20:01:00Z",
      "magnitude": 8.2,
      "mag_type": "mww",
      "place": "Peru",
      "city": null,
      "region": null,
      "country": null,
      "longitude": 0.0,
      "latitude": 0.0,
      "depth_km": 100.0,
      "event_type": "earthquake",
      "review_status": "reviewed",
      "tsunami": false,
      "felt": null,
      "cdi": null,
      "mmi": null,
      "alert": null,
      "sig": 298,
      "net": "us",
      "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us6000u2n7",
      "title": "M 8.2 - Peru",
      "revision_count": 1,
      "first_seen_at": "2026-10-13T00:00:00Z",
      "last_applied_at": "2026-10-13T00:00:00Z"
    }
  ],
  "total": 2,
  "limit": 1,
  "offset": 0
}
```

### A place filter

A case-insensitive substring of `place`. `%` and `_` match themselves, not as SQL wildcards, so `50%` (encoded `50%25`) finds only places containing those characters.

```http
GET /api/v1/earthquakes?place=50%25&limit=1
```

Response: `200 OK`

```json
{
  "items": [
    {
      "event_id": "hv11",
      "ids": [
        "hv11"
      ],
      "event_time": "2026-10-10T10:00:00Z",
      "source_updated": "2026-10-10T10:01:00Z",
      "magnitude": 3.3,
      "mag_type": "ml",
      "place": "Hawaii 50% zone",
      "city": null,
      "region": null,
      "country": null,
      "longitude": 0.0,
      "latitude": 0.0,
      "depth_km": 1.0,
      "event_type": "earthquake",
      "review_status": "automatic",
      "tsunami": false,
      "felt": null,
      "cdi": null,
      "mmi": null,
      "alert": null,
      "sig": 298,
      "net": "hv",
      "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us6000u2n7",
      "title": "M 3.3 - Hawaii 50% zone",
      "revision_count": 1,
      "first_seen_at": "2026-10-13T00:00:00Z",
      "last_applied_at": "2026-10-13T00:00:00Z"
    }
  ],
  "total": 1,
  "limit": 1,
  "offset": 0
}
```

### An event by a former ID

`ak20` and `us20` were two reports of one quake, merged when a revision listed both IDs. The old ID still finds the event, under its current ID; `revision_count` counts the three messages that changed it.

```http
GET /api/v1/earthquakes/ak20
```

Response: `200 OK`

```json
{
  "event_id": "us20",
  "ids": [
    "ak20",
    "us20"
  ],
  "event_time": "2026-10-11T02:00:00Z",
  "source_updated": "2026-10-11T02:10:00Z",
  "magnitude": 3.4,
  "mag_type": "mww",
  "place": "Alaska Peninsula",
  "city": null,
  "region": null,
  "country": null,
  "longitude": 0.0,
  "latitude": 0.0,
  "depth_km": 10.0,
  "event_type": "earthquake",
  "review_status": "reviewed",
  "tsunami": false,
  "felt": null,
  "cdi": null,
  "mmi": null,
  "alert": null,
  "sig": 298,
  "net": "us",
  "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us6000u2n7",
  "title": "M 3.2 - Alaska Peninsula",
  "revision_count": 3,
  "first_seen_at": "2026-10-13T00:00:00Z",
  "last_applied_at": "2026-10-13T00:00:00Z"
}
```

### Deleted events

Events USGS deleted are kept, with `review_status` `deleted`, and hidden unless `include_deleted=true`.

```http
GET /api/v1/earthquakes?review_status=deleted&include_deleted=true&limit=1
```

Response: `200 OK`

```json
{
  "items": [
    {
      "event_id": "us18",
      "ids": [
        "us18"
      ],
      "event_time": "2026-10-10T23:00:00Z",
      "source_updated": "2026-10-10T23:01:00Z",
      "magnitude": 3.0,
      "mag_type": "ml",
      "place": "Deleted event",
      "city": null,
      "region": null,
      "country": null,
      "longitude": 0.0,
      "latitude": 0.0,
      "depth_km": 10.0,
      "event_type": "earthquake",
      "review_status": "deleted",
      "tsunami": false,
      "felt": null,
      "cdi": null,
      "mmi": null,
      "alert": null,
      "sig": 298,
      "net": "us",
      "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us6000u2n7",
      "title": "M 3.0 - Deleted event",
      "revision_count": 1,
      "first_seen_at": "2026-10-13T00:00:00Z",
      "last_applied_at": "2026-10-13T00:00:00Z"
    }
  ],
  "total": 1,
  "limit": 1,
  "offset": 0
}
```

### Statistics

Over the filtered events. `magnitude` mixes magnitude scales, so `by_magnitude_type` says which ones went into it.

```http
GET /api/v1/earthquakes/stats?start_time=2026-10-10T00:00:00Z&end_time=2026-10-10T12:00:00Z
```

Response: `200 OK`

```json
{
  "count": 12,
  "magnitude": {
    "min": -0.5,
    "max": 6.1,
    "avg": 2.3
  },
  "by_event_type": {
    "earthquake": 10,
    "explosion": 1,
    "quarry blast": 1
  },
  "by_magnitude_type": {
    "mb": 2,
    "md": 4,
    "ml": 5,
    "mww": 1
  }
}
```

### Events per hour

Every UTC hour in the range is listed, empty hours included. Without a range, the 24 hours up to the end of the current hour.

```http
GET /api/v1/earthquakes/summary/hourly?start_time=2026-10-10T00:00:00Z&end_time=2026-10-10T06:00:00Z
```

Response: `200 OK`

```json
{
  "start_time": "2026-10-10T00:00:00Z",
  "end_time": "2026-10-10T06:00:00Z",
  "buckets": [
    {
      "hour": "2026-10-10T00:00:00Z",
      "count": 2
    },
    {
      "hour": "2026-10-10T01:00:00Z",
      "count": 2
    },
    {
      "hour": "2026-10-10T02:00:00Z",
      "count": 0
    },
    {
      "hour": "2026-10-10T03:00:00Z",
      "count": 2
    },
    {
      "hour": "2026-10-10T04:00:00Z",
      "count": 0
    },
    {
      "hour": "2026-10-10T05:00:00Z",
      "count": 2
    }
  ]
}
```

### Events per magnitude range

Every range is listed, empty ones included, with `unknown` for events without a magnitude. A range includes its lower bound.

```http
GET /api/v1/earthquakes/summary/magnitude?event_type=earthquake
```

Response: `200 OK`

```json
{
  "buckets": [
    {
      "label": "<0",
      "min": null,
      "max": 0.0,
      "count": 1
    },
    {
      "label": "0-1",
      "min": 0.0,
      "max": 1.0,
      "count": 2
    },
    {
      "label": "1-2",
      "min": 1.0,
      "max": 2.0,
      "count": 2
    },
    {
      "label": "2-3",
      "min": 2.0,
      "max": 3.0,
      "count": 1
    },
    {
      "label": "3-4",
      "min": 3.0,
      "max": 4.0,
      "count": 2
    },
    {
      "label": "4-5",
      "min": 4.0,
      "max": 5.0,
      "count": 2
    },
    {
      "label": "5-6",
      "min": 5.0,
      "max": 6.0,
      "count": 1
    },
    {
      "label": "6-7",
      "min": 6.0,
      "max": 7.0,
      "count": 2
    },
    {
      "label": ">=7",
      "min": 7.0,
      "max": null,
      "count": 2
    },
    {
      "label": "unknown",
      "min": null,
      "max": null,
      "count": 1
    }
  ]
}
```

### Export page

Every column of every row, exactly as stored, deleted events included, in `id` order, for `make export-r2`. Pass `next_after_id` as `after_id` until it is `null`.

```http
GET /api/v1/export/earthquakes?after_id=0&limit=1
```

Response: `200 OK`

```json
{
  "rows": [
    {
      "id": 1,
      "preferred_event_id": "us01",
      "event_time": "2026-10-10T00:15:00Z",
      "event_time_ms": 1791591300000,
      "source_updated": "2026-10-10T00:16:00Z",
      "source_updated_ms": 1791591360000,
      "magnitude": 4.4,
      "mag_type": "mb",
      "place": "100 km S of Tokyo, Japan",
      "city": "Tokyo",
      "region": null,
      "country": "Japan",
      "longitude": 139.7,
      "latitude": 34.7,
      "depth_km": 35.0,
      "event_type": "earthquake",
      "review_status": "reviewed",
      "tsunami": false,
      "felt": null,
      "cdi": null,
      "mmi": null,
      "alert": null,
      "sig": 298,
      "net": "us",
      "url": "https://earthquake.usgs.gov/earthquakes/eventpage/us6000u2n7",
      "title": "M 4.4 - 100 km S of Tokyo, Japan",
      "revision_ids": [
        "us01"
      ],
      "revision_count": 1,
      "first_seen_at": "2026-10-13T00:00:00Z",
      "last_applied_at": "2026-10-13T00:00:00Z"
    }
  ],
  "next_after_id": 1
}
```

## Error examples

### Every invalid parameter at once

A `422` lists every problem, not only the first.

```http
GET /api/v1/earthquakes?limit=501&start_time=yesterday
```

Response: `422 Unprocessable Content`

```json
{
  "error": {
    "code": "invalid_parameters",
    "message": "one or more parameters are invalid",
    "details": [
      {
        "parameter": "start_time",
        "message": "Value error, must be an ISO 8601 date-time, e.g. 2026-10-16T08:00:00Z"
      },
      {
        "parameter": "limit",
        "message": "Input should be less than or equal to 500"
      }
    ]
  }
}
```

### Ranges checked across parameters

Each parameter can be valid on its own and the pair still be refused.

```http
GET /api/v1/earthquakes?start_time=2026-10-11T00:00:00Z&end_time=2026-10-10T00:00:00Z
```

Response: `422 Unprocessable Content`

```json
{
  "error": {
    "code": "invalid_parameters",
    "message": "one or more parameters are invalid",
    "details": [
      {
        "parameter": "start_time",
        "message": "must be before end_time"
      }
    ]
  }
}
```

### The hourly range limit

At most 31 days, so a response has at most 744 buckets.

```http
GET /api/v1/earthquakes/summary/hourly?start_time=2026-09-01T00:00:00Z&end_time=2026-10-10T00:00:00Z
```

Response: `422 Unprocessable Content`

```json
{
  "error": {
    "code": "invalid_parameters",
    "message": "one or more parameters are invalid",
    "details": [
      {
        "parameter": "end_time",
        "message": "range must not exceed 31 days"
      }
    ]
  }
}
```

### An unknown ID

IDs match exactly, case included.

```http
GET /api/v1/earthquakes/nope
```

Response: `404 Not Found`

```json
{
  "error": {
    "code": "not_found",
    "message": "no event with ID 'nope'",
    "details": []
  }
}
```

### Read-only

Every endpoint answers `GET` only, and says so in `Allow`.

```http
POST /api/v1/earthquakes
```

Response: `405 Method Not Allowed`, with `Allow: GET`

```json
{
  "error": {
    "code": "method_not_allowed",
    "message": "Method Not Allowed",
    "details": []
  }
}
```
