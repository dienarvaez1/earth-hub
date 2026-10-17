# DEF-015: Hourly summary refused a valid request with only `start_time`

| | |
| --- | --- |
| **Severity** | Medium (a valid, documented request was refused) |
| **Priority** | P2 |
| **Status** | Fixed in `4d2748f` (Milestone 4) |
| **Component** | `GET /api/v1/earthquakes/summary/hourly` (`src/earth_hub/api/routes.py`) |
| **Found by** | Schemathesis, property-based contract testing (`positive_data_acceptance` check) |

## Environment
FastAPI 0.143, Schemathesis 4.31, the API tests' fixed dataset.

## Steps to reproduce
```bash
curl -s 'http://127.0.0.1:8000/api/v1/earthquakes/summary/hourly?start_time=2000-01-01T00:00:00Z'
```

## Expected result
`200` with an hourly summary. The request matches the documented schema: `start_time` is an optional ISO 8601 date-time.

## Actual result
```json
{"error":{"code":"invalid_parameters","message":"one or more parameters are invalid",
 "details":[{"parameter":"end_time","message":"range must not exceed 31 days"}]}}
```

## Root cause
When `end_time` was missing, it defaulted to the end of the current hour, whatever `start_time` was. A lone start time older than 31 days therefore always produced a window past the limit. The error named `end_time`, a parameter the client never sent.

## Fix
Defaults are now anchored on the time that was given:
- **Neither time:** the 24 hours up to the end of the current hour, as before.
- **Only `start_time`:** the 24 hours after it.
- **Only `end_time`:** the 24 hours before it.

The OpenAPI description of the endpoint documents these rules.

## Regression test
- `tests/api/test_aggregates.py::test_hourly_window_anchors_on_the_one_time_given` covers both one-sided cases.
- `tests/api/test_aggregates.py::test_hourly_range_limits` was updated to the case that must still fail: a start after an explicit end.

## Prevention
- When a parameter has a default, test it alone and combined with every other parameter.
- The limit itself (31 days), and min ≤ max and start < end, compare two parameters, which OpenAPI cannot express. So Schemathesis's "valid requests must be accepted" check is turned off, and those rules have explicit tests instead. This defect was found before the check was turned off, and the reason for turning it off is documented in `tests/api/test_contract.py`.
