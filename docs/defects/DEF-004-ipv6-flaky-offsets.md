# DEF-004: Intermittent Kafka offset lookups through IPv6 `localhost`

| | |
| --- | --- |
| **Severity** | High (unreliable results; affects the poller's restart path) |
| **Priority** | P1 |
| **Status** | Fixed in `e76c9df` (Milestone 2) |
| **Component** | Docker Compose Kafka listeners, configuration defaults |
| **Found by** | Running the same integration tests several times |

## Environment
macOS 27 (arm64), Docker 29.8, `apache/kafka:4.2.2` with the host listener published as `127.0.0.1:29092`, confluent-kafka 2.16 (librdkafka).

## Steps to reproduce
1. Advertise the host listener as `EXTERNAL://localhost:29092` and connect with `KAFKA_BOOTSTRAP_SERVERS=localhost:29092`.
2. Run `uv run pytest -m integration tests/integration/test_ingestion.py` three times.

## Expected result
All tests pass every time.

## Actual result
The failures varied from run to run, **3, then 1, then 0**, always in `offsets_for_times` or `get_watermark_offsets`:

```
KafkaError{code=_TRANSPORT,val=-195,str="Failed to get offsets: Local: Broker transport failure"}
Connect to ipv6#[::1]:29092 failed: Connection refused
```

## Root cause
On macOS, `localhost` resolves to IPv6 `::1` first. Docker published the port on IPv4 `127.0.0.1` only. librdkafka tried `::1`, was refused, and then fell back to IPv4. Ordinary producing and consuming survived the fallback, but a one-off metadata request that hit the refused attempt failed straight away instead of retrying. Whether a run failed depended on which address a given connection tried first.

## Fix
- Kafka advertises `EXTERNAL://127.0.0.1:29092`.
- The default and `.env.example` use `KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:29092`.
- The README's troubleshooting section and `CLAUDE.md` explain why it must be an IP address.

## Regression test
After the fix, 6 runs in a row passed with **0** IPv6 connection attempts in the output. Since then, every integration and end-to-end run has been a repeat test of this, including `make flake-check`, which runs the end-to-end suite 5 times.

## Prevention
- Use IP addresses rather than `localhost` for anything published by Docker.
- Treat any test that fails "sometimes" as a defect to investigate, not as noise to re-run away.

Related: DEF-019 is a different cause with the same symptom (offset lookups on a brand-new topic). It led to retries in `earth_hub/kafka_metadata.py`.
