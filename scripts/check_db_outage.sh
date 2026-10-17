#!/usr/bin/env bash
# Outage check: stop PostgreSQL while the consumer is receiving messages, start it again,
# and prove every message was stored exactly once. Uses its own topic, consumer group and
# database, so real data is untouched. Needs `make up`.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a; source .env; set +a

RUN="$(date +%s)$RANDOM"
TOPIC="outage.check.$RUN"
GROUP="outage-check-$RUN"
CHECK_DB="earth_hub_outage_$RUN"
HEARTBEAT="${TMPDIR:-/tmp}/earth-hub-outage-$RUN.heartbeat"
BATCH=25
LOG="${TMPDIR:-/tmp}/earth-hub-outage-$RUN.log"
KAFKA_BIN=/opt/kafka/bin
CONSUMER_PID=""

# Overrides go only to the app's own commands. Exporting them would leak into
# docker compose, which then sees changed config and recreates containers.
APP_ENV=(POSTGRES_DB="$CHECK_DB" KAFKA_TOPIC_RAW="$TOPIC" KAFKA_CONSUMER_GROUP="$GROUP"
  CONSUMER_HEARTBEAT_PATH="$HEARTBEAT")
app() { env "${APP_ENV[@]}" "$@"; }
wait_for_postgres() {
  for _ in $(seq 60); do
    docker compose exec -T postgres pg_isready -q -U "$POSTGRES_USER" 2>/dev/null && return 0
    sleep 1
  done
  echo "FAIL: PostgreSQL did not come back" >&2
  return 1
}
psql_main() {
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -qtA -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"
}
psql_check() {
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -qtA -U "$POSTGRES_USER" -d "$CHECK_DB" -c "$1"
}
wait_for_rows() {  # wait_for_rows <count> <seconds>
  for _ in $(seq "$2"); do
    [[ "$(psql_check "SELECT count(*) FROM raw_events" 2>/dev/null || echo 0)" -ge "$1" ]] && return 0
    sleep 1
  done
  echo "FAIL: expected $1 raw_events rows, have $(psql_check "SELECT count(*) FROM raw_events")" >&2
  return 1
}
publish() {  # publish <first index> <count>
  app uv run python - "$1" "$2" <<'EOF'
import copy, json, os, sys
from datetime import UTC, datetime
from confluent_kafka import Producer
from earth_hub.ingestion.events import build_message

start, count = int(sys.argv[1]), int(sys.argv[2])
feature = json.load(open("tests/fixtures/usgs/all_day_sample.geojson"))["features"][0]
producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"], "acks": "all"})
for i in range(start, start + count):
    f = copy.deepcopy(feature)
    f["id"] = f"outage{i:04d}"
    f["properties"]["ids"] = f",{f['id']},"
    message = build_message(f, fetched_at=datetime.now(UTC), feed_generated_ms=None)
    producer.produce(os.environ["KAFKA_TOPIC_RAW"], key=message.key, value=message.to_json())
assert producer.flush(30) == 0
EOF
}

cleanup() {
  [[ -n "$CONSUMER_PID" ]] && kill "$CONSUMER_PID" 2>/dev/null && wait "$CONSUMER_PID" 2>/dev/null || true
  docker compose start postgres >/dev/null 2>&1 || true
  wait_for_postgres >/dev/null 2>&1 || true
  psql_main "DROP DATABASE IF EXISTS \"$CHECK_DB\" WITH (FORCE)" >/dev/null 2>&1 || true
  docker compose exec -T kafka "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server kafka:9092 \
    --delete --if-exists --topic "$TOPIC" >/dev/null 2>&1 || true
  rm -f "$HEARTBEAT"
}
trap cleanup EXIT

echo "1. Creating topic $TOPIC and database $CHECK_DB"
docker compose exec -T kafka "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server kafka:9092 --create \
  --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null 2>&1
psql_main "CREATE DATABASE \"$CHECK_DB\""
app uv run alembic upgrade head >/dev/null 2>&1

echo "2. Starting the consumer (log: $LOG)"
# Not through app(): a backgrounded function runs in a subshell, and $! would be the
# subshell's PID, so the kill below would miss the consumer.
env "${APP_ENV[@]}" uv run python -m earth_hub.consumers >"$LOG" 2>&1 &
CONSUMER_PID=$!

echo "3. Publishing $BATCH messages with PostgreSQL up"
publish 0 "$BATCH"
wait_for_rows "$BATCH" 60
echo "   stored: $(psql_check "SELECT count(*) FROM raw_events")"

echo "4. Stopping PostgreSQL and publishing $BATCH more"
docker compose stop postgres >/dev/null
publish "$BATCH" "$BATCH"
sleep 8
if ! grep -q "database unavailable, retrying the same message" "$LOG"; then
  echo "FAIL: consumer did not report the outage" >&2
  exit 1
fi
echo "   consumer is retrying ($(grep -c 'database unavailable' "$LOG") retries logged)"

echo "5. Starting PostgreSQL again"
docker compose start postgres >/dev/null
wait_for_postgres
wait_for_rows $((BATCH * 2)) 90

echo "6. Stopping the consumer cleanly"
kill -TERM "$CONSUMER_PID"
wait "$CONSUMER_PID" || true
CONSUMER_PID=""
if ! grep -q '"consumer stopped"' "$LOG"; then
  echo "FAIL: consumer did not shut down cleanly" >&2
  exit 1
fi
for _ in $(seq 10); do pgrep -f "earth_hub.consumers" >/dev/null || break; sleep 1; done
if pgrep -f "earth_hub.consumers" >/dev/null; then
  echo "FAIL: a consumer process is still running 10 s after SIGTERM" >&2
  exit 1
fi
echo "   consumer stopped cleanly"

echo "7. Checking every message was stored exactly once"
events=$(psql_check "SELECT count(*) FROM earthquakes")
raws=$(psql_check "SELECT count(*) FROM raw_events")
applied=$(psql_check "SELECT count(*) FROM raw_events WHERE outcome = 'applied'")
dupes=$(psql_check "SELECT count(*) FROM (SELECT event_id FROM raw_events GROUP BY event_id HAVING count(*) > 1) d")
echo "   earthquakes=$events raw_events=$raws applied=$applied duplicated_event_ids=$dupes"
if [[ "$events" != $((BATCH * 2)) || "$raws" != $((BATCH * 2)) || "$applied" != $((BATCH * 2)) || "$dupes" != 0 ]]; then
  echo "FAIL: expected $((BATCH * 2)) events, rows and applied outcomes, and no duplicates" >&2
  exit 1
fi

echo "8. Reconciling topic against database"
app uv run python -m earth_hub.reconcile
echo "PASS: database outage recovered with no loss and no duplicates"
