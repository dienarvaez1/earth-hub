#!/usr/bin/env bash
# Restart check: data written before a Kafka and PostgreSQL restart must still be there after it.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a; source .env; set +a

TOKEN="restart-$(date +%s)-$RANDOM"
TOPIC="smoke.restart"
KAFKA_BIN=/opt/kafka/bin

psql_exec() {
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -qtA -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"
}
kafka_exec() {
  docker compose exec -T kafka "$@"
}

cleanup() {
  psql_exec "DROP TABLE IF EXISTS smoke_restart" >/dev/null 2>&1 || true
  kafka_exec "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server kafka:9092 --delete --if-exists --topic "$TOPIC" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "1. Writing token $TOKEN to PostgreSQL and Kafka"
psql_exec "CREATE TABLE IF NOT EXISTS smoke_restart (token text PRIMARY KEY)"
psql_exec "INSERT INTO smoke_restart VALUES ('$TOKEN')"
kafka_exec "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic "$TOPIC" --partitions 1 --replication-factor 1 2>/dev/null
echo "$TOKEN" | kafka_exec "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server kafka:9092 --topic "$TOPIC"

echo "2. Restarting kafka and postgres"
docker compose restart kafka postgres >/dev/null
docker compose up -d --wait kafka postgres >/dev/null

echo "3. Checking the token survived"
if [[ "$(psql_exec "SELECT token FROM smoke_restart WHERE token = '$TOKEN'")" != "$TOKEN" ]]; then
  echo "FAIL: PostgreSQL row is missing after restart" >&2
  exit 1
fi
echo "   PostgreSQL row present"

if ! kafka_exec "$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server kafka:9092 --topic "$TOPIC" \
  --from-beginning --max-messages 1 --timeout-ms 30000 2>/dev/null | grep -qx "$TOKEN"; then
  echo "FAIL: Kafka message is missing after restart" >&2
  exit 1
fi
echo "   Kafka message present"

echo "4. Running the smoke tests against the restarted services"
uv run pytest -m integration tests/integration -q
