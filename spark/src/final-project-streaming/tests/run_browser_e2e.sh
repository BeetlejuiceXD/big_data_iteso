#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd)"
compose_file="$project_root/tests/compose.jdbc-migration.yaml"
test_project="trama-browser-final-$(date +%s)-$$"
test_dir="$(mktemp -d /private/tmp/trama-browser-final.XXXXXX)"
test_password="$(openssl rand -hex 16)"
spark_container="${test_project}-spark"
api_container="${test_project}-api"
api_port=18081
vite_port=5175
release_file="$test_dir/release"
vite_pid=""
export JDBC_TEST_PASSWORD="$test_password"
mkdir -p "$test_dir/ivy" "$test_dir/jdbc-checkpoints"

cleanup() {
  local result="$?"
  trap - EXIT INT TERM
  if [[ -n "$vite_pid" ]]; then kill "$vite_pid" >/dev/null 2>&1 || true; fi
  docker rm -f "$spark_container" "$api_container" >/dev/null 2>&1 || true
  docker compose -p "$test_project" -f "$compose_file" down --remove-orphans >/dev/null 2>&1 || true
  if [[ "$test_dir" == /private/tmp/trama-browser-final.* && -d "$test_dir" ]]; then rm -r "$test_dir"; fi
  exit "$result"
}
trap cleanup EXIT INT TERM

if lsof -nP -iTCP:"$api_port" -iTCP:"$vite_port" -sTCP:LISTEN | grep -q LISTEN; then
  echo "Ports $api_port or $vite_port are already occupied" >&2
  exit 1
fi

docker compose -p "$test_project" -f "$compose_file" up -d --wait
docker compose -p "$test_project" -f "$compose_file" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9093 \
  --create --if-not-exists --topic trama-commerce-final-events --partitions 1 --replication-factor 1

docker run -d --name "$spark_container" --network "${test_project}_default" \
  --mount "type=bind,source=$project_root,target=/opt/trama,readonly" \
  --mount "type=bind,source=$test_dir,target=/test-control" \
  --mount "type=bind,source=$test_dir/ivy,target=/root/.ivy2.5.2" \
  --entrypoint bash \
  -e JDBC_DB_HOST=trama-postgres-final -e JDBC_DB_NAME=trama_commerce_final \
  -e JDBC_DB_USER=trama_final_user -e JDBC_DB_PASSWORD="$test_password" \
  -e JDBC_KAFKA_BROKER=kafka:9093 -e JDBC_KAFKA_TOPIC=trama-commerce-final-events \
  -e JDBC_CHECKPOINT_ROOT=/test-control/jdbc-checkpoints \
  -e JDBC_STREAM_NAMESPACE="$test_project" \
  -e JDBC_ALLOW_FINAL_DEMO=I_ACCEPT_NEW_DATABASE_WRITES \
  -e JDBC_STARTING_OFFSETS='{"trama-commerce-final-events":{"0":0}}' \
  -e JDBC_SPARK_MASTER='local[2]' -e JDBC_MAX_PARTITIONS=2 \
  spark-base:latest -lc '
    set -e
    python3 -m pip install --no-cache-dir psycopg2-binary==2.9.13 >/tmp/trama-pip.log 2>&1
    unset PYSPARK_DRIVER_PYTHON PYSPARK_DRIVER_PYTHON_OPTS
    PYTHONPATH=/opt/trama /opt/spark/bin/spark-submit --master local[2] \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.postgresql:postgresql:42.7.14 \
      /opt/trama/consumer.py
  ' >/dev/null

docker run -d --name "$api_container" --network "${test_project}_default" \
  -p "127.0.0.1:${api_port}:8000" \
  --mount "type=bind,source=$project_root,target=/opt/trama,readonly" \
  --entrypoint bash \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9093 -e KAFKA_TOPIC=trama-commerce-final-events \
  -e DB_HOST=trama-postgres-final -e DB_NAME=trama_commerce_final \
  -e DB_USER=trama_final_user -e DB_PASSWORD="$test_password" \
  -e CORS_ORIGINS="http://127.0.0.1:$vite_port" \
  spark-base:latest -lc '
    set -e
    python3 -m pip install --no-cache-dir -r /opt/trama/backend/requirements.txt >/tmp/trama-api-pip.log 2>&1
    cd /opt/trama/backend
    uvicorn app.main:app --host 0.0.0.0 --port 8000
  ' >/dev/null

for ((attempt=0; attempt<90; attempt++)); do
  if curl -fsS "http://127.0.0.1:$api_port/health" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "http://127.0.0.1:$api_port/health" >/dev/null

(
  cd "$project_root/frontend"
  TRAMA_API_PROXY_TARGET="http://127.0.0.1:$api_port" \
    ./node_modules/.bin/vite --configLoader native --host 127.0.0.1 \
      --port "$vite_port" --strictPort >"$test_dir/vite.log" 2>&1
) &
vite_pid="$!"
for ((attempt=0; attempt<40; attempt++)); do
  if curl -fsS "http://127.0.0.1:$vite_port" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "http://127.0.0.1:$vite_port" >/dev/null

echo "ISOLATED_PROJECT=$test_project"
echo "FRONTEND_URL=http://127.0.0.1:$vite_port"
echo "API_URL=http://127.0.0.1:$api_port"
echo "RELEASE_FILE=$release_file"
echo "READY_FOR_BROWSER=true"
while [[ ! -f "$release_file" ]]; do sleep 1; done

echo "FINAL_METRICS=$(curl -fsS "http://127.0.0.1:$api_port/api/v1/metrics")"
echo "FINAL_CONSISTENCY=$(curl -fsS "http://127.0.0.1:$api_port/api/v1/consistency")"
