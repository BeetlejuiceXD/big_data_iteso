#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd)"
compose_file="$project_root/tests/compose.jdbc-kafka-e2e.yaml"
test_project="trama-jdbc-e2e-$(date +%s)-$$"
test_dir="$(mktemp -d /private/tmp/trama-jdbc-e2e.XXXXXX)"
test_password="${JDBC_TEST_PASSWORD:-$(openssl rand -hex 16)}"
spark_container="${test_project}-spark"
topic="ecommerce-jdbc-e2e-test"
marker="$test_dir/hold.marker"
export JDBC_TEST_PASSWORD="$test_password"
mkdir -p "$test_dir/ivy"

cleanup() {
  local test_exit_code="$?"
  trap - EXIT
  docker rm -f "$spark_container" >/dev/null 2>&1 || true
  docker compose -p "$test_project" -f "$compose_file" down --remove-orphans >/dev/null 2>&1 || true
  # Only the mktemp directory created by this invocation is removed.
  if [[ "$test_dir" == /private/tmp/trama-jdbc-e2e.* && -d "$test_dir" ]]; then
    rm -r "$test_dir"
  fi
  exit "$test_exit_code"
}
trap cleanup EXIT

db_query() {
  docker compose -p "$test_project" -f "$compose_file" exec -T db \
    psql -U jdbc_test_user -d trama_jdbc_test -At -F '|' -c "$1"
}

metrics() {
  db_query "SELECT
    (SELECT count(*) FROM events),
    (SELECT count(*) FROM events WHERE event_type = 'purchase'),
    (SELECT coalesce(sum(total_amount), 0) FROM events),
    (SELECT coalesce(sum(total_events), 0) FROM window_stats),
    (SELECT coalesce(sum(purchases), 0) FROM window_stats),
    (SELECT coalesce(sum(revenue), 0) FROM window_stats),
    (SELECT count(*) FROM (SELECT event_id FROM events GROUP BY event_id HAVING count(*) > 1) d)"
}

wait_for_metrics() {
  local expected="$1"
  local label="$2"
  local actual=""
  for ((attempt=0; attempt<150; attempt++)); do
    actual="$(metrics)"
    if [[ "$actual" == "$expected" ]]; then
      echo "$label=$actual"
      return 0
    fi
    if [[ "$(docker inspect -f '{{.State.Running}}' "$spark_container")" != true ]]; then
      echo "Spark exited before $label; recent logs:" >&2
      docker logs "$spark_container" 2>&1 | tail -n 70 >&2
      return 1
    fi
    sleep 1
  done
  echo "Timed out waiting for $label; last result: $actual" >&2
  docker logs "$spark_container" 2>&1 | tail -n 70 >&2
  return 1
}

start_spark() {
  local pause_marker="$1"
  docker run -d --name "$spark_container" \
    --network "${test_project}_default" \
    --mount "type=bind,source=$project_root,target=/opt/trama,readonly" \
    --mount "type=bind,source=$test_dir,target=/test-control" \
    --mount "type=bind,source=$test_dir/ivy,target=/root/.ivy2.5.2" \
    --entrypoint bash \
    -e JDBC_DB_HOST=db -e JDBC_DB_NAME=trama_jdbc_test \
    -e JDBC_DB_USER=jdbc_test_user -e JDBC_DB_PASSWORD="$test_password" \
    -e JDBC_STREAM_NAMESPACE="$test_project" \
    -e JDBC_CHECKPOINT_ROOT=/test-control/jdbc-checkpoints \
    -e JDBC_KAFKA_BROKER=kafka:9092 -e JDBC_KAFKA_TOPIC="$topic" \
    -e JDBC_MAX_PARTITIONS=2 -e JDBC_BATCH_SIZE=25 \
    -e JDBC_SPARK_MASTER='local[2]' \
    -e JDBC_TEST_PAUSE_AFTER_STAGE_MARKER="$pause_marker" \
    spark-base:latest -lc '
      set -e
      python3 -m pip install --no-cache-dir psycopg2-binary==2.9.13 >/tmp/trama-pip.log 2>&1
      unset PYSPARK_DRIVER_PYTHON PYSPARK_DRIVER_PYTHON_OPTS
      PYTHONPATH=/opt/trama /opt/spark/bin/spark-submit --master local[2] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.postgresql:postgresql:42.7.14 \
        /opt/trama/consumer.py
    ' >/dev/null
}

echo "ISOLATED_PROJECT=$test_project"
docker compose -p "$test_project" -f "$compose_file" up -d --wait
docker compose -p "$test_project" -f "$compose_file" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
python3 "$project_root/tests/jdbc_kafka_fixture.py" "$test_dir"

docker compose -p "$test_project" -f "$compose_file" exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic "$topic" \
  < "$test_dir/initial.jsonl"
echo "PUBLISHED_INITIAL=6"
start_spark /test-control/hold.marker

for ((attempt=0; attempt<150; attempt++)); do
  if [[ -f "$marker" ]]; then break; fi
  if [[ "$(docker inspect -f '{{.State.Running}}' "$spark_container")" != true ]]; then
    docker logs "$spark_container" 2>&1 | tail -n 70 >&2
    exit 1
  fi
  sleep 1
done
if [[ ! -f "$marker" ]]; then
  echo "Spark did not reach the post-JDBC staging marker" >&2
  docker logs "$spark_container" 2>&1 | tail -n 70 >&2
  exit 1
fi
staged_before="$(db_query "SELECT count(*) FROM jdbc_stage_events WHERE batch_id = 0")"
commits_before="$(db_query "SELECT count(*) FROM jdbc_batch_commits WHERE stream_id = '$test_project:events' AND batch_id = 0")"
echo "BEFORE_KILL=stage_events:$staged_before,event_batch_commits:$commits_before"
[[ "$staged_before" == 6 && "$commits_before" == 0 ]]
docker kill "$spark_container" >/dev/null
docker rm "$spark_container" >/dev/null
rm "$marker"
echo "INTERRUPTED_AFTER_JDBC_STAGE=true"

start_spark ""
wait_for_metrics '6|2|2047.00|6|2|2047.00|0' AFTER_RESTART
docker compose -p "$test_project" -f "$compose_file" exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic "$topic" \
  < "$test_dir/after-restart.jsonl"
echo "PUBLISHED_AFTER_RESTART=3"
wait_for_metrics '9|3|2945.00|9|3|2945.00|0' FINAL

category_rows="$(db_query "SELECT category, count(*), count(*) FILTER (WHERE event_type = 'purchase'), sum(total_amount) FROM events GROUP BY category ORDER BY category")"
window_rows="$(db_query "SELECT category, total_events, purchases, revenue FROM window_stats ORDER BY category")"
expected_categories=$'Electronics|4|1|1598.00\nHome|5|2|1347.00'
[[ "$category_rows" == "$expected_categories" ]]
[[ "$window_rows" == "$expected_categories" ]]
echo "CATEGORY_RESULTS=$category_rows"
echo "WINDOW_RESULTS=$window_rows"
echo "CHECKPOINT_DIRECTORIES=$(find "$test_dir/jdbc-checkpoints" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort | tr '\n' ',')"
echo "PASS: official consumer.py Kafka -> Spark Structured Streaming -> Spark JDBC -> PostgreSQL 18.6; SIGKILL recovery; exact windows; zero duplicate UUIDs"
