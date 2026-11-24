#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd)"
compose_file="$project_root/tests/compose.jdbc-migration.yaml"
test_project="trama-jdbc-migration-$(date +%s)-$$"
test_dir="$(mktemp -d /private/tmp/trama-jdbc-migration.XXXXXX)"
test_password="${JDBC_TEST_PASSWORD:-$(openssl rand -hex 16)}"
spark_container="${test_project}-spark"
topic="ecommerce-events" # Historical topic in disposable Kafka only.
final_topic="trama-commerce-final-events"
current_topic="$topic"
active_db="streaming_project"
active_user="streaming_user"
export JDBC_TEST_PASSWORD="$test_password"
mkdir -p "$test_dir/ivy" "$test_dir/old-checkpoints" "$test_dir/jdbc-checkpoints"

cleanup() {
  local result="$?"
  trap - EXIT
  docker rm -f "$spark_container" >/dev/null 2>&1 || true
  docker compose -p "$test_project" -f "$compose_file" down --remove-orphans >/dev/null 2>&1 || true
  if [[ "$test_dir" == /private/tmp/trama-jdbc-migration.* && -d "$test_dir" ]]; then rm -r "$test_dir"; fi
  exit "$result"
}
trap cleanup EXIT

db_query() {
  docker compose -p "$test_project" -f "$compose_file" exec -T db \
    psql -U "$active_user" -d "$active_db" -At -F '|' -c "$1"
}
metrics() {
  db_query "SELECT (SELECT count(*) FROM events),
    (SELECT count(*) FROM events WHERE event_type='purchase'),
    (SELECT coalesce(sum(total_amount),0) FROM events),
    (SELECT coalesce(sum(total_events),0) FROM window_stats),
    (SELECT coalesce(sum(purchases),0) FROM window_stats),
    (SELECT coalesce(sum(revenue),0) FROM window_stats)"
}
wait_for_metrics() {
  local expected="$1" label="$2" actual=""
  for ((attempt=0; attempt<150; attempt++)); do
    actual="$(metrics)"
    if [[ "$actual" == "$expected" ]]; then echo "$label=$actual"; return 0; fi
    if [[ "$(docker inspect -f '{{.State.Running}}' "$spark_container")" != true ]]; then
      docker logs "$spark_container" 2>&1 | tail -n 90 >&2; return 1
    fi
    sleep 1
  done
  echo "Timed out waiting for $label: $actual" >&2
  docker logs "$spark_container" 2>&1 | tail -n 90 >&2
  return 1
}
wait_for_new_commits() {
  local before_events="$1" before_windows="$2"
  local events windows
  for ((attempt=0; attempt<150; attempt++)); do
    events="$(db_query "SELECT coalesce(max(batch_id),-1) FROM jdbc_batch_commits WHERE stream_id='$test_project:events'")"
    windows="$(db_query "SELECT coalesce(max(batch_id),-1) FROM jdbc_batch_commits WHERE stream_id='$test_project:windows'")"
    if (( events > before_events && windows > before_windows )); then return 0; fi
    sleep 1
  done
  echo "Timed out waiting for both JDBC streams to commit new Kafka input" >&2
  return 1
}
publish() {
  docker compose -p "$test_project" -f "$compose_file" exec -T kafka \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9093 --topic "$current_topic" \
    < "$test_dir/$1.jsonl"
}
start_spark() {
  local implementation="$1" offsets="${2:-}"
  local env_args=(-e DB_PASSWORD="$test_password")
  if [[ "$implementation" == consumer.py ]]; then
    env_args=(
      -e JDBC_DB_HOST=trama-postgres-final -e JDBC_DB_NAME=trama_commerce_final
      -e JDBC_DB_USER=trama_final_user -e JDBC_DB_PASSWORD="$test_password"
      -e JDBC_KAFKA_BROKER=kafka:9093 -e JDBC_KAFKA_TOPIC="$final_topic"
      -e JDBC_CHECKPOINT_ROOT=/test-control/jdbc-checkpoints
      -e JDBC_STREAM_NAMESPACE="$test_project"
      -e JDBC_ALLOW_FINAL_DEMO=I_ACCEPT_NEW_DATABASE_WRITES
      -e JDBC_STARTING_OFFSETS="$offsets"
      -e JDBC_MAX_PARTITIONS=2 -e JDBC_BATCH_SIZE=25
      -e JDBC_SPARK_MASTER='local[2]'
    )
  fi
  docker run -d --name "$spark_container" --network "${test_project}_default" \
    --mount "type=bind,source=$project_root,target=/opt/spark/work-dir/src/final-project-streaming,readonly" \
    --mount "type=bind,source=$test_dir/old-checkpoints,target=/opt/spark/work-dir/src/final-project-streaming/checkpoints" \
    --mount "type=bind,source=$test_dir,target=/test-control" \
    --mount "type=bind,source=$test_dir/ivy,target=/root/.ivy2.5.2" \
    --entrypoint bash "${env_args[@]}" spark-base:latest -lc "
      set -e
      python3 -m pip install --no-cache-dir psycopg2-binary==2.9.13 >/tmp/trama-pip.log 2>&1
      unset PYSPARK_DRIVER_PYTHON PYSPARK_DRIVER_PYTHON_OPTS
      PYTHONPATH=/opt/spark/work-dir/src/final-project-streaming /opt/spark/bin/spark-submit --master 'local[2]' \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.postgresql:postgresql:42.7.14 \\
        /opt/spark/work-dir/src/final-project-streaming/$implementation
    " >/dev/null
}
api_check() {
  docker run --rm --network "${test_project}_default" \
    --mount "type=bind,source=$project_root,target=/opt/trama,readonly" \
    --entrypoint bash -e KAFKA_BOOTSTRAP_SERVERS=kafka:9093 -e KAFKA_TOPIC="$final_topic" \
    -e DB_HOST=trama-postgres-final -e DB_NAME=trama_commerce_final -e DB_USER=trama_final_user \
    -e DB_PASSWORD="$test_password" spark-base:latest -lc "
      set -e
      python3 -m pip install --no-cache-dir -r /opt/trama/backend/requirements.txt httpx >/tmp/trama-api-pip.log 2>&1
      PYTHONPATH=/opt/trama python3 /opt/trama/tests/check_migration_api.py $1
    "
}

echo "ISOLATED_PROJECT=$test_project"
docker compose -p "$test_project" -f "$compose_file" up -d --wait
docker compose -p "$test_project" -f "$compose_file" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9093 \
  --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
docker compose -p "$test_project" -f "$compose_file" exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9093 \
  --create --if-not-exists --topic "$final_topic" --partitions 1 --replication-factor 1
python3 "$project_root/tests/jdbc_migration_fixture.py" "$test_dir"

publish old-first
start_spark tests/fixtures/consumer_psycopg2_original.py
wait_for_metrics '2|1|799.00|2|1|799.00' OLD_FIRST
publish old-late
wait_for_metrics '3|1|799.00|3|1|799.00' OLD_LATE_WITHIN_WATERMARK
fence="$(docker compose -p "$test_project" -f "$compose_file" exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9093 --topic "$topic" \
  | awk -F: '$2==0 {print $3}')"
[[ "$fence" == 3 ]]
echo "FENCE_OFFSETS={\"$topic\":{\"0\":$fence}}"
final_start="$(docker compose -p "$test_project" -f "$compose_file" exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9093 --topic "$final_topic" \
  | awk -F: '$2==0 {print $3}')"
[[ "$final_start" == 0 ]]
echo "FINAL_START_OFFSETS={\"$final_topic\":{\"0\":$final_start}}"
historical_snapshot="$(metrics)"
active_db="trama_commerce_final"
active_user="trama_final_user"
[[ "$(metrics)" == '0|0|0|0|0|0' ]]
docker kill "$spark_container" >/dev/null
docker rm "$spark_container" >/dev/null

offsets="{\"$final_topic\":{\"0\":$final_start}}"
current_topic="$final_topic"
start_spark consumer.py "$offsets"
publish new-first
wait_for_metrics '2|1|449.00|2|1|449.00' NEW_FIRST
docker kill "$spark_container" >/dev/null
docker rm "$spark_container" >/dev/null
start_spark consumer.py "$offsets"
before_events="$(db_query "SELECT coalesce(max(batch_id),-1) FROM jdbc_batch_commits WHERE stream_id='$test_project:events'")"
before_windows="$(db_query "SELECT coalesce(max(batch_id),-1) FROM jdbc_batch_commits WHERE stream_id='$test_project:windows'")"
publish new-retry
wait_for_new_commits "$before_events" "$before_windows"
wait_for_metrics '2|1|449.00|2|1|449.00' NEW_RETRY
publish new-late
wait_for_metrics '3|1|449.00|3|1|449.00' LATE_AFTER_WATERMARK
api_check purchase
wait_for_metrics '4|2|1248.00|4|2|1248.00' API_PURCHASE
api_check verify

# The professor's producer.py is run unchanged against this disposable Kafka.
docker run --rm --network "${test_project}_default" \
  --mount "type=bind,source=$project_root,target=/opt/trama,readonly" \
  --entrypoint bash spark-base:latest -lc '
    set -e
    python3 -m pip install --no-cache-dir kafka-python==2.2.15 >/tmp/trama-producer-pip.log 2>&1
    python3 /opt/trama/producer.py --broker kafka:9093 --topic trama-commerce-final-events --records 3 --interval 0
  '
for ((attempt=0; attempt<150; attempt++)); do
  count="$(db_query 'SELECT count(*) FROM events')"
  window_count="$(db_query 'SELECT coalesce(sum(total_events),0) FROM window_stats')"
  if [[ "$count" == 7 && "$window_count" == 7 ]]; then break; fi
  sleep 1
done
[[ "$count" == 7 && "$window_count" == 7 ]]
[[ "$(db_query "SELECT (SELECT coalesce(sum(total_amount),0) FROM events) - (SELECT coalesce(sum(revenue),0) FROM window_stats)")" == 0.00 ]]
echo "PRODUCER_PY_EVENTS=3 FINAL_EVENTS=$count FINAL_WINDOW_EVENTS=$window_count"

window_rows="$(db_query "SELECT to_char(window_start AT TIME ZONE 'UTC','HH24:MI'), category, total_events, purchases, revenue FROM window_stats ORDER BY window_start,category")"
active_db="streaming_project"
active_user="streaming_user"
[[ "$(metrics)" == "$historical_snapshot" ]]
echo "HISTORICAL_DB_UNCHANGED=$historical_snapshot"
echo "WINDOW_ROWS=$window_rows"
echo "OLD_CHECKPOINTS=$(find "$test_dir/old-checkpoints" -mindepth 1 -maxdepth 1 -type d -print | sed 's|.*/||' | sort | tr '\n' ',')"
echo "NEW_CHECKPOINTS=$(find "$test_dir/jdbc-checkpoints" -mindepth 1 -maxdepth 1 -type d -print | sed 's|.*/||' | sort | tr '\n' ',')"
echo "PASS: isolated original-to-JDBC offset cutover, late windows, restarts, duplicate UUIDs and FastAPI"
