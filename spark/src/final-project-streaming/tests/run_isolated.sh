#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd)"
compose_file="$project_root/tests/compose.jdbc-test.yaml"
test_project="trama-jdbc-test-$(date +%s)-$$"
test_password="${JDBC_TEST_PASSWORD:-$(openssl rand -hex 16)}"
export JDBC_TEST_PASSWORD="$test_password"

cleanup() {
  docker compose -p "$test_project" -f "$compose_file" down --remove-orphans
}
trap cleanup EXIT

docker compose -p "$test_project" -f "$compose_file" up -d --wait
docker run --rm \
  --network "${test_project}_default" \
  --mount "type=bind,source=$project_root,target=/opt/trama,readonly" \
  --entrypoint bash \
  -e JDBC_DB_HOST=db \
  -e JDBC_DB_NAME=trama_jdbc_test \
  -e JDBC_DB_USER=jdbc_test_user \
  -e JDBC_DB_PASSWORD="$test_password" \
  -e JDBC_STREAM_NAMESPACE=isolated-run-001 \
  -e JDBC_CHECKPOINT_ROOT=/tmp/trama-jdbc-checkpoints \
  -e JDBC_KAFKA_BROKER=unused-test-broker:9093 \
  -e JDBC_KAFKA_TOPIC=ecommerce-jdbc-test \
  -e JDBC_MAX_PARTITIONS=2 \
  -e JDBC_BATCH_SIZE=25 \
  spark-base:latest -lc '
    set -e
    python3 -m pip install --no-cache-dir psycopg2-binary==2.9.13 >/tmp/trama-pip.log 2>&1
    unset PYSPARK_DRIVER_PYTHON PYSPARK_DRIVER_PYTHON_OPTS
    PYTHONPATH=/opt/trama /opt/spark/bin/spark-submit --master local[2] \
      --packages org.postgresql:postgresql:42.7.14 \
      /opt/trama/tests/run_jdbc_integration.py >/tmp/trama-jdbc.log 2>&1 || {
        tail -n 60 /tmp/trama-jdbc.log; exit 1;
      }
    grep "PASS:" /tmp/trama-jdbc.log
    PYTHONPATH=/opt/trama /opt/spark/bin/spark-submit --master local[2] \
      --packages org.postgresql:postgresql:42.7.14 \
      /opt/trama/tests/run_stream_recovery.py >/tmp/trama-recovery.log 2>&1 || {
        tail -n 60 /tmp/trama-recovery.log; exit 1;
      }
    grep "PASS:" /tmp/trama-recovery.log
  '
