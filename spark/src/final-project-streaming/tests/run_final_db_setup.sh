#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "$0")/.." && pwd)"
container="trama-final-db-setup-$(date +%s)-$$"
test_password="$(openssl rand -hex 16)"

cleanup() {
  local result="$?"
  trap - EXIT
  docker rm -f "$container" >/dev/null 2>&1 || true
  exit "$result"
}
trap cleanup EXIT

docker run -d --name "$container" --network none --tmpfs /var/lib/postgresql \
  -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres \
  -e POSTGRES_PASSWORD="$test_password" -e APP_DB_PASSWORD="$test_password" \
  -e POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
  --mount "type=bind,source=$project_root/database/init/20-create-final.sh,target=/docker-entrypoint-initdb.d/20-create-final.sh,readonly" \
  --mount "type=bind,source=$project_root/database/schema.sql,target=/opt/trama/schema.sql,readonly" \
  --mount "type=bind,source=$project_root/database/jdbc_staging.sql,target=/opt/trama/jdbc_staging.sql,readonly" \
  postgres:18.6 >/dev/null

for ((attempt=0; attempt<60; attempt++)); do
  if docker exec "$container" pg_isready -U trama_final_user -d trama_commerce_final >/dev/null 2>&1; then break; fi
  if [[ "$(docker inspect -f '{{.State.Running}}' "$container")" != true ]]; then
    docker logs "$container" 2>&1 | tail -n 60 >&2; exit 1
  fi
  sleep 1
done

query() {
  docker exec -e PGPASSWORD="$test_password" "$container" \
    psql -h 127.0.0.1 -U trama_final_user -d trama_commerce_final -At -F '|' -c "$1"
}

actual="$(query "SELECT current_user,
  (SELECT count(*) FROM events),
  (SELECT count(*) FROM window_stats),
  (SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename IN
   ('events','window_stats','jdbc_stage_events','jdbc_stage_windows','jdbc_stream_locks','jdbc_batch_commits')),
  has_schema_privilege(current_user,'public','CREATE')")"
[[ "$actual" == 'trama_final_user|0|0|6|t' ]]
owners="$(query "SELECT string_agg(DISTINCT tableowner, ',') FROM pg_tables WHERE schemaname='public' AND tablename IN
  ('events','window_stats','jdbc_stage_events','jdbc_stage_windows','jdbc_stream_locks','jdbc_batch_commits')")"
[[ "$owners" == trama_final_user ]]
echo "POSTGRES_VERSION=$(query 'SHOW server_version')"
echo "FINAL_SCHEMA=$actual OWNERS=$owners"
echo "PASS: original schema and JDBC staging initialized in a fresh PostgreSQL 18.6 tmpfs with no external network"
