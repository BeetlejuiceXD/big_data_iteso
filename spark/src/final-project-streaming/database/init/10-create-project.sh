#!/bin/sh
set -eu

: "${APP_DB_PASSWORD:?APP_DB_PASSWORD is required}"

# The official image runs this file only when its data directory is empty.
# psql's quoted variable substitution keeps passwords as SQL values.
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" --dbname postgres \
  -v app_password="$APP_DB_PASSWORD" <<'SQL'
CREATE ROLE streaming_user LOGIN PASSWORD :'app_password';
CREATE DATABASE streaming_project OWNER postgres;
\connect streaming_project
GRANT CONNECT ON DATABASE streaming_project TO streaming_user;
GRANT USAGE, CREATE ON SCHEMA public TO streaming_user;
SET ROLE streaming_user;
\i /opt/trama/schema.sql
RESET ROLE;
SQL
