#!/bin/sh
set -eu

: "${APP_DB_PASSWORD:?APP_DB_PASSWORD is required}"

# Runs only when the dedicated final-demo volume is initialized for the first time.
psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" --dbname postgres \
  -v app_password="$APP_DB_PASSWORD" <<'SQL'
CREATE ROLE trama_final_user LOGIN PASSWORD :'app_password';
CREATE DATABASE trama_commerce_final OWNER postgres;
\connect trama_commerce_final
GRANT CONNECT ON DATABASE trama_commerce_final TO trama_final_user;
GRANT USAGE, CREATE ON SCHEMA public TO trama_final_user;
SET ROLE trama_final_user;
\i /opt/trama/schema.sql
\i /opt/trama/jdbc_staging.sql
RESET ROLE;
SQL
