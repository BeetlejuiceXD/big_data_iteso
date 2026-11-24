#!/bin/sh
set -eu
: "${JDBC_TEST_PASSWORD:?JDBC_TEST_PASSWORD is required}"
psql -v ON_ERROR_STOP=1 --username postgres --dbname postgres \
  -v test_password="$JDBC_TEST_PASSWORD" <<'SQL'
CREATE ROLE streaming_user LOGIN PASSWORD :'test_password';
CREATE DATABASE streaming_project OWNER postgres;
\connect streaming_project
GRANT CONNECT ON DATABASE streaming_project TO streaming_user;
GRANT USAGE, CREATE ON SCHEMA public TO streaming_user;
SET ROLE streaming_user;
\i /opt/trama/schema.sql
RESET ROLE;
\connect postgres
CREATE ROLE trama_final_user LOGIN PASSWORD :'test_password';
CREATE DATABASE trama_commerce_final OWNER postgres;
\connect trama_commerce_final
GRANT CONNECT ON DATABASE trama_commerce_final TO trama_final_user;
GRANT USAGE, CREATE ON SCHEMA public TO trama_final_user;
SET ROLE trama_final_user;
\i /opt/trama/schema.sql
\i /opt/trama/jdbc_staging.sql
RESET ROLE;
SQL
