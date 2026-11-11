#!/bin/sh
# Source database for the PostgreSQL capture path. Runs once from /docker-entrypoint-initdb.d.
#  - shop_app: the application's login; DML only on the shop tables.
#  - debezium: REPLICATION plus SELECT on the captured tables (for the initial snapshot); it does not own the
#    tables and cannot create publications, so the publication is created here, scoped to exactly these tables.
# The server runs with wal_level=logical (set on the command line in docker-compose.yml and the test fixture).
set -eu
: "${SHOP_APP_PASSWORD:?SHOP_APP_PASSWORD must be set}"
: "${DBZ_PASSWORD:?DBZ_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v app_pw="$SHOP_APP_PASSWORD" -v dbz_pw="$DBZ_PASSWORD" <<'SQL'
CREATE ROLE shop_app LOGIN PASSWORD :'app_pw';
CREATE ROLE debezium LOGIN REPLICATION PASSWORD :'dbz_pw';

CREATE TABLE customers (
  id          bigint       PRIMARY KEY,
  full_name   text         NOT NULL,
  email       text         NOT NULL,
  city        text,
  version     integer      NOT NULL DEFAULT 1,
  updated_at  timestamptz  NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE orders (
  id           bigint         PRIMARY KEY,
  customer_id  bigint         NOT NULL,
  status       text           NOT NULL,
  total_amount numeric(12,2)  NOT NULL,
  version      integer        NOT NULL DEFAULT 1,
  updated_at   timestamptz    NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE order_lines (
  order_id    bigint         NOT NULL,
  line_no     integer        NOT NULL,
  sku         text           NOT NULL,
  quantity    integer        NOT NULL,
  unit_price  numeric(12,2)  NOT NULL,
  version     integer        NOT NULL DEFAULT 1,
  updated_at  timestamptz    NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (order_id, line_no)
);

GRANT USAGE ON SCHEMA public TO shop_app, debezium;
GRANT SELECT, INSERT, UPDATE, DELETE ON customers, orders, order_lines TO shop_app;
GRANT SELECT ON customers, orders, order_lines TO debezium;

CREATE PUBLICATION cdc_shop FOR TABLE customers, orders, order_lines;
SQL
