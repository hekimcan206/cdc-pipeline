#!/bin/bash
# Prepares Oracle Database 23ai Free (gvenzl/oracle-free) for Debezium's LogMiner adapter.
# Runs once, from /container-entrypoint-initdb.d, as the oracle OS user, before the image reports ready.
#
#  1. ARCHIVELOG mode: LogMiner must be able to read redo that has already been switched out of the online logs,
#     otherwise a consumer that falls behind (or a connector restart) loses changes. Switching modes needs the
#     database mounted but not open, so this script bounces the instance once.
#  2. Minimal supplemental logging at database level, full (ALL COLUMNS) logging on the captured tables, so update
#     events carry complete before/after images and the primary key even when only one column changed.
#  3. A common user c##dbzuser with the LogMiner privileges Debezium documents, and the application schema SHOP.
#
# Passwords come from the environment so that tests and docker compose can inject throwaway values.
set -euo pipefail

: "${DBZ_PASSWORD:?DBZ_PASSWORD must be set}"
: "${SHOP_PASSWORD:?SHOP_PASSWORD must be set}"

FRA=/opt/oracle/oradata/FREE/fra
mkdir -p "${FRA}"

echo "cdc-pipeline: enabling ARCHIVELOG mode"
sqlplus -s / as sysdba <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SYSTEM SET db_recovery_file_dest_size = 8G SCOPE=BOTH;
ALTER SYSTEM SET db_recovery_file_dest = '${FRA}' SCOPE=BOTH;
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER PLUGGABLE DATABASE ALL SAVE STATE;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER SYSTEM SWITCH LOGFILE;
EXIT
SQL

echo "cdc-pipeline: creating LogMiner user"
sqlplus -s / as sysdba <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/FREE/logminer_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
ALTER SESSION SET CONTAINER = FREEPDB1;
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/FREE/FREEPDB1/logminer_tbs.dbf'
  SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
ALTER SESSION SET CONTAINER = CDB\$ROOT;

CREATE USER c##dbzuser IDENTIFIED BY "${DBZ_PASSWORD}"
  DEFAULT TABLESPACE logminer_tbs QUOTA UNLIMITED ON logminer_tbs CONTAINER=ALL;
GRANT CREATE SESSION, SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$DATABASE TO c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE, SELECT ANY TABLE, SELECT ANY TRANSACTION, SELECT ANY DICTIONARY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE, EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;
-- Debezium keeps its own LOG_MINING_FLUSH table and takes a short table lock during the initial snapshot.
GRANT CREATE TABLE, LOCK ANY TABLE, CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$LOGFILE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$MYSTAT TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_\$STATNAME TO c##dbzuser CONTAINER=ALL;
EXIT
SQL

echo "cdc-pipeline: creating application schema SHOP in FREEPDB1"
sqlplus -s / as sysdba <<SQL
WHENEVER SQLERROR EXIT SQL.SQLCODE
ALTER SESSION SET CONTAINER = FREEPDB1;
CREATE USER shop IDENTIFIED BY "${SHOP_PASSWORD}" DEFAULT TABLESPACE users QUOTA UNLIMITED ON users;
GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE TO shop;
ALTER SESSION SET CURRENT_SCHEMA = shop;

CREATE TABLE customers (
  id          NUMBER(19)     PRIMARY KEY,
  full_name   VARCHAR2(200)  NOT NULL,
  email       VARCHAR2(320)  NOT NULL,
  city        VARCHAR2(100),
  version     NUMBER(10)     DEFAULT 1 NOT NULL,
  updated_at  TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);
CREATE TABLE orders (
  id           NUMBER(19)    PRIMARY KEY,
  customer_id  NUMBER(19)    NOT NULL,
  status       VARCHAR2(20)  NOT NULL,
  total_amount NUMBER(12,2)  NOT NULL,
  version      NUMBER(10)    DEFAULT 1 NOT NULL,
  updated_at   TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
);
CREATE TABLE order_lines (
  order_id    NUMBER(19)    NOT NULL,
  line_no     NUMBER(5)     NOT NULL,
  sku         VARCHAR2(40)  NOT NULL,
  quantity    NUMBER(10)    NOT NULL,
  unit_price  NUMBER(12,2)  NOT NULL,
  version     NUMBER(10)    DEFAULT 1 NOT NULL,
  updated_at  TIMESTAMP(6) WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT order_lines_pk PRIMARY KEY (order_id, line_no)
);
ALTER TABLE customers   ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE orders      ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE order_lines ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
EXIT
SQL

sqlplus -s / as sysdba <<'SQL'
SET HEADING OFF FEEDBACK OFF
SELECT 'cdc-pipeline: log_mode=' || log_mode || ' supplemental_min=' || supplemental_log_data_min FROM v$database;
EXIT
SQL
