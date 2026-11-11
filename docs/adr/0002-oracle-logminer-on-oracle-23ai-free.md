# 0002: Oracle LogMiner on Oracle Database 23ai Free (gvenzl image)

- Status: accepted
- Date: 2026-11-11

## Context

Debezium's Oracle connector reads redo through LogMiner (default adapter) or XStream (needs a GoldenGate licence).
The container must start with zero manual steps, in tests and in `docker compose`, on the image the other portfolio
repositories already use: `gvenzl/oracle-free:23-slim-faststart` (a pre-built database, about 1-2 minutes to ready).

LogMiner needs: ARCHIVELOG mode, minimal supplemental logging at database level, full (ALL COLUMNS) supplemental
logging on captured tables, and a common user with the LogMiner privileges, created in the CDB because Free is a
multitenant database (CDB `FREE`, PDB `FREEPDB1`).

## Options considered

1. **A custom image** with the database pre-switched to ARCHIVELOG. Fast start, but another 6 GB image to build and
   store on a disk that is already 96 % full, and a fork of a maintained image.
2. **Manual setup documented in the README.** Not reproducible, not testable in CI.
3. **An init script in `/container-entrypoint-initdb.d`** (chosen). The gvenzl entrypoint runs it once, as the
   `oracle` OS user with local SYSDBA, before it reports `DATABASE IS READY TO USE!` — so Testcontainers' default wait
   strategy and the compose health check both wait for the LogMiner setup to finish.

## Decision

`deploy/oracle/init/01-logminer.sh`, derived from Debezium's own `setup-logminer.sh` for its Vagrant box and adapted to
the Free image's names:

1. Set `db_recovery_file_dest` (8 GB, inside the data volume), then `SHUTDOWN IMMEDIATE`, `STARTUP MOUNT`,
   `ALTER DATABASE ARCHIVELOG`, `ALTER DATABASE OPEN`, and re-open the PDB with `SAVE STATE`.
2. `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA`; per table `ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`.
3. `LOGMINER_TBS` in the CDB and in `FREEPDB1`, then `c##dbzuser ... CONTAINER=ALL` with the privileges from the
   Debezium documentation (LOGMINING, SELECT ANY TRANSACTION, the V$ views, EXECUTE on DBMS_LOGMNR, ...).
4. The application schema `SHOP` in `FREEPDB1`.

Connector settings (`deploy/debezium/oracle.properties`): `database.dbname=FREE`, `database.pdb.name=FREEPDB1`,
`log.mining.strategy=hybrid`, `log.mining.query.filter.mode=in`, `decimal.handling.mode=string`, file schema history.
The Oracle JDBC driver (`ojdbc11-23.26`) already ships in the Debezium Server 3.7.1 image; no derived image is needed.

`hybrid` instead of `online_catalog`: the online catalog is the fastest strategy, but it cannot reconstruct a change
made with a dictionary that has since changed. `hybrid` uses the online catalog until it meets a DDL, then falls back
for that window, so the schema-evolution scenario (ADD, RENAME, DROP COLUMN mid-stream) is captured correctly. This
was verified on the captured events in `tests/CdcPipeline.UnitTests/Fixtures/oracle/`.

## Consequences

- Measured: the init script adds about 40 s to the first start (instance bounce plus user and schema creation); the
  Oracle container uses about 2.3 GB at rest. With Debezium, Redpanda, the read model and Meilisearch, the Oracle
  stack needs about 3.5 GB, so it never runs at the same time as the PostgreSQL stack in tests or benchmarks.
- The slim image keeps LogMiner (the slim variant removes Text, Spatial, XDB-adjacent options and the installer, not
  LogMiner). Debezium logs online redo groups of 10 MB on this image; that means frequent log switches under load,
  which LogMiner handles but which is the first thing to enlarge outside a demo.
- Oracle reports `source.ts_ms` with one-second resolution (LogMiner's TIMESTAMP is a DATE). Lag is therefore measured
  from the row's own `updated_at TIMESTAMP(6)` (docs/benchmarks.md), never from `ts_ms`.
- Oracle refuses an in-place scale change on a non-empty NUMBER column (ORA-01440); the type change is done as an
  expand/contract migration (docs/schema-evolution.md).
- Known Debezium issue on this exact image: ROLLBACK TO SAVEPOINT on an index-organised table is not applied
  (debezium/dbz#2785). The shop tables are heap tables and the workload uses no savepoints; it is listed under
  limitations.

## Sources

- Debezium Oracle connector (prerequisites, privileges, mining strategies):
  https://debezium.io/documentation/reference/stable/connectors/oracle.html
- Debezium's LogMiner setup script for its Oracle Vagrant box:
  https://github.com/debezium/oracle-vagrant-box/blob/main/setup-logminer.sh
- gvenzl image details (slim variant contents, init scripts): https://github.com/gvenzl/oci-oracle-xe/blob/main/ImageDetails.md
- Community walkthrough using `gvenzl/oracle-free:23.6-slim-faststart` with Debezium:
  https://oracle4us.wordpress.com/2026/04/15/cdc-with-debezium-and-oracle-26ai-free-on-docker/
- LogMiner rollback-to-savepoint issue reproduced on `gvenzl/oracle-free:23-slim`: https://github.com/debezium/dbz/issues/2785
