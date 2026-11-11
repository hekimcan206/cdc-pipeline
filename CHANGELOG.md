# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-11-12

### Added

- Debezium Server 3.7.1 capture from PostgreSQL 17 (pgoutput, least-privilege publication) and Oracle Database 23ai Free
  (LogMiner, `hybrid` strategy) into a single Redpanda node.
- Oracle init script for `gvenzl/oracle-free:23-slim-faststart`: recovery area, ARCHIVELOG, supplemental logging,
  `c##dbzuser` with the documented LogMiner privileges, and the demo schema.
- Debezium envelope parser for both connectors with `SourcePosition` ordering (snapshot rows below every streamed change).
- Versioned entity contracts (v1, v2) with aliases, `extra` for unknown fields and lossless type coercion.
- .NET 10 consumer: at-least-once batches with guarded upserts, tombstones, transaction completeness from BEGIN/END
  markers, fail-closed halt with event coordinates, health and stats endpoints, OpenTelemetry metrics.
- Meilisearch indexer fed from the read model through a dirty flag, gated on transaction completeness.
- Golden tests on captured Debezium output, FsCheck delivery-model properties, Testcontainers tests for the read model,
  the PostgreSQL pipeline (replay, hard kills of the consumer and Debezium, live schema evolution) and the Oracle pipeline.
- Benchmark harness for end-to-end lag, drain throughput and catch-up time.
- `docker compose up` demo with a self-checking scenario; CI with package audit, format check, container build,
  compose demo, a separate Oracle job, and CodeQL.
- Measured results (docs/benchmarks.md): commit-to-read-model p50 0.26-0.31 s for PostgreSQL at 71-699 changes/s and
  2.0-2.4 s for Oracle LogMiner at 37-281 changes/s; catch-up after 30 s of consumer downtime 9.6 s (PostgreSQL,
  12,476 events) and 6.7 s (Oracle, 6,301 events).

### Fixed during development

- The PostgreSQL connector stops when `ALTER COLUMN ... TYPE` overtakes it; the evolution test and the rollout guide
  now let it catch up first.
- The crash test killed the consumer so often (every 4th batch) that consumer-group rejoins, not correctness, decided
  whether it converged in time; it now kills every 10th batch of 50 and uses static group membership (new optional
  `Consumer:GroupInstanceId`), so a restarted consumer takes its partitions back at once. As hardening, the source
  runs with `wal_sender_timeout=10s` and the connector retries its replication slot for up to 60 s.

[Unreleased]: https://github.com/hekimcan206/cdc-pipeline/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/hekimcan206/cdc-pipeline/releases/tag/v1.0.0
