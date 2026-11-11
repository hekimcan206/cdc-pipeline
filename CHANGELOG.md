# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
