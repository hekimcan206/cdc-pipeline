# Threat model

Scope: the capture pipeline as shipped in this repository (source databases, Debezium Server, Redpanda, the .NET
consumer, the read-model PostgreSQL and Meilisearch) and its configuration. CDC copies data out of its system of record,
so the main question is who can read or change that copy, and whether the copy can silently diverge from the source.

## Assets

| Asset | Why it matters |
|---|---|
| Row data in topics, read model and index | A full copy of customer and order data, including personal data (names, e-mail addresses) |
| Debezium credentials (`c##dbzuser`, `debezium`) | Can read every captured table and, on Oracle, all redo (LogMiner sees every schema) |
| Read-model and Meilisearch credentials | Write access to what users search and read |
| Debezium offsets, schema history, consumer offsets | Lose or corrupt them and the copy silently skips or replays changes |
| Correctness of the copy | Downstream decisions are made on the read model |

## Actors

- **Operator / developer** with Docker access (trusted, but mistakes happen: wrong contract version, lost volume).
- **Application user** of the source database (writes data that ends up in the copy; may write hostile strings).
- **Network neighbour** on the Docker network or host (untrusted).
- **Compromised consumer or index host** (assume it can happen; limit what it can reach).

## Threats and mitigations (STRIDE)

| # | Threat | Category | Mitigation in this repo | Residual / production note |
|---|---|---|---|---|
| 1 | The CDC user is over-privileged and becomes a data-exfiltration path | Information disclosure, Elevation | PostgreSQL: `debezium` has `REPLICATION` + `SELECT` on the three captured tables only; the publication is created by the init script for exactly those tables (`publication.autocreate.mode=disabled`). Oracle: the privilege list Debezium documents, nothing more | LogMiner inherently needs `SELECT ANY TRANSACTION` / `LOGMINING`, i.e. it can read redo of every schema. Isolate the CDB, audit `c##dbzuser` logins |
| 2 | Topics expose personal data to anyone who can reach the broker | Information disclosure | Broker only on the compose network and `localhost`; synthetic data only | Production: TLS + SASL/SCRAM on Redpanda, ACLs per topic (Debezium produce, consumer read), encryption at rest; consider column filtering (`column.exclude.list`) or masking SMTs for fields the read model does not need |
| 3 | Credentials leak through the repository | Information disclosure | No secrets committed; compose uses `${VAR:-dev-only}` defaults named as such, `.env` is git-ignored, `.env.example` documents the variables; Debezium reads passwords from environment variables, not from the properties file | Production: secrets manager, rotate the CDC user's password with a connector restart |
| 4 | A hostile string in source data injects SQL into the read model or breaks the index | Tampering | Every value is a bound parameter (`NpgsqlBatch`); identifiers in generated SQL come only from compiled-in contracts, never from event data; Meilisearch receives JSON built with `System.Text.Json` | None known |
| 5 | Redelivery or replay applies an old value over a newer one | Tampering (integrity) | Position guard per row, tombstones for deletes (ADR 0003); proven by property tests and hard-kill integration tests | A re-snapshot after lost Debezium offsets needs a read-model rebuild (documented) |
| 6 | An event the consumer cannot map is skipped and the copy silently diverges | Tampering (integrity), Repudiation | Fail closed: halt without committing, `/health/ready` 503, `cdc.halts` metric, log line with `topic[partition]@offset` and the field (ADR 0005) | Alert on the readiness probe and the halt counter |
| 7 | Forged events written straight to a topic by a network neighbour | Spoofing, Tampering | Broker reachable only on the private network / localhost in this setup | Production: ACLs so only Debezium's principal can produce to CDC topics; the consumer cannot verify event origin by itself |
| 8 | Lost Debezium volume (offsets, Oracle schema history) | Denial of service, integrity | Named volumes in compose; documented recovery (re-snapshot + read-model rebuild) | Back up the volume or move offsets to Redis/JDBC storage |
| 9 | Unbounded growth: archived redo (Oracle), WAL retained by an inactive replication slot (PostgreSQL), broker retention | Denial of service | `db_recovery_file_dest_size` caps archive space at 8 GB; the slot is owned by a running Debezium | Production: monitor `pg_replication_slots` lag and FRA usage; RMAN deletion policy for applied archive logs; `max_slot_wal_keep_size` |
| 10 | Search index admin key used by the consumer leaks | Elevation | Development master key only; never exposed beyond the compose network and localhost | Production: a scoped Meilisearch API key limited to document writes on the three indexes; a separate search-only key for readers |
| 11 | Fault-injection hook used to kill the consumer in production | Denial of service | Disabled unless `Consumer:FaultInjection:Enabled=true`; only tests and benchmarks set it | Do not set it outside tests |
| 12 | Personal data retained in the copy after it is deleted at the source | Information disclosure (KVKK/GDPR) | Deletes become tombstones that keep the last known values (needed for integrity) | Add a scheduled purge that nulls personal columns of tombstones older than the replay window; topic retention bounds the broker copy |

## Data protection note

All data in this repository is synthetic. A CDC copy of real personal data falls under the same obligations as the
source (for Turkey, KVKK, the Personal Data Protection Law): purpose limitation (project only the columns the read model
needs), retention (topic retention, tombstone purge, row 12 above), and access control on every copy (rows 2 and 10).
