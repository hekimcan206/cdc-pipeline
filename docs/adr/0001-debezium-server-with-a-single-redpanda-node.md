# 0001: Debezium Server with a single Redpanda node as the change log

- Status: accepted
- Date: 2026-11-11

## Context

Two source databases (PostgreSQL 17 and Oracle Database 23ai Free) must stream row changes to a .NET consumer. The
consumer needs three things from the transport: per-key ordering, the ability to re-read from a committed position
after a crash (at-least-once), and enough retention to absorb consumer downtime. The whole stack has to run next to
another project on a Docker VM with about 7.6 GB of memory, and in GitHub Actions.

## Options considered

1. **Kafka Connect + Apache Kafka (KRaft).** The reference deployment. Two JVMs (Connect worker, broker), each wanting
   roughly 512 MB-1 GB of heap to be comfortable, plus internal topics for configs, offsets and status. Connectors are
   managed over a REST API, which adds a provisioning step to the demo.
2. **Debezium Server with a non-broker sink (HTTP, Redis Streams).** Smallest footprint, but the consumer would have to
   rebuild what a log gives for free: partitioned ordering, consumer-group offsets, replay.
3. **Debezium Server with the Kafka sink, writing to one Redpanda node** (chosen). One JVM per source with a 256-384 MB
   heap; Redpanda is a single C++ process speaking the Kafka protocol, started with `--smp 1 --memory 512M`.

## Decision

- Debezium Server 3.7.1.Final (`quay.io/debezium/server`), one instance per source, configured by a mounted
  `application.properties` (`deploy/debezium/`). Offsets and the Oracle schema history live in files on a volume;
  the producer is idempotent with `acks=all`, so a retried send cannot reorder or duplicate within a partition.
- Redpanda v26.2.6 in `dev-container` mode, one node, `default_topic_partitions: 3` set through `.bootstrap.yaml`
  so Debezium's lazily created topics get three partitions without a provisioning step.
- Debezium keys every record by the table's primary key (JSON, `schemas.enable=false`); the Kafka producer's default
  partitioner hashes that key, so all changes of one row land in one partition, in source order.
- Transaction metadata is on (`provide.transaction.metadata=true`): BEGIN/END markers on `<prefix>.transaction`, and
  every data event carries `transaction.id` and `total_order` (see ADR 0003).

## Consequences

- Measured on this machine (docs/benchmarks.md): Redpanda and Debezium Server stay well inside their 768 MB container
  limits under the benchmark load; the PostgreSQL stack starts in about 25 s.
- A single broker node has no replication: losing its volume loses the log. Acceptable for a demo and CI; production
  would run three nodes, `replication.factor=3` and `min.insync.replicas=2`. The consumer and the idempotent
  projection do not change.
- Debezium Server has no REST API for connector management; changing a connector means changing the file and
  restarting. That is the intended operating model here (config in Git).
- File offsets mean the Debezium container must keep its volume. If the offsets are lost, Debezium re-snapshots; the
  read model then has to be rebuilt (ADR 0003 explains why snapshot rows never overwrite streamed rows).

## Sources

- Debezium Server reference (sinks, offset storage, Kafka sink properties):
  https://debezium.io/documentation/reference/stable/operations/debezium-server.html
- Debezium Server images moved to Quay: https://hub.docker.com/r/debezium/server
- Redpanda `dev-container` mode and cluster bootstrap file: https://docs.redpanda.com/current/reference/rpk/rpk-redpanda/rpk-redpanda-start/
