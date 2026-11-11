# 0003: At-least-once delivery, made harmless by a source-position guard per row

- Status: accepted
- Date: 2026-11-11

## Context

Every hop can deliver a change twice: Debezium republishes everything after its last flushed offset when it restarts,
and the consumer re-reads everything after its last committed broker offset when it crashes. Exactly-once delivery
across Debezium, the broker, PostgreSQL and Meilisearch would need distributed transactions none of them offer
together. The read model must still end up equal to the source, with every change applied once.

## Options considered

1. **Deduplication ledger** of every processed event id (topic/partition/offset, or a hash), checked per event.
   Broker offsets change when Debezium republishes, so offsets are not a stable identity; a ledger keyed by source
   position works but grows without bound and costs a write per event.
2. **Kafka transactions / exactly-once semantics.** Only cover Kafka-to-Kafka; the side effect here is a PostgreSQL
   write, outside the Kafka transaction.
3. **Guarded upsert keyed by primary key, ordered by source position** (chosen). Each read-model row stores the
   position of the last change applied to it; a change is applied only if its position is strictly greater.

## Decision

**Position.** `SourcePosition(Major, Minor)`, derived from the Debezium source block (`src/CdcPipeline.Core/Events/SourcePosition.cs`):

| Source | Major | Minor |
|---|---|---|
| PostgreSQL, streamed | `lastCommitLsn` from `source.sequence` (1 for the first streamed transaction) | change `lsn` |
| Oracle, streamed | `commit_scn` | `transaction.total_order` (fallback: change `scn`) |
| Snapshot read (`op=r`) | 0 | snapshot LSN / SCN |

The order only has to be correct **per primary key**, and it is, because both databases serialise writers of the same
row: a second transaction can change a row only after the first committed, so its commit position is higher. Within a
transaction the change LSN (PostgreSQL) or `total_order` (Oracle) increases. Snapshot reads get `Major = 0` because a
transaction that started before the snapshot point but committed after it can carry a change LSN lower than the
snapshot LSN, yet it is newer than the snapshot row. The raw change SCN is not used for Oracle: several changes of one
transaction can share an SCN.

**Guard.** One statement per change (`ProjectionSql`):

```sql
INSERT INTO rm_customer AS t (...) VALUES (...)
ON CONFLICT (source, id) DO UPDATE SET ..., apply_count = t.apply_count + 1
WHERE (t.pos_major, t.pos_minor) < (excluded.pos_major, excluded.pos_minor)
RETURNING 1
```

Deletes are tombstones (`deleted = true`, last known values kept) with the same guard, so a late redelivery of an
older insert cannot resurrect a deleted row. A batch of up to 500 changes is sent as one `NpgsqlBatch` (one round trip)
inside one transaction, in partition order; then the broker offsets are committed. A crash between the two replays the
batch, and every replayed change hits the guard.

**Ordering.** Debezium keys records by primary key, the producer hashes the key to a partition, and the consumer
applies each batch in partition order, so changes to one row are applied in source order. Rows in different
partitions have no relative order, which is fine for per-row state.

**Transaction boundaries.** Data events carry `transaction.id` and `total_order`; END markers carry the event count.
`rm_tx_event` (a small ledger per transaction, pruned once the transaction completes) counts each event once, END sets
the expected count, and a transaction is complete when the two agree, whichever arrives first. Markers and data events
are on different topics and partitions, so arrival order is arbitrary. The PostgreSQL connector names a transaction
`xid:lsn` with a different LSN on BEGIN and END, so markers are matched on the xid. Completeness drives the search
index (ADR 0004), not the read model: the read model applies per row immediately and stays per-row consistent.

## Consequences

- Proven by tests, not just argued: a model-based FsCheck property checks random crash schedules converge to the
  exactly-once state with every change applied once; the integration suite replays an entire topic into a populated
  read model (0 re-applied) and kills the consumer process repeatedly between the database commit and the offset
  commit while Debezium is also killed mid-run (`CrashRecoveryTests`), then checks `apply_count == version` for every row.
- Requires a primary key on every captured table (a keyless change is rejected as malformed).
- If Debezium loses its offsets and re-snapshots, snapshot rows (`Major = 0`) never overwrite streamed rows. Changes made
  while it was down are in the snapshot, not in the stream, so the right procedure is to rebuild the read model from
  the new snapshot; this is documented, not automated.
- A transaction whose END marker never arrives would hold its rows out of the index forever; a configurable wait
  (5 s) indexes them anyway and counts them in `cdc.index.forced`.

## Sources

- Debezium PostgreSQL connector, `source.sequence` and transaction metadata:
  https://debezium.io/documentation/reference/stable/connectors/postgresql.html
- Debezium Oracle connector source fields (`scn`, `commit_scn`): https://debezium.io/documentation/reference/stable/connectors/oracle.html
