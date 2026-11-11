# Benchmarks

Every number here was measured on the machine described below with the harness in `benchmarks/CdcPipeline.Bench`.
Raw results: `benchmark-results/pg.json` and `benchmark-results/ora.json`.

## Environment

| | |
|---|---|
| Host | Intel Core i7-13620H (16 logical cores), 15.7 GB RAM, Windows 11 (10.0.26200) |
| Docker | Docker Desktop 29.9.1, WSL2 VM with 7.62 GB, **shared with another project's containers** (Prometheus, Grafana, PostgreSQL, Toxiproxy, an API; about 1 GB in use, 5-20 % CPU in the snapshot taken before the runs) |
| .NET | 10.0.12, Release build; the consumer runs in-process in the harness (same code path as `Program`) |
| Pipeline | PostgreSQL 17-alpine / Oracle 23ai Free (`gvenzl/oracle-free:23-slim-faststart`), Debezium Server 3.7.1.Final (heap 256 MB PostgreSQL, 384 MB Oracle, `poll.interval.ms=100`), Redpanda v26.2.6 (1 node, `--smp 1 --memory 512M`), read-model PostgreSQL 17, Meilisearch v1.56.2; consumer batch size 500 |
| Date | 2026-11-11/12 |

## Method

**Workload.** `ShopWorkload`, open loop (operations are scheduled at the offered rate whether or not the pipeline
keeps up, up to 8 in flight): 40 % new customer, 35 % customer update, 20 % order with 1-3 lines in one transaction,
5 % order-line delete. One operation is 1.4 row changes on average. Each load level runs for 20 s on fresh ids after a
100-operation warm-up.

**Lag.** Per row, from the row's own `updated_at` (stamped with the source database clock at the statement, i.e.
immediately before the autocommit or the order transaction's commit) to `applied_at` (read-model clock when the change
landed) and `indexed_at` (read-model clock when the Meilisearch task for it had succeeded). All containers run on the
same Docker VM kernel, so the clocks are the same clock. Oracle's `source.ts_ms` has one-second resolution and is not
used. For updated rows only the final change is measured (it overwrites the timestamps). Percentiles over all rows
of the run (deleted rows excluded).

**Catch-up.** A consumer group is brought up to date and stopped; the workload then runs for 30 s with no consumer;
once the broker's high watermarks stop moving the consumer starts. Catch-up time is from the consumer's start (read-model
clock, includes process start and joining the group) to the last applied row of the backlog (read model) and to the last
indexed row (index). Drain rate is backlog events divided by the read-model catch-up time.

**Memory.** `docker stats` per container after the run.

Reproduce: `dotnet run -c Release --project benchmarks/CdcPipeline.Bench -- pg` (or `oracle`), Docker running.

## Results: commit to read model and to search index

### PostgreSQL (pgoutput)

| Offered | Achieved | Rows | Read model p50 / p95 / p99 / max | Index p50 / p95 / p99 / max |
|---|---|---|---|---|
| 50 ops/s | 71 changes/s | 973 | 255 / 455 / 557 / 591 ms | 356 / 573 / 679 / 713 ms |
| 200 ops/s | 281 changes/s | 3,857 | 311 / 543 / 1,124 / 1,450 ms | 513 / 1,876 / 2,589 / 2,803 ms |
| 500 ops/s | 699 changes/s | 9,472 | 277 / 409 / 517 / 651 ms | 733 / 1,100 / 1,281 / 1,522 ms |

An earlier run at 50/100/200 ops/s gave read-model p50 253 / 299 / 291 ms and index p50 460 / 433 / 489 ms; the 200
ops/s tail above (p99 1.1 s) did not show up there (p99 474 ms), so treat single-run tails on this shared host as noisy.

### Oracle 23ai Free (LogMiner)

| Offered | Achieved | Rows | Read model p50 / p95 / p99 / max | Index p50 / p95 / p99 / max |
|---|---|---|---|---|
| 25 ops/s | 37 changes/s | 501 | 2,004 / 3,705 / 3,864 / 4,012 ms | 2,231 / 3,884 / 4,004 / 4,222 ms |
| 100 ops/s | 140 changes/s | 1,904 | 2,221 / 3,584 / 3,941 / 4,138 ms | 2,595 / 3,864 / 4,247 / 4,476 ms |
| 200 ops/s | 281 changes/s | 3,858 | 2,421 / 3,651 / 3,999 / 4,158 ms | 2,835 / 4,026 / 4,359 / 4,615 ms |

## Results: catch-up after consumer downtime

| Source | Downtime load | Backlog | Read model caught up | Index caught up | Drain rate |
|---|---|---|---|---|---|
| PostgreSQL | 300 ops/s × 30 s | 12,476 events | 9.56 s | 10.46 s | 1,305 events/s |
| Oracle | 150 ops/s × 30 s | 6,301 events | 6.74 s | 7.71 s | 934 events/s |

The backlog equals the source row changes (12,479 / 6,304 written; the few-event difference is updates that hit a
customer whose insert was still in flight and changed no row).

## Results: memory after the run (docker stats)

| Container | PostgreSQL stack | Oracle stack |
|---|---|---|
| Source database | 61 MiB | 3,005 MiB |
| Redpanda | 126 MiB | 126 MiB |
| Debezium Server | 386 MiB | 463 MiB |
| Read-model PostgreSQL | 91 MiB | 75 MiB |
| Meilisearch | 94 MiB | 83 MiB |
| Stack ready (containers started, Debezium streaming) | 18.6 s | 84.6 s |

## Reading the numbers

- **pgoutput lag is flat at ~0.3 s p50 from 70 to 700 changes/s.** The pipeline is far from saturated in that range;
  the floor is Debezium's poll interval (100 ms) plus the producer linger (5 ms), the broker hop and one consumer batch.
- **LogMiner lag is flat at ~2-2.4 s p50.** LogMiner mines redo in cycles; the floor comes from the mining loop, not
  from load. The mining sleep/batch settings (`log.mining.sleep.time.*`, `log.mining.batch.size.*`) were not tuned,
  and the Free image's 10 MB redo groups switch often under load.
- **The index adds 0.1-0.5 s p50** on top of the read model: one Meilisearch task per indexer round, waited for before
  the row is marked clean, and held back until the source transaction is complete.
- **Drain rate (~1,300 events/s PostgreSQL, ~930 Oracle) is the consumer's ceiling here**, with one guarded upsert per
  change sent as one round trip per batch, and the indexer and the harness's convergence check sharing the read-model
  database. Why it is lower for the Oracle backlog was not investigated (the fixed group-join time weighs more in a
  smaller backlog). A set-based upsert (deduplicate per key inside the batch, then one
  `INSERT ... SELECT FROM unnest(...)`) is the obvious next step and is listed in the README.
- **Stability caveat.** Docker Desktop on this machine stopped three times during the session (daemon unreachable, VM
  restarted). A first PostgreSQL attempt, run while the VM was struggling, showed consumer-group session timeouts and
  200 ops/s lag of 8 s p50; it was discarded and the runs above were taken after a restart. Between that attempt and
  the runs above, Debezium's `poll.interval.ms` was also lowered from 500 to 100 ms, so the two are not comparable.
- **Not measured:** OpenSearch (see ADR 0004), multi-consumer scaling, lag beyond 700 changes/s on PostgreSQL.
