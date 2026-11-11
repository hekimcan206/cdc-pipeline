# 0004: Meilisearch as the search index, fed from the read model rather than from the stream

- Status: accepted
- Date: 2026-11-11

## Context

The brief asks for a search index next to the PostgreSQL read model, either OpenSearch or Meilisearch, whichever is
lighter. The index must not lose changes when the consumer crashes, must not show stale data after a redelivery, and
should not show half of a source transaction.

## Options considered

**Engine**

1. **OpenSearch 3.x.** Full Lucene feature set (aggregations, complex queries, analyzers), but a JVM: the project's own
   guidance is to give the heap half the container memory, and a single node is normally sized in gigabytes. On a
   shared 7.6 GB VM that already holds a broker, Debezium and, in the Oracle stack, a 2.3 GB database, that is the
   budget of the whole pipeline.
2. **Meilisearch v1.56** (chosen). A single Rust binary, memory-mapped storage, typo-tolerant full-text search with
   filters out of the box, a small REST API and asynchronous tasks with explicit status.

**Feeding**

1. **Index straight from the event stream** in the consumer loop. A crash between the read-model commit and the index
   call loses the index update (the offset is committed, nothing replays it); writing the index first and the read
   model second just moves the problem.
2. **Index from the read model with a dirty flag** (chosen), an outbox in all but name.

## Decision

- Meilisearch `getmeili/meilisearch:v1.56.2`, one index per entity (`customers`, `orders`, `order_lines`, with an
  optional prefix), primary key `doc_id = <source>-<key...>`, `source` filterable.
- Every applied change sets `index_dirty = true` in the same transaction that changes the row. `SearchIndexer`
  selects dirty rows, pushes their **current** state (`addOrReplace`, or delete-batch for tombstones), waits for the
  Meilisearch task to succeed, and clears the flag only where the row still has the position it pushed. A crash after
  the push but before the flag update pushes the same document again: idempotent. A redelivered change never reaches the
  index because it never moved the row.
- Rows whose source transaction is not yet complete (ADR 0003) are held back, so a search never returns an order
  without its lines. After 5 s without the END marker they are indexed anyway and counted (`cdc.index.forced`).
- Fields the contract does not know (an added column, kept in `extra`) are merged into the document, so they are
  searchable before the contract catches up.
- The indexer is woken right after each batch commits, so index lag is not bounded below by its 250 ms idle poll.

## Consequences

- Measured here (docs/benchmarks.md): Meilisearch's resident memory after the benchmark runs. OpenSearch was **not**
  measured on this machine (disk and memory budget); the comparison above rests on its documented sizing guidance and
  is stated as such.
- The index is a projection of a projection: its lag is read-model lag plus one indexing round trip, reported
  separately in the benchmarks.
- Meilisearch is not a system of record: losing it means re-marking every row dirty (`UPDATE ... SET index_dirty = true`)
  and letting the indexer rebuild it.
- Meilisearch lacks OpenSearch's aggregation and analytics features; the read model (SQL) is where those queries go.

## Sources

- Meilisearch: comparison with OpenSearch (vendor page, read with that in mind):
  https://www.meilisearch.com/docs/resources/comparisons/opensearch
- Meilisearch asynchronous tasks: https://www.meilisearch.com/docs/learn/async/asynchronous_operations
- OpenSearch important settings (JVM heap sizing): https://docs.opensearch.org/latest/install-and-configure/install-opensearch/index/
- Open edX discussion of replacing OpenSearch with Meilisearch for memory reasons:
  https://discuss.openedx.org/t/is-meilisearch-a-viable-upgrade-alternative-to-opensearch/12400
