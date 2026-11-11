# 0005: Versioned entity contracts; unknown fields are kept, unmappable events stop the consumer

- Status: accepted
- Date: 2026-11-11

## Context

Source schemas change while the pipeline runs: columns are added, renamed, retyped. Debezium follows (pgoutput sends a
new relation message; LogMiner's `hybrid` strategy re-reads the dictionary), so events change shape mid-stream, and old
and new shapes coexist in the topic for as long as the retention lasts. The consumer must keep projecting through every
step without silently losing or corrupting data.

## Options considered

1. **Schema registry with Avro/Protobuf and compatibility rules.** Catches incompatible changes at the producer, but a
   rename is a breaking change for Avro (it needs aliases in the reader schema anyway), adds another service to a
   memory-constrained stack, and Debezium Server's JSON output is enough to project by name.
2. **Schema-less pass-through** (store the whole `after` image as JSON). Never fails, and never notices that a
   required field disappeared.
3. **A versioned contract per entity in the consumer** (chosen): typed fields, aliases for renamed fields, unknown
   fields kept aside, lossless coercion, and a hard stop when an event cannot be mapped.

## Decision

`ShopContracts` (`src/CdcPipeline.Core/Contracts/`) defines v1 (initial schema) and v2 (current). Mapping rules:

- **Unknown field** (a new column the contract does not know): kept in the row's `extra jsonb` and merged into the
  search document. Never an error.
- **Alias**: a contract field lists the source names it accepts (`email` ← `email`, `email_address`). Both stay
  accepted as long as old events may be replayed. Both present in one event is an error (ambiguous).
- **Lossless coercion**: `1`, `"1"` and `"1.000"` are all an integer 1; `"1.500"` is never an integer. Text accepts
  numbers verbatim (a widening). Timestamps must be ISO-8601 strings (epoch numbers are ambiguous without the schema).
- **Fail closed**: a missing required field, a lossy conversion or an ambiguous alias throws. The consumer rolls the
  batch back, does **not** commit offsets, marks itself halted (`/health/ready` 503 with the reason and the broker
  coordinates of the event) and stops consuming. Deploying a contract that handles the event and restarting resumes
  from exactly that event; at-least-once plus the position guard make the retry safe.
- Deletes map only the key: PostgreSQL's default replica identity fills the other columns of a delete's before-image
  with type defaults (`""`, `0`, 1970-01-01), which must not overwrite the last known values.

## Consequences

- Skipping an unmappable event is never an option the code offers: it would break per-key ordering for every later
  change of that row. Halting is loud by design; the halt counter (`cdc.halts`) and readiness probe are the alert.
- A consumer must be upgraded **before** a breaking source change (rename, narrowing type change) reaches it, or it
  will halt until it is. The runbook in docs/schema-evolution.md orders the steps.
- Read-model columns are created for the newest contract (v2) up front; promoting an `extra` field to a typed column in
  a later version needs a migration plus `UPDATE ... SET col = extra->>'col'` for rows written before the promotion.
- Tested on real connector output: the unit tests replay captured PostgreSQL and Oracle events through v1 and v2, and
  `SchemaEvolutionTests` performs the DDL live, shows v1 halting with the event's coordinates, and v2 resuming from the
  same offsets.
