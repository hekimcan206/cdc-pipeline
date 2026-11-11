# Captured Debezium events

Real output of Debezium Server 3.7.1.Final (JSON converter, `schemas.enable=false`, `decimal.handling.mode=string`,
`provide.transaction.metadata=true`), read back from Redpanda with `rpk topic consume` and stored one record per line
as `{"topic", "partition", "key", "value"}`.

- `postgres/`: PostgreSQL 17, pgoutput. Snapshot of one customer, then a multi-row transaction (order + two lines),
  an update, a delete, `ADD COLUMN loyalty_tier`, `RENAME COLUMN email TO email_address` and
  `ALTER COLUMN quantity TYPE numeric(12,3)`, each followed by an update.
- `oracle/`: Oracle Database 23ai Free, LogMiner (`hybrid` mining strategy). The same steps, except the type change:
  Oracle refuses to change the scale of a non-empty `NUMBER` column in place (ORA-01440), see
  `docs/schema-evolution.md`.

All values are synthetic. `golden/` holds the canonical parse of every file; regenerate with
`UPDATE_GOLDEN=1 dotnet test` and review the diff.
