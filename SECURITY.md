# Security policy

## Supported versions

Only the latest release receives fixes.

## Reporting a vulnerability

Please **do not open a public issue**. Use GitHub's private vulnerability reporting instead:
**Security → Report a vulnerability** on this repository. You can expect an acknowledgement within 5 working days. If
the report is confirmed, a fix or mitigation plan follows within 30 days.

Please include the affected version or commit, steps to reproduce and the impact you observed.

## Security-relevant design

- The CDC users are least-privilege: PostgreSQL `debezium` has `REPLICATION` and `SELECT` on the captured tables only,
  and the publication is created for exactly those tables; Oracle `c##dbzuser` has the LogMiner privileges Debezium
  documents and nothing else.
- No secret is committed. `docker-compose.yml` uses clearly named development defaults that can be overridden from
  `.env` (see `.env.example`); Debezium reads passwords from environment variables.
- Every value written to the read model is a bound parameter; generated SQL identifiers come only from compiled-in
  contracts, never from event data.
- The consumer fails closed: an event it cannot map stops it without committing, instead of being skipped.
- The consumer container runs as a non-root user on a chiseled image without a shell.

See `docs/threat-model.md` for the full analysis, including what a production deployment adds (broker TLS/SASL and
ACLs, scoped Meilisearch keys, tombstone purging for personal data).
