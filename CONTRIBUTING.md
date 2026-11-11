# Contributing

Thanks for taking the time to improve cdc-pipeline.

## Prerequisites

- .NET SDK 10.0.400 or later (`global.json` rolls forward to the latest feature band)
- Docker with about 2 GB free for the PostgreSQL pipeline tests, about 3.5 GB for the Oracle ones
  (`CDC_ORACLE_TESTS=1`). The two pipelines never run at the same time.

## Workflow

1. Open an issue first for anything larger than a small fix, so we can agree on the approach.
2. Branch from `main` and keep each pull request to one concern.
3. Before pushing:

   ```bash
   dotnet build                          # must be warning-free: warnings are errors
   dotnet test                           # unit, golden, property and PostgreSQL pipeline tests (Docker running)
   CDC_ORACLE_TESTS=1 dotnet test --project tests/CdcPipeline.IntegrationTests -- --filter-class "*OraclePipelineTests"
   dotnet format --verify-no-changes
   ```

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/) are required (`feat:`, `fix:`, `docs:`, `test:`, `build:`,
`ci:`, `refactor:`, `perf:`, `chore:`; `feat!:` or a `BREAKING CHANGE:` footer for breaking changes). They keep the
history readable and make the hand-written CHANGELOG easy to assemble at release time.

## Changing the event parser

`tests/CdcPipeline.UnitTests/Fixtures/` holds real Debezium output. When the parser changes, regenerate the golden
summaries with `UPDATE_GOLDEN=1 dotnet test --project tests/CdcPipeline.UnitTests` and review the diff like code. When
Debezium is upgraded, capture fresh fixtures (see `Fixtures/README.md`) and compare.

## Changing delivery semantics

The position guard, tombstones and offset handling are what the guarantees rest on (ADR 0003). Changes need a test
that shows the effect on the read model, and `CrashRecoveryTests` (hard kills of the consumer and Debezium) must stay
green.

## Changing a contract

Add a new contract version in `ShopContracts` instead of editing a released one, extend the schema-evolution tests to
cover old and new event shapes, and follow the rollout order in `docs/schema-evolution.md`.

## Database migrations

Read-model migrations are numbered SQL files in `src/CdcPipeline.Consumer/Persistence/Migrations/` (`NNNN_name.sql`),
embedded and applied in order at start-up. An applied migration must never change (its SHA-256 is checked); add a new
file instead.

## Architecture decisions

A change to a decision recorded in `docs/adr/` needs a new ADR that supersedes the old one (MADR format).

## Benchmarks

Numbers in the README and `docs/benchmarks.md` are measured, never estimated. Rerun
`dotnet run -c Release --project benchmarks/CdcPipeline.Bench -- pg` (or `oracle`), commit the JSON from
`benchmark-results/`, and update the docs with the environment and host load.
