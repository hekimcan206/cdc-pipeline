using CdcPipeline.Core.Contracts;
using CdcPipeline.Core.Events;
using Npgsql;
using NpgsqlTypes;

namespace CdcPipeline.Consumer.Persistence;

/// <summary>Record <see cref="RecordIndex"/> of the batch does not fit the contract; nothing of the batch was written.</summary>
internal sealed class RecordRejectedException(int recordIndex, ContractViolationException violation)
    : Exception(violation.Message, violation)
{
    public int RecordIndex { get; } = recordIndex;

    public ContractViolationException Violation { get; } = violation;
}

/// <summary>Counts for one applied batch.</summary>
/// <param name="Applied">Changes that moved a row forward.</param>
/// <param name="Skipped">Changes whose position was not newer than the row's: redeliveries and replays.</param>
/// <param name="Unmapped">Changes for tables that have no contract.</param>
/// <param name="AppliedLags">Source commit timestamp to now, per applied change.</param>
internal sealed record BatchOutcome(int Applied, int Skipped, int Unmapped, int Boundaries, IReadOnlyList<TimeSpan> AppliedLags);

/// <summary>
/// Applies a batch of parsed records to the read model in one database transaction. Each change is its own guarded
/// upsert (applied only if its source position is newer than the row's), sent together as one
/// <see cref="NpgsqlBatch"/> round trip, so per-key order inside the batch is preserved exactly.
/// </summary>
internal sealed class ReadModelWriter
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly ContractSet _contracts;
    private readonly Dictionary<string, ProjectionSql> _sql;
    private readonly TimeProvider _time;

    public ReadModelWriter(NpgsqlDataSource dataSource, ContractSet contracts, TimeProvider time)
    {
        _dataSource = dataSource;
        _contracts = contracts;
        _time = time;
        _sql = contracts.Entities.ToDictionary(e => e.SourceTable, e => new ProjectionSql(e), StringComparer.Ordinal);
    }

    public IReadOnlyCollection<ProjectionSql> Projections => _sql.Values;

    public string ContractVersion => _contracts.Version;

    /// <summary>
    /// Maps every record first (a <see cref="RecordRejectedException"/> aborts the whole batch before anything is
    /// written), then writes. Throws on database errors; the caller retries the same batch.
    /// </summary>
    public async Task<BatchOutcome> ApplyAsync(IReadOnlyList<ParsedRecord> records, CancellationToken ct)
    {
        var rows = new List<(ProjectedRow Row, ChangeEvent Change)>(records.Count);
        var boundaries = new List<TransactionBoundary>();
        var unmapped = 0;
        for (var index = 0; index < records.Count; index++)
        {
            switch (records[index])
            {
                case ParsedRecord.Change { Event: var change }:
                    if (_contracts.ForTable(change.Table) is { } contract)
                    {
                        try
                        {
                            rows.Add((RowMapper.Map(contract, change), change));
                        }
                        catch (ContractViolationException ex)
                        {
                            throw new RecordRejectedException(index, ex);
                        }
                    }
                    else
                    {
                        unmapped++;
                    }

                    break;
                case ParsedRecord.Boundary { Transaction: { Status: TransactionStatus.End } end }:
                    boundaries.Add(end);
                    break;
            }
        }

        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await connection.BeginTransactionAsync(ct);

        var appliedFlags = await UpsertRowsAsync(connection, tx, rows, ct);
        var applied = appliedFlags.Count(a => a);
        await TrackTransactionsAsync(connection, tx, rows, boundaries, applied, rows.Count - applied, ct);
        await tx.CommitAsync(ct);

        var now = _time.GetUtcNow();
        var lags = new List<TimeSpan>(applied);
        for (var i = 0; i < rows.Count; i++)
        {
            if (appliedFlags[i])
            {
                lags.Add(now - rows[i].Change.SourceTimestamp);
            }
        }

        return new BatchOutcome(applied, rows.Count - applied, unmapped, boundaries.Count, lags);
    }

    private async Task<bool[]> UpsertRowsAsync(NpgsqlConnection connection, NpgsqlTransaction tx,
        List<(ProjectedRow Row, ChangeEvent Change)> rows, CancellationToken ct)
    {
        var applied = new bool[rows.Count];
        if (rows.Count == 0)
        {
            return applied;
        }

        await using var batch = new NpgsqlBatch(connection, tx);
        foreach (var (row, change) in rows)
        {
            batch.BatchCommands.Add(BuildCommand(_sql[row.Contract.SourceTable], row, change));
        }

        await using var reader = await batch.ExecuteReaderAsync(ct);
        var index = 0;
        do
        {
            applied[index++] = await reader.ReadAsync(ct);
        }
        while (await reader.NextResultAsync(ct));

        return applied;
    }

    private static NpgsqlBatchCommand BuildCommand(ProjectionSql sql, ProjectedRow row, ChangeEvent change)
    {
        var command = new NpgsqlBatchCommand(row.IsDelete ? sql.Delete : sql.Upsert);
        var p = command.Parameters;
        p.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = change.Source });
        for (var i = 0; i < row.Contract.Key.Count; i++)
        {
            p.Add(new NpgsqlParameter { NpgsqlDbType = ProjectionSql.DbType(row.Contract.Key[i].Type), Value = row.KeyValues[i] });
        }

        if (!row.IsDelete)
        {
            foreach (var field in row.Contract.Fields)
            {
                var value = row.Values[field.Name];
                if (value is DateTimeOffset ts)
                {
                    value = ts.ToUniversalTime();
                }

                p.Add(new NpgsqlParameter { NpgsqlDbType = ProjectionSql.DbType(field.Type), Value = value ?? DBNull.Value });
            }

            p.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = row.Extra.ToJsonString() });
        }

        p.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Numeric, Value = change.Position.Major });
        p.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Numeric, Value = change.Position.Minor });
        p.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)change.TransactionId ?? DBNull.Value });
        return command;
    }

    /// <summary>
    /// Ledger of (source, tx, total_order) so that a redelivered event is counted once, END markers for the expected
    /// count, completion whenever both sides agree, and the durable counters. One round trip.
    /// </summary>
    private static async Task TrackTransactionsAsync(NpgsqlConnection connection, NpgsqlTransaction tx,
        List<(ProjectedRow Row, ChangeEvent Change)> rows, List<TransactionBoundary> ends, int applied, int skipped, CancellationToken ct)
    {
        await using var batch = new NpgsqlBatch(connection, tx);

        var txEvents = rows.Where(r => r.Change is { TransactionId: not null, TransactionOrder: not null }).Select(r => r.Change).ToList();
        if (txEvents.Count > 0)
        {
            var ledger = new NpgsqlBatchCommand("""
                WITH ins AS (
                    INSERT INTO rm_tx_event (source, tx_id, total_order)
                    SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[])
                    ON CONFLICT DO NOTHING
                    RETURNING source, tx_id)
                INSERT INTO rm_tx AS t (source, tx_id, applied_events)
                SELECT source, tx_id, count(*) FROM ins GROUP BY source, tx_id
                ON CONFLICT (source, tx_id) DO UPDATE SET applied_events = t.applied_events + excluded.applied_events
                """);
            ledger.Parameters.Add(new NpgsqlParameter { Value = txEvents.Select(e => e.Source).ToArray() });
            ledger.Parameters.Add(new NpgsqlParameter { Value = txEvents.Select(e => e.TransactionId!).ToArray() });
            ledger.Parameters.Add(new NpgsqlParameter { Value = txEvents.Select(e => e.TransactionOrder!.Value).ToArray() });
            batch.BatchCommands.Add(ledger);
        }

        var endsWithCount = ends.Where(e => e.EventCount is not null).ToList();
        if (endsWithCount.Count > 0)
        {
            var end = new NpgsqlBatchCommand("""
                INSERT INTO rm_tx AS t (source, tx_id, expected_events)
                SELECT * FROM unnest($1::text[], $2::text[], $3::integer[])
                ON CONFLICT (source, tx_id) DO UPDATE SET expected_events = excluded.expected_events
                """);
            end.Parameters.Add(new NpgsqlParameter { Value = endsWithCount.Select(e => e.Source).ToArray() });
            end.Parameters.Add(new NpgsqlParameter { Value = endsWithCount.Select(e => e.TransactionId).ToArray() });
            end.Parameters.Add(new NpgsqlParameter { Value = endsWithCount.Select(e => e.EventCount!.Value).ToArray() });
            batch.BatchCommands.Add(end);
        }

        if (txEvents.Count > 0 || endsWithCount.Count > 0)
        {
            batch.BatchCommands.Add(new NpgsqlBatchCommand("""
                UPDATE rm_tx SET completed_at = clock_timestamp()
                WHERE completed_at IS NULL AND expected_events IS NOT NULL AND applied_events >= expected_events
                """));
        }

        var counters = new NpgsqlBatchCommand("""
            UPDATE rm_counter SET value = value + CASE name
                WHEN 'events_applied' THEN $1 WHEN 'events_skipped' THEN $2 WHEN 'batches' THEN 1 ELSE 0 END
            WHERE name IN ('events_applied', 'events_skipped', 'batches')
            """);
        counters.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (long)applied });
        counters.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (long)skipped });
        batch.BatchCommands.Add(counters);

        await batch.ExecuteNonQueryAsync(ct);
    }
}
