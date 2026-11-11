using System.Globalization;
using System.Text.Json.Nodes;
using CdcPipeline.Consumer.Infrastructure;
using CdcPipeline.Consumer.Persistence;
using CdcPipeline.Consumer.Pipeline;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

namespace CdcPipeline.Consumer.Search;

/// <summary>Wakes the indexer right after a batch commits, so index lag is not bounded below by the poll interval.</summary>
public sealed class IndexerSignal : IDisposable
{
    private readonly SemaphoreSlim _signal = new(0, 1);

    public void Wake()
    {
        if (_signal.CurrentCount == 0)
        {
            try
            {
                _signal.Release();
            }
            catch (SemaphoreFullException)
            {
                // Already signalled.
            }
        }
    }

    public Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct) => _signal.WaitAsync(timeout, ct);

    public void Dispose() => _signal.Dispose();
}

/// <summary>
/// Projects the read model (not the event stream) into Meilisearch. Rows changed by the consumer are flagged dirty in
/// the same transaction; this loop pushes their current state and clears the flag only if the row has not moved on in
/// the meantime. Pushing current state is idempotent, so a crash between the push and the flag update just pushes the
/// same document again. Rows of a source transaction that is not complete yet are held back (up to
/// <see cref="SearchOptions.TransactionWaitMs"/>) so search never shows half a transaction.
/// </summary>
internal sealed partial class SearchIndexer(
    IOptions<ConsumerOptions> options,
    NpgsqlDataSource dataSource,
    ReadModelWriter writer,
    MeiliClient meili,
    IndexerSignal signal,
    PipelineState state,
    PipelineMetrics metrics,
    ILogger<SearchIndexer> logger) : BackgroundService
{
    private readonly SearchOptions _options = options.Value.Search;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Enabled)
        {
            return;
        }

        await EnsureIndexesAsync(stoppingToken);
        var idle = TimeSpan.FromMilliseconds(_options.PollIntervalMs);
        var nextPrune = DateTime.UtcNow;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var pushed = 0;
                foreach (var projection in writer.Projections)
                {
                    pushed += await IndexPendingAsync(projection, stoppingToken);
                }

                if (DateTime.UtcNow >= nextPrune)
                {
                    await PruneTransactionLedgerAsync(stoppingToken);
                    nextPrune = DateTime.UtcNow.AddSeconds(30);
                }

                if (pushed == 0)
                {
                    await signal.WaitAsync(idle, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex) when (ex is HttpRequestException or NpgsqlException or TaskCanceledException)
            {
                LogIndexerError(ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }
    }

    /// <summary>
    /// The per-event ledger only has to outlive redeliveries of a transaction, which come from uncommitted offsets, i.e.
    /// seconds. Ten minutes after completion its rows go; the rm_tx summary row stays.
    /// </summary>
    private async Task PruneTransactionLedgerAsync(CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand("""
            DELETE FROM rm_tx_event e USING rm_tx t
            WHERE e.source = t.source AND e.tx_id = t.tx_id AND t.completed_at < clock_timestamp() - interval '10 minutes'
            """);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task EnsureIndexesAsync(CancellationToken ct)
    {
        foreach (var projection in writer.Projections)
        {
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    await meili.EnsureIndexAsync(IndexUid(projection), ct);
                    break;
                }
                catch (HttpRequestException ex) when (attempt < 30)
                {
                    LogIndexerError($"search index not reachable yet ({ex.Message})");
                    await Task.Delay(TimeSpan.FromSeconds(1), ct);
                }
            }
        }
    }

    private async Task<int> IndexPendingAsync(ProjectionSql projection, CancellationToken ct)
    {
        var contract = projection.Contract;
        var upserts = new JsonArray();
        var deletes = new List<string>();
        var marks = new List<(string Source, object[] Key, decimal Major, decimal Minor)>();
        var forced = 0;

        await using (var cmd = dataSource.CreateCommand(projection.PendingIndex))
        {
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = _options.TransactionWaitMs / 1000.0 });
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = _options.BatchSize });
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            var keyCount = contract.Key.Count;
            var fieldCount = contract.Fields.Count;
            while (await reader.ReadAsync(ct))
            {
                var source = reader.GetString(0);
                var key = new object[keyCount];
                for (var i = 0; i < keyCount; i++)
                {
                    key[i] = reader.GetValue(1 + i);
                }

                var docId = DocumentId(source, key);
                var col = 1 + keyCount + fieldCount;
                var extraJson = reader.GetString(col);
                var deleted = reader.GetBoolean(col + 1);
                marks.Add((source, key, reader.GetDecimal(col + 2), reader.GetDecimal(col + 3)));
                if (reader.GetBoolean(col + 4))
                {
                    forced++;
                }

                if (deleted)
                {
                    deletes.Add(docId);
                    continue;
                }

                var doc = new JsonObject { [MeiliClient.PrimaryKey] = docId, ["source"] = source };
                for (var i = 0; i < keyCount; i++)
                {
                    doc[contract.Key[i].Name] = ToJson(key[i]);
                }

                for (var i = 0; i < fieldCount; i++)
                {
                    doc[contract.Fields[i].Name] = reader.IsDBNull(1 + keyCount + i) ? null : ToJson(reader.GetValue(1 + keyCount + i));
                }

                // Fields the contract does not know yet (an added column) are searchable right away.
                if (JsonNode.Parse(extraJson) is JsonObject extra)
                {
                    foreach (var (name, value) in extra)
                    {
                        if (!doc.ContainsKey(name))
                        {
                            doc[name] = value?.DeepClone();
                        }
                    }
                }

                upserts.Add(doc);
            }
        }

        if (marks.Count == 0)
        {
            return 0;
        }

        var uid = IndexUid(projection);
        if (upserts.Count > 0)
        {
            await meili.AddOrReplaceAsync(uid, upserts, ct);
        }

        if (deletes.Count > 0)
        {
            await meili.DeleteAsync(uid, deletes, ct);
        }

        await using (var mark = dataSource.CreateCommand(projection.MarkIndexed))
        {
            mark.Parameters.Add(new NpgsqlParameter { Value = marks.Select(m => m.Source).ToArray() });
            for (var i = 0; i < contract.Key.Count; i++)
            {
                var index = i;
                mark.Parameters.Add(contract.Key[i].Type switch
                {
                    Core.Contracts.FieldType.Int64 => new NpgsqlParameter { Value = marks.Select(m => (long)m.Key[index]).ToArray() },
                    Core.Contracts.FieldType.Int32 => new NpgsqlParameter { Value = marks.Select(m => (int)m.Key[index]).ToArray() },
                    _ => new NpgsqlParameter { Value = marks.Select(m => m.Key[index].ToString()!).ToArray() },
                });
            }

            mark.Parameters.Add(new NpgsqlParameter { Value = marks.Select(m => m.Major).ToArray() });
            mark.Parameters.Add(new NpgsqlParameter { Value = marks.Select(m => m.Minor).ToArray() });
            await mark.ExecuteNonQueryAsync(ct);
        }

        state.RecordIndexed(marks.Count, forced);
        metrics.Indexed.Add(marks.Count);
        if (forced > 0)
        {
            metrics.ForcedIndexed.Add(forced);
        }

        return marks.Count;
    }

    private string IndexUid(ProjectionSql projection) => _options.IndexPrefix + projection.Contract.SourceTable;

    public static string DocumentId(string source, IEnumerable<object> key) =>
        string.Join('-', key.Select(k => Convert.ToString(k, CultureInfo.InvariantCulture)).Prepend(source));

    private static JsonValue? ToJson(object value) => value switch
    {
        DateTime dt => JsonValue.Create(DateTime.SpecifyKind(dt, DateTimeKind.Utc).ToString("O", CultureInfo.InvariantCulture)),
        DateTimeOffset dto => JsonValue.Create(dto.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture)),
        decimal d => JsonValue.Create(d),
        long l => JsonValue.Create(l),
        int i => JsonValue.Create(i),
        string s => JsonValue.Create(s),
        _ => JsonValue.Create(Convert.ToString(value, CultureInfo.InvariantCulture)),
    };

    [LoggerMessage(Level = LogLevel.Warning, Message = "Indexer: {Message}; retrying")]
    private partial void LogIndexerError(string message);
}
