using System.Globalization;
using Npgsql;

namespace CdcPipeline.TestKit;

/// <summary>A read-model row with its bookkeeping, as the reconciler sees it.</summary>
public sealed record ReadModelRow(string Table, string Key, int? Version, string Fingerprint, bool Deleted, int ApplyCount, bool IndexDirty);

/// <summary>Difference found by <see cref="ReadModelProbe.ReconcileAsync"/>.</summary>
public sealed record Discrepancy(string Table, string Key, string Problem);

/// <summary>Queries the read model for assertions and waits.</summary>
public sealed class ReadModelProbe(string connectionString, string source)
{
    public string ConnectionString => connectionString;

    public async Task<T?> ScalarAsync<T>(string sql, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        var value = await cmd.ExecuteScalarAsync(ct);
        return value is null or DBNull ? default : (T)Convert.ChangeType(value, Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T), CultureInfo.InvariantCulture);
    }

    public async Task<IReadOnlyList<ReadModelRow>> RowsAsync(long fromId, long toId, CancellationToken ct = default)
    {
        var rows = new List<ReadModelRow>();
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);
        foreach (var (table, rmTable, key, columns, idColumn) in new[]
        {
            ("customers", "rm_customer", "id::text", ShopWriter.CustomerColumns, "id"),
            ("orders", "rm_order", "id::text", ShopWriter.OrderColumns, "id"),
            ("order_lines", "rm_order_line", "order_id || '-' || line_no", ShopWriter.LineColumns, "order_id"),
        })
        {
            await using var cmd = new NpgsqlCommand(
                $"SELECT {key}, version, deleted, apply_count, index_dirty, {columns} FROM {rmTable} WHERE source = $1 AND {idColumn} BETWEEN $2 AND $3", conn);
            cmd.Parameters.Add(new NpgsqlParameter { Value = source });
            cmd.Parameters.Add(new NpgsqlParameter { Value = fromId });
            cmd.Parameters.Add(new NpgsqlParameter { Value = toId });
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var parts = new List<string>();
                for (var i = 5; i < r.FieldCount; i++)
                {
                    parts.Add(r.IsDBNull(i) ? "null" : Convert.ToString(r.GetValue(i), CultureInfo.InvariantCulture)!);
                }

                rows.Add(new ReadModelRow(table, r.GetString(0), r.IsDBNull(1) ? null : r.GetInt32(1), string.Join("|", parts),
                    r.GetBoolean(2), r.GetInt32(3), r.GetBoolean(4)));
            }
        }

        return rows;
    }

    /// <summary>
    /// Compares source and read model for an id range. Live source rows must match exactly (values and version);
    /// rows missing from the source must be tombstones. With <paramref name="checkApplyCount"/>, every live row's
    /// apply_count must equal its version: each insert and update applied exactly once, none lost, none doubled.
    /// </summary>
    public async Task<IReadOnlyList<Discrepancy>> ReconcileAsync(ShopWriter writer, long fromId, long toId, bool checkApplyCount, CancellationToken ct = default)
    {
        var sourceRows = (await writer.SnapshotAsync(fromId, toId, ct)).ToDictionary(r => (r.Table, r.Key));
        var readRows = (await RowsAsync(fromId, toId, ct)).ToDictionary(r => (r.Table, r.Key));
        var problems = new List<Discrepancy>();
        foreach (var (key, s) in sourceRows)
        {
            if (!readRows.TryGetValue(key, out var rm))
            {
                problems.Add(new Discrepancy(key.Table, key.Key, "missing from read model"));
                continue;
            }

            if (rm.Deleted || rm.Version != s.Version || rm.Fingerprint != s.Fingerprint)
            {
                problems.Add(new Discrepancy(key.Table, key.Key, $"source v{s.Version} '{s.Fingerprint}' vs read model v{rm.Version} '{rm.Fingerprint}' deleted={rm.Deleted}"));
            }
            else if (checkApplyCount && rm.ApplyCount != s.Version)
            {
                problems.Add(new Discrepancy(key.Table, key.Key, $"applied {rm.ApplyCount} times for {s.Version} source changes"));
            }
        }

        foreach (var (key, rm) in readRows)
        {
            if (!sourceRows.ContainsKey(key) && !rm.Deleted)
            {
                problems.Add(new Discrepancy(key.Table, key.Key, "deleted in source but live in read model"));
            }
        }

        return problems;
    }

    public async Task WaitForConvergenceAsync(ShopWriter writer, long fromId, long toId, TimeSpan timeout, CancellationToken ct = default)
    {
        IReadOnlyList<Discrepancy> last = [];
        try
        {
            await Waiter.UntilAsync(async () =>
            {
                last = await ReconcileAsync(writer, fromId, toId, checkApplyCount: false, ct);
                return last.Count == 0;
            }, timeout, "read model to match the source", ct);
        }
        catch (TimeoutException ex)
        {
            throw new TimeoutException($"{ex.Message} {last.Count} discrepancies, first: {string.Join("; ", last.Take(5))}", ex);
        }
    }

    public Task<long> CounterAsync(string name, CancellationToken ct = default) =>
        ScalarAsync<long>($"SELECT value FROM rm_counter WHERE name = '{name}'", ct);

    public Task<long> DirtyRowsAsync(CancellationToken ct = default) => ScalarAsync<long>(
        "SELECT (SELECT count(*) FROM rm_customer WHERE index_dirty) + (SELECT count(*) FROM rm_order WHERE index_dirty) + (SELECT count(*) FROM rm_order_line WHERE index_dirty)", ct);
}
