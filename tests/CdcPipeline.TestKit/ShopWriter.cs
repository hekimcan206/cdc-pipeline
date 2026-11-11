using System.Data.Common;
using System.Globalization;
using Npgsql;
using Oracle.ManagedDataAccess.Client;

namespace CdcPipeline.TestKit;

/// <summary>A source row as the reconciler compares it with the read model.</summary>
public sealed record SourceRow(string Table, string Key, int Version, string Fingerprint);

/// <summary>
/// Writes the synthetic shop workload into the source database the way an application would: short autocommit
/// statements and one multi-row transaction per order. Every change bumps <c>version</c> by one and stamps
/// <c>updated_at</c> with the database clock, which is what lag measurement and the apply-count check rely on.
/// </summary>
public abstract class ShopWriter
{
    public static ShopWriter For(PipelineEnvironment stack) => stack.Source == SourceKind.Postgres
        ? new PostgresShopWriter(stack.SourceConnectionString, stack.SourceAdminConnectionString)
        : new OracleShopWriter(stack.SourceConnectionString);

    public abstract Task InsertCustomerAsync(long id, string name, string email, string city, CancellationToken ct = default);

    public abstract Task UpdateCustomerCityAsync(long id, string city, CancellationToken ct = default);

    /// <summary>Order header and its lines in one source transaction.</summary>
    public abstract Task InsertOrderAsync(long orderId, long customerId, int lines, CancellationToken ct = default);

    public abstract Task UpdateOrderStatusAsync(long orderId, string status, CancellationToken ct = default);

    public abstract Task DeleteOrderLineAsync(long orderId, int lineNo, CancellationToken ct = default);

    /// <summary>Runs DDL as a user allowed to alter the shop tables.</summary>
    public abstract Task ExecuteDdlAsync(string sql, CancellationToken ct = default);

    public abstract Task ExecuteAsync(string sql, CancellationToken ct = default);

    /// <summary>Current source rows (customers, orders, order lines) with ids in the range, for reconciliation.</summary>
    public abstract Task<IReadOnlyList<SourceRow>> SnapshotAsync(long fromId, long toId, CancellationToken ct = default);

    protected static string Fingerprint(DbDataReader r, int from)
    {
        var parts = new List<string>();
        for (var i = from; i < r.FieldCount; i++)
        {
            parts.Add(r.IsDBNull(i) ? "null" : Convert.ToString(r.GetValue(i), CultureInfo.InvariantCulture)!);
        }

        return string.Join("|", parts);
    }

    /// <summary>Same column order the reconciler reads from the read model.</summary>
    public const string CustomerColumns = "full_name, city";
    public const string OrderColumns = "customer_id, status";
    public const string LineColumns = "sku";
}

public sealed class PostgresShopWriter(string connectionString, string adminConnectionString) : ShopWriter
{
    public override Task InsertCustomerAsync(long id, string name, string email, string city, CancellationToken ct = default) =>
        Exec("INSERT INTO customers (id, full_name, email, city) VALUES ($1, $2, $3, $4)", ct, id, name, email, city);

    public override Task UpdateCustomerCityAsync(long id, string city, CancellationToken ct = default) =>
        Exec("UPDATE customers SET city = $2, version = version + 1, updated_at = clock_timestamp() WHERE id = $1", ct, id, city);

    public override async Task InsertOrderAsync(long orderId, long customerId, int lines, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);
        await using (var order = new NpgsqlCommand("INSERT INTO orders (id, customer_id, status, total_amount) VALUES ($1, $2, 'NEW', $3)", conn, tx))
        {
            order.Parameters.Add(new NpgsqlParameter { Value = orderId });
            order.Parameters.Add(new NpgsqlParameter { Value = customerId });
            order.Parameters.Add(new NpgsqlParameter { Value = 10.50m * lines });
            await order.ExecuteNonQueryAsync(ct);
        }

        for (var line = 1; line <= lines; line++)
        {
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO order_lines (order_id, line_no, sku, quantity, unit_price) VALUES ($1, $2, $3, $4, 10.50)", conn, tx);
            cmd.Parameters.Add(new NpgsqlParameter { Value = orderId });
            cmd.Parameters.Add(new NpgsqlParameter { Value = line });
            cmd.Parameters.Add(new NpgsqlParameter { Value = $"SKU-{line:D3}" });
            cmd.Parameters.Add(new NpgsqlParameter { Value = line });
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
    }

    public override Task UpdateOrderStatusAsync(long orderId, string status, CancellationToken ct = default) =>
        Exec("UPDATE orders SET status = $2, version = version + 1, updated_at = clock_timestamp() WHERE id = $1", ct, orderId, status);

    public override Task DeleteOrderLineAsync(long orderId, int lineNo, CancellationToken ct = default) =>
        Exec("DELETE FROM order_lines WHERE order_id = $1 AND line_no = $2", ct, orderId, lineNo);

    public override async Task ExecuteDdlAsync(string sql, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(adminConnectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public override Task ExecuteAsync(string sql, CancellationToken ct = default) => Exec(sql, ct);

    public override async Task<IReadOnlyList<SourceRow>> SnapshotAsync(long fromId, long toId, CancellationToken ct = default)
    {
        var rows = new List<SourceRow>();
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);
        foreach (var (table, key, columns) in new[]
        {
            ("customers", "id::text", CustomerColumns),
            ("orders", "id::text", OrderColumns),
            ("order_lines", "order_id || '-' || line_no", LineColumns),
        })
        {
            var idColumn = table == "order_lines" ? "order_id" : "id";
            await using var cmd = new NpgsqlCommand($"SELECT {key}, version, {columns} FROM {table} WHERE {idColumn} BETWEEN $1 AND $2", conn);
            cmd.Parameters.Add(new NpgsqlParameter { Value = fromId });
            cmd.Parameters.Add(new NpgsqlParameter { Value = toId });
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                rows.Add(new SourceRow(table, r.GetString(0), r.GetInt32(1), Fingerprint(r, 2)));
            }
        }

        return rows;
    }

    private async Task Exec(string sql, CancellationToken ct, params object[] args)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var arg in args)
        {
            cmd.Parameters.Add(new NpgsqlParameter { Value = arg });
        }

        await cmd.ExecuteNonQueryAsync(ct);
    }
}

public sealed class OracleShopWriter(string connectionString) : ShopWriter
{
    public override Task InsertCustomerAsync(long id, string name, string email, string city, CancellationToken ct = default) =>
        Exec("INSERT INTO customers (id, full_name, email, city) VALUES (:1, :2, :3, :4)", ct, id, name, email, city);

    public override Task UpdateCustomerCityAsync(long id, string city, CancellationToken ct = default) =>
        Exec("UPDATE customers SET city = :2, version = version + 1, updated_at = SYSTIMESTAMP WHERE id = :1", ct, city, id);

    public override async Task InsertOrderAsync(long orderId, long customerId, int lines, CancellationToken ct = default)
    {
        await using var conn = new OracleConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var tx = (OracleTransaction)await conn.BeginTransactionAsync(ct);
        await using (var order = new OracleCommand("INSERT INTO orders (id, customer_id, status, total_amount) VALUES (:1, :2, 'NEW', :3)", conn))
        {
            order.Transaction = tx;
            order.Parameters.Add(new OracleParameter { Value = orderId });
            order.Parameters.Add(new OracleParameter { Value = customerId });
            order.Parameters.Add(new OracleParameter { Value = 10.50m * lines });
            await order.ExecuteNonQueryAsync(ct);
        }

        for (var line = 1; line <= lines; line++)
        {
            await using var cmd = new OracleCommand(
                "INSERT INTO order_lines (order_id, line_no, sku, quantity, unit_price) VALUES (:1, :2, :3, :4, 10.50)", conn);
            cmd.Transaction = tx;
            cmd.Parameters.Add(new OracleParameter { Value = orderId });
            cmd.Parameters.Add(new OracleParameter { Value = line });
            cmd.Parameters.Add(new OracleParameter { Value = $"SKU-{line:D3}" });
            cmd.Parameters.Add(new OracleParameter { Value = line });
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
    }

    public override Task UpdateOrderStatusAsync(long orderId, string status, CancellationToken ct = default) =>
        Exec("UPDATE orders SET status = :2, version = version + 1, updated_at = SYSTIMESTAMP WHERE id = :1", ct, status, orderId);

    public override Task DeleteOrderLineAsync(long orderId, int lineNo, CancellationToken ct = default) =>
        Exec("DELETE FROM order_lines WHERE order_id = :1 AND line_no = :2", ct, orderId, lineNo);

    // The shop schema owner runs its own DDL.
    public override Task ExecuteDdlAsync(string sql, CancellationToken ct = default) => Exec(sql, ct);

    public override Task ExecuteAsync(string sql, CancellationToken ct = default) => Exec(sql, ct);

    public override async Task<IReadOnlyList<SourceRow>> SnapshotAsync(long fromId, long toId, CancellationToken ct = default)
    {
        var rows = new List<SourceRow>();
        await using var conn = new OracleConnection(connectionString);
        await conn.OpenAsync(ct);
        foreach (var (table, key, columns) in new[]
        {
            ("customers", "TO_CHAR(id)", CustomerColumns),
            ("orders", "TO_CHAR(id)", OrderColumns),
            ("order_lines", "order_id || '-' || line_no", LineColumns),
        })
        {
            var idColumn = table == "order_lines" ? "order_id" : "id";
            await using var cmd = new OracleCommand($"SELECT {key}, version, {columns} FROM {table} WHERE {idColumn} BETWEEN :1 AND :2", conn);
            cmd.Parameters.Add(new OracleParameter { Value = fromId });
            cmd.Parameters.Add(new OracleParameter { Value = toId });
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                rows.Add(new SourceRow(table, r.GetString(0), Convert.ToInt32(r.GetValue(1), CultureInfo.InvariantCulture), Fingerprint(r, 2)));
            }
        }

        return rows;
    }

    // ODP.NET binds by position by default; parameters are passed in placeholder order.
    private async Task Exec(string sql, CancellationToken ct, params object[] args)
    {
        await using var conn = new OracleConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new OracleCommand(sql, conn);
        foreach (var arg in args)
        {
            cmd.Parameters.Add(new OracleParameter { Value = arg });
        }

        await cmd.ExecuteNonQueryAsync(ct);
    }
}
