using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Npgsql;

namespace CdcPipeline.Consumer.Persistence;

/// <summary>
/// Applies the embedded, numbered SQL scripts in order, each in its own transaction, under an advisory lock so that
/// two consumer instances starting together apply each script once. An applied script must never change.
/// </summary>
internal sealed partial class MigrationRunner(NpgsqlDataSource dataSource, ILogger<MigrationRunner> logger)
{
    private const long LockKey = 0x4344_4350_4950; // "CDCPIP"

    public async Task MigrateAsync(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await Execute(connection, null, $"SELECT pg_advisory_lock({LockKey})", cancellationToken);
        try
        {
            await Execute(connection, null, """
                CREATE TABLE IF NOT EXISTS rm_schema_migrations (
                    version    integer     PRIMARY KEY,
                    name       text        NOT NULL,
                    sha256     text        NOT NULL,
                    applied_at timestamptz NOT NULL DEFAULT now())
                """, cancellationToken);

            var applied = new Dictionary<int, string>();
            await using (var cmd = new NpgsqlCommand("SELECT version, sha256 FROM rm_schema_migrations", connection))
            await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    applied[reader.GetInt32(0)] = reader.GetString(1);
                }
            }

            foreach (var (version, name, sql) in Load())
            {
                var sha = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(sql)));
                if (applied.TryGetValue(version, out var existing))
                {
                    if (existing != sha)
                    {
                        throw new InvalidOperationException($"Migration {version:D4}_{name} changed after it was applied. Add a new migration instead.");
                    }

                    continue;
                }

                await using var tx = await connection.BeginTransactionAsync(cancellationToken);
                await Execute(connection, tx, sql, cancellationToken);
                await using (var record = new NpgsqlCommand(
                    "INSERT INTO rm_schema_migrations (version, name, sha256) VALUES ($1, $2, $3)", connection, tx))
                {
                    record.Parameters.Add(new NpgsqlParameter { Value = version });
                    record.Parameters.Add(new NpgsqlParameter { Value = name });
                    record.Parameters.Add(new NpgsqlParameter { Value = sha });
                    await record.ExecuteNonQueryAsync(cancellationToken);
                }

                await tx.CommitAsync(cancellationToken);
                LogApplied(version, name);
            }
        }
        finally
        {
            await Execute(connection, null, $"SELECT pg_advisory_unlock({LockKey})", CancellationToken.None);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Applied read-model migration {Version:D4}_{Name}")]
    private partial void LogApplied(int version, string name);

    private static IEnumerable<(int Version, string Name, string Sql)> Load()
    {
        var assembly = Assembly.GetExecutingAssembly();
        foreach (var resource in assembly.GetManifestResourceNames().Where(n => n.StartsWith("migrations/", StringComparison.Ordinal)).Order(StringComparer.Ordinal))
        {
            var file = resource["migrations/".Length..^".sql".Length];
            using var stream = assembly.GetManifestResourceStream(resource)!;
            using var reader = new StreamReader(stream);
            yield return (int.Parse(file[..4], System.Globalization.CultureInfo.InvariantCulture), file[5..], reader.ReadToEnd());
        }
    }

    private static async Task Execute(NpgsqlConnection connection, NpgsqlTransaction? tx, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection, tx);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
