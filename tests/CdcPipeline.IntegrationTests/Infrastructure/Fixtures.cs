using CdcPipeline.TestKit;
using Testcontainers.PostgreSql;

namespace CdcPipeline.IntegrationTests.Infrastructure;

/// <summary>A read-model PostgreSQL only: for projection tests that feed captured events straight into the writer.</summary>
public sealed class ReadModelDatabaseFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder(PipelineEnvironment.PostgresImage)
        .WithLabel("project", "cdc-pipeline-tests")
        .Build();

    private int _databases;

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async Task<string> CreateDatabaseAsync()
    {
        var name = $"rm_{Interlocked.Increment(ref _databases)}";
        await using var conn = new Npgsql.NpgsqlConnection(_container.GetConnectionString());
        await conn.OpenAsync();
        await using var cmd = new Npgsql.NpgsqlCommand($"CREATE DATABASE {name}", conn);
        await cmd.ExecuteNonQueryAsync();
        return new Npgsql.NpgsqlConnectionStringBuilder(_container.GetConnectionString()) { Database = name }.ConnectionString;
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}

/// <summary>The complete PostgreSQL capture pipeline, shared by every test in the collection.</summary>
public sealed class PostgresPipelineFixture : IAsyncLifetime
{
    private int _next;

    public PipelineEnvironment Stack { get; private set; } = null!;

    public async ValueTask InitializeAsync() => Stack = await PipelineEnvironment.StartAsync(SourceKind.Postgres);

    /// <summary>A unique name for a test's read-model database, consumer group and index prefix.</summary>
    public string NextName(string prefix) => $"{prefix}{Interlocked.Increment(ref _next)}";

    public async ValueTask DisposeAsync()
    {
        if (Stack is not null)
        {
            await Stack.DisposeAsync();
        }
    }
}

/// <summary>
/// The Oracle LogMiner pipeline. Started only when <c>CDC_ORACLE_TESTS=1</c>: Oracle Database Free alone needs 2.3-3 GB
/// (measured), so it is opt-in locally and runs in its own CI job.
/// </summary>
public sealed class OraclePipelineFixture : IAsyncLifetime
{
    public static bool Enabled => Environment.GetEnvironmentVariable("CDC_ORACLE_TESTS") == "1";

    public PipelineEnvironment? Stack { get; private set; }

    public async ValueTask InitializeAsync()
    {
        if (Enabled)
        {
            Stack = await PipelineEnvironment.StartAsync(SourceKind.Oracle);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Stack is not null)
        {
            await Stack.DisposeAsync();
        }
    }
}

[CollectionDefinition(Name)]
public sealed class PostgresPipelineGroup : ICollectionFixture<PostgresPipelineFixture>
{
    public const string Name = "postgres-pipeline";
}

[CollectionDefinition(Name)]
public sealed class OraclePipelineGroup : ICollectionFixture<OraclePipelineFixture>
{
    public const string Name = "oracle-pipeline";
}
