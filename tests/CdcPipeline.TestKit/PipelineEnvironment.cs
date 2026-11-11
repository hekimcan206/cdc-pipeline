using System.Net;
using System.Net.Sockets;
using CdcPipeline.Consumer.Search;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Testcontainers.Oracle;
using Testcontainers.PostgreSql;

namespace CdcPipeline.TestKit;

public enum SourceKind
{
    Postgres,
    Oracle,
}

/// <summary>
/// One complete capture pipeline in containers: source database, Redpanda (single node), Debezium Server, a
/// read-model PostgreSQL and Meilisearch. Sized for a shared 7.6 GB Docker VM: Redpanda gets 512 MB, Debezium a
/// 256-384 MB heap. Only one source kind runs per stack, so Oracle and PostgreSQL pipelines never run together.
/// </summary>
public sealed class PipelineEnvironment : IAsyncDisposable
{
    public const string DebeziumImage = "quay.io/debezium/server:3.7.1.Final";
    public const string RedpandaImage = "redpandadata/redpanda:v26.2.6";
    public const string MeiliImage = "getmeili/meilisearch:v1.56.2";
    public const string PostgresImage = "postgres:17-alpine";
    public const string OracleImage = "gvenzl/oracle-free:23-slim-faststart";

    // Throwaway credentials for test containers only.
    public const string DebeziumPassword = "DbzTest1";
    public const string ShopPassword = "ShopTest1";
    public const string MeiliKey = "meili-test-master-key-0123456789";

    private readonly List<IAsyncDisposable> _resources = [];
    private INetwork _network = null!;
    private IContainer _debezium = null!;
    private IContainer _redpanda = null!;
    private PostgreSqlContainer _readModel = null!;
    private IContainer _meili = null!;
    private PostgreSqlContainer? _sourcePg;
    private OracleContainer? _oracle;

    private PipelineEnvironment(SourceKind source) => Source = source;

    public SourceKind Source { get; }

    /// <summary>Debezium topic prefix and read-model <c>source</c> value: <c>pg</c> or <c>ora</c>.</summary>
    public string SourceName => Source == SourceKind.Postgres ? "pg" : "ora";

    public string KafkaBootstrap { get; private set; } = "";

    public string MeiliUrl => $"http://{_meili.Hostname}:{_meili.GetMappedPublicPort(7700)}";

    /// <summary>The application login of the source database (DML only).</summary>
    public string SourceConnectionString { get; private set; } = "";

    /// <summary>A privileged login for DDL (schema evolution).</summary>
    public string SourceAdminConnectionString { get; private set; } = "";

    public string ReadModelAdminConnectionString => _readModel.GetConnectionString();

    /// <summary>Container ids by role, for resource measurements (docker stats).</summary>
    public IReadOnlyDictionary<string, string> ContainerIds => new Dictionary<string, string>
    {
        ["source"] = ((IContainer?)_sourcePg ?? _oracle!).Id,
        ["redpanda"] = _redpanda.Id,
        ["debezium"] = _debezium.Id,
        ["readmodel"] = _readModel.Id,
        ["meilisearch"] = _meili.Id,
    };

    public static string DeployPath(params string[] parts) => Path.Combine([AppContext.BaseDirectory, "deploy", .. parts]);

    public static async Task<PipelineEnvironment> StartAsync(SourceKind source, CancellationToken ct = default)
    {
        var stack = new PipelineEnvironment(source);
        try
        {
            await stack.StartCoreAsync(ct);
            return stack;
        }
        catch
        {
            await stack.DisposeAsync();
            throw;
        }
    }

    private async Task StartCoreAsync(CancellationToken ct)
    {
        _network = new NetworkBuilder().WithName($"cdcp-{Guid.NewGuid():N}"[..20]).Build();
        await _network.CreateAsync(ct);
        _resources.Add(_network);

        // The broker's external listener must advertise the host port, so it is chosen before start.
        var kafkaPort = FreeTcpPort();
        var redpanda = _redpanda = new ContainerBuilder(RedpandaImage)
            .WithNetwork(_network).WithNetworkAliases("redpanda")
            .WithPortBinding(kafkaPort, 19092)
            .WithResourceMapping(await File.ReadAllBytesAsync(DeployPath("redpanda", "bootstrap.yaml"), ct), "/etc/redpanda/.bootstrap.yaml")
            .WithCommand("redpanda", "start", "--mode", "dev-container", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M",
                "--overprovisioned", "--kafka-addr", "internal://0.0.0.0:9092,external://0.0.0.0:19092",
                "--advertise-kafka-addr", $"internal://redpanda:9092,external://localhost:{kafkaPort}")
            .WithCreateParameterModifier(p => p.HostConfig!.Memory = 768L * 1024 * 1024)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Successfully started Redpanda!"))
            .WithLabel("project", "cdc-pipeline-tests")
            .Build();
        KafkaBootstrap = $"localhost:{kafkaPort}";

        _readModel = new PostgreSqlBuilder(PostgresImage)
            .WithDatabase("readmodel")
            .WithLabel("project", "cdc-pipeline-tests")
            .Build();

        _meili = new ContainerBuilder(MeiliImage)
            .WithPortBinding(7700, true)
            .WithEnvironment("MEILI_MASTER_KEY", MeiliKey)
            .WithEnvironment("MEILI_NO_ANALYTICS", "true")
            .WithEnvironment("MEILI_ENV", "development")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(r => r.ForPort(7700).ForPath("/health")))
            .WithLabel("project", "cdc-pipeline-tests")
            .Build();

        IContainer sourceContainer = Source == SourceKind.Postgres ? BuildSourcePostgres() : await BuildOracleAsync(ct);
        _resources.AddRange([redpanda, _readModel, _meili, sourceContainer]);

        await Task.WhenAll(redpanda.StartAsync(ct), _readModel.StartAsync(ct), _meili.StartAsync(ct), sourceContainer.StartAsync(ct));
        await ConfigureSourceAsync(ct);

        var (config, heap) = Source == SourceKind.Postgres ? ("postgres.properties", "-Xmx256m") : ("oracle.properties", "-Xmx384m");
        _debezium = new ContainerBuilder(DebeziumImage)
            .WithNetwork(_network)
            .WithResourceMapping(await File.ReadAllBytesAsync(DeployPath("debezium", config), ct), "/debezium/config/application.properties")
            .WithEnvironment("JAVA_OPTS", $"-Xms64m {heap}")
            .WithEnvironment("DBZ_PASSWORD", DebeziumPassword)
            .WithEnvironment("KAFKA_BOOTSTRAP", "redpanda:9092")
            .WithEnvironment("PG_SOURCE_HOST", "source-pg")
            .WithEnvironment("ORACLE_HOST", "oracle")
            .WithCreateParameterModifier(p => p.HostConfig!.Memory = 768L * 1024 * 1024)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Starting streaming"))
            .WithLabel("project", "cdc-pipeline-tests")
            .Build();
        _resources.Add(_debezium);
        await _debezium.StartAsync(ct);
    }

    private PostgreSqlContainer BuildSourcePostgres()
    {
        _sourcePg = new PostgreSqlBuilder(PostgresImage)
            .WithDatabase("shop")
            .WithNetwork(_network).WithNetworkAliases("source-pg")
            .WithEnvironment("SHOP_APP_PASSWORD", ShopPassword)
            .WithEnvironment("DBZ_PASSWORD", DebeziumPassword)
            .WithResourceMapping(File.ReadAllBytes(DeployPath("postgres-source", "01-shop.sh")), "/docker-entrypoint-initdb.d/01-shop.sh",
                fileMode: UnixFileModes.UserRead | UnixFileModes.UserExecute | UnixFileModes.GroupRead | UnixFileModes.OtherRead)
            .WithCommand("-c", "wal_level=logical", "-c", "max_wal_senders=4", "-c", "max_replication_slots=4", "-c", "wal_sender_timeout=10s")
            .WithLabel("project", "cdc-pipeline-tests")
            .Build();
        return _sourcePg;
    }

    private async Task<OracleContainer> BuildOracleAsync(CancellationToken ct)
    {
        _oracle = new OracleBuilder(OracleImage)
            .WithNetwork(_network).WithNetworkAliases("oracle")
            .WithEnvironment("DBZ_PASSWORD", DebeziumPassword)
            .WithEnvironment("SHOP_PASSWORD", ShopPassword)
            .WithResourceMapping(await File.ReadAllBytesAsync(DeployPath("oracle", "init", "01-logminer.sh"), ct), "/container-entrypoint-initdb.d/01-logminer.sh",
                fileMode: UnixFileModes.UserRead | UnixFileModes.UserWrite | UnixFileModes.UserExecute | UnixFileModes.GroupRead | UnixFileModes.OtherRead)
            .WithLabel("project", "cdc-pipeline-tests")
            .Build();
        return _oracle;
    }

    private async Task ConfigureSourceAsync(CancellationToken ct)
    {
        if (_sourcePg is not null)
        {
            SourceAdminConnectionString = _sourcePg.GetConnectionString();
            SourceConnectionString = new NpgsqlConnectionStringBuilder(SourceAdminConnectionString) { Username = "shop_app", Password = ShopPassword }.ConnectionString;

            // The image's readiness check can pass while the init script still runs on the temporary server; wait
            // until the application login works and the publication exists.
            await Waiter.UntilAsync(async () =>
            {
                try
                {
                    await using var conn = new NpgsqlConnection(SourceConnectionString);
                    await conn.OpenAsync(ct);
                    await using var cmd = new NpgsqlCommand("SELECT count(*) FROM pg_publication WHERE pubname = 'cdc_shop'", conn);
                    return (long)(await cmd.ExecuteScalarAsync(ct))! == 1;
                }
                catch (NpgsqlException)
                {
                    return false;
                }
            }, TimeSpan.FromSeconds(60), "source PostgreSQL init", ct);
        }
        else
        {
            var builder = new OracleConnectionStringBuilder(_oracle!.GetConnectionString());
            builder.UserID = "shop";
            builder.Password = ShopPassword;
            SourceConnectionString = builder.ConnectionString;
            // The schema owner runs the evolution DDL on Oracle.
            SourceAdminConnectionString = SourceConnectionString;
        }
    }

    /// <summary>A fresh read-model database, so tests and benchmark runs never see each other's rows.</summary>
    public async Task<string> CreateReadModelDatabaseAsync(string name, CancellationToken ct = default)
    {
        await using (var conn = new NpgsqlConnection(ReadModelAdminConnectionString))
        {
            await conn.OpenAsync(ct);
            await using var cmd = new NpgsqlCommand($"CREATE DATABASE \"{name}\"", conn);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        return new NpgsqlConnectionStringBuilder(ReadModelAdminConnectionString) { Database = name }.ConnectionString;
    }

    /// <summary>
    /// Kills Debezium Server (SIGKILL, no graceful shutdown, no final offset flush) and starts it again. It resumes from
    /// its last flushed offset, so everything after that point is published a second time.
    /// </summary>
    public async Task CrashDebeziumAsync(CancellationToken ct = default)
    {
        using (var kill = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo("docker", ["kill", "--signal", "KILL", _debezium.Id])
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        })!)
        {
            await kill.WaitForExitAsync(ct);
        }

        await _debezium.StartAsync(ct);
    }

    /// <summary>Topics on the broker with their total record count (sum of high watermarks).</summary>
    public Task<IReadOnlyList<string>> ListTopicsAsync() => Task.Run<IReadOnlyList<string>>(() =>
    {
        using var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = KafkaBootstrap }).Build();
        using var consumer = new Confluent.Kafka.ConsumerBuilder<Confluent.Kafka.Ignore, Confluent.Kafka.Ignore>(
            new Confluent.Kafka.ConsumerConfig { BootstrapServers = KafkaBootstrap, GroupId = "diagnostics" }).Build();
        return [.. admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics
            .Where(t => !t.Topic.StartsWith('_'))
            .Select(t => $"{t.Topic}:{t.Partitions.Sum(p => consumer.QueryWatermarkOffsets(new Confluent.Kafka.TopicPartition(t.Topic, p.PartitionId), TimeSpan.FromSeconds(5)).High.Value)}")];
    });

    /// <summary>
    /// Commits the current end of every pipeline topic for <paramref name="groupId"/>, so a consumer in that group starts
    /// with what is written from now on instead of the history earlier tests left in the shared broker.
    /// </summary>
    public Task PositionGroupAtEndAsync(string groupId) => Task.Run(() =>
    {
        using var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = KafkaBootstrap }).Build();
        using var consumer = new Confluent.Kafka.ConsumerBuilder<Confluent.Kafka.Ignore, Confluent.Kafka.Ignore>(
            new Confluent.Kafka.ConsumerConfig { BootstrapServers = KafkaBootstrap, GroupId = groupId, EnableAutoCommit = false }).Build();
        var ends = admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics
            .Where(t => t.Topic.StartsWith(SourceName + ".", StringComparison.Ordinal))
            .SelectMany(t => t.Partitions.Select(p => new Confluent.Kafka.TopicPartition(t.Topic, p.PartitionId)))
            .Select(tp => new Confluent.Kafka.TopicPartitionOffset(tp, consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(5)).High))
            .ToList();
        if (ends.Count > 0)
        {
            consumer.Commit(ends);
        }
    });

    public MeiliClient CreateMeiliClient()
    {
        var http = new HttpClient();
        MeiliClient.Configure(http, MeiliUrl, MeiliKey);
        return new MeiliClient(http);
    }

    public Task<(string Stdout, string Stderr)> DebeziumLogsAsync() => _debezium.GetLogsAsync();

    public async ValueTask DisposeAsync()
    {
        for (var i = _resources.Count - 1; i >= 0; i--)
        {
            try
            {
                await _resources[i].DisposeAsync();
            }
            catch (Exception) when (i >= 0)
            {
                // Best effort: one failed cleanup must not leak the rest.
            }
        }
    }

    private static int FreeTcpPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
