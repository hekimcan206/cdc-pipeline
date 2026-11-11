using CdcPipeline.Consumer.Infrastructure;
using CdcPipeline.Consumer.Persistence;
using CdcPipeline.Consumer.Pipeline;
using CdcPipeline.Consumer.Search;
using CdcPipeline.Core.Contracts;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CdcPipeline.Consumer;

/// <summary>Builds the consumer host; used by <c>Program</c>, the integration tests and the benchmark harness.</summary>
public static class ConsumerApp
{
    public static WebApplication Build(string[] args, Action<WebApplicationBuilder>? configure = null)
    {
        var builder = WebApplication.CreateBuilder(args);
        configure?.Invoke(builder);
        builder.AddObservability();

        builder.Services.AddOptions<ConsumerOptions>()
            .Bind(builder.Configuration.GetSection(ConsumerOptions.Section))
            .ValidateDataAnnotations()
            .Validate(o => !string.IsNullOrWhiteSpace(o.ReadModelConnectionString), "Consumer:ReadModelConnectionString is required")
            .ValidateOnStart();

        builder.Services.AddSingleton(TimeProvider.System);
        builder.Services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<IOptions<ConsumerOptions>>().Value;
            return new NpgsqlDataSourceBuilder(options.ReadModelConnectionString).Build();
        });
        builder.Services.AddSingleton(sp => ShopContracts.ByVersion(sp.GetRequiredService<IOptions<ConsumerOptions>>().Value.ContractVersion));
        builder.Services.AddSingleton<ReadModelWriter>();
        builder.Services.AddSingleton<MigrationRunner>();
        builder.Services.AddSingleton<PipelineState>();
        builder.Services.AddSingleton<IndexerSignal>();
        builder.Services.AddHttpClient<MeiliClient>((sp, http) =>
        {
            var search = sp.GetRequiredService<IOptions<ConsumerOptions>>().Value.Search;
            MeiliClient.Configure(http, search.Url, search.ApiKey);
        });
        builder.Services.AddHostedService<ChangeConsumer>();
        builder.Services.AddHostedService<SearchIndexer>();

        var app = builder.Build();

        app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));

        // Ready means "applying changes". A halted consumer is alive but not ready: it needs an operator.
        app.MapGet("/health/ready", (PipelineState state) => state.Status == ConsumerStatus.Running
            ? Results.Ok(new { status = "ready" })
            : Results.Json(new { status = state.Status.ToString().ToLowerInvariant(), reason = state.HaltReason, at = state.HaltedAt },
                statusCode: StatusCodes.Status503ServiceUnavailable));

        app.MapGet("/stats", (PipelineState state, ReadModelWriter writer) => Results.Ok(new
        {
            status = state.Status.ToString().ToLowerInvariant(),
            contract = writer.ContractVersion,
            applied = state.Applied,
            skipped = state.Skipped,
            unmapped = state.Unmapped,
            batches = state.Batches,
            indexed = state.Indexed,
            forcedIndexed = state.ForcedIndexed,
            lastBatchAt = state.LastBatchAt,
            haltReason = state.HaltReason,
            haltedAt = state.HaltedAt,
        }));
        return app;
    }

    /// <summary>Applies read-model migrations, then starts the consumer and indexer. The schema must exist first.</summary>
    public static async Task StartAsync(WebApplication app, CancellationToken cancellationToken = default)
    {
        await app.Services.GetRequiredService<MigrationRunner>().MigrateAsync(cancellationToken);
        await app.StartAsync(cancellationToken);
    }

    public static async Task RunAsync(string[] args)
    {
        await using var app = Build(args);
        await app.Services.GetRequiredService<MigrationRunner>().MigrateAsync(CancellationToken.None);
        await app.RunAsync();
    }
}
