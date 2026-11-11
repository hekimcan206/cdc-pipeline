using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using CdcPipeline.Consumer;
using CdcPipeline.Consumer.Pipeline;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace CdcPipeline.TestKit;

/// <summary>Settings for one consumer: its own read-model database, consumer group and index prefix.</summary>
public sealed record ConsumerSettings(string KafkaBootstrap, string ReadModelConnectionString, string GroupId, string MeiliUrl, string IndexPrefix)
{
    public string ContractVersion { get; init; } = "v2";

    public int MaxBatchSize { get; init; } = 500;

    public int TransactionWaitMs { get; init; } = 5000;

    public int KillAfterDbCommitEveryNBatches { get; init; }

    public static ConsumerSettings For(PipelineEnvironment stack, string readModel, string name) =>
        new(stack.KafkaBootstrap, readModel, $"group-{name}", stack.MeiliUrl, $"{name}_");

    public Dictionary<string, string?> ToConfiguration() => new()
    {
        ["Consumer:KafkaBootstrap"] = KafkaBootstrap,
        ["Consumer:ReadModelConnectionString"] = ReadModelConnectionString,
        ["Consumer:GroupId"] = GroupId,
        ["Consumer:ContractVersion"] = ContractVersion,
        ["Consumer:MaxBatchSize"] = MaxBatchSize.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ["Consumer:RetryDelayMs"] = "200",
        ["Consumer:Search:Url"] = MeiliUrl,
        ["Consumer:Search:ApiKey"] = PipelineEnvironment.MeiliKey,
        ["Consumer:Search:IndexPrefix"] = IndexPrefix,
        ["Consumer:Search:PollIntervalMs"] = "100",
        ["Consumer:Search:TransactionWaitMs"] = TransactionWaitMs.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ["Consumer:FaultInjection:Enabled"] = KillAfterDbCommitEveryNBatches > 0 ? "true" : "false",
        ["Consumer:FaultInjection:KillAfterDbCommitEveryNBatches"] = KillAfterDbCommitEveryNBatches.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ["Serilog:MinimumLevel:Default"] = "Warning",
        ["Serilog:MinimumLevel:Override:CdcPipeline.Consumer.Pipeline.ChangeConsumer"] = "Information",
    };
}

/// <summary>The consumer running inside the test process (same code path as <c>Program</c>).</summary>
public sealed class ConsumerInstance : IAsyncDisposable
{
    private readonly WebApplication _app;

    private ConsumerInstance(WebApplication app, Uri baseAddress)
    {
        _app = app;
        Http = new HttpClient { BaseAddress = baseAddress };
    }

    public HttpClient Http { get; }

    public PipelineState State => _app.Services.GetRequiredService<PipelineState>();

    public static async Task<ConsumerInstance> StartAsync(ConsumerSettings settings)
    {
        var app = ConsumerApp.Build([], b =>
        {
            b.Configuration.AddInMemoryCollection(settings.ToConfiguration());
            b.WebHost.UseUrls("http://127.0.0.1:0");
        });
        await ConsumerApp.StartAsync(app);
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new ConsumerInstance(app, new Uri(address + "/"));
    }

    public async ValueTask DisposeAsync()
    {
        Http.Dispose();
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}

/// <summary>
/// The consumer as a separate OS process, so it can be killed for real (SIGKILL / TerminateProcess): no
/// shutdown hooks, no offset commit, no graceful consumer-group leave.
/// </summary>
public sealed class ConsumerProcess : IDisposable
{
    private readonly Process _process;

    private ConsumerProcess(Process process, int port)
    {
        _process = process;
        Http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}/"), Timeout = TimeSpan.FromSeconds(5) };
    }

    public HttpClient Http { get; }

    public bool HasExited => _process.HasExited;

    public static ConsumerProcess Start(ConsumerSettings settings, int port)
    {
        var dll = Path.Combine(AppContext.BaseDirectory, "CdcPipeline.Consumer.dll");
        var info = new ProcessStartInfo("dotnet", [dll])
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = AppContext.BaseDirectory,
        };
        foreach (var (key, value) in settings.ToConfiguration())
        {
            info.Environment[key.Replace(":", "__", StringComparison.Ordinal)] = value;
        }

        info.Environment["ASPNETCORE_URLS"] = $"http://127.0.0.1:{port}";
        info.Environment["DOTNET_ENVIRONMENT"] = "Production";
        var process = Process.Start(info)!;
        // Drain the pipes (a full buffer would block the child) into a log file; CDC_CONSUMER_LOG overrides the path.
        var log = Environment.GetEnvironmentVariable("CDC_CONSUMER_LOG") ?? Path.Combine(Path.GetTempPath(), "cdc-pipeline-consumer.log");
        var gate = new object();
        void Append(string? line)
        {
            if (line is null)
            {
                return;
            }

            lock (gate)
            {
                File.AppendAllText(log, line + Environment.NewLine);
            }
        }

        process.OutputDataReceived += (_, e) => Append(e.Data);
        process.ErrorDataReceived += (_, e) => Append(e.Data);
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        return new ConsumerProcess(process, port);
    }

    public async Task<JsonElement?> StatsAsync()
    {
        try
        {
            return await Http.GetFromJsonAsync<JsonElement>("stats");
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            return null;
        }
    }

    public void Kill()
    {
        if (!_process.HasExited)
        {
            _process.Kill(entireProcessTree: true);
        }

        _process.WaitForExit(10_000);
    }

    public Task WaitForExitAsync(CancellationToken ct) => _process.WaitForExitAsync(ct);

    public void Dispose()
    {
        Kill();
        Http.Dispose();
        _process.Dispose();
    }
}
