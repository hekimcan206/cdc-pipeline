using System.ComponentModel.DataAnnotations;

namespace CdcPipeline.Consumer;

public sealed class ConsumerOptions
{
    public const string Section = "Consumer";

    [Required]
    public string KafkaBootstrap { get; set; } = "localhost:59092";

    [Required]
    public string GroupId { get; set; } = "cdc-read-model";

    /// <summary>Regex subscription: data topics of both pipelines plus their transaction topics.</summary>
    [Required]
    public string TopicPattern { get; set; } = @"^(pg|ora)\..+";

    [Required]
    public string ReadModelConnectionString { get; set; } = "";

    /// <summary>Contract version to project with (<c>v1</c> or <c>v2</c>); see <c>ShopContracts</c>.</summary>
    public string ContractVersion { get; set; } = "v2";

    [Range(1, 10_000)]
    public int MaxBatchSize { get; set; } = 500;

    /// <summary>Retry delay after a transient read-model failure; the batch is kept and retried, never skipped.</summary>
    [Range(10, 60_000)]
    public int RetryDelayMs { get; set; } = 1000;

    public SearchOptions Search { get; set; } = new();

    public FaultInjectionOptions FaultInjection { get; set; } = new();
}

public sealed class SearchOptions
{
    public bool Enabled { get; set; } = true;

    public string Url { get; set; } = "http://localhost:57700";

    public string? ApiKey { get; set; }

    [Range(1, 10_000)]
    public int BatchSize { get; set; } = 1000;

    /// <summary>Idle poll interval of the indexer; it is also woken directly after every applied batch.</summary>
    [Range(10, 60_000)]
    public int PollIntervalMs { get; set; } = 250;

    /// <summary>
    /// How long a row waits for its source transaction to be reported complete before it is indexed anyway (for
    /// example when the END marker was lost). Forced pushes are counted in the metrics.
    /// </summary>
    [Range(0, 600_000)]
    public int TransactionWaitMs { get; set; } = 5000;
}

/// <summary>Test-only hooks. Ignored unless <c>Enabled</c> is set, which only the crash tests and benchmarks do.</summary>
public sealed class FaultInjectionOptions
{
    public bool Enabled { get; set; }

    /// <summary>
    /// Kill the process (SIGKILL / TerminateProcess, no shutdown hooks) after the read-model transaction of every
    /// Nth batch has committed but before the broker offsets are committed: the worst moment for at-least-once.
    /// </summary>
    public int KillAfterDbCommitEveryNBatches { get; set; }
}
