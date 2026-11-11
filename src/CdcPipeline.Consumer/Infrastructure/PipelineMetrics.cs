using System.Diagnostics.Metrics;

namespace CdcPipeline.Consumer.Infrastructure;

/// <summary>OpenTelemetry instruments for the pipeline. Names follow the <c>cdc.*</c> namespace.</summary>
public sealed class PipelineMetrics : IDisposable
{
    public const string MeterName = "CdcPipeline.Consumer";

    private readonly Meter _meter;

    public PipelineMetrics(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create(MeterName);
        EventsApplied = _meter.CreateCounter<long>("cdc.events.applied", description: "Changes that moved a read-model row forward.");
        EventsSkipped = _meter.CreateCounter<long>("cdc.events.skipped", description: "Redelivered or replayed changes absorbed by the position guard.");
        Batches = _meter.CreateCounter<long>("cdc.batches", description: "Read-model transactions committed.");
        BatchSize = _meter.CreateHistogram<int>("cdc.batch.size", unit: "{event}");
        ApplyLag = _meter.CreateHistogram<double>("cdc.lag.read_model", unit: "ms",
            description: "Source commit timestamp to read-model commit (Oracle LogMiner timestamps have 1 s resolution).");
        Indexed = _meter.CreateCounter<long>("cdc.index.documents", description: "Documents pushed to the search index.");
        ForcedIndexed = _meter.CreateCounter<long>("cdc.index.forced",
            description: "Documents indexed before their source transaction was reported complete (END marker late or missing).");
        Halts = _meter.CreateCounter<long>("cdc.halts", description: "Times the consumer stopped on an event it must not skip.");
    }

    public Counter<long> EventsApplied { get; }

    public Counter<long> EventsSkipped { get; }

    public Counter<long> Batches { get; }

    public Histogram<int> BatchSize { get; }

    public Histogram<double> ApplyLag { get; }

    public Counter<long> Indexed { get; }

    public Counter<long> ForcedIndexed { get; }

    public Counter<long> Halts { get; }

    public void Dispose() => _meter.Dispose();
}
