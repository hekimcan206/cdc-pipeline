namespace CdcPipeline.Consumer.Pipeline;

public enum ConsumerStatus
{
    Starting,
    Running,
    /// <summary>Stopped on an event it must not skip; needs a contract or source fix and a restart.</summary>
    Halted,
}

/// <summary>Live state shared between the consumer loop, the indexer and the HTTP endpoints.</summary>
public sealed class PipelineState
{
    private readonly Lock _gate = new();
    private long _applied;
    private long _skipped;
    private long _unmapped;
    private long _batches;
    private long _indexed;
    private long _forcedIndexed;

    public ConsumerStatus Status { get; private set; } = ConsumerStatus.Starting;

    public string? HaltReason { get; private set; }

    public string? HaltedAt { get; private set; }

    public DateTimeOffset? LastBatchAt { get; private set; }

    public long Applied => Interlocked.Read(ref _applied);

    public long Skipped => Interlocked.Read(ref _skipped);

    public long Unmapped => Interlocked.Read(ref _unmapped);

    public long Batches => Interlocked.Read(ref _batches);

    public long Indexed => Interlocked.Read(ref _indexed);

    public long ForcedIndexed => Interlocked.Read(ref _forcedIndexed);

    public void MarkRunning()
    {
        lock (_gate)
        {
            if (Status == ConsumerStatus.Starting)
            {
                Status = ConsumerStatus.Running;
            }
        }
    }

    public void Halt(string reason, string coordinates)
    {
        lock (_gate)
        {
            Status = ConsumerStatus.Halted;
            HaltReason = reason;
            HaltedAt = coordinates;
        }
    }

    public void RecordBatch(int applied, int skipped, int unmapped, DateTimeOffset at)
    {
        Interlocked.Add(ref _applied, applied);
        Interlocked.Add(ref _skipped, skipped);
        Interlocked.Add(ref _unmapped, unmapped);
        Interlocked.Increment(ref _batches);
        LastBatchAt = at;
    }

    public void RecordIndexed(int documents, int forced)
    {
        Interlocked.Add(ref _indexed, documents);
        Interlocked.Add(ref _forcedIndexed, forced);
    }
}
