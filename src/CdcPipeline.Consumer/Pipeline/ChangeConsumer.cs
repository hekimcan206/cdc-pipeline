using System.Diagnostics;
using CdcPipeline.Consumer.Infrastructure;
using CdcPipeline.Consumer.Persistence;
using CdcPipeline.Consumer.Search;
using CdcPipeline.Core.Events;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Npgsql;

namespace CdcPipeline.Consumer.Pipeline;

/// <summary>
/// The at-least-once loop: poll a batch, apply it to the read model in one transaction, and only then commit the
/// broker offsets. A crash anywhere before the offset commit makes the broker redeliver the batch; the position guard
/// in the read model turns the redelivery into no-ops. Per-key order holds because Debezium keys every record by
/// primary key, the broker keeps each key in one partition, and a batch is applied in partition order.
/// </summary>
internal sealed partial class ChangeConsumer(
    IOptions<ConsumerOptions> options,
    ReadModelWriter writer,
    PipelineState state,
    IndexerSignal indexerSignal,
    PipelineMetrics metrics,
    TimeProvider time,
    ILogger<ChangeConsumer> logger) : BackgroundService
{
    private readonly ConsumerOptions _options = options.Value;

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        // librdkafka's Consume blocks; give the loop its own thread instead of a thread-pool worker.
        Task.Factory.StartNew(() => RunAsync(stoppingToken), stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private async Task RunAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.KafkaBootstrap,
            GroupId = _options.GroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // Regex subscriptions only see topics after a metadata refresh; Debezium creates topics lazily.
            TopicMetadataRefreshIntervalMs = 2000,
            SessionTimeoutMs = 10_000,
            HeartbeatIntervalMs = 2_000,
            MaxPollIntervalMs = 300_000,
            ClientId = $"cdc-consumer-{Environment.MachineName}",
        };

        using var consumer = new ConsumerBuilder<byte[]?, byte[]?>(config)
            .SetErrorHandler((_, error) => LogBrokerError(error.Code, error.Reason, error.IsFatal))
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                state.SetAssignedPartitions(partitions.Count);
                LogAssigned(partitions.Count);
            })
            .SetPartitionsRevokedHandler((_, _) => state.SetAssignedPartitions(0))
            .Build();
        consumer.Subscribe(_options.TopicPattern);
        state.MarkRunning();
        LogStarted(_options.TopicPattern, _options.GroupId, writer.Projections.Count);

        var batchNumber = 0L;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var batch = Poll(consumer, stoppingToken);
                if (batch.Count == 0)
                {
                    continue;
                }

                if (!await ApplyWithRetryAsync(batch, stoppingToken))
                {
                    break; // halted
                }

                batchNumber++;
                if (_options.FaultInjection is { Enabled: true, KillAfterDbCommitEveryNBatches: > 0 } fault
                    && batchNumber % fault.KillAfterDbCommitEveryNBatches == 0)
                {
                    LogFaultInjection(batchNumber);
                    Process.GetCurrentProcess().Kill(); // SIGKILL / TerminateProcess: offsets stay uncommitted
                }

                CommitOffsets(consumer, batch);
                indexerSignal.Wake();
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Shutdown: whatever was not committed is redelivered to the next owner.
        }
        finally
        {
            consumer.Close();
        }
    }

    private List<ConsumeResult<byte[]?, byte[]?>> Poll(IConsumer<byte[]?, byte[]?> consumer, CancellationToken ct)
    {
        var batch = new List<ConsumeResult<byte[]?, byte[]?>>(_options.MaxBatchSize);
        try
        {
            var first = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (first is null || first.IsPartitionEOF)
            {
                return batch;
            }

            batch.Add(first);
            // Drain what librdkafka has already prefetched, without waiting: small batches under light load (low
            // latency), large ones under backlog (high throughput).
            while (batch.Count < _options.MaxBatchSize && consumer.Consume(TimeSpan.Zero) is { IsPartitionEOF: false } next)
            {
                batch.Add(next);
            }
        }
        catch (ConsumeException ex) when (!ex.Error.IsFatal)
        {
            LogBrokerError(ex.Error.Code, ex.Error.Reason, false);
        }

        ct.ThrowIfCancellationRequested();
        return batch;
    }

    /// <returns><c>false</c> when the consumer halted.</returns>
    private async Task<bool> ApplyWithRetryAsync(List<ConsumeResult<byte[]?, byte[]?>> batch, CancellationToken ct)
    {
        var parsed = new List<ParsedRecord>(batch.Count);
        foreach (var record in batch)
        {
            try
            {
                parsed.Add(DebeziumParser.Parse(record.Topic, record.Message.Key, record.Message.Value));
            }
            catch (MalformedEventException ex)
            {
                Halt($"malformed event: {ex.Message}", record);
                return false;
            }
        }

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var outcome = await writer.ApplyAsync(parsed, ct);
                Record(outcome, batch.Count);
                return true;
            }
            catch (RecordRejectedException ex)
            {
                var violation = ex.Violation;
                Halt($"contract {writer.ContractVersion} violation ({violation.Kind}): {violation.Message}", batch[ex.RecordIndex]);
                return false;
            }
            catch (PostgresException ex) when (!ex.IsTransient)
            {
                // Data the read model refuses (overflow, constraint): skipping it would silently diverge.
                Halt($"read model rejected the batch: {ex.SqlState} {ex.MessageText}", batch[0]);
                return false;
            }
            catch (Exception ex) when (ex is NpgsqlException or TimeoutException or System.Net.Sockets.SocketException)
            {
                LogTransient(attempt, ex.Message);
                await Task.Delay(_options.RetryDelayMs, ct);
            }
        }
    }

    private void Record(BatchOutcome outcome, int size)
    {
        state.RecordBatch(outcome.Applied, outcome.Skipped, outcome.Unmapped, time.GetUtcNow());
        metrics.Batches.Add(1);
        metrics.BatchSize.Record(size);
        metrics.EventsApplied.Add(outcome.Applied);
        metrics.EventsSkipped.Add(outcome.Skipped);
        foreach (var lag in outcome.AppliedLags)
        {
            metrics.ApplyLag.Record(lag.TotalMilliseconds);
        }

        if (outcome.Skipped > 0)
        {
            LogSkipped(outcome.Skipped, size);
        }
    }

    private static void CommitOffsets(IConsumer<byte[]?, byte[]?> consumer, List<ConsumeResult<byte[]?, byte[]?>> batch)
    {
        var next = batch
            .GroupBy(r => r.TopicPartition)
            .Select(g => new TopicPartitionOffset(g.Key, new Offset(g.Max(r => r.Offset.Value) + 1)))
            .ToList();
        try
        {
            consumer.Commit(next);
        }
        catch (KafkaException)
        {
            // Lost the partitions in a rebalance: the batch is redelivered to the new owner and absorbed there.
        }
    }

    private void Halt(string reason, ConsumeResult<byte[]?, byte[]?> record)
    {
        var coordinates = new BrokerCoordinates(record.Topic, record.Partition.Value, record.Offset.Value).ToString();
        state.Halt(reason, coordinates);
        metrics.Halts.Add(1);
        LogHalted(coordinates, reason);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Consuming {Pattern} as group {GroupId} with {Entities} entity projections")]
    private partial void LogStarted(string pattern, string groupId, int entities);

    [LoggerMessage(Level = LogLevel.Information, Message = "Assigned {Count} partitions")]
    private partial void LogAssigned(int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker error {Code}: {Reason} (fatal: {Fatal})")]
    private partial void LogBrokerError(ErrorCode code, string reason, bool fatal);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Transient read-model failure, retrying the same batch (attempt {Attempt}): {Message}")]
    private partial void LogTransient(int attempt, string message);

    [LoggerMessage(Level = LogLevel.Information, Message = "Absorbed {Skipped} of {Size} redelivered or replayed changes")]
    private partial void LogSkipped(int skipped, int size);

    [LoggerMessage(Level = LogLevel.Critical, Message = "Consumer HALTED at {Coordinates}: {Reason}. Offsets are not committed; fix and restart to resume from here.")]
    private partial void LogHalted(string coordinates, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Fault injection: killing the process after the read-model commit of batch {Batch}, before the offset commit")]
    private partial void LogFaultInjection(long batch);
}
