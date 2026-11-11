using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using CdcPipeline.Bench;
using CdcPipeline.TestKit;

// Usage: dotnet run -c Release --project benchmarks/CdcPipeline.Bench -- <pg|oracle> [--rates 50,200,500]
//        [--seconds 20] [--backlog-rate 300] [--backlog-seconds 30] [--out benchmark-results]
var source = args.Length > 0 && args[0] is "oracle" or "ora" ? SourceKind.Oracle : SourceKind.Postgres;
var options = BenchOptions.Parse(args.Skip(1).ToArray(), source);
var ct = CancellationToken.None;

Console.WriteLine($"cdc-pipeline benchmark: source={source} rates={string.Join(",", options.Rates)} ops/s x {options.Seconds}s, backlog {options.BacklogRate} ops/s x {options.BacklogSeconds}s");
var environment = Bench.Environment();
Console.WriteLine(JsonSerializer.Serialize(environment));

var clock = Stopwatch.StartNew();
await using var stack = await PipelineEnvironment.StartAsync(source, ct);
var startup = clock.Elapsed;
Console.WriteLine($"stack ready in {startup.TotalSeconds:0.0} s");

var writer = ShopWriter.For(stack);
var results = new Dictionary<string, object>
{
    ["source"] = stack.SourceName,
    ["environment"] = environment,
    ["stackStartupSeconds"] = Math.Round(startup.TotalSeconds, 1),
};

// 1. Lag at several offered loads, one consumer, steady state.
var readModel = await stack.CreateReadModelDatabaseAsync("bench_lag", ct);
var probe = new ReadModelProbe(readModel, stack.SourceName);
var lagRuns = new List<LagResult>();
await using (await Bench.StartConsumerAsync(stack, readModel, "bench_lag"))
{
    // Warm-up: JIT, connection pools, topic creation and the initial Meilisearch index setup.
    var warm = new ShopWorkload(writer, 90_000_000);
    await warm.RunAsync(100, 50, ct);
    await probe.WaitForConvergenceAsync(writer, warm.FirstId, warm.LastId, TimeSpan.FromMinutes(3), ct);

    var firstId = 10_000_000L;
    foreach (var rate in options.Rates)
    {
        var run = await Bench.MeasureLagAsync(stack, writer, probe, firstId, rate, options.Seconds, ct);
        lagRuns.Add(run);
        Console.WriteLine(run.ToMarkdownRow());
        firstId += 10_000_000;
    }
}

results["lag"] = lagRuns;

// 2. Consumer downtime: the source keeps writing while no consumer runs, then the consumer starts against the
//    backlog. Catch-up time is consumer start -> read model equal to the source; drain throughput is the rate at
//    which it applied the backlog.
var backlog = await Bench.MeasureCatchUpAsync(stack, writer, options.BacklogRate, options.BacklogSeconds, ct);
Console.WriteLine(JsonSerializer.Serialize(backlog));
results["catchUp"] = backlog;

results["memoryAfterRunMiB"] = await Bench.ContainerMemoryAsync(stack);
Console.WriteLine(JsonSerializer.Serialize(results["memoryAfterRunMiB"]));

Directory.CreateDirectory(options.OutputDirectory);
var file = Path.Combine(options.OutputDirectory, $"{stack.SourceName}.json");
await File.WriteAllTextAsync(file, JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }), ct);
Console.WriteLine($"wrote {file}");

namespace CdcPipeline.Bench
{
    internal sealed record BenchOptions(int[] Rates, int Seconds, int BacklogRate, int BacklogSeconds, string OutputDirectory)
    {
        public static BenchOptions Parse(string[] args, SourceKind source)
        {
            string? Get(string name) => args.SkipWhile(a => a != name).Skip(1).FirstOrDefault();
            var rates = Get("--rates")?.Split(',').Select(r => int.Parse(r, CultureInfo.InvariantCulture)).ToArray()
                ?? (source == SourceKind.Postgres ? [50, 200, 500] : [25, 100, 200]);
            return new BenchOptions(
                rates,
                int.Parse(Get("--seconds") ?? "20", CultureInfo.InvariantCulture),
                int.Parse(Get("--backlog-rate") ?? (source == SourceKind.Postgres ? "300" : "150"), CultureInfo.InvariantCulture),
                int.Parse(Get("--backlog-seconds") ?? "30", CultureInfo.InvariantCulture),
                Get("--out") ?? "benchmark-results");
        }
    }

    internal sealed record Percentiles(double P50, double P95, double P99, double Max)
    {
        public static Percentiles Of(List<double> values)
        {
            values.Sort();
            double At(double q) => values.Count == 0 ? double.NaN : values[Math.Min(values.Count - 1, (int)Math.Ceiling(q * values.Count) - 1)];
            return new Percentiles(Math.Round(At(0.50), 1), Math.Round(At(0.95), 1), Math.Round(At(0.99), 1), Math.Round(values.Count == 0 ? double.NaN : values[^1], 1));
        }
    }

    internal sealed record LagResult(int OfferedOpsPerSecond, int Seconds, long Operations, long RowChanges, double AchievedRowChangesPerSecond,
        int MeasuredRows, Percentiles ReadModelLagMs, Percentiles IndexLagMs)
    {
        public string ToMarkdownRow() => string.Create(CultureInfo.InvariantCulture,
            $"| {OfferedOpsPerSecond} ops/s | {AchievedRowChangesPerSecond:0} changes/s | {MeasuredRows} rows | rm p50 {ReadModelLagMs.P50} p95 {ReadModelLagMs.P95} p99 {ReadModelLagMs.P99} max {ReadModelLagMs.Max} ms | index p50 {IndexLagMs.P50} p95 {IndexLagMs.P95} p99 {IndexLagMs.P99} max {IndexLagMs.Max} ms |");
    }

    internal sealed record CatchUpResult(int OfferedOpsPerSecond, int DowntimeSeconds, long RowChanges, long BacklogEvents,
        double ReadModelCatchUpSeconds, double IndexCatchUpSeconds, double DrainEventsPerSecond);

    internal static class Bench
    {
        public static Dictionary<string, string> Environment() => new()
        {
            ["os"] = RuntimeInformation.OSDescription,
            ["cpu"] = $"{System.Environment.ProcessorCount} logical cores",
            ["dotnet"] = RuntimeInformation.FrameworkDescription,
            ["config"] =
#if DEBUG
                "Debug",
#else
                "Release",
#endif
            ["date"] = DateTime.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        };

        public static async Task<ConsumerInstance> StartConsumerAsync(PipelineEnvironment stack, string readModel, string name) =>
            await ConsumerInstance.StartAsync(ConsumerSettings.For(stack, readModel, name));

        public static async Task<LagResult> MeasureLagAsync(PipelineEnvironment stack, ShopWriter writer, ReadModelProbe probe,
            long firstId, int rate, int seconds, CancellationToken ct)
        {
            var workload = new ShopWorkload(writer, firstId, seed: rate);
            var clock = Stopwatch.StartNew();
            await workload.RunAsync(rate * seconds, rate, ct);
            var elapsed = clock.Elapsed.TotalSeconds;
            await probe.WaitForConvergenceAsync(writer, workload.FirstId, workload.LastId, TimeSpan.FromMinutes(5), ct);
            await Waiter.UntilAsync(async () => await probe.DirtyRowsAsync(ct) == 0, TimeSpan.FromMinutes(2), "index", ct);

            // Lag per row, from the row's own source timestamp (stamped by the source database clock at the
            // statement, i.e. just before commit) to the read-model clock when the change was applied / indexed.
            // All containers share the Docker VM kernel clock.
            var (rm, index) = await LagsAsync(probe, stack.SourceName, workload.FirstId, workload.LastId, ct);
            return new LagResult(rate, seconds, workload.Operations, workload.RowChanges, Math.Round(workload.RowChanges / elapsed, 1),
                rm.Count, Percentiles.Of(rm), Percentiles.Of(index));
        }

        private static async Task<(List<double> ReadModel, List<double> Index)> LagsAsync(ReadModelProbe probe, string source, long from, long to, CancellationToken ct)
        {
            var rm = new List<double>();
            var index = new List<double>();
            await using var conn = new Npgsql.NpgsqlConnection(probe.ConnectionString);
            await conn.OpenAsync(ct);
            await using var cmd = new Npgsql.NpgsqlCommand("""
                SELECT extract(epoch FROM applied_at - updated_at) * 1000, extract(epoch FROM indexed_at - updated_at) * 1000
                FROM (SELECT applied_at, indexed_at, updated_at FROM rm_customer WHERE source = $1 AND id BETWEEN $2 AND $3 AND NOT deleted
                      UNION ALL SELECT applied_at, indexed_at, updated_at FROM rm_order WHERE source = $1 AND id BETWEEN $2 AND $3 AND NOT deleted
                      UNION ALL SELECT applied_at, indexed_at, updated_at FROM rm_order_line WHERE source = $1 AND order_id BETWEEN $2 AND $3 AND NOT deleted) x
                """, conn);
            cmd.Parameters.Add(new Npgsql.NpgsqlParameter { Value = source });
            cmd.Parameters.Add(new Npgsql.NpgsqlParameter { Value = from });
            cmd.Parameters.Add(new Npgsql.NpgsqlParameter { Value = to });
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                rm.Add((double)reader.GetDecimal(0));
                if (!reader.IsDBNull(1))
                {
                    index.Add((double)reader.GetDecimal(1));
                }
            }

            return (rm, index);
        }

        public static async Task<CatchUpResult> MeasureCatchUpAsync(PipelineEnvironment stack, ShopWriter writer, int rate, int seconds, CancellationToken ct)
        {
            var readModel = await stack.CreateReadModelDatabaseAsync("bench_catchup", ct);
            var probe = new ReadModelProbe(readModel, stack.SourceName);

            // Bring a consumer group up to date first, so the backlog is only what is written during the downtime.
            await using (await StartConsumerAsync(stack, readModel, "bench_catchup"))
            {
                await Waiter.UntilAsync(async () => await TopicsSettledAsync(stack, ct) && await probe.DirtyRowsAsync(ct) == 0
                    && await IdleAsync(probe, ct), TimeSpan.FromMinutes(5), "catch-up consumer warm", ct);
            }

            var appliedBefore = await probe.CounterAsync("events_applied", ct);
            var workload = new ShopWorkload(writer, 70_000_000, seed: 99);
            await workload.RunAsync(rate * seconds, rate, ct);
            await Waiter.UntilAsync(() => TopicsSettledAsync(stack, ct), TimeSpan.FromMinutes(5), "Debezium to publish the backlog", ct);

            // Timestamps come from the read-model database clock (start, last applied row, last indexed row), so the
            // convergence polling below only decides when to stop looking; its 1 s interval does not round the result.
            var started = await probe.ScalarAsync<DateTime>("SELECT clock_timestamp()", ct);
            await using (await StartConsumerAsync(stack, readModel, "bench_catchup"))
            {
                await PollUntilAsync(async () => (await probe.ReconcileAsync(writer, workload.FirstId, workload.LastId, false, ct)).Count == 0, "read model catch-up", ct);
                await PollUntilAsync(async () => await probe.DirtyRowsAsync(ct) == 0, "index catch-up", ct);
            }

            var window = $"BETWEEN {workload.FirstId} AND {workload.LastId}";
            var lastApplied = await probe.ScalarAsync<DateTime>(
                $"SELECT max(t) FROM (SELECT max(applied_at) t FROM rm_customer WHERE id {window} UNION ALL SELECT max(applied_at) FROM rm_order WHERE id {window} UNION ALL SELECT max(applied_at) FROM rm_order_line WHERE order_id {window}) x", ct);
            var lastIndexed = await probe.ScalarAsync<DateTime>(
                $"SELECT max(t) FROM (SELECT max(indexed_at) t FROM rm_customer WHERE id {window} UNION ALL SELECT max(indexed_at) FROM rm_order WHERE id {window} UNION ALL SELECT max(indexed_at) FROM rm_order_line WHERE order_id {window}) x", ct);
            var rmSeconds = (lastApplied - started).TotalSeconds;
            var indexSeconds = (lastIndexed - started).TotalSeconds;

            var backlogEvents = await probe.CounterAsync("events_applied", ct) - appliedBefore;
            return new CatchUpResult(rate, seconds, workload.RowChanges, backlogEvents, Math.Round(rmSeconds, 2), Math.Round(indexSeconds, 2),
                Math.Round(backlogEvents / rmSeconds, 0));
        }

        private static async Task PollUntilAsync(Func<Task<bool>> condition, string what, CancellationToken ct)
        {
            var deadline = DateTime.UtcNow.AddMinutes(10);
            while (!await condition())
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"Timed out waiting for {what}.");
                }

                await Task.Delay(1000, ct);
            }
        }

        private static async Task<bool> IdleAsync(ReadModelProbe probe, CancellationToken ct)
        {
            var before = await probe.CounterAsync("batches", ct);
            await Task.Delay(1500, ct);
            return await probe.CounterAsync("batches", ct) == before;
        }

        private static long _lastHighWatermarkSum = -1;

        /// <summary>True once the sum of all topic high watermarks has not moved for two seconds.</summary>
        private static async Task<bool> TopicsSettledAsync(PipelineEnvironment stack, CancellationToken ct)
        {
            var sum = await HighWatermarkSumAsync(stack.KafkaBootstrap, stack.SourceName);
            var settled = sum == _lastHighWatermarkSum;
            _lastHighWatermarkSum = sum;
            if (!settled)
            {
                await Task.Delay(2000, ct);
            }

            return settled;
        }

        private static Task<long> HighWatermarkSumAsync(string bootstrap, string prefix) => Task.Run(() =>
        {
            using var admin = new Confluent.Kafka.AdminClientBuilder(new Confluent.Kafka.AdminClientConfig { BootstrapServers = bootstrap }).Build();
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
            using var consumer = new Confluent.Kafka.ConsumerBuilder<Confluent.Kafka.Ignore, Confluent.Kafka.Ignore>(
                new Confluent.Kafka.ConsumerConfig { BootstrapServers = bootstrap, GroupId = "bench-watermarks" }).Build();
            long sum = 0;
            foreach (var topic in metadata.Topics.Where(t => t.Topic.StartsWith(prefix + ".", StringComparison.Ordinal)))
            {
                foreach (var partition in topic.Partitions)
                {
                    sum += consumer.QueryWatermarkOffsets(new Confluent.Kafka.TopicPartition(topic.Topic, partition.PartitionId), TimeSpan.FromSeconds(10)).High.Value;
                }
            }

            return sum;
        });

        public static async Task<Dictionary<string, double>> ContainerMemoryAsync(PipelineEnvironment stack)
        {
            var result = new Dictionary<string, double>();
            foreach (var (role, id) in stack.ContainerIds)
            {
                var info = new ProcessStartInfo("docker", ["stats", "--no-stream", "--format", "{{.MemUsage}}", id]) { RedirectStandardOutput = true };
                using var process = Process.Start(info)!;
                var output = (await process.StandardOutput.ReadToEndAsync()).Trim();
                await process.WaitForExitAsync();
                result[role] = ParseMiB(output.Split('/')[0].Trim());
            }

            return result;
        }

        private static double ParseMiB(string value)
        {
            var units = new (string Suffix, double Factor)[] { ("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024.0), ("B", 1 / 1048576.0) };
            foreach (var (suffix, factor) in units)
            {
                if (value.EndsWith(suffix, StringComparison.Ordinal))
                {
                    return Math.Round(double.Parse(value[..^suffix.Length], CultureInfo.InvariantCulture) * factor, 1);
                }
            }

            return double.NaN;
        }
    }
}
