using System.Net;
using System.Net.Sockets;
using CdcPipeline.IntegrationTests.Infrastructure;
using CdcPipeline.TestKit;

namespace CdcPipeline.IntegrationTests;

/// <summary>
/// The consumer is killed over and over while the source keeps writing: by itself, right after a read-model commit
/// and before the offset commit (the worst moment), and by the test at random moments. Debezium Server is killed and
/// restarted once mid-run as well, which replays source events from its last flushed offset. Afterwards the read model must
/// equal the source row for row, and every row must have been applied exactly as many times as it changed.
/// </summary>
[Collection(PostgresPipelineGroup.Name)]
public sealed class CrashRecoveryTests(PostgresPipelineFixture fixture, ITestOutputHelper output)
{
    [Fact]
    public async Task Repeated_hard_kills_lose_nothing_and_apply_nothing_twice()
    {
        var ct = TestContext.Current.CancellationToken;
        var stack = fixture.Stack;
        var name = fixture.NextName("crash");
        var readModel = await stack.CreateReadModelDatabaseAsync(name, ct);
        var writer = ShopWriter.For(stack);
        var probe = new ReadModelProbe(readModel, "pg");
        var workload = new ShopWorkload(writer, firstId: 3_000_000, seed: 7);

        // Small batches and a self-kill every 12th batch. Every restart pays a consumer-group rejoin (the dead member is only
        // evicted after the session timeout), so killing more often mostly measures rejoin time, not correctness.
        var crashing = ConsumerSettings.For(stack, readModel, name) with { MaxBatchSize = 25, KillAfterDbCommitEveryNBatches = 12 };
        var random = new Random(11);
        var restarts = 0;
        var externalKills = 0;
        await stack.PositionGroupAtEndAsync(crashing.GroupId);
        var consumer = ConsumerProcess.Start(crashing, FreePort());
        try
        {
            var load = workload.RunAsync(1500, perSecond: 150, ct);
            var debeziumRestarted = false;
            var started = DateTime.UtcNow;
            while (!load.IsCompleted)
            {
                await Task.Delay(random.Next(300, 1200), ct);
                if (consumer.HasExited)
                {
                    consumer.Dispose();
                    consumer = ConsumerProcess.Start(crashing, FreePort());
                    restarts++;
                }
                else if (random.NextDouble() < 0.1)
                {
                    consumer.Kill();
                    externalKills++;
                }

                if (!debeziumRestarted && DateTime.UtcNow - started > TimeSpan.FromSeconds(4))
                {
                    await stack.CrashDebeziumAsync(ct);
                    debeziumRestarted = true;
                }
            }

            await load;

            // Keep the crash-prone consumer running (and restarting) until it has caught up with the source.
            var deadline = DateTime.UtcNow.AddSeconds(120);
            while ((await probe.ReconcileAsync(writer, workload.FirstId, workload.LastId, checkApplyCount: false, ct)).Count > 0)
            {
                if (DateTime.UtcNow > deadline)
                {
                    var remaining = await probe.ReconcileAsync(writer, workload.FirstId, workload.LastId, checkApplyCount: false, ct);
                    var (logs, _) = await stack.DebeziumLogsAsync();
                    throw new TimeoutException(
                        $"Not converged 120 s after the load stopped: {remaining.Count} discrepancies, first {string.Join("; ", remaining.Take(3))}. " +
                        $"Topics: {string.Join(",", await stack.ListTopicsAsync())}\nDebezium:\n{string.Join("\n", logs.Split('\n').TakeLast(25))}");
                }

                if (consumer.HasExited)
                {
                    consumer.Dispose();
                    consumer = ConsumerProcess.Start(crashing, FreePort());
                    restarts++;
                }

                await Task.Delay(500, ct);
            }
        }
        finally
        {
            consumer.Dispose();
        }

        // A clean consumer on the same group drains whatever the last crash left uncommitted and finishes indexing.
        using (var clean = ConsumerProcess.Start(crashing with { KillAfterDbCommitEveryNBatches = 0 }, FreePort()))
        {
            await Waiter.UntilAsync(async () => await probe.DirtyRowsAsync(ct) == 0, TimeSpan.FromSeconds(60), "index to catch up", ct);
            await Task.Delay(2000, ct);
        }

        var problems = await probe.ReconcileAsync(writer, workload.FirstId, workload.LastId, checkApplyCount: true, ct);
        var skipped = await probe.CounterAsync("events_skipped", ct);
        output.WriteLine($"operations={workload.Operations} rowChanges={workload.RowChanges} selfKillRestarts={restarts} externalKills={externalKills} redeliveredAndAbsorbed={skipped}");

        problems.Should().BeEmpty();
        restarts.Should().BeGreaterThanOrEqualTo(3);
        skipped.Should().BeGreaterThan(0, "the kills happened between the read-model commit and the offset commit, so batches were redelivered");

        // The index mirrors the read model: one document per live row.
        var meili = stack.CreateMeiliClient();
        foreach (var (table, index) in new[] { ("rm_customer", "customers"), ("rm_order", "orders"), ("rm_order_line", "order_lines") })
        {
            var live = await probe.ScalarAsync<long>($"SELECT count(*) FROM {table} WHERE NOT deleted", ct);
            (await meili.CountAsync($"{name}_{index}", ct)).Should().Be(live, $"index {index} must hold exactly the live rows");
        }
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
