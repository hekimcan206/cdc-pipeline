using System.Net;
using System.Net.Sockets;
using CdcPipeline.IntegrationTests.Infrastructure;
using CdcPipeline.TestKit;

namespace CdcPipeline.IntegrationTests;

/// <summary>
/// Oracle Database 23ai Free → Debezium LogMiner → Redpanda → consumer. Opt-in (<c>CDC_ORACLE_TESTS=1</c>): the
/// stack needs about 3.8 GB. Covers a mixed workload across a hard consumer kill, and Oracle's version of the schema
/// changes, including the expand/contract migration Oracle needs for a scale change on a non-empty NUMBER column.
/// </summary>
[Collection(OraclePipelineGroup.Name)]
public sealed class OraclePipelineTests(OraclePipelineFixture fixture, ITestOutputHelper output)
{
    [Fact]
    public async Task LogMiner_changes_survive_a_consumer_kill_and_schema_evolution()
    {
        Assert.SkipUnless(OraclePipelineFixture.Enabled, "Set CDC_ORACLE_TESTS=1 to run the Oracle LogMiner pipeline (about 3.8 GB of Docker memory).");
        var ct = TestContext.Current.CancellationToken;
        var stack = fixture.Stack!;
        var readModel = await stack.CreateReadModelDatabaseAsync("ora_e2e", ct);
        var settings = ConsumerSettings.For(stack, readModel, "ora_e2e") with { MaxBatchSize = 50 };
        var writer = ShopWriter.For(stack);
        var probe = new ReadModelProbe(readModel, "ora");
        var workload = new ShopWorkload(writer, firstId: 5_000_000, seed: 3);

        var consumer = ConsumerProcess.Start(settings, FreePort());
        try
        {
            var load = workload.RunAsync(400, perSecond: 40, ct);
            await Task.Delay(4000, ct);
            consumer.Kill();
            consumer.Dispose();
            await Task.Delay(1500, ct);
            consumer = ConsumerProcess.Start(settings, FreePort());
            await load;
            try
            {
                await probe.WaitForConvergenceAsync(writer, workload.FirstId, workload.LastId, TimeSpan.FromSeconds(180), ct);
            }
            catch (TimeoutException ex)
            {
                var (logs, _) = await stack.DebeziumLogsAsync();
                var stats = await consumer.StatsAsync();
                throw new TimeoutException(
                    $"{ex.Message}\nconsumer exited={consumer.HasExited} stats={stats}\ntopics={string.Join(",", await stack.ListTopicsAsync())}\n" +
                    $"Debezium:\n{string.Join("\n", logs.Split('\n').Where(l => l.Contains("WARN", StringComparison.Ordinal) || l.Contains("ERROR", StringComparison.Ordinal)).TakeLast(15))}", ex);
            }
        }
        finally
        {
            consumer.Dispose();
        }

        (await probe.ReconcileAsync(writer, workload.FirstId, workload.LastId, checkApplyCount: true, ct)).Should().BeEmpty();
        output.WriteLine($"oracle: {workload.RowChanges} row changes reconciled; skipped(redelivered)={await probe.CounterAsync("events_skipped", ct)}");

        await using var v2 = await ConsumerInstance.StartAsync(settings);
        const long id = 5_900_001;
        await writer.InsertCustomerAsync(id, "Zeynep Arslan", "zeynep@example.test", "Konya", ct);
        await writer.InsertOrderAsync(id, id, lines: 1, ct);

        await writer.ExecuteDdlAsync("ALTER TABLE customers ADD (loyalty_tier VARCHAR2(20))", ct);
        await writer.ExecuteAsync($"UPDATE customers SET loyalty_tier = 'SILVER', version = version + 1, updated_at = SYSTIMESTAMP WHERE id = {id}", ct);
        await writer.ExecuteDdlAsync("ALTER TABLE customers RENAME COLUMN email TO email_address", ct);
        await writer.ExecuteAsync($"UPDATE customers SET email_address = 'z.arslan@example.test', version = version + 1, updated_at = SYSTIMESTAMP WHERE id = {id}", ct);

        // Expand/contract: Oracle refuses NUMBER(10) -> NUMBER(12,3) in place on a non-empty column (ORA-01440).
        await writer.ExecuteDdlAsync("ALTER TABLE order_lines ADD (quantity_new NUMBER(12,3))", ct);
        await writer.ExecuteAsync($"UPDATE order_lines SET quantity_new = quantity, version = version + 1, updated_at = SYSTIMESTAMP WHERE order_id = {id}", ct);
        await writer.ExecuteDdlAsync("ALTER TABLE order_lines DROP COLUMN quantity", ct);
        await writer.ExecuteDdlAsync("ALTER TABLE order_lines RENAME COLUMN quantity_new TO quantity", ct);
        await writer.ExecuteAsync($"UPDATE order_lines SET quantity = 1.25, version = version + 1, updated_at = SYSTIMESTAMP WHERE order_id = {id}", ct);

        await Waiter.UntilAsync(async () => await probe.ScalarAsync<decimal?>($"SELECT quantity FROM rm_order_line WHERE source = 'ora' AND order_id = {id}", ct) == 1.250m,
            TimeSpan.FromSeconds(90), "the Oracle type change to be projected", ct);
        (await probe.ScalarAsync<string>($"SELECT email FROM rm_customer WHERE source = 'ora' AND id = {id}", ct)).Should().Be("z.arslan@example.test");
        (await probe.ScalarAsync<string>($"SELECT loyalty_tier FROM rm_customer WHERE source = 'ora' AND id = {id}", ct)).Should().Be("SILVER");
        (await probe.ScalarAsync<int>($"SELECT apply_count FROM rm_order_line WHERE source = 'ora' AND order_id = {id}", ct)).Should().Be(3);

        await writer.ExecuteDdlAsync("ALTER TABLE customers RENAME COLUMN email_address TO email", ct);
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
