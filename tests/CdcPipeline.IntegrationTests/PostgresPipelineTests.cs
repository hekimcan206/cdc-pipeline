using CdcPipeline.IntegrationTests.Infrastructure;
using CdcPipeline.TestKit;

namespace CdcPipeline.IntegrationTests;

/// <summary>
/// End to end through real infrastructure: PostgreSQL (pgoutput) → Debezium Server 3.7 → Redpanda → consumer →
/// read-model PostgreSQL → Meilisearch. Each test owns an id range, a read-model database, a consumer group and an
/// index prefix; the stack is shared.
/// </summary>
[Collection(PostgresPipelineGroup.Name)]
public sealed class PostgresPipelineTests(PostgresPipelineFixture fixture)
{
    private PipelineEnvironment Stack => fixture.Stack;

    [Fact]
    public async Task Inserts_updates_deletes_and_transactions_reach_the_read_model_and_the_index()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = fixture.NextName("e2e");
        var readModel = await Stack.CreateReadModelDatabaseAsync(name, ct);
        var settings = ConsumerSettings.For(Stack, readModel, name);
        await using var consumer = await ConsumerInstance.StartAsync(settings);
        var writer = ShopWriter.For(Stack);
        var probe = new ReadModelProbe(readModel, "pg");

        const long id = 1_000_001;
        await writer.InsertCustomerAsync(id, "Elif Demir", "elif@example.test", "Ankara", ct);
        await writer.InsertOrderAsync(id, id, lines: 3, ct);
        await writer.UpdateCustomerCityAsync(id, "Izmir", ct);
        await writer.DeleteOrderLineAsync(id, 2, ct);
        await writer.UpdateOrderStatusAsync(id, "PAID", ct);

        await probe.WaitForConvergenceAsync(writer, id, id, TimeSpan.FromSeconds(60), ct);
        (await probe.ReconcileAsync(writer, id, id, checkApplyCount: true, ct)).Should().BeEmpty();

        // The order and its three lines were one source transaction: four events, all accounted for.
        (await probe.ScalarAsync<int>($"SELECT t.expected_events FROM rm_tx t JOIN rm_order_line l ON l.source = t.source AND l.tx_id = t.tx_id WHERE l.order_id = {id} AND l.line_no = 1", ct))
            .Should().Be(4);

        await Waiter.UntilAsync(async () => await probe.DirtyRowsAsync(ct) == 0, TimeSpan.FromSeconds(30), "index to catch up", ct);
        var meili = Stack.CreateMeiliClient();
        var customer = await meili.GetDocumentAsync($"{name}_customers", $"pg-{id}", ct);
        customer!["city"]!.GetValue<string>().Should().Be("Izmir");
        (await meili.GetDocumentAsync($"{name}_order_lines", $"pg-{id}-1", ct)).Should().NotBeNull();
        (await meili.GetDocumentAsync($"{name}_order_lines", $"pg-{id}-2", ct)).Should().BeNull("the deleted line is removed from the index");
        var hits = await meili.SearchAsync($"{name}_customers", "Elif Demir", "source = pg", 5, ct);
        hits["hits"]!.AsArray().Select(h => h!["doc_id"]!.GetValue<string>()).Should().Contain($"pg-{id}");
    }

    [Fact]
    public async Task Redelivering_the_entire_topic_history_is_absorbed_without_a_single_reapply()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = fixture.NextName("redeliver");
        var readModel = await Stack.CreateReadModelDatabaseAsync(name, ct);
        var writer = ShopWriter.For(Stack);
        var probe = new ReadModelProbe(readModel, "pg");
        var workload = new ShopWorkload(writer, firstId: 2_000_000);

        await using (await ConsumerInstance.StartAsync(ConsumerSettings.For(Stack, readModel, name)))
        {
            await workload.RunAsync(200, perSecond: 200, ct);
            await probe.WaitForConvergenceAsync(writer, workload.FirstId, workload.LastId, TimeSpan.FromSeconds(60), ct);
        }

        var appliedBefore = await probe.CounterAsync("events_applied", ct);

        // A new consumer group on the same read model: the broker hands over everything from offset 0.
        var replay = ConsumerSettings.For(Stack, readModel, name) with { GroupId = $"{name}-replay" };
        await using (var consumer = await ConsumerInstance.StartAsync(replay))
        {
            await Waiter.UntilAsync(() => Task.FromResult(consumer.State.Skipped >= appliedBefore
                && consumer.State.LastBatchAt < DateTimeOffset.UtcNow.AddSeconds(-2)), TimeSpan.FromSeconds(90), "the replay to drain", ct);
            consumer.State.Applied.Should().Be(0, "every redelivered change is at or behind its row's position");
        }

        (await probe.CounterAsync("events_applied", ct)).Should().Be(appliedBefore);
        (await probe.ReconcileAsync(writer, workload.FirstId, workload.LastId, checkApplyCount: true, ct)).Should().BeEmpty();
    }
}
