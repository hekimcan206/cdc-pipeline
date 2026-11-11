using CdcPipeline.Consumer.Pipeline;
using CdcPipeline.IntegrationTests.Infrastructure;
using CdcPipeline.TestKit;

namespace CdcPipeline.IntegrationTests;

/// <summary>
/// The three schema changes of docs/schema-evolution.md performed live on the PostgreSQL source while consumers run:
/// ADD COLUMN, RENAME COLUMN and a type change, plus a column the contract never heard of. One test, because the
/// steps depend on each other and on the order in which the DDL happens.
/// </summary>
[Collection(PostgresPipelineGroup.Name)]
public sealed class SchemaEvolutionTests(PostgresPipelineFixture fixture)
{
    [Fact]
    public async Task Add_rename_and_type_change_are_projected_by_v2_while_v1_halts_and_resumes_after_upgrade()
    {
        var ct = TestContext.Current.CancellationToken;
        var stack = fixture.Stack;
        var writer = ShopWriter.For(stack);
        const long id = 4_000_001;

        try
        {
            await RunAsync(stack, writer, id, ct);
        }
        finally
        {
            // Restore the original column name even when an assertion failed, so the tests sharing this stack (and the
            // workload, which writes 'email') are not broken by this one. The v2 alias accepts both names.
            await writer.ExecuteDdlAsync("""
                DO $$ BEGIN
                    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'customers' AND column_name = 'email_address') THEN
                        ALTER TABLE customers RENAME COLUMN email_address TO email;
                    END IF;
                END $$
                """, CancellationToken.None);
        }
    }

    private async Task RunAsync(PipelineEnvironment stack, ShopWriter writer, long id, CancellationToken ct)
    {
        var v2Name = fixture.NextName("evo");
        var v2ReadModel = await stack.CreateReadModelDatabaseAsync(v2Name, ct);
        var v2Probe = new ReadModelProbe(v2ReadModel, "pg");
        await using (var v2 = await ConsumerInstance.StartAsync(ConsumerSettings.For(stack, v2ReadModel, v2Name)))
        {
            await writer.InsertCustomerAsync(id, "Mert Kaya", "mert@example.test", "Bursa", ct);
            await writer.InsertOrderAsync(id, id, lines: 1, ct);

            // 1. Add a column the contract knows (typed) and one it does not (lands in extra).
            await writer.ExecuteDdlAsync("ALTER TABLE customers ADD COLUMN loyalty_tier text, ADD COLUMN nickname text", ct);
            await writer.ExecuteAsync($"UPDATE customers SET loyalty_tier = 'GOLD', nickname = 'mk', version = version + 1, updated_at = clock_timestamp() WHERE id = {id}", ct);

            // 2. Rename a required column.
            await writer.ExecuteDdlAsync("ALTER TABLE customers RENAME COLUMN email TO email_address", ct);
            await writer.ExecuteAsync($"UPDATE customers SET email_address = 'mert.kaya@example.test', version = version + 1, updated_at = clock_timestamp() WHERE id = {id}", ct);

            // 3. Change a type: integer quantity becomes numeric(12,3) (sold by weight).
            await writer.ExecuteDdlAsync("ALTER TABLE order_lines ALTER COLUMN quantity TYPE numeric(12,3)", ct);
            await writer.ExecuteAsync($"UPDATE order_lines SET quantity = 1.250, version = version + 1, updated_at = clock_timestamp() WHERE order_id = {id}", ct);

            try
            {
                await Waiter.UntilAsync(async () => await v2Probe.ScalarAsync<decimal?>($"SELECT quantity FROM rm_order_line WHERE order_id = {id}", ct) == 1.250m,
                    TimeSpan.FromSeconds(90), "the type change to be projected", ct);
            }
            catch (TimeoutException ex)
            {
                var (stdout, _) = await stack.DebeziumLogsAsync();
                var debeziumProblems = string.Join("\n", stdout.Split('\n').Where(l => l.Contains(" WARN ", StringComparison.Ordinal) || l.Contains("ERROR", StringComparison.Ordinal)).TakeLast(15));
                throw new TimeoutException(
                    $"{ex.Message} consumer={v2.State.Status} halt={v2.State.HaltReason} at={v2.State.HaltedAt} applied={v2.State.Applied} " +
                    $"quantity={await v2Probe.ScalarAsync<string>($"SELECT quantity::text FROM rm_order_line WHERE order_id = {id}", ct)} " +
                    $"customerVersion={await v2Probe.ScalarAsync<int?>($"SELECT version FROM rm_customer WHERE id = {id}", ct)}\nDebezium:\n{debeziumProblems}", ex);
            }

            v2.State.Status.Should().Be(ConsumerStatus.Running);

            (await v2Probe.ScalarAsync<string>($"SELECT email FROM rm_customer WHERE id = {id}", ct)).Should().Be("mert.kaya@example.test");
            (await v2Probe.ScalarAsync<string>($"SELECT loyalty_tier FROM rm_customer WHERE id = {id}", ct)).Should().Be("GOLD");
            (await v2Probe.ScalarAsync<string>($"SELECT extra->>'nickname' FROM rm_customer WHERE id = {id}", ct)).Should().Be("mk");
            (await v2Probe.ScalarAsync<int>($"SELECT apply_count FROM rm_customer WHERE id = {id}", ct)).Should().Be(3);

            await Waiter.UntilAsync(async () => await v2Probe.DirtyRowsAsync(ct) == 0, TimeSpan.FromSeconds(30), "index to catch up", ct);
            var doc = await stack.CreateMeiliClient().GetDocumentAsync($"{v2Name}_customers", $"pg-{id}", ct);
            doc!["nickname"]!.GetValue<string>().Should().Be("mk", "unknown columns are searchable before the contract knows them");
            doc["email"]!.GetValue<string>().Should().Be("mert.kaya@example.test");
        }

        // A consumer still on contract v1 replays the same history: it halts at the first evolved event it cannot map, commits
        // nothing past it, and reports where it stopped.
        var v1Name = fixture.NextName("evo-v1-");
        var v1ReadModel = await stack.CreateReadModelDatabaseAsync(v1Name, ct);
        var v1Settings = ConsumerSettings.For(stack, v1ReadModel, v1Name) with { ContractVersion = "v1" };
        await using (var v1 = await ConsumerInstance.StartAsync(v1Settings))
        {
            await Waiter.UntilAsync(() => Task.FromResult(v1.State.Status == ConsumerStatus.Halted), TimeSpan.FromSeconds(60), "v1 to halt", ct);
            // Customers and order lines are different topics, so whichever evolved event v1 meets first stops it.
            v1.State.HaltReason.Should().MatchRegex("MissingRequiredField.*email|LossyConversion.*quantity");
            v1.State.HaltedAt.Should().MatchRegex(@"^pg\.public\.(customers|order_lines)\[");
            using var ready = await v1.Http.GetAsync(new Uri("health/ready", UriKind.Relative), ct);
            ((int)ready.StatusCode).Should().Be(503);
        }

        // Upgrading to v2 on the same group and read model resumes exactly where v1 stopped.
        await using (await ConsumerInstance.StartAsync(v1Settings with { ContractVersion = "v2" }))
        {
            var v1Probe = new ReadModelProbe(v1ReadModel, "pg");
            await Waiter.UntilAsync(async () => await v1Probe.ScalarAsync<decimal?>($"SELECT quantity FROM rm_order_line WHERE order_id = {id}", ct) == 1.250m,
                TimeSpan.FromSeconds(60), "the upgraded consumer to catch up", ct);
            (await v1Probe.ScalarAsync<string>($"SELECT email FROM rm_customer WHERE id = {id}", ct)).Should().Be("mert.kaya@example.test");
            (await v1Probe.ScalarAsync<int>($"SELECT apply_count FROM rm_customer WHERE id = {id}", ct)).Should().Be(3);
        }
    }
}
