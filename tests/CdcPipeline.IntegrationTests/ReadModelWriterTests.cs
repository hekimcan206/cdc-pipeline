using System.Text;
using System.Text.Json;
using CdcPipeline.Consumer.Persistence;
using CdcPipeline.Core.Contracts;
using CdcPipeline.Core.Events;
using CdcPipeline.IntegrationTests.Infrastructure;
using CdcPipeline.TestKit;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace CdcPipeline.IntegrationTests;

/// <summary>
/// The projection's SQL against a real PostgreSQL, fed with events the real connectors produced. No broker here:
/// the tests control delivery order exactly, including the orders a broker would produce after crashes.
/// </summary>
public sealed class ReadModelWriterTests(ReadModelDatabaseFixture db) : IClassFixture<ReadModelDatabaseFixture>
{
    private static readonly string[] PgTopics = ["pg.public.customers", "pg.public.orders", "pg.public.order_lines", "pg.transaction"];
    private static readonly string[] OraTopics = ["ora.SHOP.CUSTOMERS", "ora.SHOP.ORDER_LINES", "ora.transaction"];

    private static List<ParsedRecord> Load(string connector, params string[] topics) =>
    [
        .. topics.SelectMany(t => File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "Fixtures", connector, t + ".jsonl")))
            .Where(l => l.Length > 0)
            .Select(line =>
            {
                using var doc = JsonDocument.Parse(line);
                var r = doc.RootElement;
                return DebeziumParser.Parse(r.GetProperty("topic").GetString()!,
                    Encoding.UTF8.GetBytes(r.GetProperty("key").GetRawText()), Encoding.UTF8.GetBytes(r.GetProperty("value").GetRawText()));
            }),
    ];

    private async Task<(ReadModelWriter Writer, ReadModelProbe Probe, NpgsqlDataSource DataSource)> NewReadModelAsync(ContractSet? contracts = null)
    {
        var cs = await db.CreateDatabaseAsync();
        var dataSource = NpgsqlDataSource.Create(cs);
        await new MigrationRunner(dataSource, NullLogger<MigrationRunner>.Instance).MigrateAsync(TestContext.Current.CancellationToken);
        return (new ReadModelWriter(dataSource, contracts ?? ShopContracts.Current, TimeProvider.System), new ReadModelProbe(cs, "pg"), dataSource);
    }

    [Fact]
    public async Task Replaying_the_whole_history_changes_nothing()
    {
        var (writer, probe, ds) = await NewReadModelAsync();
        await using var _ = ds;
        var history = Load("postgres", PgTopics);
        var ct = TestContext.Current.CancellationToken;

        var first = await writer.ApplyAsync(history, ct);
        first.Skipped.Should().Be(0);
        var rowsAfterFirst = await probe.RowsAsync(0, long.MaxValue, ct);

        var second = await writer.ApplyAsync(history, ct);
        second.Applied.Should().Be(0);
        second.Skipped.Should().Be(first.Applied);
        (await probe.RowsAsync(0, long.MaxValue, ct)).Should().BeEquivalentTo(rowsAfterFirst);

        var customer = rowsAfterFirst.Single(r => r.Table == "customers");
        customer.ApplyCount.Should().Be(4, "snapshot read + three updates, each applied once");
        (await probe.CounterAsync("events_skipped", ct)).Should().Be(first.Applied);
    }

    [Fact]
    public async Task An_older_change_never_overwrites_a_newer_one()
    {
        var (writer, probe, ds) = await NewReadModelAsync();
        await using var _ = ds;
        var customers = Load("postgres", "pg.public.customers");
        var ct = TestContext.Current.CancellationToken;

        await writer.ApplyAsync([customers[^1]], ct);
        var stale = await writer.ApplyAsync(customers[..^1], ct);

        stale.Applied.Should().Be(0);
        (await probe.ScalarAsync<string>("SELECT email FROM rm_customer WHERE source = 'pg' AND id = 1", ct)).Should().Be("ayse.y@example.test");
        (await probe.ScalarAsync<int>("SELECT version FROM rm_customer WHERE source = 'pg' AND id = 1", ct)).Should().Be(4);
    }

    [Fact]
    public async Task A_delete_is_a_tombstone_that_a_late_redelivery_cannot_resurrect()
    {
        var (writer, probe, ds) = await NewReadModelAsync();
        await using var _ = ds;
        var lines = Load("postgres", "pg.public.order_lines");
        var ct = TestContext.Current.CancellationToken;

        await writer.ApplyAsync(lines, ct);
        var create = lines.First(r => r is ParsedRecord.Change { Event.Operation: ChangeOperation.Create, Event.Key: var k } && k["line_no"].GetInt32() == 1);
        (await writer.ApplyAsync([create], ct)).Skipped.Should().Be(1);

        (await probe.ScalarAsync<bool>("SELECT deleted FROM rm_order_line WHERE source = 'pg' AND order_id = 10 AND line_no = 1", ct)).Should().BeTrue();
        (await probe.ScalarAsync<string>("SELECT sku FROM rm_order_line WHERE source = 'pg' AND order_id = 10 AND line_no = 1", ct))
            .Should().Be("SKU-1", "a delete keeps the last known values instead of the connector's default-valued before-image");
    }

    [Fact]
    public async Task A_transaction_completes_whichever_arrives_first_its_end_marker_or_its_events()
    {
        var (writer, probe, ds) = await NewReadModelAsync();
        await using var _ = ds;
        var ct = TestContext.Current.CancellationToken;
        var tx762 = Load("postgres", "pg.public.orders", "pg.public.order_lines", "pg.transaction")
            .Where(r => r switch
            {
                ParsedRecord.Change { Event.TransactionId: "762" } => true,
                ParsedRecord.Boundary { Transaction: { TransactionId: "762", Status: TransactionStatus.End } } => true,
                _ => false,
            }).ToList();
        tx762.Should().HaveCount(4, "order + two lines + END");

        var end = tx762.Single(r => r is ParsedRecord.Boundary);
        var events = tx762.Where(r => r is ParsedRecord.Change).ToList();

        await writer.ApplyAsync([end, events[0]], ct);
        (await probe.ScalarAsync<int>("SELECT expected_events FROM rm_tx WHERE tx_id = '762'", ct)).Should().Be(3);
        (await probe.ScalarAsync<DateTime?>("SELECT completed_at FROM rm_tx WHERE tx_id = '762'", ct)).Should().BeNull();

        // The indexer holds back rows of the incomplete transaction (wait = 60 s here).
        var orders = writer.Projections.Single(p => p.Contract.SourceTable == "orders");
        (await PendingAsync(ds, orders, 60, ct)).Should().Be(0);
        (await PendingAsync(ds, orders, 0, ct)).Should().Be(1, "after the wait the row is pushed anyway and counted as forced");

        await writer.ApplyAsync([.. events.Skip(1), events[0]], ct); // the redelivered first event is not double-counted
        (await probe.ScalarAsync<int>("SELECT applied_events FROM rm_tx WHERE tx_id = '762'", ct)).Should().Be(3);
        (await probe.ScalarAsync<DateTime?>("SELECT completed_at FROM rm_tx WHERE tx_id = '762'", ct)).Should().NotBeNull();
        (await PendingAsync(ds, orders, 60, ct)).Should().Be(1);
    }

    [Fact]
    public async Task A_contract_violation_rejects_the_whole_batch_and_names_the_record()
    {
        var (writer, probe, ds) = await NewReadModelAsync(ShopContracts.V1);
        await using var _ = ds;
        var customers = Load("postgres", "pg.public.customers");
        var ct = TestContext.Current.CancellationToken;

        var act = () => writer.ApplyAsync(customers, ct);
        var rejection = (await act.Should().ThrowAsync<RecordRejectedException>()).Which;
        rejection.RecordIndex.Should().Be(3, "the fourth event is the first one after RENAME COLUMN email TO email_address");
        rejection.Violation.Field.Should().Be("email");

        (await probe.ScalarAsync<long>("SELECT count(*) FROM rm_customer", ct)).Should().Be(0, "nothing of a rejected batch is written");
        (await probe.CounterAsync("batches", ct)).Should().Be(0);

        // Under v1, the events before the rename apply, with the added column parked in extra.
        await writer.ApplyAsync(customers[..3], ct);
        (await probe.ScalarAsync<string>("SELECT extra->>'loyalty_tier' FROM rm_customer WHERE id = 1", ct)).Should().Be("GOLD");
        (await probe.ScalarAsync<string>("SELECT loyalty_tier FROM rm_customer WHERE id = 1", ct)).Should().BeNull();
    }

    [Fact]
    public async Task Oracle_and_postgres_rows_with_the_same_id_live_side_by_side()
    {
        var (writer, probe, ds) = await NewReadModelAsync();
        await using var _ = ds;
        var ct = TestContext.Current.CancellationToken;

        await writer.ApplyAsync(Load("postgres", PgTopics), ct);
        await writer.ApplyAsync(Load("oracle", OraTopics), ct);

        (await probe.ScalarAsync<long>("SELECT count(*) FROM rm_customer WHERE id = 1", ct)).Should().Be(2);
        (await probe.ScalarAsync<string>("SELECT email FROM rm_customer WHERE source = 'ora' AND id = 1", ct)).Should().Be("ayse.y@example.test");
        (await probe.ScalarAsync<string>("SELECT loyalty_tier FROM rm_customer WHERE source = 'ora' AND id = 1", ct)).Should().Be("GOLD");
        (await probe.ScalarAsync<long>("SELECT count(*) FROM rm_tx WHERE source = 'ora' AND completed_at IS NOT NULL", ct)).Should().BeGreaterThan(0);
    }

    private static async Task<long> PendingAsync(NpgsqlDataSource ds, ProjectionSql projection, double waitSeconds, CancellationToken ct)
    {
        await using var cmd = ds.CreateCommand(projection.PendingIndex);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Double, Value = waitSeconds });
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = 100 });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var count = 0;
        while (await reader.ReadAsync(ct))
        {
            count++;
        }

        return count;
    }
}
