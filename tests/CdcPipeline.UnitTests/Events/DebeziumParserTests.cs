using System.Text;
using CdcPipeline.Core.Events;

namespace CdcPipeline.UnitTests.Events;

public sealed class DebeziumParserTests
{
    [Theory]
    [InlineData("postgres", "pg.public.customers")]
    [InlineData("postgres", "pg.public.order_lines")]
    [InlineData("oracle", "ora.SHOP.CUSTOMERS")]
    [InlineData("oracle", "ora.SHOP.ORDER_LINES")]
    public void Positions_increase_per_key_in_topic_order(string connector, string topic)
    {
        var byKey = Fixtures.Load(connector, topic)
            .Select(r => r.Change)
            .GroupBy(e => string.Join(",", e.Key.OrderBy(k => k.Key, StringComparer.Ordinal).Select(k => k.Value.GetRawText())));

        foreach (var key in byKey)
        {
            var positions = key.Select(e => e.Position).ToList();
            positions.Should().BeInAscendingOrder().And.OnlyHaveUniqueItems();
        }
    }

    [Fact]
    public void Snapshot_reads_sort_before_every_streamed_change()
    {
        var events = Fixtures.Load("postgres", "pg.public.customers").Select(r => r.Change).ToList();
        events[0].Operation.Should().Be(ChangeOperation.Read);
        events[0].Position.Major.Should().Be(0);
        events.Skip(1).Should().OnlyContain(e => e.Position > events[0].Position);
    }

    [Fact]
    public void First_streamed_postgres_transaction_without_a_previous_commit_still_beats_the_snapshot()
    {
        // Its source.sequence is [null, lsn]; the lsn can even be lower than the snapshot LSN when the transaction
        // started before the snapshot point. Major = 1 orders it after every snapshot read (Major = 0).
        var lines = Fixtures.Load("postgres", "pg.public.order_lines").Select(r => r.Change).ToList();
        var first = lines.First(e => e.Operation == ChangeOperation.Create);
        first.Position.Major.Should().Be(1);
        first.Position.Should().BeGreaterThan(new SourcePosition(0, decimal.MaxValue));
    }

    [Fact]
    public void Oracle_upper_case_identifiers_are_normalised()
    {
        var change = Fixtures.Load("oracle", "ora.SHOP.CUSTOMERS")[1].Change;
        change.Table.Should().Be("customers");
        change.Key.Keys.Should().Equal("id");
        change.After!.Keys.Should().Contain(["full_name", "email", "updated_at"]);
    }

    [Fact]
    public void Oracle_streaming_position_is_commit_scn_then_transaction_order()
    {
        var change = Fixtures.Load("oracle", "ora.SHOP.ORDER_LINES")[0].Change;
        change.Position.Should().Be(new SourcePosition(2298448, 2));
        change.TransactionId.Should().Be("0a00110052020000");
    }

    [Fact]
    public void Postgres_begin_and_end_markers_with_different_lsn_are_matched_on_the_xid()
    {
        var markers = Fixtures.Load("postgres", "pg.transaction").Select(r => ((ParsedRecord.Boundary)r.Parse()).Transaction).ToList();
        var tx766 = markers.Where(m => m.TransactionId == "766").ToList();
        tx766.Select(m => m.Status).Should().BeEquivalentTo([TransactionStatus.Begin, TransactionStatus.End]);
        tx766.Single(m => m.Status == TransactionStatus.End).EventCount.Should().Be(1);
    }

    [Theory]
    [InlineData("766:26746008", "766")]
    [InlineData("0a00110052020000", "0a00110052020000")]
    [InlineData("abc:123", "abc:123")]
    [InlineData(":5", ":5")]
    public void Transaction_ids_are_normalised(string raw, string expected) =>
        DebeziumParser.NormaliseTransactionId(raw).Should().Be(expected);

    [Fact]
    public void Tombstones_are_ignored()
    {
        DebeziumParser.Parse("pg.public.customers", "{\"id\":1}"u8, ReadOnlySpan<byte>.Empty).Should().BeOfType<ParsedRecord.Ignored>();
        DebeziumParser.Parse("pg.public.customers", "{\"id\":1}"u8, "null"u8).Should().BeOfType<ParsedRecord.Ignored>();
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("{\"op\":\"c\",\"after\":{\"id\":1}}")]
    [InlineData("{\"op\":\"x\",\"after\":{\"id\":1},\"source\":{\"connector\":\"postgresql\",\"name\":\"pg\",\"table\":\"t\",\"lsn\":1,\"ts_ms\":1}}")]
    [InlineData("{\"op\":\"c\",\"after\":null,\"source\":{\"connector\":\"postgresql\",\"name\":\"pg\",\"table\":\"t\",\"lsn\":1,\"ts_ms\":1}}")]
    [InlineData("{\"op\":\"c\",\"after\":{\"id\":1},\"source\":{\"connector\":\"mysql\",\"name\":\"pg\",\"table\":\"t\",\"ts_ms\":1}}")]
    [InlineData("{\"op\":\"u\",\"after\":{\"id\":1},\"source\":{\"connector\":\"oracle\",\"name\":\"ora\",\"table\":\"T\",\"scn\":\"5\",\"commit_scn\":null,\"ts_ms\":1}}")]
    public void Envelopes_it_cannot_order_or_project_are_rejected_not_skipped(string value)
    {
        var act = () => DebeziumParser.Parse("pg.public.t", "{\"id\":1}"u8, Encoding.UTF8.GetBytes(value));
        act.Should().Throw<MalformedEventException>();
    }

    [Fact]
    public void A_change_without_a_key_is_rejected()
    {
        var value = Fixtures.Load("postgres", "pg.public.customers")[1].Value;
        var act = () => DebeziumParser.Parse("pg.public.customers", ReadOnlySpan<byte>.Empty, value);
        act.Should().Throw<MalformedEventException>().WithMessage("*primary key*");
    }
}
