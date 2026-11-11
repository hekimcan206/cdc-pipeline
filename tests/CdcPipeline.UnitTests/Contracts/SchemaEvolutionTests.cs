using CdcPipeline.Core.Contracts;
using CdcPipeline.Core.Events;

namespace CdcPipeline.UnitTests.Contracts;

/// <summary>
/// The three schema changes from docs/schema-evolution.md, replayed from the events the real connectors emitted:
/// ADD COLUMN loyalty_tier, RENAME COLUMN email TO email_address, and quantity integer to numeric(12,3)
/// (PostgreSQL only; Oracle refuses an in-place scale change on a non-empty column, ORA-01440).
/// </summary>
public sealed class SchemaEvolutionTests
{
    public static TheoryData<string, string> CustomerTopics() => new()
    {
        { "postgres", "pg.public.customers" },
        { "oracle", "ora.SHOP.CUSTOMERS" },
    };

    private static List<ChangeEvent> Customers(string connector, string topic) =>
        [.. Fixtures.Load(connector, topic).Select(r => r.Change).OrderBy(e => e.Position)];

    [Theory]
    [MemberData(nameof(CustomerTopics))]
    public void Added_column_lands_in_extra_under_v1_and_in_its_typed_column_under_v2(string connector, string topic)
    {
        var withTier = Customers(connector, topic).First(e => e.After!.ContainsKey("loyalty_tier") && e.After["loyalty_tier"].ValueKind != System.Text.Json.JsonValueKind.Null);
        var contractV1 = ShopContracts.V1.ForTable("customers")!;
        var contractV2 = ShopContracts.V2.ForTable("customers")!;

        var v1 = RowMapper.Map(contractV1, withTier);
        v1.Values.Should().NotContainKey("loyalty_tier");
        v1.Extra["loyalty_tier"]!.GetValue<string>().Should().Be("GOLD");

        var v2 = RowMapper.Map(contractV2, withTier);
        v2.Values["loyalty_tier"].Should().Be("GOLD");
        v2.Extra.Should().BeEmpty();
    }

    [Theory]
    [MemberData(nameof(CustomerTopics))]
    public void Events_from_before_the_column_was_added_still_map_under_v2(string connector, string topic)
    {
        var first = Customers(connector, topic)[0];
        first.After!.Should().NotContainKey("loyalty_tier");
        RowMapper.Map(ShopContracts.V2.ForTable("customers")!, first).Values["loyalty_tier"].Should().BeNull();
    }

    [Theory]
    [MemberData(nameof(CustomerTopics))]
    public void Renamed_column_halts_v1_and_maps_through_the_alias_under_v2(string connector, string topic)
    {
        var renamed = Customers(connector, topic).Last();
        renamed.After!.Should().ContainKey("email_address").And.NotContainKey("email");

        var act = () => RowMapper.Map(ShopContracts.V1.ForTable("customers")!, renamed);
        act.Should().Throw<ContractViolationException>()
            .Where(e => e.Kind == ViolationKind.MissingRequiredField && e.Field == "email");

        var v2 = RowMapper.Map(ShopContracts.V2.ForTable("customers")!, renamed);
        v2.Values["email"].Should().Be("ayse.y@example.test");
    }

    [Theory]
    [MemberData(nameof(CustomerTopics))]
    public void Events_from_before_the_rename_still_map_under_v2(string connector, string topic)
    {
        var old = Customers(connector, topic)[0];
        RowMapper.Map(ShopContracts.V2.ForTable("customers")!, old).Values["email"].Should().Be("ayse@example.test");
    }

    [Fact]
    public void Type_change_to_a_fractional_quantity_halts_v1_instead_of_truncating()
    {
        var widened = Fixtures.Load("postgres", "pg.public.order_lines").Select(r => r.Change).OrderBy(e => e.Position).Last();
        widened.After!["quantity"].GetString().Should().Be("1.500");

        var act = () => RowMapper.Map(ShopContracts.V1.ForTable("order_lines")!, widened);
        act.Should().Throw<ContractViolationException>()
            .Where(e => e.Kind == ViolationKind.LossyConversion && e.Field == "quantity");

        RowMapper.Map(ShopContracts.V2.ForTable("order_lines")!, widened).Values["quantity"].Should().Be(1.500m);
    }

    [Fact]
    public void Integer_quantities_from_before_the_type_change_map_under_both_versions()
    {
        var before = Fixtures.Load("postgres", "pg.public.order_lines").Select(r => r.Change)
            .First(e => e.Operation == ChangeOperation.Create && e.After!["quantity"].ValueKind == System.Text.Json.JsonValueKind.Number);
        RowMapper.Map(ShopContracts.V1.ForTable("order_lines")!, before).Values["quantity"].Should().Be(before.After!["quantity"].GetInt32());
        RowMapper.Map(ShopContracts.V2.ForTable("order_lines")!, before).Values["quantity"].Should().Be((decimal)before.After!["quantity"].GetInt32());
    }

    [Fact]
    public void Oracle_numbers_arrive_as_strings_and_map_to_the_same_values()
    {
        var oracle = RowMapper.Map(ShopContracts.V2.ForTable("customers")!, Customers("oracle", "ora.SHOP.CUSTOMERS")[0]);
        var postgres = RowMapper.Map(ShopContracts.V2.ForTable("customers")!, Customers("postgres", "pg.public.customers")[0]);
        oracle.KeyValues.Should().Equal(postgres.KeyValues);
        oracle.Values["version"].Should().Be(postgres.Values["version"]);
        oracle.Values["full_name"].Should().Be(postgres.Values["full_name"]);
    }

    [Fact]
    public void Deletes_ignore_the_default_valued_before_image()
    {
        // PostgreSQL with REPLICA IDENTITY DEFAULT fills non-key columns of a delete with "", 0 and 1970-01-01.
        var delete = Fixtures.Load("postgres", "pg.public.order_lines").Select(r => r.Change).Single(e => e.Operation == ChangeOperation.Delete);
        delete.Before!["sku"].GetString().Should().BeEmpty();

        var row = RowMapper.Map(ShopContracts.V2.ForTable("order_lines")!, delete);
        row.IsDelete.Should().BeTrue();
        row.Values.Should().BeEmpty();
        row.KeyValues.Should().Equal(10L, 1);
    }

    [Fact]
    public void Ambiguous_alias_is_rejected()
    {
        var change = Customers("postgres", "pg.public.customers")[0];
        var both = change with
        {
            After = new Dictionary<string, System.Text.Json.JsonElement>(change.After!)
            {
                ["email_address"] = change.After!["email"],
            },
        };

        var act = () => RowMapper.Map(ShopContracts.V2.ForTable("customers")!, both);
        act.Should().Throw<ContractViolationException>().Where(e => e.Kind == ViolationKind.AmbiguousField);
    }

    [Fact]
    public void Contract_versions_are_resolved_by_name()
    {
        ShopContracts.ByVersion("v1").Should().BeSameAs(ShopContracts.V1);
        ShopContracts.ByVersion("current").Should().BeSameAs(ShopContracts.Current);
        var act = () => ShopContracts.ByVersion("v9");
        act.Should().Throw<ArgumentException>();
    }
}
