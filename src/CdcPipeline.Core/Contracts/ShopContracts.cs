namespace CdcPipeline.Core.Contracts;

/// <summary>
/// The contract versions of the demo shop schema. Each version is what a consumer deployed at that time knew;
/// <c>docs/schema-evolution.md</c> walks through what happens when an older consumer meets newer events.
/// </summary>
public static class ShopContracts
{
    /// <summary>
    /// v1: the schema as first captured. Faced with the evolved source it adds <c>loyalty_tier</c> to <c>extra</c>
    /// (no harm), halts on the renamed <c>email_address</c> (a required field disappears) and halts on a fractional
    /// <c>quantity</c> (it would have to truncate).
    /// </summary>
    public static ContractSet V1 { get; } = new("v1",
    [
        Customer(email: new ContractField("email", FieldType.Text, Required: true), loyaltyTier: false),
        Order(),
        OrderLine(quantity: new ContractField("quantity", FieldType.Int32, Required: true)),
    ]);

    /// <summary>
    /// v2 (current): <c>loyalty_tier</c> is a typed optional column; <c>email</c> accepts its new source name
    /// <c>email_address</c>; <c>quantity</c> is a decimal and still accepts the integer values of older events.
    /// </summary>
    public static ContractSet V2 { get; } = new("v2",
    [
        Customer(email: new ContractField("email", FieldType.Text, Required: true, "email_address"), loyaltyTier: true),
        Order(),
        OrderLine(quantity: new ContractField("quantity", FieldType.Decimal, Required: true)),
    ]);

    public static ContractSet Current => V2;

    public static ContractSet ByVersion(string version) => version switch
    {
        "v1" => V1,
        "v2" or "current" => V2,
        _ => throw new ArgumentException($"Unknown contract version '{version}'. Known: v1, v2.", nameof(version)),
    };

    private static EntityContract Customer(ContractField email, bool loyaltyTier) => new(
        "customer", "customers", "rm_customer",
        Key: [new ContractField("id", FieldType.Int64, Required: true)],
        Fields:
        [
            new ContractField("full_name", FieldType.Text, Required: true),
            email,
            new ContractField("city", FieldType.Text),
            .. loyaltyTier ? [new ContractField("loyalty_tier", FieldType.Text)] : Array.Empty<ContractField>(),
            new ContractField("version", FieldType.Int32, Required: true),
            new ContractField("updated_at", FieldType.Timestamp, Required: true),
        ]);

    private static EntityContract Order() => new(
        "order", "orders", "rm_order",
        Key: [new ContractField("id", FieldType.Int64, Required: true)],
        Fields:
        [
            new ContractField("customer_id", FieldType.Int64, Required: true),
            new ContractField("status", FieldType.Text, Required: true),
            new ContractField("total_amount", FieldType.Decimal, Required: true),
            new ContractField("version", FieldType.Int32, Required: true),
            new ContractField("updated_at", FieldType.Timestamp, Required: true),
        ]);

    private static EntityContract OrderLine(ContractField quantity) => new(
        "order_line", "order_lines", "rm_order_line",
        Key:
        [
            new ContractField("order_id", FieldType.Int64, Required: true),
            new ContractField("line_no", FieldType.Int32, Required: true),
        ],
        Fields:
        [
            new ContractField("sku", FieldType.Text, Required: true),
            quantity,
            new ContractField("unit_price", FieldType.Decimal, Required: true),
            new ContractField("version", FieldType.Int32, Required: true),
            new ContractField("updated_at", FieldType.Timestamp, Required: true),
        ]);
}
