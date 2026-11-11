using System.Globalization;
using System.Text.Json;
using CdcPipeline.Core.Contracts;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;

namespace CdcPipeline.UnitTests.Contracts;

public sealed class ValueCoercerTests
{
    private static JsonElement Json(string raw) => JsonDocument.Parse(raw).RootElement.Clone();

    private static object? Coerce(string raw, FieldType type) => ValueCoercer.Coerce(Json(raw), type, "e", "f");

    [Theory]
    [InlineData("42", FieldType.Int32, 42)]
    [InlineData("\"42\"", FieldType.Int32, 42)]
    [InlineData("\"42.000\"", FieldType.Int32, 42)]
    [InlineData("\"9007199254740993\"", FieldType.Int64, 9007199254740993L)]
    public void Integers_are_accepted_in_every_lossless_representation(string raw, FieldType type, object expected) =>
        Coerce(raw, type).Should().Be(expected);

    [Theory]
    [InlineData("1.5", FieldType.Int32)]
    [InlineData("\"1.500\"", FieldType.Int64)]
    [InlineData("\"2147483648\"", FieldType.Int32)]
    public void Integer_coercion_never_truncates_or_overflows(string raw, FieldType type)
    {
        var act = () => Coerce(raw, type);
        act.Should().Throw<ContractViolationException>().Where(e => e.Kind == ViolationKind.LossyConversion);
    }

    [Theory]
    [InlineData("\"12.50\"", "12.50")]
    [InlineData("12.5", "12.5")]
    [InlineData("3", "3")]
    public void Decimals_keep_their_scale(string raw, string expected) =>
        ((decimal)Coerce(raw, FieldType.Decimal)!).ToString(CultureInfo.InvariantCulture).Should().Be(expected);

    [Fact]
    public void Numbers_widened_to_text_keep_their_digits() => Coerce("1.50", FieldType.Text).Should().Be("1.50");

    [Fact]
    public void Zoned_timestamps_parse_to_utc() =>
        Coerce("\"2026-11-11T19:08:38.981878Z\"", FieldType.Timestamp).Should()
            .Be(new DateTimeOffset(2026, 11, 11, 19, 8, 38, TimeSpan.Zero).AddTicks(9818780));

    [Theory]
    [InlineData("1794424131000")]
    [InlineData("\"yesterday\"")]
    public void Ambiguous_or_invalid_timestamps_are_rejected(string raw)
    {
        var act = () => Coerce(raw, FieldType.Timestamp);
        act.Should().Throw<ContractViolationException>().Where(e => e.Kind == ViolationKind.InvalidValue);
    }

    [Theory]
    [InlineData("{}", FieldType.Decimal)]
    [InlineData("\"abc\"", FieldType.Int64)]
    [InlineData("[]", FieldType.Text)]
    public void Wrong_shapes_are_rejected(string raw, FieldType type)
    {
        var act = () => Coerce(raw, type);
        act.Should().Throw<ContractViolationException>().Where(e => e.Kind == ViolationKind.InvalidValue);
    }

    [Fact]
    public void Null_stays_null() => Coerce("null", FieldType.Int64).Should().BeNull();

    [Property]
    public Property Any_long_round_trips_as_number_string_or_scaled_string(long value)
    {
        var s = value.ToString(CultureInfo.InvariantCulture);
        return (Coerce(s, FieldType.Int64) is long a && a == value
                && Coerce($"\"{s}\"", FieldType.Int64) is long b && b == value
                && Coerce($"\"{s}.000\"", FieldType.Int64) is long c && c == value).ToProperty();
    }

    [Property]
    public Property Any_fractional_value_is_refused_by_integer_fields(int whole, PositiveInt fraction)
    {
        var raw = $"\"{whole.ToString(CultureInfo.InvariantCulture)}.{(fraction.Get % 999) + 1:D3}\"";
        var refused = false;
        try
        {
            Coerce(raw, FieldType.Int32);
        }
        catch (ContractViolationException e) when (e.Kind == ViolationKind.LossyConversion)
        {
            refused = true;
        }

        return refused.ToProperty();
    }
}
