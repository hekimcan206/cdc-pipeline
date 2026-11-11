using System.Globalization;
using System.Text.Json;

namespace CdcPipeline.Core.Contracts;

/// <summary>Why an event cannot be projected under the current contract.</summary>
public enum ViolationKind
{
    MissingRequiredField,
    AmbiguousField,
    LossyConversion,
    InvalidValue,
}

/// <summary>
/// The event does not fit the contract. The consumer fails closed on this: it stops before committing the offset,
/// so nothing is skipped and the same event is retried once the contract (or the source) is fixed.
/// </summary>
public sealed class ContractViolationException(ViolationKind kind, string entity, string field, string message)
    : Exception($"{entity}.{field}: {message}")
{
    public ViolationKind Kind { get; } = kind;

    public string Entity { get; } = entity;

    public string Field { get; } = field;
}

/// <summary>
/// Converts Debezium JSON values (<c>decimal.handling.mode=string</c>) into read-model types without losing
/// information. The connectors disagree on representation: PostgreSQL sends <c>integer</c> as a JSON number,
/// Oracle sends <c>NUMBER(10)</c> as the string <c>"1"</c>, and after a type change from <c>integer</c> to
/// <c>numeric(12,3)</c> PostgreSQL sends <c>"1.500"</c>. All of these are accepted where they fit; a value that
/// would be truncated is rejected instead of silently rounded.
/// </summary>
public static class ValueCoercer
{
    public static object? Coerce(JsonElement value, FieldType type, string entity, string field)
    {
        if (value.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        return type switch
        {
            FieldType.Int32 => (int)ToInteger(value, entity, field, int.MinValue, int.MaxValue),
            FieldType.Int64 => ToInteger(value, entity, field, long.MinValue, long.MaxValue),
            FieldType.Decimal => ToDecimal(value, entity, field),
            FieldType.Text => ToText(value, entity, field),
            FieldType.Timestamp => ToTimestamp(value, entity, field),
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, null),
        };
    }

    private static long ToInteger(JsonElement value, string entity, string field, long min, long max)
    {
        var number = ToDecimal(value, entity, field);
        if (number != decimal.Truncate(number))
        {
            throw new ContractViolationException(ViolationKind.LossyConversion, entity, field,
                $"value {number.ToString(CultureInfo.InvariantCulture)} has a fractional part but the contract stores an integer");
        }

        if (number < min || number > max)
        {
            throw new ContractViolationException(ViolationKind.LossyConversion, entity, field,
                $"value {number.ToString(CultureInfo.InvariantCulture)} is outside the integer range of the contract");
        }

        return (long)number;
    }

    private static decimal ToDecimal(JsonElement value, string entity, string field)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Number when value.TryGetDecimal(out var d):
                return d;
            case JsonValueKind.String when decimal.TryParse(value.GetString(), NumberStyles.Number | NumberStyles.AllowExponent,
                CultureInfo.InvariantCulture, out var parsed):
                return parsed;
            default:
                throw new ContractViolationException(ViolationKind.InvalidValue, entity, field,
                    $"expected a number, got {value.ValueKind} '{Truncate(value.GetRawText())}'");
        }
    }

    private static string ToText(JsonElement value, string entity, string field) => value.ValueKind switch
    {
        JsonValueKind.String => value.GetString()!,
        // Widening a number column to text is a common evolution; keep the exact source digits.
        JsonValueKind.Number => value.GetRawText(),
        JsonValueKind.True => "true",
        JsonValueKind.False => "false",
        _ => throw new ContractViolationException(ViolationKind.InvalidValue, entity, field,
            $"expected text, got {value.ValueKind}"),
    };

    private static DateTimeOffset ToTimestamp(JsonElement value, string entity, string field)
    {
        // timestamptz / TIMESTAMP WITH TIME ZONE arrive as ISO-8601 strings (Debezium ZonedTimestamp). Epoch numbers
        // are refused: without the schema their unit (ms or µs) is ambiguous, and guessing would corrupt data.
        if (value.ValueKind == JsonValueKind.String
            && DateTimeOffset.TryParse(value.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var ts))
        {
            return ts;
        }

        throw new ContractViolationException(ViolationKind.InvalidValue, entity, field,
            $"expected an ISO-8601 timestamp string, got {value.ValueKind} '{Truncate(value.GetRawText())}'");
    }

    private static string Truncate(string text) => text.Length <= 40 ? text : text[..40] + "...";
}
