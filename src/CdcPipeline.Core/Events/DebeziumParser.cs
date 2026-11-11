using System.Globalization;
using System.Text.Json;

namespace CdcPipeline.Core.Events;

/// <summary>Result of parsing one broker record.</summary>
public abstract record ParsedRecord
{
    public sealed record Change(ChangeEvent Event) : ParsedRecord;

    public sealed record Boundary(TransactionBoundary Transaction) : ParsedRecord;

    /// <summary>A record that carries nothing to project (a tombstone or an empty value).</summary>
    public sealed record Ignored(string Reason) : ParsedRecord;
}

/// <summary>The record is not a Debezium envelope this consumer understands. Never skipped silently.</summary>
public sealed class MalformedEventException(string message, Exception? inner = null) : Exception(message, inner);

/// <summary>
/// Parses Debezium JSON envelopes (<c>schemas.enable=false</c>) from the PostgreSQL and Oracle connectors into
/// <see cref="ChangeEvent"/> and <see cref="TransactionBoundary"/> values.
/// </summary>
public static class DebeziumParser
{
    private const string TransactionTopicSuffix = ".transaction";

    public static ParsedRecord Parse(string topic, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty)
        {
            return new ParsedRecord.Ignored("tombstone");
        }

        try
        {
            using var valueDoc = JsonDocument.Parse(value.ToArray());
            var root = valueDoc.RootElement;
            if (root.ValueKind == JsonValueKind.Null)
            {
                return new ParsedRecord.Ignored("tombstone");
            }

            if (topic.EndsWith(TransactionTopicSuffix, StringComparison.Ordinal))
            {
                return new ParsedRecord.Boundary(ParseBoundary(topic, root));
            }

            using var keyDoc = key.IsEmpty ? null : JsonDocument.Parse(key.ToArray());
            return new ParsedRecord.Change(ParseChange(root, keyDoc?.RootElement));
        }
        catch (JsonException ex)
        {
            throw new MalformedEventException($"Record on '{topic}' is not valid JSON: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// The PostgreSQL connector names a transaction <c>xid:lsn</c>, and the LSN differs between BEGIN (first change)
    /// and END (commit), so markers are matched on the xid alone. Oracle transaction ids are stable hex strings.
    /// </summary>
    public static string NormaliseTransactionId(string id)
    {
        var colon = id.IndexOf(':', StringComparison.Ordinal);
        return colon > 0 && id.AsSpan(0, colon).IndexOfAnyExceptInRange('0', '9') < 0 ? id[..colon] : id;
    }

    private static TransactionBoundary ParseBoundary(string topic, JsonElement root)
    {
        var source = topic[..^TransactionTopicSuffix.Length];
        var status = RequiredString(root, "status") switch
        {
            "BEGIN" => TransactionStatus.Begin,
            "END" => TransactionStatus.End,
            var other => throw new MalformedEventException($"Unknown transaction status '{other}'."),
        };
        int? count = root.TryGetProperty("event_count", out var c) && c.ValueKind == JsonValueKind.Number ? c.GetInt32() : null;
        return new TransactionBoundary(source, NormaliseTransactionId(RequiredString(root, "id")), status, count);
    }

    private static ChangeEvent ParseChange(JsonElement root, JsonElement? key)
    {
        if (!root.TryGetProperty("source", out var source) || source.ValueKind != JsonValueKind.Object)
        {
            throw new MalformedEventException("Envelope has no 'source' block.");
        }

        var op = RequiredString(root, "op") switch
        {
            "r" => ChangeOperation.Read,
            "c" => ChangeOperation.Create,
            "u" => ChangeOperation.Update,
            "d" => ChangeOperation.Delete,
            var other => throw new MalformedEventException($"Unsupported operation '{other}'."),
        };

        var before = ReadImage(root, "before");
        var after = ReadImage(root, "after");
        if ((op == ChangeOperation.Delete ? before : after) is null)
        {
            throw new MalformedEventException($"Operation '{op}' has no row image.");
        }

        string? txId = null;
        long? txOrder = null;
        if (root.TryGetProperty("transaction", out var tx) && tx.ValueKind == JsonValueKind.Object)
        {
            txId = NormaliseTransactionId(RequiredString(tx, "id"));
            txOrder = tx.TryGetProperty("total_order", out var order) && order.ValueKind == JsonValueKind.Number ? order.GetInt64() : null;
        }

        var connector = RequiredString(source, "connector");
        var position = connector switch
        {
            "postgresql" => PostgresPosition(source, op),
            "oracle" => OraclePosition(source, op, txOrder),
            _ => throw new MalformedEventException($"Connector '{connector}' is not supported."),
        };

        var keyFields = key is { ValueKind: JsonValueKind.Object } k ? ReadObject(k) : null;
        return new ChangeEvent(
            Source: RequiredString(source, "name"),
            Table: RequiredString(source, "table").ToLowerInvariant(),
            Operation: op,
            Key: keyFields ?? throw new MalformedEventException("Record has no key; the table needs a primary key."),
            Before: before,
            After: after,
            Position: position,
            TransactionId: txId,
            TransactionOrder: txOrder,
            SourceTimestamp: ReadTimestamp(source));
    }

    private static SourcePosition PostgresPosition(JsonElement source, ChangeOperation op)
    {
        var lsn = RequiredDecimal(source, "lsn");
        if (op == ChangeOperation.Read)
        {
            return new SourcePosition(0, lsn);
        }

        // source.sequence is a JSON array serialised into a string: ["<lastCommitLsn>", "<lsn>"].
        decimal lastCommit = 1;
        if (source.TryGetProperty("sequence", out var seq) && seq.ValueKind == JsonValueKind.String)
        {
            using var parts = JsonDocument.Parse(seq.GetString()!);
            if (parts.RootElement.ValueKind == JsonValueKind.Array && parts.RootElement.GetArrayLength() == 2
                && ToDecimal(parts.RootElement[0]) is { } commit)
            {
                lastCommit = commit;
            }
        }

        return new SourcePosition(lastCommit, lsn);
    }

    private static SourcePosition OraclePosition(JsonElement source, ChangeOperation op, long? txOrder)
    {
        var scn = RequiredDecimal(source, "scn");
        if (op == ChangeOperation.Read)
        {
            return new SourcePosition(0, scn);
        }

        var commitScn = source.TryGetProperty("commit_scn", out var c) ? ToDecimal(c) : null;
        return new SourcePosition(
            commitScn ?? throw new MalformedEventException("Oracle streaming event without commit_scn."),
            txOrder ?? scn);
    }

    private static DateTimeOffset ReadTimestamp(JsonElement source)
    {
        if (source.TryGetProperty("ts_us", out var us) && us.ValueKind == JsonValueKind.Number)
        {
            return DateTimeOffset.UnixEpoch.AddTicks(us.GetInt64() * 10);
        }

        return DateTimeOffset.FromUnixTimeMilliseconds((long)RequiredDecimal(source, "ts_ms"));
    }

    private static Dictionary<string, JsonElement>? ReadImage(JsonElement root, string name) =>
        root.TryGetProperty(name, out var image) && image.ValueKind == JsonValueKind.Object ? ReadObject(image) : null;

    private static Dictionary<string, JsonElement> ReadObject(JsonElement element)
    {
        var fields = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        foreach (var property in element.EnumerateObject())
        {
            // Clone: the values must outlive the JsonDocument they were parsed from.
            fields[property.Name.ToLowerInvariant()] = property.Value.Clone();
        }

        return fields;
    }

    private static string RequiredString(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()!
            : throw new MalformedEventException($"Missing string field '{name}'.");

    private static decimal RequiredDecimal(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && ToDecimal(value) is { } number
            ? number
            : throw new MalformedEventException($"Missing numeric field '{name}'.");

    private static decimal? ToDecimal(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Number => value.GetDecimal(),
        JsonValueKind.String when decimal.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var d) => d,
        _ => null,
    };
}
