using System.Text.Json;
using System.Text.Json.Nodes;
using CdcPipeline.Core.Events;

namespace CdcPipeline.Core.Contracts;

/// <summary>A change mapped onto a contract: typed key and field values plus the fields the contract does not know.</summary>
/// <param name="Values">Contract field values by read-model column; empty for deletes, which only flag the row.</param>
/// <param name="Extra">Source fields outside the contract, as a JSON object (empty object when there are none).</param>
public sealed record ProjectedRow(
    EntityContract Contract,
    IReadOnlyList<object> KeyValues,
    IReadOnlyDictionary<string, object?> Values,
    JsonObject Extra,
    bool IsDelete);

/// <summary>Maps a <see cref="ChangeEvent"/> onto an <see cref="EntityContract"/>.</summary>
public static class RowMapper
{
    public static ProjectedRow Map(EntityContract contract, ChangeEvent change)
    {
        var keyValues = new object[contract.Key.Count];
        for (var i = 0; i < contract.Key.Count; i++)
        {
            var field = contract.Key[i];
            var raw = Find(contract, field, change.Key)
                ?? throw new ContractViolationException(ViolationKind.MissingRequiredField, contract.Name, field.Name,
                    "key field is missing from the record key");
            keyValues[i] = ValueCoercer.Coerce(raw, field.Type, contract.Name, field.Name)
                ?? throw new ContractViolationException(ViolationKind.MissingRequiredField, contract.Name, field.Name, "key field is null");
        }

        // A delete only marks the row. Its before-image cannot be trusted for non-key columns: with PostgreSQL's
        // default REPLICA IDENTITY the connector fills them with type defaults ("", 0, 1970-01-01).
        if (change.Operation == ChangeOperation.Delete)
        {
            return new ProjectedRow(contract, keyValues, new Dictionary<string, object?>(), [], IsDelete: true);
        }

        var image = change.After!;
        var values = new Dictionary<string, object?>(StringComparer.Ordinal);
        var consumed = new HashSet<string>(StringComparer.Ordinal);
        foreach (var keyField in contract.Key)
        {
            consumed.UnionWith(keyField.SourceNames);
        }

        foreach (var field in contract.Fields)
        {
            consumed.UnionWith(field.SourceNames);
            var raw = Find(contract, field, image);
            if (raw is null || raw.Value.ValueKind == JsonValueKind.Null)
            {
                if (field.Required)
                {
                    throw new ContractViolationException(ViolationKind.MissingRequiredField, contract.Name, field.Name,
                        $"required field is missing or null (accepted source names: {string.Join(", ", field.SourceNames)})");
                }

                values[field.Name] = null;
                continue;
            }

            values[field.Name] = ValueCoercer.Coerce(raw.Value, field.Type, contract.Name, field.Name);
        }

        var extra = new JsonObject();
        foreach (var (name, value) in image)
        {
            if (!consumed.Contains(name))
            {
                extra[name] = JsonNode.Parse(value.GetRawText());
            }
        }

        return new ProjectedRow(contract, keyValues, values, extra, IsDelete: false);
    }

    private static JsonElement? Find(EntityContract contract, ContractField field, IReadOnlyDictionary<string, JsonElement> image)
    {
        JsonElement? found = null;
        string? foundName = null;
        foreach (var name in field.SourceNames)
        {
            if (!image.TryGetValue(name, out var value))
            {
                continue;
            }

            if (found is not null)
            {
                throw new ContractViolationException(ViolationKind.AmbiguousField, contract.Name, field.Name,
                    $"both '{foundName}' and '{name}' are present; an alias must not coexist with the field it replaces");
            }

            found = value;
            foundName = name;
        }

        return found;
    }
}
