namespace CdcPipeline.Core.Contracts;

/// <summary>Types the read model stores. Coercion into them is lossless or it fails (see <see cref="ValueCoercer"/>).</summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1720", Justification = "The members name the CLR type each value is coerced into.")]
public enum FieldType
{
    Int32,
    Int64,
    Decimal,
    Text,
    Timestamp,
}

/// <summary>
/// One column of the read model and the source field names it accepts.
/// </summary>
/// <param name="Name">Read-model column name (also the search document field).</param>
/// <param name="Aliases">Other source field names that mean the same thing, e.g. the old and new name of a renamed
/// column. Both stay accepted for as long as old events can be replayed from the broker.</param>
public sealed record ContractField(string Name, FieldType Type, bool Required = false, params string[] Aliases)
{
    public IEnumerable<string> SourceNames => Aliases.Prepend(Name);
}

/// <summary>
/// The consumer's view of one captured table: which fields it projects, how they are typed, which are required, and
/// which source names map onto them. Fields the contract does not know are kept in the row's <c>extra</c> JSON instead
/// of being dropped, so an added column is never lost while the contract catches up.
/// </summary>
public sealed record EntityContract(
    string Name,
    string SourceTable,
    string ReadModelTable,
    IReadOnlyList<ContractField> Key,
    IReadOnlyList<ContractField> Fields)
{
    public IEnumerable<ContractField> AllFields => Key.Concat(Fields);
}

/// <summary>A versioned set of entity contracts, looked up by source table.</summary>
public sealed class ContractSet
{
    private readonly Dictionary<string, EntityContract> _byTable;

    public ContractSet(string version, IEnumerable<EntityContract> contracts)
    {
        Version = version;
        Entities = [.. contracts];
        _byTable = Entities.ToDictionary(c => c.SourceTable, StringComparer.Ordinal);
    }

    public string Version { get; }

    public IReadOnlyList<EntityContract> Entities { get; }

    public EntityContract? ForTable(string table) => _byTable.GetValueOrDefault(table);
}
