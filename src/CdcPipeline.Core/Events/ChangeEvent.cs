using System.Text.Json;

namespace CdcPipeline.Core.Events;

public enum ChangeOperation
{
    /// <summary>Row read by the initial snapshot.</summary>
    Read,
    Create,
    Update,
    Delete,
}

/// <summary>Where a record came from in the broker; used for logging, offset commits and the halt report.</summary>
public readonly record struct BrokerCoordinates(string Topic, int Partition, long Offset)
{
    public override string ToString() => $"{Topic}[{Partition}]@{Offset}";
}

/// <summary>
/// One row change, normalised across connectors: field names are lower-cased (Oracle reports upper-case
/// identifiers), and the position is computed per <see cref="SourcePosition"/>.
/// </summary>
public sealed record ChangeEvent(
    string Source,
    string Table,
    ChangeOperation Operation,
    IReadOnlyDictionary<string, JsonElement> Key,
    IReadOnlyDictionary<string, JsonElement>? Before,
    IReadOnlyDictionary<string, JsonElement>? After,
    SourcePosition Position,
    string? TransactionId,
    long? TransactionOrder,
    DateTimeOffset SourceTimestamp)
{
    /// <summary>The row image the read model should reflect: <c>after</c>, or <c>before</c> for a delete.</summary>
    public IReadOnlyDictionary<string, JsonElement>? Image => Operation == ChangeOperation.Delete ? Before : After;
}

public enum TransactionStatus
{
    Begin,
    End,
}

/// <summary>A BEGIN or END marker from the connector's transaction topic.</summary>
/// <param name="EventCount">On END: how many data events the transaction produced across all captured tables.</param>
public sealed record TransactionBoundary(string Source, string TransactionId, TransactionStatus Status, int? EventCount);
