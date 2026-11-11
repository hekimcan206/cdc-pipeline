namespace CdcPipeline.Core.Events;

/// <summary>
/// Where a change sits in its source database's commit history, reduced to two numbers that order every change
/// to the same row correctly. The read model stores the position of the last change applied to each row and
/// applies an incoming change only if its position is strictly greater, which makes redelivery harmless.
/// </summary>
/// <remarks>
/// <para>
/// The ordering only has to hold per primary key, and it does because both databases serialise writers of the same
/// row: a second transaction can only change a row after the first one committed.
/// </para>
/// <list type="bullet">
/// <item>PostgreSQL (pgoutput): <c>Major</c> = <c>lastCommitLsn</c> from <c>source.sequence</c> (non-decreasing in
/// stream order, and different for every streamed transaction), <c>Minor</c> = the change's own <c>lsn</c>
/// (increasing within a transaction). The very first streamed transaction has no previous commit and gets
/// <c>Major = 1</c>.</item>
/// <item>Oracle (LogMiner): <c>Major</c> = <c>commit_scn</c>, <c>Minor</c> = <c>transaction.total_order</c> when
/// transaction metadata is on, otherwise the change SCN (several changes can share one SCN, so this fallback is
/// weaker and is documented as such).</item>
/// <item>Snapshot reads (<c>op = r</c>) always get <c>Major = 0</c>: everything streamed after a snapshot is newer
/// than the snapshot, even a transaction that started before the snapshot point and whose individual change LSN is
/// therefore lower than the snapshot LSN.</item>
/// </list>
/// </remarks>
public readonly record struct SourcePosition(decimal Major, decimal Minor) : IComparable<SourcePosition>
{
    public static readonly SourcePosition Zero = new(0, 0);

    public int CompareTo(SourcePosition other)
    {
        var major = Major.CompareTo(other.Major);
        return major != 0 ? major : Minor.CompareTo(other.Minor);
    }

    public static bool operator <(SourcePosition left, SourcePosition right) => left.CompareTo(right) < 0;

    public static bool operator >(SourcePosition left, SourcePosition right) => left.CompareTo(right) > 0;

    public static bool operator <=(SourcePosition left, SourcePosition right) => left.CompareTo(right) <= 0;

    public static bool operator >=(SourcePosition left, SourcePosition right) => left.CompareTo(right) >= 0;

    public override string ToString() => $"{Major}/{Minor}";
}
