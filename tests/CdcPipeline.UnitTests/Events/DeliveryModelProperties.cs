using CdcPipeline.Core.Events;
using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;

namespace CdcPipeline.UnitTests.Events;

/// <summary>
/// A model of the consumer's delivery contract, checked over random workloads and random crash points:
/// the broker redelivers everything after the last committed offset of a partition, and the read model applies a
/// change only if its position is greater than the row's. Whatever the crash schedule, the result must equal an
/// exactly-once run, and every change must be applied exactly once. The real SQL guard is exercised by the
/// integration tests; this pins down why the design is sufficient.
/// </summary>
public sealed class DeliveryModelProperties
{
    private sealed record Change(int Key, SourcePosition Position, int Value);

    private sealed class ReadModel
    {
        public Dictionary<int, (SourcePosition Position, int Value, int ApplyCount)> Rows { get; } = [];

        public void Apply(Change change)
        {
            if (Rows.TryGetValue(change.Key, out var row) && row.Position >= change.Position)
            {
                return; // the WHERE (t.pos_major, t.pos_minor) < (excluded...) guard
            }

            Rows[change.Key] = (change.Position, change.Value, (Rows.TryGetValue(change.Key, out var r) ? r.ApplyCount : 0) + 1);
        }
    }

    [Property(MaxTest = 300)]
    public Property Any_crash_schedule_converges_to_the_exactly_once_state(PositiveInt changeCount, PositiveInt keyCount, int seed)
    {
        var random = new Random(seed);
        var keys = Math.Min(keyCount.Get, 20);
        var partitions = 3;

        // Source history: commit positions increase globally; each change touches one random key.
        var history = Enumerable.Range(1, Math.Min(changeCount.Get, 400))
            .Select(i => new Change(random.Next(keys), new SourcePosition(i, random.Next(1, 4)), random.Next()))
            .ToList();

        // Debezium keys records by primary key; the broker hashes the key to a partition.
        var log = Enumerable.Range(0, partitions).Select(p => history.Where(c => c.Key % partitions == p).ToList()).ToArray();

        var exactlyOnce = new ReadModel();
        history.ForEach(exactlyOnce.Apply);

        var atLeastOnce = new ReadModel();
        var committed = new int[partitions];
        var consumed = new int[partitions];
        while (Enumerable.Range(0, partitions).Any(p => committed[p] < log[p].Count))
        {
            // Poll a batch from a random partition, apply it, then maybe crash before committing the offset.
            var p = random.Next(partitions);
            var batch = log[p].Skip(consumed[p]).Take(random.Next(1, 6)).ToList();
            batch.ForEach(atLeastOnce.Apply);
            consumed[p] += batch.Count;

            if (random.NextDouble() < 0.3)
            {
                // Crash: every partition rewinds to its committed offset.
                Array.Copy(committed, consumed, partitions);
            }
            else
            {
                committed[p] = consumed[p];
            }
        }

        var perKeyChanges = history.GroupBy(c => c.Key).ToDictionary(g => g.Key, g => g.Count());
        return (atLeastOnce.Rows.Count == exactlyOnce.Rows.Count
                && atLeastOnce.Rows.All(r => exactlyOnce.Rows[r.Key].Value == r.Value.Value
                                             && r.Value.ApplyCount == perKeyChanges[r.Key]))
            .ToProperty();
    }

    [Property]
    public Property Position_order_is_a_total_order(decimal a1, decimal a2, decimal b1, decimal b2, decimal c1, decimal c2)
    {
        var a = new SourcePosition(a1, a2);
        var b = new SourcePosition(b1, b2);
        var c = new SourcePosition(c1, c2);
        var antisymmetric = Math.Sign(a.CompareTo(b)) == -Math.Sign(b.CompareTo(a));
        var transitive = !(a <= b && b <= c) || a <= c;
        var consistentWithEquality = (a.CompareTo(b) == 0) == (a == b);
        return (antisymmetric && transitive && consistentWithEquality).ToProperty();
    }
}
