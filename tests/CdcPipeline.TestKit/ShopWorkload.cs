using System.Diagnostics;

namespace CdcPipeline.TestKit;

/// <summary>
/// Open-loop synthetic shop traffic: operations are scheduled at a fixed rate whether or not the pipeline keeps up,
/// so lag is measured against the offered load rather than hidden by back-pressure. Mix: 40 % new customer,
/// 35 % customer update, 20 % order with 1-3 lines (one transaction), 5 % order-line delete.
/// Every id written lies in [<see cref="FirstId"/>, <see cref="LastId"/>].
/// </summary>
public sealed class ShopWorkload(ShopWriter writer, long firstId, int seed = 42)
{
    private readonly Random _random = new(seed);
    private readonly List<long> _customers = [];
    private readonly List<(long Order, int Lines)> _orders = [];
    private long _nextCustomer = firstId;
    private long _nextOrder = firstId;

    public long FirstId { get; } = firstId;

    public long LastId => Math.Max(_nextCustomer, _nextOrder);

    /// <summary>Source row changes produced (one per inserted, updated or deleted row).</summary>
    public long RowChanges { get; private set; }

    public long Operations { get; private set; }

    /// <summary>Runs <paramref name="operations"/> operations at <paramref name="perSecond"/> with up to 8 in flight.</summary>
    public async Task RunAsync(int operations, double perSecond, CancellationToken ct = default)
    {
        var interval = TimeSpan.FromSeconds(1 / perSecond);
        var clock = Stopwatch.StartNew();
        var inFlight = new List<Task>();
        for (var i = 0; i < operations; i++)
        {
            var due = interval * i;
            var wait = due - clock.Elapsed;
            if (wait > TimeSpan.FromMilliseconds(1))
            {
                await Task.Delay(wait, ct);
            }

            inFlight.Add(NextAsync(ct));
            if (inFlight.Count >= 8)
            {
                var done = await Task.WhenAny(inFlight);
                inFlight.Remove(done);
                await done;
            }
        }

        await Task.WhenAll(inFlight);
    }

    private Task NextAsync(CancellationToken ct)
    {
        // Decide the operation and its ids synchronously so the workload is deterministic for a seed.
        Operations++;
        var roll = _random.Next(100);
        if (roll < 40 || _customers.Count < 5)
        {
            var id = _nextCustomer++;
            _customers.Add(id);
            RowChanges++;
            return writer.InsertCustomerAsync(id, $"Customer {id}", $"c{id}@example.test", Cities[_random.Next(Cities.Length)], ct);
        }

        if (roll < 75)
        {
            var id = _customers[_random.Next(_customers.Count)];
            RowChanges++;
            return writer.UpdateCustomerCityAsync(id, Cities[_random.Next(Cities.Length)], ct);
        }

        if (roll < 95 || _orders.Count == 0)
        {
            var order = _nextOrder++;
            var lines = _random.Next(1, 4);
            _orders.Add((order, lines));
            RowChanges += 1 + lines;
            return writer.InsertOrderAsync(order, _customers[_random.Next(_customers.Count)], lines, ct);
        }

        var index = _random.Next(_orders.Count);
        var (orderId, lineCount) = _orders[index];
        _orders.RemoveAt(index); // delete each order's last line at most once
        RowChanges++;
        return writer.DeleteOrderLineAsync(orderId, lineCount, ct);
    }

    private static readonly string[] Cities = ["Ankara", "Istanbul", "Izmir", "Bursa", "Antalya", "Konya", "Adana", "Trabzon"];
}
