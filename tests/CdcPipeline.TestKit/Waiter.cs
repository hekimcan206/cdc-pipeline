namespace CdcPipeline.TestKit;

public static class Waiter
{
    /// <summary>Polls <paramref name="condition"/> every 100 ms until it holds or <paramref name="timeout"/> elapses.</summary>
    public static async Task UntilAsync(Func<Task<bool>> condition, TimeSpan timeout, string what, CancellationToken ct = default)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (await condition())
            {
                return;
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"Timed out after {timeout.TotalSeconds:0} s waiting for {what}.");
            }

            await Task.Delay(100, ct);
        }
    }
}
