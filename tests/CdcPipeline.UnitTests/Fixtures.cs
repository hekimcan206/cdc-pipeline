using System.Text;
using System.Text.Json;
using CdcPipeline.Core.Events;

namespace CdcPipeline.UnitTests;

/// <summary>One broker record as captured with <c>rpk topic consume</c> from the real pipelines.</summary>
public sealed record CapturedRecord(string Topic, int Partition, byte[] Key, byte[] Value)
{
    public ParsedRecord Parse() => DebeziumParser.Parse(Topic, Key, Value);

    public ChangeEvent Change => ((ParsedRecord.Change)Parse()).Event;
}

public static class Fixtures
{
    public static string Root => Path.Combine(AppContext.BaseDirectory, "Fixtures");

    public static IReadOnlyList<CapturedRecord> Load(string connector, string topic)
    {
        var path = Path.Combine(Root, connector, topic + ".jsonl");
        return [.. File.ReadAllLines(path).Where(l => l.Length > 0).Select(ParseLine)];
    }

    public static IEnumerable<(string Connector, string Topic)> All() =>
        Directory.GetDirectories(Root)
            .Where(d => Path.GetFileName(d) is "postgres" or "oracle")
            .SelectMany(d => Directory.GetFiles(d, "*.jsonl").Select(f => (Path.GetFileName(d), Path.GetFileNameWithoutExtension(f))))
            .OrderBy(x => x.Item1, StringComparer.Ordinal).ThenBy(x => x.Item2, StringComparer.Ordinal);

    private static CapturedRecord ParseLine(string line)
    {
        using var doc = JsonDocument.Parse(line);
        var root = doc.RootElement;
        return new CapturedRecord(
            root.GetProperty("topic").GetString()!,
            root.GetProperty("partition").GetInt32(),
            Encoding.UTF8.GetBytes(root.GetProperty("key").GetRawText()),
            Encoding.UTF8.GetBytes(root.GetProperty("value").GetRawText()));
    }
}
