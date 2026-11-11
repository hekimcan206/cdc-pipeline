using System.Globalization;
using System.Text;
using CdcPipeline.Core.Events;

namespace CdcPipeline.UnitTests.Events;

/// <summary>
/// Golden tests over real Debezium 3.7.1 output from both connectors. Every captured record is rendered into a
/// canonical one-line summary; the summaries must match the committed golden file exactly. Regenerate with
/// <c>UPDATE_GOLDEN=1 dotnet test</c> after a deliberate change and review the diff.
/// </summary>
public sealed class DebeziumParserGoldenTests
{
    public static TheoryData<string, string> Topics()
    {
        var data = new TheoryData<string, string>();
        foreach (var (connector, topic) in Fixtures.All())
        {
            data.Add(connector, topic);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Topics))]
    public void Parsed_records_match_the_golden_summary(string connector, string topic)
    {
        var actual = new StringBuilder();
        foreach (var record in Fixtures.Load(connector, topic))
        {
            actual.Append(Render(record.Parse())).Append('\n');
        }

        var goldenPath = Path.Combine(Fixtures.Root, "golden", $"{connector}.{topic}.txt");
        if (Environment.GetEnvironmentVariable("UPDATE_GOLDEN") == "1")
        {
            var sourcePath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "Fixtures", "golden", $"{connector}.{topic}.txt"));
            Directory.CreateDirectory(Path.GetDirectoryName(sourcePath)!);
            File.WriteAllText(sourcePath, actual.ToString());
            return;
        }

        File.Exists(goldenPath).Should().BeTrue($"golden file {goldenPath} must be committed");
        actual.ToString().Should().Be(File.ReadAllText(goldenPath).ReplaceLineEndings("\n"));
    }

    private static string Render(ParsedRecord record) => record switch
    {
        ParsedRecord.Change { Event: var e } => string.Create(CultureInfo.InvariantCulture,
            $"{e.Source} {e.Table} {e.Operation} key={Fields(e.Key)} pos={e.Position} tx={e.TransactionId ?? "-"}#{e.TransactionOrder?.ToString(CultureInfo.InvariantCulture) ?? "-"} " +
            $"ts={e.SourceTimestamp:O} image={(e.Image is null ? "-" : Fields(e.Image))}"),
        ParsedRecord.Boundary { Transaction: var t } => $"{t.Source} tx {t.Status} id={t.TransactionId} events={t.EventCount?.ToString(CultureInfo.InvariantCulture) ?? "-"}",
        ParsedRecord.Ignored { Reason: var reason } => $"ignored {reason}",
        _ => throw new InvalidOperationException(),
    };

    private static string Fields(IReadOnlyDictionary<string, System.Text.Json.JsonElement> fields) =>
        "{" + string.Join(",", fields.OrderBy(f => f.Key, StringComparer.Ordinal).Select(f => $"{f.Key}={f.Value.GetRawText()}")) + "}";
}
