using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CdcPipeline.Consumer.Search;

/// <summary>
/// The handful of Meilisearch endpoints the indexer needs. Writes are asynchronous tasks in Meilisearch; a write
/// only counts once its task has succeeded, so every write here waits for its task.
/// </summary>
public sealed class MeiliClient(HttpClient http)
{
    public const string PrimaryKey = "doc_id";

    public static void Configure(HttpClient http, string url, string? apiKey)
    {
        http.BaseAddress = new Uri(url.TrimEnd('/') + "/");
        http.Timeout = TimeSpan.FromSeconds(30);
        if (!string.IsNullOrEmpty(apiKey))
        {
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
        }
    }

    public async Task EnsureIndexAsync(string uid, CancellationToken ct)
    {
        using var get = await http.GetAsync(new Uri($"indexes/{uid}", UriKind.Relative), ct);
        if (get.StatusCode == HttpStatusCode.NotFound)
        {
            var task = await SendAsync(HttpMethod.Post, "indexes", new JsonObject { ["uid"] = uid, ["primaryKey"] = PrimaryKey }, ct);
            await WaitAsync(task, ct, allowError: "index_already_exists");
        }
        else
        {
            get.EnsureSuccessStatusCode();
        }

        var settings = new JsonObject { ["filterableAttributes"] = new JsonArray("source", "deleted") };
        await WaitAsync(await SendAsync(HttpMethod.Patch, $"indexes/{uid}/settings", settings, ct), ct);
    }

    public async Task AddOrReplaceAsync(string uid, JsonArray documents, CancellationToken ct) =>
        await WaitAsync(await SendAsync(HttpMethod.Post, $"indexes/{uid}/documents?primaryKey={PrimaryKey}", documents, ct), ct);

    public async Task DeleteAsync(string uid, IEnumerable<string> ids, CancellationToken ct) =>
        await WaitAsync(await SendAsync(HttpMethod.Post, $"indexes/{uid}/documents/delete-batch", new JsonArray([.. ids.Select(i => (JsonNode)i)]), ct), ct);

    public async Task<JsonObject?> GetDocumentAsync(string uid, string id, CancellationToken ct)
    {
        using var response = await http.GetAsync(new Uri($"indexes/{uid}/documents/{Uri.EscapeDataString(id)}", UriKind.Relative), ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<JsonObject>(ct);
    }

    public async Task<JsonObject> SearchAsync(string uid, string query, string? filter, int limit, CancellationToken ct)
    {
        var body = new JsonObject { ["q"] = query, ["limit"] = limit };
        if (filter is not null)
        {
            body["filter"] = filter;
        }

        using var response = await http.PostAsJsonAsync(new Uri($"indexes/{uid}/search", UriKind.Relative), body, ct);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<JsonObject>(ct))!;
    }

    public async Task<long> CountAsync(string uid, CancellationToken ct)
    {
        using var response = await http.GetAsync(new Uri($"indexes/{uid}/stats", UriKind.Relative), ct);
        response.EnsureSuccessStatusCode();
        var stats = await response.Content.ReadFromJsonAsync<JsonObject>(ct);
        return stats!["numberOfDocuments"]!.GetValue<long>();
    }

    private async Task<long> SendAsync(HttpMethod method, string path, JsonNode body, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(method, new Uri(path, UriKind.Relative))
        {
            Content = new StringContent(body.ToJsonString(), System.Text.Encoding.UTF8, "application/json"),
        };
        using var response = await http.SendAsync(request, ct);
        if (!response.IsSuccessStatusCode)
        {
            throw new HttpRequestException($"Meilisearch {method} {path} returned {(int)response.StatusCode}: {await response.Content.ReadAsStringAsync(ct)}");
        }

        var json = await response.Content.ReadFromJsonAsync<JsonObject>(ct);
        return json!["taskUid"]!.GetValue<long>();
    }

    private async Task WaitAsync(long taskUid, CancellationToken ct, string? allowError = null)
    {
        // Meilisearch batches queued tasks; poll quickly at first, then back off to 100 ms.
        var delay = 2;
        while (true)
        {
            using var response = await http.GetAsync(new Uri($"tasks/{taskUid}", UriKind.Relative), ct);
            response.EnsureSuccessStatusCode();
            using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
            var status = doc.RootElement.GetProperty("status").GetString();
            switch (status)
            {
                case "succeeded":
                    return;
                case "failed" or "canceled":
                    var error = doc.RootElement.TryGetProperty("error", out var e) && e.ValueKind == JsonValueKind.Object ? e : default;
                    var code = error.ValueKind == JsonValueKind.Object ? error.GetProperty("code").GetString() : null;
                    if (allowError is not null && code == allowError)
                    {
                        return;
                    }

                    throw new HttpRequestException($"Meilisearch task {taskUid} {status}: {(error.ValueKind == JsonValueKind.Object ? error.GetRawText() : "")}");
            }

            await Task.Delay(delay, ct);
            delay = Math.Min(delay * 2, 100);
        }
    }
}
