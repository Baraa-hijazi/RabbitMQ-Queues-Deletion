using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace RabbitMqQueuesDelete;

abstract class Program
{
    // adjust these to your environment
    private const string ManagementUrl = "https://HOST_URL/api/queues";
    private const string AmqpHostName = "HOST_URL";
    private const string Username = "Username";
    private const string Password = "Password";
    private const string Prefix = "amq.gen-";

    private static async Task<int> Main()
    {
        // 1) fetch queue list
        var list = await GetQueuesAsync();

        // 2) connect AMQP
        var factory = new ConnectionFactory
        {
            HostName = AmqpHostName,
            UserName = Username,
            Password = Password,
            Port = Convert.ToInt32("5671"),
            Ssl = new SslOption
            {
                Enabled = true,
                ServerName = AmqpHostName
            }
        };
        var conn = await factory.CreateConnectionAsync();
        var ch = await conn.CreateChannelAsync();

        var count = 0;
        // 3) delete matching queues
        foreach (var q in list)
        {
            if (!q.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
                continue;

            Console.Write($"Deleting queue '{q}'… ");
            try
            {
                ++count;
                await ch.QueueDeleteAsync(q, ifUnused: true, ifEmpty: true);
                Console.WriteLine($"OK, {count} queues deleted.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"FAILED: {ex.Message}");
            }
        }

        return 0;
    }

    private static async Task<string[]> GetQueuesAsync(CancellationToken ct = default)
    {
        // Build handler to avoid gzip truncation + force HTTP/1.1
        var handler = new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.None,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            SslOptions = { }
        };

        using var client = new HttpClient(handler);
        client.Timeout = TimeSpan.FromMinutes(5);

        // Basic auth
        var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{Username}:{Password}"));

        // If your vhost is "/", URL-encode to %2F. Also ask only for 'name'.
        // If your vhost is something else, replace %2F with Uri.EscapeDataString(vhost).
        var url = $"{ManagementUrl.TrimEnd('/')}/%2F?columns=name";

        // Some installations support pagination; uncomment to reduce payload further:
        // url += "&pagination=true&page=1&page_size=500";

        // Build request that prefers HTTP/1.1 and streams the body
        using var req = new HttpRequestMessage(HttpMethod.Get, url)
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrLower,
        };
        req.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        req.Headers.Accept.ParseAdd("application/json");
        // Also disable gzip at the request level (defensive)
        req.Headers.AcceptEncoding.Clear();

        // Simple transient retry on EOF
        const int maxAttempts = 3;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                using var res = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);
                res.EnsureSuccessStatusCode();

                await using var stream = await res.Content.ReadAsStreamAsync(ct);
                using var doc = await JsonDocument.ParseAsync(stream, cancellationToken: ct);
                // Expecting: [{ "name": "..." }, ...]
                return doc.RootElement
                    .EnumerateArray()
                    .Select(e => e.GetProperty("name").GetString()!)
                    .ToArray();
            }
            catch (IOException) when (attempt < maxAttempts)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(300 * attempt), ct);
            }
            catch (HttpRequestException) when (attempt < maxAttempts)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(300 * attempt), ct);
            }
        }

        throw new HttpRequestException("Failed to fetch queue list after retries.");
    }
}