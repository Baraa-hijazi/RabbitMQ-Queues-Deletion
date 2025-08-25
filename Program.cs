using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

namespace RabbitMqQueuesDelete;

abstract class Program
{
    // adjust these to your environment
    private const string ManagementUrl = "HOST_URL/api/queues";
    private const string AmqpHostName = "HOST_URL";
    private const string Username = "Username";
    private const string Password = "Password";
    private const string Prefix = "prod-kpg-";

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

        // 3) delete matching queues
        foreach (var q in list)
        {
            if (!q.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
                continue;

            Console.Write($"Deleting queue '{q}'… ");
            try
            {
                await ch.QueueDeleteAsync(q, ifUnused: false, ifEmpty: false);
                Console.WriteLine("OK");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"FAILED: {ex.Message}");
            }
        }

        return 0;
    }

    private static async Task<string[]> GetQueuesAsync()
    {
        using var client = new HttpClient();
        // set up Basic Auth
        var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{Username}:{Password}"));
        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);

        // call the management API
        var response = await client.GetAsync(ManagementUrl);
        response.EnsureSuccessStatusCode();

        // parse JSON and extract queue names
        await using var stream = await response.Content.ReadAsStreamAsync();
        using var document = await JsonDocument.ParseAsync(stream);
        var queueNames = document.RootElement
            .EnumerateArray()
            .Select(elem => elem.GetProperty("name").GetString()!)
            .ToArray();

        return queueNames;
    }
}