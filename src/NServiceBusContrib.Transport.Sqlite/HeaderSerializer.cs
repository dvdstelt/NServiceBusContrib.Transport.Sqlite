namespace NServiceBusContrib.Transport.Sqlite;

using System.Text.Json;

static class HeaderSerializer
{
    public static string Serialize(Dictionary<string, string> headers) => JsonSerializer.Serialize(headers);

    public static Dictionary<string, string> Deserialize(string json) =>
        JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? [];
}
