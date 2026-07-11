namespace NServiceBusContrib.Transport.Sqlite;

sealed class TransportTables(string tablePrefix)
{
    internal const string DelayedTableSuffix = "nsbc.delayed";
    internal const string SubscriptionsTableSuffix = "nsbc.subscriptions";

    public string DelayedTable { get; } = Quote(tablePrefix + DelayedTableSuffix);

    public string SubscriptionsTable { get; } = Quote(tablePrefix + SubscriptionsTableSuffix);

    public string QueueTable(string address) => Quote(tablePrefix + address);

    public static string Quote(string identifier) => $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

    public string CreateQueueSql(string address) => $"""
        CREATE TABLE IF NOT EXISTS {QueueTable(address)} (
            Seq INTEGER PRIMARY KEY AUTOINCREMENT,
            Id TEXT NOT NULL,
            Headers TEXT NOT NULL,
            Body BLOB NOT NULL,
            Expires INTEGER NULL,
            LockedUntil INTEGER NULL,
            LeaseId TEXT NULL
        );
        CREATE INDEX IF NOT EXISTS {Quote(tablePrefix + address + "_lease_idx")} ON {QueueTable(address)} (LockedUntil, Seq);
        """;

    public string CreateDelayedSql() => $"""
        CREATE TABLE IF NOT EXISTS {DelayedTable} (
            Seq INTEGER PRIMARY KEY AUTOINCREMENT,
            Id TEXT NOT NULL,
            Destination TEXT NOT NULL,
            Due INTEGER NOT NULL,
            Headers TEXT NOT NULL,
            Body BLOB NOT NULL,
            Expires INTEGER NULL,
            FailedAttempts INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS {Quote(tablePrefix + DelayedTableSuffix + "_due_idx")} ON {DelayedTable} (Due);
        """;

    public string CreateSubscriptionsSql() => $"""
        CREATE TABLE IF NOT EXISTS {SubscriptionsTable} (
            Topic TEXT NOT NULL,
            Endpoint TEXT NOT NULL,
            Queue TEXT NOT NULL,
            PRIMARY KEY (Topic, Endpoint)
        ) WITHOUT ROWID;
        """;
}
