namespace NServiceBusContrib.Transport.Sqlite;

using Microsoft.Data.Sqlite;

sealed record QueueRow(string Table, string Id, string Headers, byte[] Body, long? Expires);

sealed record DelayedRow(string Id, string Destination, long Due, string Headers, byte[] Body, long? Expires);

/// <summary>
/// Outgoing rows buffered during message processing in SendsAtomicWithReceive mode. The receiver
/// stashes an instance in the TransportTransaction; the dispatcher appends to it instead of writing,
/// and the receiver flushes it in the same SQLite transaction that deletes the incoming message.
/// </summary>
sealed class PendingOperations
{
    public List<QueueRow> QueueRows { get; } = [];

    public List<DelayedRow> DelayedRows { get; } = [];

    public bool IsEmpty => QueueRows.Count == 0 && DelayedRows.Count == 0;
}

static class RowWriter
{
    public static async Task Write(SqliteConnection connection, SqliteTransaction transaction, TransportTables tables, PendingOperations operations, CancellationToken cancellationToken = default)
    {
        foreach (var row in operations.QueueRows)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"INSERT INTO {row.Table} (Id, Headers, Body, Expires) VALUES (@Id, @Headers, @Body, @Expires);";
            command.Parameters.AddWithValue("@Id", row.Id);
            command.Parameters.AddWithValue("@Headers", row.Headers);
            command.Parameters.AddWithValue("@Body", row.Body);
            command.Parameters.AddWithValue("@Expires", (object?)row.Expires ?? DBNull.Value);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        foreach (var row in operations.DelayedRows)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"INSERT INTO {tables.DelayedTable} (Id, Destination, Due, Headers, Body, Expires) VALUES (@Id, @Destination, @Due, @Headers, @Body, @Expires);";
            command.Parameters.AddWithValue("@Id", row.Id);
            command.Parameters.AddWithValue("@Destination", row.Destination);
            command.Parameters.AddWithValue("@Due", row.Due);
            command.Parameters.AddWithValue("@Headers", row.Headers);
            command.Parameters.AddWithValue("@Body", row.Body);
            command.Parameters.AddWithValue("@Expires", (object?)row.Expires ?? DBNull.Value);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
