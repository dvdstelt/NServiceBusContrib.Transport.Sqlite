namespace NServiceBusContrib.Transport.Sqlite;

using Microsoft.Data.Sqlite;
using NServiceBus.Logging;

/// <summary>
/// Moves due messages from the delayed table to their destination queue tables. Every endpoint runs
/// a pump against the shared delayed table; competing pumps are safe because each row is claimed by
/// a DELETE inside the same transaction that inserts it into the destination queue.
/// </summary>
sealed class DelayedMessagePump(IConnectionFactory connectionFactory, TransportTables tables, TimeSpan pollInterval)
{
    static readonly ILog Logger = LogManager.GetLogger<DelayedMessagePump>();

    CancellationTokenSource? stopSource;
    Task? pumpTask;

    public void Start()
    {
        stopSource = new CancellationTokenSource();
        var cancellationToken = stopSource.Token;
        pumpTask = Task.Run(() => Loop(cancellationToken), CancellationToken.None);
    }

    public async Task Stop(CancellationToken cancellationToken = default)
    {
        if (stopSource == null)
        {
            return;
        }

        await stopSource.CancelAsync().ConfigureAwait(false);
        if (pumpTask != null)
        {
            try
            {
                await pumpTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // pump stopped
            }
        }

        stopSource.Dispose();
        stopSource = null;
        pumpTask = null;
    }

    async Task Loop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await MoveDueMessages(cancellationToken).ConfigureAwait(false);
                await Task.Delay(pollInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                Logger.Error("Moving due delayed messages failed", exception);
                try
                {
                    await Task.Delay(pollInterval, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    async Task MoveDueMessages(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);

        var rows = new List<DelayedRowWithSeq>();
        await using (var select = connection.CreateCommand())
        {
            select.CommandText = $"SELECT Seq, Id, Destination, Headers, Body, Expires FROM {tables.DelayedTable} WHERE Due <= @Now ORDER BY Due, Seq LIMIT 100;";
            select.Parameters.AddWithValue("@Now", now);
            await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rows.Add(new DelayedRowWithSeq(
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.GetFieldValue<byte[]>(4),
                    await reader.IsDBNullAsync(5, cancellationToken).ConfigureAwait(false) ? null : reader.GetInt64(5)));
            }
        }

        foreach (var row in rows)
        {
            try
            {
                await MoveMessage(connection, row, now, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                Logger.Error($"Failed to move due delayed message `{row.Id}` to `{row.Destination}`. It will be retried.", exception);
                await Postpone(connection, row.Seq, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    async Task MoveMessage(SqliteConnection connection, DelayedRowWithSeq row, long now, CancellationToken cancellationToken)
    {
        await using var transaction = connection.BeginTransaction();

        await using (var delete = connection.CreateCommand())
        {
            delete.Transaction = transaction;
            delete.CommandText = $"DELETE FROM {tables.DelayedTable} WHERE Seq = @Seq;";
            delete.Parameters.AddWithValue("@Seq", row.Seq);
            var deleted = await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (deleted == 0)
            {
                // Another pump moved this row first.
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
        }

        if (row.Expires is { } expires && expires <= now)
        {
            // Time to be received elapsed while the message was waiting; discard it.
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        await using (var insert = connection.CreateCommand())
        {
            insert.Transaction = transaction;
            insert.CommandText = $"INSERT INTO {tables.QueueTable(row.Destination)} (Id, Headers, Body, Expires) VALUES (@Id, @Headers, @Body, @Expires);";
            insert.Parameters.AddWithValue("@Id", row.Id);
            insert.Parameters.AddWithValue("@Headers", row.Headers);
            insert.Parameters.AddWithValue("@Body", row.Body);
            insert.Parameters.AddWithValue("@Expires", (object?)row.Expires ?? DBNull.Value);
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    async Task Postpone(SqliteConnection connection, long seq, CancellationToken cancellationToken)
    {
        await using var postpone = connection.CreateCommand();
        postpone.CommandText = $"UPDATE {tables.DelayedTable} SET Due = @Due WHERE Seq = @Seq;";
        postpone.Parameters.AddWithValue("@Due", DateTimeOffset.UtcNow.AddSeconds(30).ToUnixTimeMilliseconds());
        postpone.Parameters.AddWithValue("@Seq", seq);
        await postpone.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    sealed record DelayedRowWithSeq(long Seq, string Id, string Destination, string Headers, byte[] Body, long? Expires);
}
