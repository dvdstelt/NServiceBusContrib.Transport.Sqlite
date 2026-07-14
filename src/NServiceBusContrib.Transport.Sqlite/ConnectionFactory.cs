namespace NServiceBusContrib.Transport.Sqlite;

using Microsoft.Data.Sqlite;

interface IConnectionFactory
{
    ValueTask<SqliteConnection> OpenConnection(CancellationToken cancellationToken = default);
}

sealed class DefaultConnectionFactory(string connectionString) : IConnectionFactory
{
    public async ValueTask<SqliteConnection> OpenConnection(CancellationToken cancellationToken = default)
    {
        var connection = new SqliteConnection(connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await ApplyPragmas(connection, cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    static async Task ApplyPragmas(SqliteConnection connection, CancellationToken cancellationToken)
    {
        // journal_mode is database-persistent; set it only when not already WAL. Re-issuing it
        // unnecessarily on a WAL database has been observed to leave Microsoft.Data.Sqlite in a
        // state where subsequent write transactions on other connections time out with "database is locked".
        await using (var probe = connection.CreateCommand())
        {
            probe.CommandText = "PRAGMA journal_mode;";
            var current = (string?)await probe.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (!string.Equals(current, "wal", StringComparison.OrdinalIgnoreCase))
            {
                await using var set = connection.CreateCommand();
                set.CommandText = "PRAGMA journal_mode = WAL;";
                await set.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        await using var rest = connection.CreateCommand();
        rest.CommandText =
            "PRAGMA synchronous = NORMAL;" +
            "PRAGMA busy_timeout = 5000;";
        await rest.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
