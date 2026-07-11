namespace NServiceBusContrib.Transport.Sqlite;

using System.Collections.Concurrent;
using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Logging;
using NServiceBus.Transport;

/// <summary>
/// Polling message pump. Receiving never holds a SQLite write transaction open while a handler runs;
/// instead a message row is leased with a single atomic UPDATE ... RETURNING and removed with a short
/// transaction on completion. In SendsAtomicWithReceive mode that completing transaction also flushes
/// the outgoing messages buffered during processing, making sends atomic with the receive.
/// </summary>
sealed class SqliteMessageReceiver : IMessageReceiver
{
    static readonly ILog Logger = LogManager.GetLogger<SqliteMessageReceiver>();
    static readonly TimeSpan ExpiredPurgeInterval = TimeSpan.FromSeconds(60);

    readonly SqliteTransport transport;
    readonly IConnectionFactory connectionFactory;
    readonly TransportTables tables;
    readonly Action<string, Exception, CancellationToken> criticalErrorAction;
    readonly ConcurrentDictionary<string, int> failureCounts = new();
    readonly string queueTable;
    readonly string fetchSql;
    readonly string completeSql;
    readonly string releaseSql;
    readonly string reinsertSql;
    readonly string purgeExpiredSql;
    readonly bool leaseBased;

    OnMessage? onMessage;
    OnError? onError;
    int maxConcurrency = 1;
    volatile SemaphoreSlim? limiter;
    CancellationTokenSource? pumpCancellation;
    CancellationTokenSource? processingCancellation;
    CancellationToken messageProcessingToken;
    Task? pumpTask;
    long nextExpiredPurge;

    public SqliteMessageReceiver(
        string id,
        string receiveAddress,
        SqliteTransport transport,
        IConnectionFactory connectionFactory,
        TransportTables tables,
        Action<string, Exception, CancellationToken> criticalErrorAction,
        ISubscriptionManager? subscriptionManager)
    {
        Id = id;
        ReceiveAddress = receiveAddress;
        this.transport = transport;
        this.connectionFactory = connectionFactory;
        this.tables = tables;
        this.criticalErrorAction = criticalErrorAction;
        Subscriptions = subscriptionManager!;
        leaseBased = transport.TransportTransactionMode != TransportTransactionMode.None;

        queueTable = tables.QueueTable(receiveAddress);
        var eligible = $"""
            SELECT Seq FROM {queueTable}
            WHERE (LockedUntil IS NULL OR LockedUntil <= @Now)
              AND (Expires IS NULL OR Expires > @Now)
            ORDER BY Seq
            LIMIT 1
            """;
        fetchSql = leaseBased
            ? $"UPDATE {queueTable} SET LockedUntil = @LockedUntil, LeaseId = @LeaseId WHERE Seq = ({eligible}) RETURNING Seq, Id, Headers, Body;"
            : $"DELETE FROM {queueTable} WHERE Seq = ({eligible}) RETURNING Seq, Id, Headers, Body;";
        completeSql = $"DELETE FROM {queueTable} WHERE Seq = @Seq AND LeaseId = @LeaseId;";
        releaseSql = $"UPDATE {queueTable} SET LockedUntil = NULL, LeaseId = NULL WHERE Seq = @Seq AND LeaseId = @LeaseId;";
        reinsertSql = $"INSERT INTO {queueTable} (Id, Headers, Body, Expires) VALUES (@Id, @Headers, @Body, NULL);";
        purgeExpiredSql = $"DELETE FROM {queueTable} WHERE Expires IS NOT NULL AND Expires <= @Now AND (LockedUntil IS NULL OR LockedUntil <= @Now);";
    }

    public ISubscriptionManager Subscriptions { get; }

    public string Id { get; }

    public string ReceiveAddress { get; }

    public Task Initialize(PushRuntimeSettings limitations, OnMessage onMessage, OnError onError, CancellationToken cancellationToken = default)
    {
        this.onMessage = onMessage;
        this.onError = onError;
        maxConcurrency = limitations.MaxConcurrency;
        return Task.CompletedTask;
    }

    public Task StartReceive(CancellationToken cancellationToken = default)
    {
        limiter = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        pumpCancellation = new CancellationTokenSource();
        processingCancellation = new CancellationTokenSource();
        messageProcessingToken = processingCancellation.Token;
        var pumpToken = pumpCancellation.Token;
        pumpTask = Task.Run(() => PumpLoop(pumpToken), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task ChangeConcurrency(PushRuntimeSettings limitations, CancellationToken cancellationToken = default)
    {
        var oldLimiter = limiter;
        var oldMax = maxConcurrency;
        maxConcurrency = limitations.MaxConcurrency;
        if (oldLimiter == null)
        {
            return;
        }

        limiter = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        for (var i = 0; i < oldMax; i++)
        {
            await oldLimiter.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        oldLimiter.Dispose();
    }

    public async Task StopReceive(CancellationToken cancellationToken = default)
    {
        if (pumpCancellation == null)
        {
            return;
        }

        await pumpCancellation.CancelAsync().ConfigureAwait(false);

        using var forcefulStop = cancellationToken.Register(() => processingCancellation?.Cancel());
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

        var currentLimiter = limiter;
        if (currentLimiter != null)
        {
            for (var i = 0; i < maxConcurrency; i++)
            {
                await currentLimiter.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            currentLimiter.Dispose();
        }

        pumpCancellation.Dispose();
        processingCancellation?.Dispose();
        pumpCancellation = null;
        processingCancellation = null;
        pumpTask = null;
        limiter = null;
    }

    async Task PumpLoop(CancellationToken pumpCancellationToken)
    {
        while (!pumpCancellationToken.IsCancellationRequested)
        {
            FetchedMessage? message = null;
            SemaphoreSlim? acquired = null;
            try
            {
                await PurgeExpiredIfDue(pumpCancellationToken).ConfigureAwait(false);

                var currentLimiter = limiter!;
                await currentLimiter.WaitAsync(pumpCancellationToken).ConfigureAwait(false);
                acquired = currentLimiter;

                try
                {
                    message = await TryFetch(pumpCancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    if (message == null)
                    {
                        acquired.Release();
                        acquired = null;
                    }
                }

                if (message == null)
                {
                    await Task.Delay(transport.PeekInterval, pumpCancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (pumpCancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                Logger.Error($"Receiving from queue `{ReceiveAddress}` failed", exception);
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), pumpCancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (pumpCancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            if (message != null && acquired != null)
            {
                // Processing is only cancelled on forceful shutdown, not when the pump stops fetching.
                _ = ProcessAndRelease(message, acquired, messageProcessingToken);
            }
        }
    }

    async Task PurgeExpiredIfDue(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (now < nextExpiredPurge)
        {
            return;
        }

        nextExpiredPurge = now + (long)ExpiredPurgeInterval.TotalMilliseconds;
        await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = purgeExpiredSql;
        command.Parameters.AddWithValue("@Now", now);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    async Task<FetchedMessage?> TryFetch(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = fetchSql;
        command.Parameters.AddWithValue("@Now", now);

        string? leaseId = null;
        if (leaseBased)
        {
            leaseId = Guid.NewGuid().ToString("N");
            command.Parameters.AddWithValue("@LeaseId", leaseId);
            command.Parameters.AddWithValue("@LockedUntil", now + (long)transport.MessageLeaseDuration.TotalMilliseconds);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return new FetchedMessage(
            reader.GetInt64(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetFieldValue<byte[]>(3),
            leaseId);
    }

    async Task ProcessAndRelease(FetchedMessage message, SemaphoreSlim slot, CancellationToken cancellationToken)
    {
        try
        {
            await ProcessMessage(message, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // shutdown; the message lease will expire or the message was already put back
        }
        catch (Exception exception)
        {
            Logger.Error($"Processing of message with native ID `{message.Id}` failed unexpectedly", exception);
        }
        finally
        {
            slot.Release();
        }
    }

    async Task ProcessMessage(FetchedMessage message, CancellationToken cancellationToken)
    {
        var headers = HeaderSerializer.Deserialize(message.HeadersJson);
        var body = (ReadOnlyMemory<byte>)message.Body;

        var transportTransaction = new TransportTransaction();
        PendingOperations? pending = null;
        if (transport.TransportTransactionMode == TransportTransactionMode.SendsAtomicWithReceive)
        {
            pending = new PendingOperations();
            transportTransaction.Set(pending);
        }

        try
        {
            var context = new MessageContext(message.Id, new Dictionary<string, string>(headers), body, transportTransaction, ReceiveAddress, new ContextBag());
            await onMessage!(context, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await Abandon(message, CancellationToken.None).ConfigureAwait(false);
            return;
        }
        catch (Exception exception)
        {
            await HandleFailure(message, headers, body, exception, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (await TryComplete(message, pending, cancellationToken).ConfigureAwait(false))
        {
            failureCounts.TryRemove(message.Id, out _);
        }
    }

    async Task HandleFailure(FetchedMessage message, Dictionary<string, string> headers, ReadOnlyMemory<byte> body, Exception exception, CancellationToken cancellationToken)
    {
        var attempts = failureCounts.AddOrUpdate(message.Id, 1, (_, count) => count + 1);

        var errorTransaction = new TransportTransaction();
        PendingOperations? errorPending = null;
        if (transport.TransportTransactionMode == TransportTransactionMode.SendsAtomicWithReceive)
        {
            errorPending = new PendingOperations();
            errorTransaction.Set(errorPending);
        }

        ErrorHandleResult result;
        try
        {
            var errorContext = new ErrorContext(exception, new Dictionary<string, string>(headers), message.Id, body, errorTransaction, attempts, ReceiveAddress, new ContextBag());
            result = await onError!(errorContext, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await Abandon(message, CancellationToken.None).ConfigureAwait(false);
            return;
        }
        catch (Exception onErrorException)
        {
            criticalErrorAction($"Failed to execute recoverability policy for message with native ID: `{message.Id}`", onErrorException, cancellationToken);
            await Abandon(message, CancellationToken.None).ConfigureAwait(false);
            return;
        }

        if (result == ErrorHandleResult.RetryRequired)
        {
            await Abandon(message, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (await TryComplete(message, errorPending, cancellationToken).ConfigureAwait(false))
        {
            failureCounts.TryRemove(message.Id, out _);
        }
    }

    async Task<bool> TryComplete(FetchedMessage message, PendingOperations? pending, CancellationToken cancellationToken)
    {
        if (message.LeaseId == null)
        {
            // TransportTransactionMode.None: the row was already deleted when it was fetched.
            return true;
        }

        await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);
        await using var transaction = connection.BeginTransaction();

        await using (var delete = connection.CreateCommand())
        {
            delete.Transaction = transaction;
            delete.CommandText = completeSql;
            delete.Parameters.AddWithValue("@Seq", message.Seq);
            delete.Parameters.AddWithValue("@LeaseId", message.LeaseId);
            var deleted = await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (deleted != 1)
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                Logger.WarnFormat("The lease for message with native ID `{0}` expired during processing. The message will be processed again; outgoing messages of this attempt were discarded.", message.Id);
                return false;
            }
        }

        if (pending != null && !pending.IsEmpty)
        {
            await RowWriter.Write(connection, transaction, tables, pending, cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return true;
    }

    async Task Abandon(FetchedMessage message, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            if (message.LeaseId == null)
            {
                // TransportTransactionMode.None: the row is gone, so put the message back at the
                // end of the queue to make it available again.
                command.CommandText = reinsertSql;
                command.Parameters.AddWithValue("@Id", message.Id);
                command.Parameters.AddWithValue("@Headers", message.HeadersJson);
                command.Parameters.AddWithValue("@Body", message.Body);
            }
            else
            {
                command.CommandText = releaseSql;
                command.Parameters.AddWithValue("@Seq", message.Seq);
                command.Parameters.AddWithValue("@LeaseId", message.LeaseId);
            }

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // the lease will expire on its own
        }
        catch (Exception exception)
        {
            Logger.Warn($"Failed to make message with native ID `{message.Id}` available for reprocessing. Leased messages become available again when the lease expires.", exception);
        }
    }

    sealed record FetchedMessage(long Seq, string Id, string HeadersJson, byte[] Body, string? LeaseId);
}
