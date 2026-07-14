namespace NServiceBusContrib.Transport.Sqlite.Tests;

using Microsoft.Data.Sqlite;
using NServiceBus;
using NServiceBus.Routing;
using NServiceBus.Transport;
using NUnit.Framework;

sealed class TransportTestHarness : IAsyncDisposable
{
    readonly bool deleteDatabaseOnDispose;

    TransportTestHarness(string databasePath, bool deleteDatabaseOnDispose)
    {
        DatabasePath = databasePath;
        ConnectionString = $"Data Source={databasePath}";
        this.deleteDatabaseOnDispose = deleteDatabaseOnDispose;
    }

    public string DatabasePath { get; }

    public string ConnectionString { get; }

    public SqliteTransport Transport { get; private set; } = null!;

    public TransportInfrastructure Infrastructure { get; private set; } = null!;

    public IMessageReceiver Receiver => Infrastructure.Receivers["main"];

    public static async Task<TransportTestHarness> Start(
        string endpointName = "TestEndpoint",
        TransportTransactionMode transactionMode = TransportTransactionMode.SendsAtomicWithReceive,
        bool usePublishSubscribe = false,
        bool purgeOnStartup = false,
        string? databasePath = null,
        bool deleteDatabaseOnDispose = true,
        Action<SqliteTransport>? customize = null)
    {
        databasePath ??= NewDatabasePath();
        var harness = new TransportTestHarness(databasePath, deleteDatabaseOnDispose);
        harness.Transport = new SqliteTransport(harness.ConnectionString)
        {
            PeekInterval = TimeSpan.FromMilliseconds(20),
            DelayedDeliveryPollInterval = TimeSpan.FromMilliseconds(100),
            TransportTransactionMode = transactionMode,
        };
        customize?.Invoke(harness.Transport);

        var hostSettings = new HostSettings(
            endpointName,
            endpointName,
            new StartupDiagnosticEntries(),
            (_, _, _) => { },
            setupInfrastructure: true,
            coreSettings: null);
        var receiveSettings = new ReceiveSettings("main", new QueueAddress(endpointName), usePublishSubscribe, purgeOnStartup, "error");
        harness.Infrastructure = await harness.Transport.Initialize(hostSettings, [receiveSettings], ["error"]);
        return harness;
    }

    public static string NewDatabasePath() =>
        Path.Combine(Path.GetTempPath(), $"nsbc-transport-tests-{Guid.NewGuid():N}.db");

    public Task StartReceiver(OnMessage onMessage, OnError? onError = null, int maxConcurrency = 1) =>
        StartReceiver(Receiver, onMessage, onError, maxConcurrency);

    public static async Task StartReceiver(IMessageReceiver receiver, OnMessage onMessage, OnError? onError = null, int maxConcurrency = 1)
    {
        onError ??= (_, _) => Task.FromResult(ErrorHandleResult.RetryRequired);
        await receiver.Initialize(new PushRuntimeSettings(maxConcurrency), onMessage, onError);
        await receiver.StartReceive();
    }

    public Task Dispatch(string destination, string? messageId = null, Dictionary<string, string>? headers = null, byte[]? body = null, DispatchProperties? properties = null, TransportTransaction? transaction = null)
    {
        var message = new OutgoingMessage(messageId ?? Guid.NewGuid().ToString(), headers ?? [], body ?? [1, 2, 3]);
        var operation = new TransportOperation(message, new UnicastAddressTag(destination), properties ?? [], DispatchConsistency.Default);
        return Infrastructure.Dispatcher.Dispatch(new TransportOperations(operation), transaction ?? new TransportTransaction());
    }

    public Task Publish(Type eventType, string? messageId = null, Dictionary<string, string>? headers = null, byte[]? body = null)
    {
        var message = new OutgoingMessage(messageId ?? Guid.NewGuid().ToString(), headers ?? [], body ?? [1, 2, 3]);
        var operation = new TransportOperation(message, new MulticastAddressTag(eventType), [], DispatchConsistency.Default);
        return Infrastructure.Dispatcher.Dispatch(new TransportOperations(operation), new TransportTransaction());
    }

    public async Task<long> CountRows(string table)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM \"{table}\";";
        return (long)(await command.ExecuteScalarAsync())!;
    }

    public async Task<string?> QueryScalarString(string sql)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return (string?)await command.ExecuteScalarAsync();
    }

    public async Task<bool> TableExists(string table)
    {
        await using var connection = new SqliteConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = @Name;";
        command.Parameters.AddWithValue("@Name", table);
        return (long)(await command.ExecuteScalarAsync())! == 1;
    }

    public static async Task WaitUntil(Func<Task<bool>> condition, int timeoutSeconds = 10)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));
        while (!await condition())
        {
            if (timeout.IsCancellationRequested)
            {
                Assert.Fail($"Condition not reached within {timeoutSeconds} seconds");
            }

            await Task.Delay(20);
        }
    }

    public static Task WaitUntil(Func<bool> condition, int timeoutSeconds = 10) =>
        WaitUntil(() => Task.FromResult(condition()), timeoutSeconds);

    public async ValueTask DisposeAsync()
    {
        foreach (var receiver in Infrastructure.Receivers.Values)
        {
            await receiver.StopReceive();
        }

        await Infrastructure.Shutdown();
        SqliteConnection.ClearAllPools();
        if (deleteDatabaseOnDispose)
        {
            foreach (var suffix in new[] { "", "-wal", "-shm" })
            {
                File.Delete(DatabasePath + suffix);
            }
        }
    }
}
