namespace NServiceBus.TransportTests;

using Microsoft.Data.Sqlite;
using NServiceBus.Transport;
using NServiceBusContrib.Transport.Sqlite;

public partial class TransportTestsConfiguration
{
    public IConfigureTransportInfrastructure CreateTransportConfiguration() => new ConfigureSqliteTransportInfrastructure();
}

sealed class ConfigureSqliteTransportInfrastructure : IConfigureTransportInfrastructure
{
    readonly string databasePath = Path.Combine(Path.GetTempPath(), $"nsbc-transport-tt-{Guid.NewGuid():N}.db");

    public TransportDefinition CreateTransportDefinition() =>
        new SqliteTransport($"Data Source={databasePath}")
        {
            PeekInterval = TimeSpan.FromMilliseconds(20),
            DelayedDeliveryPollInterval = TimeSpan.FromMilliseconds(100),
        };

    public Task<TransportInfrastructure> Configure(TransportDefinition transportDefinition, HostSettings hostSettings, QueueAddress inputQueueName, string errorQueueName, CancellationToken cancellationToken = default) =>
        transportDefinition.Initialize(
            hostSettings,
            [new ReceiveSettings("mainReceiver", inputQueueName, usePublishSubscribe: true, purgeOnStartup: false, errorQueue: errorQueueName)],
            [errorQueueName],
            cancellationToken);

    public Task Cleanup(CancellationToken cancellationToken = default)
    {
        SqliteConnection.ClearAllPools();
        foreach (var suffix in new[] { "", "-wal", "-shm" })
        {
            File.Delete(databasePath + suffix);
        }

        return Task.CompletedTask;
    }
}
