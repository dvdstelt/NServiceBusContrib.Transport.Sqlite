namespace NServiceBusContrib.Transport.Sqlite;

using System.Text;
using NServiceBus.Transport;

sealed class SqliteTransportInfrastructure : TransportInfrastructure
{
    readonly SqliteTransport transport;
    readonly HostSettings hostSettings;
    readonly ReceiveSettings[] receiveSettings;
    readonly string[] sendingAddresses;
    readonly IConnectionFactory connectionFactory;
    readonly TransportTables tables;
    readonly DelayedMessagePump delayedPump;

    public SqliteTransportInfrastructure(SqliteTransport transport, HostSettings hostSettings, ReceiveSettings[] receiveSettings, string[] sendingAddresses)
    {
        this.transport = transport;
        this.hostSettings = hostSettings;
        this.receiveSettings = receiveSettings;
        this.sendingAddresses = sendingAddresses;

        connectionFactory = new DefaultConnectionFactory(transport.ConnectionString);
        tables = new TransportTables(transport.TablePrefix);
        var errorQueue = receiveSettings.Select(settings => settings.ErrorQueue).FirstOrDefault(queue => !string.IsNullOrEmpty(queue));
        delayedPump = new DelayedMessagePump(connectionFactory, tables, transport.DelayedDeliveryPollInterval, errorQueue);
        Dispatcher = new SqliteMessageDispatcher(connectionFactory, tables);

        var receivers = new Dictionary<string, IMessageReceiver>();
        foreach (var settings in receiveSettings)
        {
            var address = ToTransportAddress(settings.ReceiveAddress);
            var subscriptionManager = settings.UsePublishSubscribe
                ? new SubscriptionManager(connectionFactory, tables, hostSettings.Name, address)
                : null;
            receivers.Add(settings.Id, new SqliteMessageReceiver(settings.Id, address, transport, connectionFactory, tables, hostSettings.CriticalErrorAction, subscriptionManager));
        }

        Receivers = receivers;
    }

    public async Task Initialize(CancellationToken cancellationToken = default)
    {
        if (hostSettings.SetupInfrastructure && transport.CreateSchema)
        {
            await CreateSchema(cancellationToken).ConfigureAwait(false);
        }

        foreach (var settings in receiveSettings)
        {
            if (settings.PurgeOnStartup)
            {
                await Purge(ToTransportAddress(settings.ReceiveAddress), cancellationToken).ConfigureAwait(false);
            }
        }

        delayedPump.Start();

        hostSettings.StartupDiagnostic.Add("NServiceBusContrib.Transport.Sqlite", new
        {
            TransactionMode = transport.TransportTransactionMode.ToString(),
            transport.TablePrefix,
            PeekInterval = transport.PeekInterval.ToString(),
            DelayedDeliveryPollInterval = transport.DelayedDeliveryPollInterval.ToString(),
            MessageLeaseDuration = transport.MessageLeaseDuration.ToString(),
        });
    }

    public override string ToTransportAddress(QueueAddress address) => SqliteAddressTranslator.Translate(address);

    public override async Task Shutdown(CancellationToken cancellationToken = default) =>
        await delayedPump.Stop(cancellationToken).ConfigureAwait(false);

    async Task CreateSchema(CancellationToken cancellationToken)
    {
        var addresses = new HashSet<string>(StringComparer.Ordinal);
        foreach (var settings in receiveSettings)
        {
            addresses.Add(ToTransportAddress(settings.ReceiveAddress));
            if (!string.IsNullOrEmpty(settings.ErrorQueue))
            {
                addresses.Add(settings.ErrorQueue);
            }
        }

        addresses.UnionWith(sendingAddresses);

        var sql = new StringBuilder();
        foreach (var address in addresses)
        {
            sql.AppendLine(tables.CreateQueueSql(address));
        }

        sql.AppendLine(tables.CreateDelayedSql());
        sql.AppendLine(tables.CreateSubscriptionsSql());

        await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = sql.ToString();
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    async Task Purge(string address, CancellationToken cancellationToken)
    {
        await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {tables.QueueTable(address)};";
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
