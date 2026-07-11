namespace NServiceBusContrib.Transport.Sqlite;

using NServiceBus;
using NServiceBus.Transport;

/// <summary>
/// A transport for NServiceBus that stores queues as tables in a single SQLite database file.
/// All communicating endpoints must use the same database file.
/// </summary>
public class SqliteTransport : TransportDefinition
{
    /// <summary>
    /// Creates a new SQLite transport.
    /// </summary>
    /// <param name="connectionString">A Microsoft.Data.Sqlite connection string, e.g. "Data Source=transport.db".</param>
    public SqliteTransport(string connectionString)
        : base(TransportTransactionMode.SendsAtomicWithReceive, supportsDelayedDelivery: true, supportsPublishSubscribe: true, supportsTTBR: true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ConnectionString = connectionString;
    }

    /// <summary>
    /// The connection string used for all transport operations.
    /// </summary>
    public string ConnectionString { get; }

    /// <summary>
    /// How long a receiver waits before polling again when its queue is empty.
    /// </summary>
    public TimeSpan PeekInterval { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// How often the delayed message pump checks for due messages.
    /// </summary>
    public TimeSpan DelayedDeliveryPollInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// How long a received message stays locked while it is being processed. If processing takes
    /// longer than this, the message becomes visible again and can be processed a second time.
    /// </summary>
    public TimeSpan MessageLeaseDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// A prefix applied to every table the transport creates and uses.
    /// </summary>
    public string TablePrefix { get; set; } = "";

    /// <summary>
    /// Whether queue, delayed, and subscription tables are created at startup when installers run.
    /// </summary>
    public bool CreateSchema { get; set; } = true;

    /// <inheritdoc />
    public override async Task<TransportInfrastructure> Initialize(HostSettings hostSettings, ReceiveSettings[] receivers, string[] sendingAddresses, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(hostSettings);
        ArgumentNullException.ThrowIfNull(receivers);
        ArgumentNullException.ThrowIfNull(sendingAddresses);

        var infrastructure = new SqliteTransportInfrastructure(this, hostSettings, receivers, sendingAddresses);
        await infrastructure.Initialize(cancellationToken).ConfigureAwait(false);
        return infrastructure;
    }

    /// <inheritdoc />
    public override IReadOnlyCollection<TransportTransactionMode> GetSupportedTransactionModes() =>
    [
        TransportTransactionMode.None,
        TransportTransactionMode.ReceiveOnly,
        TransportTransactionMode.SendsAtomicWithReceive,
    ];
}
