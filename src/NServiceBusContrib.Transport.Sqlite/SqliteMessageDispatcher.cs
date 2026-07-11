namespace NServiceBusContrib.Transport.Sqlite;

using NServiceBus;
using NServiceBus.Transport;

sealed class SqliteMessageDispatcher(IConnectionFactory connectionFactory, TransportTables tables) : IMessageDispatcher
{
    public async Task Dispatch(TransportOperations outgoingMessages, TransportTransaction transaction, CancellationToken cancellationToken = default)
    {
        var immediate = new PendingOperations();
        transaction.TryGet<PendingOperations>(out var receiveBuffer);

        foreach (var operation in outgoingMessages.UnicastTransportOperations)
        {
            var target = SelectTarget(immediate, receiveBuffer, operation.RequiredDispatchConsistency);
            Add(target, operation.Destination, operation.Message, operation.Properties);
        }

        foreach (var operation in outgoingMessages.MulticastTransportOperations)
        {
            var target = SelectTarget(immediate, receiveBuffer, operation.RequiredDispatchConsistency);
            var subscribers = await GetSubscribers(operation.MessageType, cancellationToken).ConfigureAwait(false);
            foreach (var subscriber in subscribers)
            {
                Add(target, subscriber, operation.Message, operation.Properties);
            }
        }

        if (!immediate.IsEmpty)
        {
            await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);
            await using var sqliteTransaction = connection.BeginTransaction();
            await RowWriter.Write(connection, sqliteTransaction, tables, immediate, cancellationToken).ConfigureAwait(false);
            await sqliteTransaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    static PendingOperations SelectTarget(PendingOperations immediate, PendingOperations? receiveBuffer, DispatchConsistency consistency) =>
        consistency == DispatchConsistency.Isolated || receiveBuffer == null ? immediate : receiveBuffer;

    void Add(PendingOperations target, string destination, OutgoingMessage message, DispatchProperties? properties)
    {
        var now = DateTimeOffset.UtcNow;
        var headers = HeaderSerializer.Serialize(message.Headers);
        var body = message.Body.ToArray();

        long? expires = null;
        if (properties?.DiscardIfNotReceivedBefore is { } discard && discard.MaxTime < TimeSpan.MaxValue)
        {
            expires = now.Add(discard.MaxTime).ToUnixTimeMilliseconds();
        }

        long? due = null;
        if (properties?.DoNotDeliverBefore is { } doNotDeliverBefore)
        {
            due = doNotDeliverBefore.At.ToUnixTimeMilliseconds();
        }
        else if (properties?.DelayDeliveryWith is { } delayDeliveryWith)
        {
            due = now.Add(delayDeliveryWith.Delay).ToUnixTimeMilliseconds();
        }

        if (due is { } dueTime)
        {
            target.DelayedRows.Add(new DelayedRow(message.MessageId, destination, dueTime, headers, body, expires));
        }
        else
        {
            target.QueueRows.Add(new QueueRow(tables.QueueTable(destination), message.MessageId, headers, body, expires));
        }
    }

    async Task<IReadOnlyCollection<string>> GetSubscribers(Type messageType, CancellationToken cancellationToken)
    {
        var topics = GetTopics(messageType);
        if (topics.Count == 0)
        {
            return [];
        }

        await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        var parameterNames = new List<string>(topics.Count);
        var index = 0;
        foreach (var topic in topics)
        {
            var name = $"@Topic{index++}";
            parameterNames.Add(name);
            command.Parameters.AddWithValue(name, topic);
        }

        command.CommandText = $"SELECT DISTINCT Queue FROM {tables.SubscriptionsTable} WHERE Topic IN ({string.Join(", ", parameterNames)});";

        var subscribers = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            subscribers.Add(reader.GetString(0));
        }

        return subscribers;
    }

    // Publishing matches subscribers of the concrete event type and of every base type and interface
    // it implements, so subscribers only ever register the exact types they handle.
    internal static IReadOnlyCollection<string> GetTopics(Type messageType)
    {
        var types = new HashSet<Type>();
        for (var type = messageType; type != null && type != typeof(object); type = type.BaseType)
        {
            types.Add(type);
        }

        types.UnionWith(messageType.GetInterfaces());

        var topics = new HashSet<string>(StringComparer.Ordinal);
        foreach (var type in types)
        {
            if (type.FullName is { } fullName &&
                type.Assembly != typeof(IMessage).Assembly &&
                !fullName.StartsWith("System.", StringComparison.Ordinal))
            {
                topics.Add(fullName);
            }
        }

        return topics;
    }
}
