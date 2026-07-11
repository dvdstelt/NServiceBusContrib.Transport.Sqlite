namespace NServiceBusContrib.Transport.Sqlite;

using NServiceBus.Extensibility;
using NServiceBus.Transport;
using NServiceBus.Unicast.Messages;

sealed class SubscriptionManager(IConnectionFactory connectionFactory, TransportTables tables, string endpointName, string subscriberQueue) : ISubscriptionManager
{
    public async Task SubscribeAll(MessageMetadata[] eventTypes, ContextBag context, CancellationToken cancellationToken = default)
    {
        await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);
        await using var transaction = connection.BeginTransaction();
        foreach (var eventType in eventTypes)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"INSERT INTO {tables.SubscriptionsTable} (Topic, Endpoint, Queue) VALUES (@Topic, @Endpoint, @Queue) ON CONFLICT (Topic, Endpoint) DO UPDATE SET Queue = excluded.Queue;";
            command.Parameters.AddWithValue("@Topic", TopicName(eventType));
            command.Parameters.AddWithValue("@Endpoint", endpointName);
            command.Parameters.AddWithValue("@Queue", subscriberQueue);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task Unsubscribe(MessageMetadata eventType, ContextBag context, CancellationToken cancellationToken = default)
    {
        await using var connection = await connectionFactory.OpenConnection(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {tables.SubscriptionsTable} WHERE Topic = @Topic AND Endpoint = @Endpoint;";
        command.Parameters.AddWithValue("@Topic", TopicName(eventType));
        command.Parameters.AddWithValue("@Endpoint", endpointName);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    static string TopicName(MessageMetadata eventType) => eventType.MessageType.FullName ?? eventType.MessageType.Name;
}
