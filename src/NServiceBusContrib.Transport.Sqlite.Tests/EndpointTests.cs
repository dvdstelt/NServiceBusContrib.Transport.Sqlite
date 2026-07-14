namespace NServiceBusContrib.Transport.Sqlite.Tests;

using Microsoft.Data.Sqlite;
using NServiceBus;
using NUnit.Framework;

[TestFixture]
public class EndpointTests
{
    static readonly TaskCompletionSource MessageHandled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    static readonly TaskCompletionSource EventHandled = new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Test]
    public async Task EndpointCanSendPublishAndReceive()
    {
        var databasePath = TransportTestHarness.NewDatabasePath();
        try
        {
            var configuration = new EndpointConfiguration("SmokeTest");
            configuration.UseTransport(new SqliteTransport($"Data Source={databasePath}"));
            configuration.UseSerialization<SystemJsonSerializer>();
            configuration.EnableInstallers();
            configuration.SendFailedMessagesTo("error");

            var endpoint = await Endpoint.Start(configuration);
            try
            {
                await endpoint.SendLocal(new SmokeCommand { Text = "hello" });
                await MessageHandled.Task.WaitAsync(TimeSpan.FromSeconds(30));

                await endpoint.Publish(new SmokeEvent());
                await EventHandled.Task.WaitAsync(TimeSpan.FromSeconds(30));
            }
            finally
            {
                await endpoint.Stop();
            }
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            foreach (var suffix in new[] { "", "-wal", "-shm" })
            {
                File.Delete(databasePath + suffix);
            }
        }
    }

    public class SmokeCommand : ICommand
    {
        public string? Text { get; set; }
    }

    public class SmokeEvent : IEvent;

    public class SmokeCommandHandler : IHandleMessages<SmokeCommand>
    {
        public Task Handle(SmokeCommand message, IMessageHandlerContext context)
        {
            MessageHandled.TrySetResult();
            return Task.CompletedTask;
        }
    }

    public class SmokeEventHandler : IHandleMessages<SmokeEvent>
    {
        public Task Handle(SmokeEvent message, IMessageHandlerContext context)
        {
            EventHandled.TrySetResult();
            return Task.CompletedTask;
        }
    }
}
