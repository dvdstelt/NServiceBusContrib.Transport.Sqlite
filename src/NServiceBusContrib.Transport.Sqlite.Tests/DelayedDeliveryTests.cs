namespace NServiceBusContrib.Transport.Sqlite.Tests;

using NServiceBus.DelayedDelivery;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class DelayedDeliveryTests
{
    [Test]
    public async Task DeliversMessageAfterDelay()
    {
        await using var harness = await TransportTestHarness.Start();
        var received = new TaskCompletionSource<MessageContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        await harness.StartReceiver((context, _) =>
        {
            received.TrySetResult(context);
            return Task.CompletedTask;
        });

        var properties = new DispatchProperties
        {
            DelayDeliveryWith = new DelayDeliveryWith(TimeSpan.FromMilliseconds(300)),
        };
        await harness.Dispatch("TestEndpoint", messageId: "delayed", properties: properties);

        // The message must land in the delayed table first, not in the queue.
        Assert.That(await harness.CountRows("nsbc.delayed"), Is.EqualTo(1));
        Assert.That(await harness.CountRows("TestEndpoint"), Is.EqualTo(0));

        var context = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.That(context.NativeMessageId, Is.EqualTo("delayed"));
        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("nsbc.delayed") == 0);
    }

    [Test]
    public async Task HonorsDoNotDeliverBefore()
    {
        await using var harness = await TransportTestHarness.Start();
        var receivedAt = new TaskCompletionSource<DateTimeOffset>(TaskCreationOptions.RunContinuationsAsynchronously);
        await harness.StartReceiver((context, _) =>
        {
            receivedAt.TrySetResult(DateTimeOffset.UtcNow);
            return Task.CompletedTask;
        });

        var deliverAt = DateTimeOffset.UtcNow.AddMilliseconds(500);
        var properties = new DispatchProperties
        {
            DoNotDeliverBefore = new DoNotDeliverBefore(deliverAt),
        };
        await harness.Dispatch("TestEndpoint", properties: properties);

        var arrived = await receivedAt.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.That(arrived, Is.GreaterThanOrEqualTo(deliverAt.AddMilliseconds(-50)));
    }

    [Test]
    public async Task DeadLettersDelayedMessageWhenDestinationIsMissing()
    {
        await using var harness = await TransportTestHarness.Start();

        // "MissingDestination" has no queue table, so the pump can never move this message.
        var properties = new DispatchProperties
        {
            DelayDeliveryWith = new DelayDeliveryWith(TimeSpan.FromMilliseconds(50)),
        };
        await harness.Dispatch("MissingDestination", messageId: "poison-delayed", properties: properties);

        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("error") == 1, timeoutSeconds: 15);
        Assert.That(await harness.CountRows("nsbc.delayed"), Is.EqualTo(0));

        var headers = await harness.QueryScalarString("SELECT Headers FROM \"error\" LIMIT 1;");
        Assert.That(headers, Does.Contain("MissingDestination"));
        Assert.That(headers, Does.Contain("DelayedDeliveryFailure"));
    }
}
