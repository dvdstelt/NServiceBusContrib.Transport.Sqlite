namespace NServiceBusContrib.Transport.Sqlite.Tests;

using NServiceBus;
using NServiceBus.Extensibility;
using NServiceBus.Unicast.Messages;
using NUnit.Framework;

[TestFixture]
public class PubSubTests
{
    [Test]
    public async Task DeliversEventToSubscriber()
    {
        await using var harness = await TransportTestHarness.Start(usePublishSubscribe: true);
        await harness.Receiver.Subscriptions.SubscribeAll([new MessageMetadata(typeof(OrderPlaced))], new ContextBag());

        await harness.Publish(typeof(OrderPlaced), messageId: "event-1");

        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("TestEndpoint") == 1);
    }

    [Test]
    public async Task DeliversDerivedEventToBaseTypeSubscriber()
    {
        await using var harness = await TransportTestHarness.Start(usePublishSubscribe: true);
        await harness.Receiver.Subscriptions.SubscribeAll([new MessageMetadata(typeof(IOrderEvent))], new ContextBag());

        await harness.Publish(typeof(OrderPlaced), messageId: "derived");

        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("TestEndpoint") == 1);
    }

    [Test]
    public async Task DeliversEventOnlyOnceWhenSubscribedToMultipleTypesInHierarchy()
    {
        await using var harness = await TransportTestHarness.Start(usePublishSubscribe: true);
        await harness.Receiver.Subscriptions.SubscribeAll(
            [new MessageMetadata(typeof(IOrderEvent)), new MessageMetadata(typeof(OrderPlaced))],
            new ContextBag());

        await harness.Publish(typeof(OrderPlaced), messageId: "once");

        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("TestEndpoint") == 1);
        await Task.Delay(200);
        Assert.That(await harness.CountRows("TestEndpoint"), Is.EqualTo(1));
    }

    [Test]
    public async Task UnsubscribeStopsDelivery()
    {
        await using var harness = await TransportTestHarness.Start(usePublishSubscribe: true);
        await harness.Receiver.Subscriptions.SubscribeAll([new MessageMetadata(typeof(OrderPlaced))], new ContextBag());
        await harness.Receiver.Subscriptions.Unsubscribe(new MessageMetadata(typeof(OrderPlaced)), new ContextBag());

        await harness.Publish(typeof(OrderPlaced), messageId: "after-unsubscribe");

        await Task.Delay(200);
        Assert.That(await harness.CountRows("TestEndpoint"), Is.EqualTo(0));
    }

    [Test]
    public async Task PublishWithoutSubscribersIsANoOp()
    {
        await using var harness = await TransportTestHarness.Start(usePublishSubscribe: true);
        await harness.Publish(typeof(OrderPlaced), messageId: "nobody-listens");
        await Task.Delay(100);
        Assert.That(await harness.CountRows("TestEndpoint"), Is.EqualTo(0));
    }

    public interface IOrderEvent : IEvent;

    public class OrderPlaced : IOrderEvent;
}
