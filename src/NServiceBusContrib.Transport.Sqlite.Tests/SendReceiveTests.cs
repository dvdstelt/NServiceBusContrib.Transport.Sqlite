namespace NServiceBusContrib.Transport.Sqlite.Tests;

using System.Collections.Concurrent;
using NServiceBus.Performance.TimeToBeReceived;
using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class SendReceiveTests
{
    [TestCase(TransportTransactionMode.None)]
    [TestCase(TransportTransactionMode.ReceiveOnly)]
    [TestCase(TransportTransactionMode.SendsAtomicWithReceive)]
    public async Task RoundtripsMessageWithHeadersAndBody(TransportTransactionMode transactionMode)
    {
        await using var harness = await TransportTestHarness.Start(transactionMode: transactionMode);
        var received = new TaskCompletionSource<MessageContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        await harness.StartReceiver((context, _) =>
        {
            received.TrySetResult(context);
            return Task.CompletedTask;
        });

        var body = new byte[] { 42, 43, 44 };
        await harness.Dispatch("TestEndpoint", messageId: "msg-1", headers: new Dictionary<string, string> { ["Key"] = "Value" }, body: body);

        var context = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Multiple(() =>
        {
            Assert.That(context.NativeMessageId, Is.EqualTo("msg-1"));
            Assert.That(context.Headers["Key"], Is.EqualTo("Value"));
            Assert.That(context.Body.ToArray(), Is.EqualTo(body));
        });

        // The message must be removed from the queue after successful processing.
        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("TestEndpoint") == 0);
    }

    [Test]
    public async Task PreservesMessageOrder()
    {
        await using var harness = await TransportTestHarness.Start();
        var received = new ConcurrentQueue<string>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await harness.StartReceiver((context, _) =>
        {
            received.Enqueue(context.NativeMessageId);
            if (received.Count == 3)
            {
                done.TrySetResult();
            }

            return Task.CompletedTask;
        });

        await harness.Dispatch("TestEndpoint", messageId: "first");
        await harness.Dispatch("TestEndpoint", messageId: "second");
        await harness.Dispatch("TestEndpoint", messageId: "third");

        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.That(received, Is.EqualTo(new[] { "first", "second", "third" }));
    }

    [Test]
    public async Task DiscardsExpiredMessages()
    {
        await using var harness = await TransportTestHarness.Start();
        var received = new ConcurrentQueue<string>();

        // Dispatch and let the message expire before the receiver starts, so it cannot legitimately
        // be consumed inside its time-to-be-received window.
        var properties = new DispatchProperties
        {
            DiscardIfNotReceivedBefore = new DiscardIfNotReceivedBefore(TimeSpan.FromMilliseconds(50)),
        };
        await harness.Dispatch("TestEndpoint", messageId: "expired", properties: properties);
        await Task.Delay(200);

        await harness.StartReceiver((context, _) =>
        {
            received.Enqueue(context.NativeMessageId);
            return Task.CompletedTask;
        });
        await harness.Dispatch("TestEndpoint", messageId: "fresh");

        await TransportTestHarness.WaitUntil(() => received.Contains("fresh"));
        Assert.That(received, Does.Not.Contain("expired"));
    }

    [Test]
    public async Task SendsFromHandlerAreDiscardedWhenReceiveIsRetried()
    {
        // SendsAtomicWithReceive: sends buffered during a failing attempt must not be dispatched.
        await using var harness = await TransportTestHarness.Start();
        var attempts = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await harness.StartReceiver(
            async (context, cancellationToken) =>
            {
                await harness.Dispatch("error", messageId: $"outgoing-{attempts}", transaction: context.TransportTransaction);
                if (Interlocked.Increment(ref attempts) == 1)
                {
                    throw new InvalidOperationException("simulated failure");
                }

                done.TrySetResult();
            },
            (_, _) => Task.FromResult(ErrorHandleResult.RetryRequired));

        await harness.Dispatch("TestEndpoint");
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Only the successful (second) attempt's outgoing message may be visible.
        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("error") == 1);
        await Task.Delay(200);
        Assert.That(await harness.CountRows("error"), Is.EqualTo(1));
    }

    [Test]
    public async Task IsolatedDispatchesAreImmediateEvenWhenReceiveFails()
    {
        await using var harness = await TransportTestHarness.Start();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await harness.StartReceiver(
            async (context, cancellationToken) =>
            {
                var message = new NServiceBus.Transport.OutgoingMessage("isolated", [], new byte[] { 1 });
                var operation = new NServiceBus.Transport.TransportOperation(message, new NServiceBus.Routing.UnicastAddressTag("error"), [], NServiceBus.Transport.DispatchConsistency.Isolated);
                await harness.Infrastructure.Dispatcher.Dispatch(new NServiceBus.Transport.TransportOperations(operation), context.TransportTransaction);
                throw new InvalidOperationException("simulated failure");
            },
            (_, _) =>
            {
                done.TrySetResult();
                return Task.FromResult(ErrorHandleResult.Handled);
            });

        await harness.Dispatch("TestEndpoint");
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("error") == 1);
    }
}
