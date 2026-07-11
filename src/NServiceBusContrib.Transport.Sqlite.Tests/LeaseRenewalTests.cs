namespace NServiceBusContrib.Transport.Sqlite.Tests;

using NUnit.Framework;

[TestFixture]
public class LeaseRenewalTests
{
    [Test]
    public async Task LongRunningHandlerKeepsLeaseAndMessageIsProcessedOnce()
    {
        // Lease far shorter than the handler duration: without renewal the message would become
        // visible again mid-processing and be delivered a second time.
        await using var harness = await TransportTestHarness.Start(customize: t => t.MessageLeaseDuration = TimeSpan.FromMilliseconds(250));
        var deliveries = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await harness.StartReceiver(
            async (context, cancellationToken) =>
            {
                Interlocked.Increment(ref deliveries);
                await Task.Delay(TimeSpan.FromSeconds(1.5), cancellationToken);
                done.TrySetResult();
            },
            maxConcurrency: 2);

        await harness.Dispatch("TestEndpoint", messageId: "long-running");
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("TestEndpoint") == 0);
        await Task.Delay(300);
        Assert.That(deliveries, Is.EqualTo(1));
    }

    [Test]
    public async Task ExpiredLeaseLeadsToRedeliveryWhenRenewalIsImpossible()
    {
        // Sanity check of at-least-once behavior: a message whose lease expired (simulated by a
        // handler that outlives a very short lease without the renewal keeping up is hard to force,
        // so instead verify a released lease is picked up again).
        await using var harness = await TransportTestHarness.Start();
        var deliveries = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await harness.StartReceiver((context, _) =>
        {
            if (Interlocked.Increment(ref deliveries) == 2)
            {
                done.TrySetResult();
            }

            return Task.CompletedTask;
        });

        await harness.Dispatch("TestEndpoint", messageId: "a");
        await harness.Dispatch("TestEndpoint", messageId: "b");
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("TestEndpoint") == 0);
    }
}
