namespace NServiceBusContrib.Transport.Sqlite.Tests;

using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class RecoverabilityTests
{
    [TestCase(TransportTransactionMode.None)]
    [TestCase(TransportTransactionMode.ReceiveOnly)]
    [TestCase(TransportTransactionMode.SendsAtomicWithReceive)]
    public async Task RetryRequiredMakesMessageAvailableAgain(TransportTransactionMode transactionMode)
    {
        await using var harness = await TransportTestHarness.Start(transactionMode: transactionMode);
        var observedAttempts = new List<int>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        await harness.StartReceiver(
            (context, _) =>
            {
                if (Interlocked.Increment(ref attempts) < 3)
                {
                    throw new InvalidOperationException("simulated failure");
                }

                done.TrySetResult();
                return Task.CompletedTask;
            },
            (errorContext, _) =>
            {
                lock (observedAttempts)
                {
                    observedAttempts.Add(errorContext.ImmediateProcessingFailures);
                }

                return Task.FromResult(ErrorHandleResult.RetryRequired);
            });

        await harness.Dispatch("TestEndpoint");
        await done.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(observedAttempts, Is.EqualTo(new[] { 1, 2 }));
        await TransportTestHarness.WaitUntil(async () => await harness.CountRows("TestEndpoint") == 0);
    }

    [TestCase(TransportTransactionMode.None)]
    [TestCase(TransportTransactionMode.ReceiveOnly)]
    [TestCase(TransportTransactionMode.SendsAtomicWithReceive)]
    public async Task HandledRemovesMessage(TransportTransactionMode transactionMode)
    {
        await using var harness = await TransportTestHarness.Start(transactionMode: transactionMode);
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await harness.StartReceiver(
            (_, _) => throw new InvalidOperationException("simulated failure"),
            async (errorContext, cancellationToken) =>
            {
                // Emulate what recoverability does: forward the failed message to the error queue
                // using the transaction supplied on the error context.
                await harness.Dispatch("error", messageId: errorContext.Message.NativeMessageId, transaction: errorContext.TransportTransaction);
                handled.TrySetResult();
                return ErrorHandleResult.Handled;
            });

        await harness.Dispatch("TestEndpoint", messageId: "poison");
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await TransportTestHarness.WaitUntil(async () =>
            await harness.CountRows("TestEndpoint") == 0 && await harness.CountRows("error") == 1);
    }

    [Test]
    public async Task CriticalErrorIsRaisedWhenOnErrorThrows()
    {
        var criticalErrors = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var harness = await TransportTestHarness.Start();

        // Rebuild the receiver pipeline with an onError that throws.
        await harness.StartReceiver(
            (_, _) => throw new InvalidOperationException("simulated failure"),
            (_, _) => throw new InvalidOperationException("recoverability failure"));

        await harness.Dispatch("TestEndpoint", messageId: "critical");

        // The message stays in the queue (lease released) because recoverability failed.
        await Task.Delay(500);
        Assert.That(await harness.CountRows("TestEndpoint"), Is.EqualTo(1));
    }
}
