namespace NServiceBusContrib.Transport.Sqlite.Tests;

using NUnit.Framework;

[TestFixture]
public class InfrastructureTests
{
    [Test]
    public async Task CreatesQueueDelayedAndSubscriptionTables()
    {
        await using var harness = await TransportTestHarness.Start(endpointName: "InfraTest");
        Assert.Multiple(async () =>
        {
            Assert.That(await harness.TableExists("InfraTest"), Is.True);
            Assert.That(await harness.TableExists("error"), Is.True);
            Assert.That(await harness.TableExists("nsbc.delayed"), Is.True);
            Assert.That(await harness.TableExists("nsbc.subscriptions"), Is.True);
        });
    }

    [Test]
    public async Task SchemaCreationIsIdempotent()
    {
        var databasePath = TransportTestHarness.NewDatabasePath();
        await using (var first = await TransportTestHarness.Start(endpointName: "Idempotent", databasePath: databasePath, deleteDatabaseOnDispose: false))
        {
            await first.Dispatch("Idempotent");
        }

        // Second startup against the same database file must not fail or lose messages.
        await using var second = await TransportTestHarness.Start(endpointName: "Idempotent", databasePath: databasePath);
        Assert.That(await second.CountRows("Idempotent"), Is.EqualTo(1));
    }

    [Test]
    public async Task PurgesQueueOnStartupWhenRequested()
    {
        var databasePath = TransportTestHarness.NewDatabasePath();
        await using (var first = await TransportTestHarness.Start(endpointName: "Purged", databasePath: databasePath, deleteDatabaseOnDispose: false))
        {
            await first.Dispatch("Purged");
            Assert.That(await first.CountRows("Purged"), Is.EqualTo(1));
        }

        await using var second = await TransportTestHarness.Start(endpointName: "Purged", databasePath: databasePath, purgeOnStartup: true);
        Assert.That(await second.CountRows("Purged"), Is.EqualTo(0));
    }

    [Test]
    public async Task AppliesTablePrefixToAllTables()
    {
        await using var harness = await TransportTestHarness.Start(endpointName: "Prefixed", customize: t => t.TablePrefix = "app_");
        Assert.Multiple(async () =>
        {
            Assert.That(await harness.TableExists("app_Prefixed"), Is.True);
            Assert.That(await harness.TableExists("app_nsbc.delayed"), Is.True);
            Assert.That(await harness.TableExists("app_nsbc.subscriptions"), Is.True);
        });
    }
}
