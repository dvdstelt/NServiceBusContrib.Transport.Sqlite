namespace NServiceBusContrib.Transport.Sqlite.Tests;

using NServiceBus.Transport;
using NUnit.Framework;

[TestFixture]
public class AddressTranslationTests
{
    [Test]
    public void BaseAddressOnly() =>
        Assert.That(SqliteAddressTranslator.Translate(new QueueAddress("Sales")), Is.EqualTo("Sales"));

    [Test]
    public void AppendsDiscriminator() =>
        Assert.That(SqliteAddressTranslator.Translate(new QueueAddress("Sales", "instance-1")), Is.EqualTo("Sales-instance-1"));

    [Test]
    public void AppendsQualifier() =>
        Assert.That(SqliteAddressTranslator.Translate(new QueueAddress("Sales", qualifier: "Retries")), Is.EqualTo("Sales.Retries"));

    [Test]
    public void AppendsQualifierAndDiscriminator() =>
        Assert.That(SqliteAddressTranslator.Translate(new QueueAddress("Sales", "a", qualifier: "Retries")), Is.EqualTo("Sales.Retries-a"));
}

[TestFixture]
public class TransportTablesTests
{
    [Test]
    public void QuotesIdentifiers() =>
        Assert.That(TransportTables.Quote("my.queue"), Is.EqualTo("\"my.queue\""));

    [Test]
    public void EscapesEmbeddedQuotes() =>
        Assert.That(TransportTables.Quote("a\"b"), Is.EqualTo("\"a\"\"b\""));

    [Test]
    public void AppliesTablePrefix()
    {
        var tables = new TransportTables("prefix_");
        Assert.Multiple(() =>
        {
            Assert.That(tables.QueueTable("Sales"), Is.EqualTo("\"prefix_Sales\""));
            Assert.That(tables.DelayedTable, Is.EqualTo("\"prefix_nsbc.delayed\""));
            Assert.That(tables.SubscriptionsTable, Is.EqualTo("\"prefix_nsbc.subscriptions\""));
        });
    }
}
