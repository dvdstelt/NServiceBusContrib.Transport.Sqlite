namespace NServiceBusContrib.Transport.Sqlite.Tests;

using NServiceBus;
using NUnit.Framework;

[TestFixture]
public class TopicTests
{
    [Test]
    public void IncludesConcreteTypeBaseTypesAndInterfaces()
    {
        var topics = SqliteMessageDispatcher.GetTopics(typeof(DerivedEvent));
        Assert.That(topics, Is.EquivalentTo(new[]
        {
            typeof(DerivedEvent).FullName,
            typeof(BaseEvent).FullName,
            typeof(ISomethingHappened).FullName,
        }));
    }

    [Test]
    public void ExcludesCoreMarkerInterfacesAndSystemTypes()
    {
        var topics = SqliteMessageDispatcher.GetTopics(typeof(DerivedEvent));
        Assert.That(topics, Does.Not.Contain(typeof(IEvent).FullName));
        Assert.That(topics, Does.Not.Contain(typeof(object).FullName));
    }

    public interface ISomethingHappened : IEvent;

    public class BaseEvent : ISomethingHappened;

    public class DerivedEvent : BaseEvent;
}
