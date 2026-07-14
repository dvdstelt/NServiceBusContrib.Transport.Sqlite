namespace NServiceBusContrib.Transport.Sqlite;

using NServiceBus.Transport;

static class SqliteAddressTranslator
{
    public static string Translate(QueueAddress address)
    {
        var value = address.BaseAddress;
        if (!string.IsNullOrEmpty(address.Qualifier))
        {
            value = $"{value}.{address.Qualifier}";
        }

        if (!string.IsNullOrEmpty(address.Discriminator))
        {
            value = $"{value}-{address.Discriminator}";
        }

        return value;
    }
}
