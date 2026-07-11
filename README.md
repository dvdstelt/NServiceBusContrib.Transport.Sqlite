# NServiceBusContrib.Transport.Sqlite

A community SQLite transport for NServiceBus 10. Every queue is a table in a single SQLite database file, similar in spirit to the SQL Server transport but designed independently for SQLite's strengths: zero infrastructure, a single file on disk, and WAL-mode concurrency.

This project is not affiliated with or endorsed by Particular Software. NServiceBus is a registered trademark of Particular Software.

## When to use it

- Local development and testing without a broker or database server
- Single-machine deployments where endpoints share a database file
- Edge/IoT scenarios where SQLite is already the storage engine

All communicating endpoints must use the same SQLite database file; this transport does not communicate across machines.

## Usage

```csharp
var endpointConfiguration = new EndpointConfiguration("Sales");

var transport = new SqliteTransport("Data Source=transport.db");
endpointConfiguration.UseTransport(transport);
```

### Options

```csharp
var transport = new SqliteTransport("Data Source=transport.db")
{
    // Poll interval used by receivers when their queue is empty (default 200 ms)
    PeekInterval = TimeSpan.FromMilliseconds(200),

    // Poll interval for due delayed messages (default 1 s)
    DelayedDeliveryPollInterval = TimeSpan.FromSeconds(1),

    // Prefix applied to every table the transport creates (default "")
    TablePrefix = "",

    // Create tables at startup (default true; also controlled by installers)
    CreateSchema = true,
};
```

## Features

| Feature | Support |
| --- | --- |
| Transaction modes | `None`, `ReceiveOnly`, `SendsAtomicWithReceive` |
| Publish/subscribe | Native, via a subscriptions table (polymorphic events supported) |
| Delayed delivery | Native, via a delayed-messages table |
| Time to be received (TTBR) | Supported |
| TransactionScope | Not supported (`Microsoft.Data.Sqlite` cannot enlist in ambient transactions) |

## How it works

- Each queue is a table `"<prefix><queue name>"` with columns `Seq`, `Id`, `Headers` (JSON), `Body`, `Expires`.
- Receiving deletes the oldest row inside a SQLite transaction (`DELETE ... RETURNING`). With `SendsAtomicWithReceive`, outgoing messages are inserted using the same connection and transaction, so sends roll back together with the receive.
- Delayed messages go to `"<prefix>nsbc.delayed"` and are moved to their destination queue by a background pump once due.
- Subscriptions are rows in `"<prefix>nsbc.subscriptions"`. Publishing walks the event type's base types and interfaces and inserts the message into every subscriber's queue in one transaction.
- The database runs in WAL mode with `busy_timeout` set, so concurrent endpoints in separate processes can share the file.

## Versioning

This package follows [Semantic Versioning 2.0.0](https://semver.org/). Versions are derived from git tags using MinVer. Current status: pre-release (`0.x`); the public API can still change.

## License

MIT, see [LICENSE.md](LICENSE.md).
