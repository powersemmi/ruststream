# Brokers

A broker connects RustStream to a message transport. The framework ships an in-memory broker for
development and tests; production brokers are separate crates you add as a dependency.

Handlers, routers, codecs, and middleware are broker-agnostic, so moving between brokers is a
one-line change at `with_broker`.

| Broker | Crate | Transport |
|---|---|---|
| [Memory](memory.md) | `ruststream` (feature `memory`) | in-process, for development and tests |
| [NATS](nats.md) | [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) | Core NATS and JetStream |
| [Redis](redis.md) | [`ruststream-fred`](https://github.com/powersemmi/ruststream-fred) | Redis Streams (standalone, cluster, sentinel) |

To implement a broker for another transport, see [Broker authors](../broker-authors/index.md).

## Switching brokers

Every broker constructs synchronously and connects lazily (the runtime calls `Broker::connect` once
at startup), so the same handlers and routers run on any of them; only the broker construction
differs by one line inside `with_broker`.

=== "Memory"

    <!-- inline-rust: side-by-side broker-switch comparison; the NATS half depends on the external ruststream-nats crate and has no in-repo compiled home, so both halves stay inline to read in parallel -->
    ```rust
    use ruststream::memory::MemoryBroker;
    use ruststream::runtime::{AppInfo, RustStream};

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(MemoryBroker::new(), |b| b.include_router(routes::orders()))
    }
    ```

=== "NATS"

    <!-- inline-rust: NATS half of the broker-switch comparison; depends on the external ruststream-nats crate, no in-repo compiled home -->
    ```rust
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream_nats::NatsBroker;

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
                b.include_router(routes::orders())
            })
    }
    ```

=== "Redis"

    <!-- inline-rust: Redis half of the broker-switch comparison; depends on the external ruststream-fred crate, no in-repo compiled home -->
    ```rust
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream_fred::RedisBroker;

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
                b.include_router(routes::orders())
            })
    }
    ```

Subscriptions that need broker-specific options (consumer groups, durable names) use that broker's
descriptor in the `#[subscriber(..)]` decorator; see
[broker-specific descriptors](../guides/subscribers.md#broker-specific-descriptors).
