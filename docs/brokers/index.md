# Brokers

A broker connects RustStream to a message transport. The framework ships an in-memory broker for
development and tests; production brokers are separate crates you add as a dependency.

Handlers, routers, codecs, and middleware are broker-agnostic, so moving between brokers is a
one-line change at `with_broker`.

## In-memory (built-in)

The `memory` feature provides `MemoryBroker`, an in-process broadcast broker. It needs no external
service, which makes it ideal for examples, unit tests, and prototypes.

```toml
ruststream = { version = "0.2", features = ["macros", "memory"] }
```

```rust
use ruststream::memory::MemoryBroker;

let broker = MemoryBroker::new();
```

It does core routing only and does not emulate any real broker's delivery semantics. See
[Testing](../guides/testing.md).

## NATS

[`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) is the NATS broker. It covers
Core NATS subjects and JetStream durable consumers.

```toml
ruststream = { version = "0.2", features = ["macros"] }
ruststream-nats = "0.2"
```

```rust
use ruststream_nats::NatsBroker;

let broker = NatsBroker::new("nats://localhost:4222");
```

See the crate's documentation for connection options, authentication, and JetStream configuration.
For how a NATS broker implements the contract from the inside, read the
[worked example](../broker-authors/example-nats.md).

## Switching brokers

The same service runs on either broker; only the construction differs.

=== "Memory"

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
