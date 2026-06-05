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
serde = { version = "1", features = ["derive"] }
```

`NatsBroker::new` is synchronous and does no I/O, so a NATS service is assembled with the same
`#[ruststream::app]` macro as any other broker. The runtime connects the broker once at startup,
before opening subscriptions.

### Core subscription

A `#[subscriber("subject")]` handler binds straight to a NATS subject:

```rust
--8<-- "examples/nats_core.rs:handler"
```

Wire it onto the broker; the `with_broker` / `include` part is identical to the in-memory broker.

```rust
--8<-- "examples/nats_core.rs:app"
```

### JetStream durable consumer

To consume from JetStream instead, override the handler's by-name source with `SubscribeOptions`,
naming the stream and a durable consumer so progress survives restarts. The handler's
`HandlerResult::Ack` acks back to JetStream. `include_on` is the source-override form, so it takes
the codec explicitly.

```rust
--8<-- "examples/nats_jetstream.rs:include_on"
```

See the crate's documentation for connection options, authentication, and JetStream configuration.
For how a NATS broker implements the contract from the inside, read the
[worked example](../broker-authors/example-nats.md).

## Switching brokers

The same handlers and routers run on either broker, and both build synchronously, so only the
broker construction differs by one line inside `with_broker`.

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
