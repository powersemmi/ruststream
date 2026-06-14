# Tutorial: build your first service

This tutorial builds an orders service from scratch, explaining each piece. It uses the in-memory
broker so there is nothing external to run; swapping in a real broker is a one-line change covered at
the end.

## 1. Create the crate

```bash
cargo new orders-service
cd orders-service
```

```toml title="Cargo.toml"
[package]
name = "orders-service"
version = "0.1.0"
edition = "2024"

[dependencies]
ruststream = { version = "0.4", features = ["macros", "memory", "json", "asyncapi"] }
serde = { version = "1", features = ["derive"] }
```

## 2. Define a message and a handler

A handler is an `async fn` whose first parameter is the decoded payload. The `#[subscriber]` macro
turns it into a mountable definition named after the function.

```rust title="src/orders.rs"
use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

--8<-- "examples/tutorial/orders.rs:order"
```

A handler returns a [`HandlerResult`](../guides/subscribers.md#acking): `Ack`, or a `nack` that drops
or requeues the message. Returning `()` or `Result<(), E>` also works - they convert into a result
(`Ok` acks, `Err` drops).

## 3. Wire it into an app

```rust title="src/main.rs"
mod orders;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

use crate::orders::handle;

--8<-- "examples/quickstart.rs:app"
```

The macro turns `handle` into a value named after the function, so you import and pass it directly.

!!! tip "Codec defaults"
    `include` decodes with the default codec - `json` if enabled, otherwise `cbor`, otherwise
    `msgpack` - so it needs no codec argument. To decode with a different one everywhere, set it
    once with `with_broker_codec(broker, codec, |b| ...)`. See
    [Codecs](../guides/codecs.md) for the full resolution rules.

Run it:

```bash
cargo run -- run
```

## 4. Reply to messages

To publish a reply, return the reply value and name the destination with `publish(..)`:

```rust title="src/orders.rs"
--8<-- "examples/tutorial/orders.rs:confirm"
```

Mount it with a publisher that carries the reply codec:

<!-- inline-rust: minimal mount fragment isolating the publisher wiring; the full compiled program is examples/tutorial/main.rs:main, pulled in below -->
```rust
use ruststream::runtime::TypedPublisher;

// inside with_broker(...), with `confirm` imported from the orders module
let replies = TypedPublisher::new(b.broker().publisher());
b.include_publishing(confirm, replies);
```

See [Publishing & replies](../guides/publishing.md) for the full picture, including publishing from
inside a handler.

## 5. Organize with a router

As handlers grow, keep them in their own module and collect them into a
[`Router`](../guides/routing.md):

```rust title="src/routes.rs"
--8<-- "examples/tutorial/routes.rs:routes"
```

```rust title="src/main.rs"
--8<-- "examples/tutorial/main.rs:main"
```

## 6. Inspect the AsyncAPI document

```bash
cargo run -- asyncapi gen --yaml
```

Every subscriber becomes a channel and a `receive` operation; payload types that derive
`schemars::JsonSchema` also contribute schemas. See [AsyncAPI](../guides/asyncapi.md).

## 7. Swap in a real broker

Nothing above is tied to the in-memory broker. The handlers, router, and codecs are unchanged; only
the broker construction differs. Add the broker crate as a dependency and swap the `with_broker`
line:

=== "Memory"

    <!-- inline-rust: side-by-side broker-swap comparison; the NATS half depends on the external ruststream-nats crate and has no in-repo compiled home, so both halves stay inline to read in parallel -->
    ```rust
    use ruststream::memory::MemoryBroker;

    .with_broker(MemoryBroker::new(), |b| {
        let router = routes::orders(b.broker());
        b.include_router(router);
    })
    ```

=== "NATS"

    <!-- inline-rust: NATS half of the broker-swap comparison; depends on the external ruststream-nats crate, no in-repo compiled home -->
    ```rust
    use ruststream_nats::NatsBroker;

    .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
        let router = routes::orders(b.broker());
        b.include_router(router);
    })
    ```

Each broker crate documents its own `Config`. Subscriptions that need broker-specific options
(consumer groups, durable names) use that broker's descriptor in the decorator, see
[broker-specific descriptors](../guides/subscribers.md#broker-specific-descriptors). The available
brokers are listed under [Brokers](../brokers/index.md).

!!! info "The complete service is a compiled example"
    Every snippet on this page is embedded from
    [`examples/tutorial`](https://github.com/powersemmi/ruststream/tree/main/examples/tutorial)
    in the repository, which CI builds on every change. Run it yourself with
    `cargo run --example tutorial --features macros,memory,json -- run`.

## Next steps

- [Middleware](../guides/middleware.md) - cross-cutting logic around handlers.
- [Lifespan](../guides/lifespan.md) - shared state and startup/shutdown hooks.
- [Testing](../guides/testing.md) - test the handlers you just wrote, in-process.
- [Metrics](../guides/metrics.md) - Prometheus counters and histograms.
