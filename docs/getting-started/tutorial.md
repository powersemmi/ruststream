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
ruststream = { version = "0.2", features = ["macros", "memory", "json", "asyncapi"] }
serde = { version = "1", features = ["derive"] }
```

## 2. Define a message and a handler

A handler is an `async fn` whose first parameter is the decoded payload. The `#[subscriber]` macro
turns it into a mountable definition named after the function.

```rust title="src/orders.rs"
use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Order {
    pub id: u64,
    pub quantity: u32,
}

#[subscriber("orders")]
pub async fn handle(order: &Order) -> HandlerResult {
    println!("order {} x{}", order.id, order.quantity);
    HandlerResult::Ack
}
```

A handler returns a [`HandlerResult`](../guides/subscribers.md#acking): `Ack`, or a `nack` that drops
or requeues the message. Returning `()` or `Result<_, E>` also works - they convert into a result.

## 3. Wire it into an app

```rust title="src/main.rs"
mod orders;

use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

use crate::orders::handle;

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
```

The macro turns `handle` into a value named after the function, so you import and pass it directly.

!!! tip "Codec defaults"
    `include` decodes with the default codec - `json` if enabled, otherwise `cbor`, otherwise
    `msgpack` - so it needs no codec argument. To decode with a different one everywhere, set it
    once with `with_broker_codec(broker, codec, |b| ...)`.

Run it:

```bash
cargo run -- run
```

## 4. Reply to messages

To publish a reply, return the reply value and name the destination with `publish(..)`:

```rust title="src/orders.rs"
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Confirmation {
    pub id: u64,
    pub accepted: bool,
}

#[subscriber("orders", publish("confirmations"))]
pub async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id, accepted: order.quantity > 0 }
}
```

Mount it with a publisher that carries the reply codec:

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
[`Router`](../guides/subscribers.md#routers):

```rust title="src/routes.rs"
use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{Router, TypedPublisher};

use crate::orders;

pub fn orders(broker: &MemoryBroker) -> Router<MemoryBroker> {
    let replies = TypedPublisher::new(broker.publisher());
    let mut router = Router::new();
    router.include_publishing(orders::confirm, replies);
    router.include(orders::handle);
    router
}
```

```rust title="src/main.rs"
#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        let router = routes::orders(b.broker());
        b.include_router(router);
    })
}
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

    ```rust
    use ruststream::memory::MemoryBroker;

    .with_broker(MemoryBroker::new(), |b| {
        let router = routes::orders(b.broker());
        b.include_router(router);
    })
    ```

=== "NATS"

    ```rust
    use ruststream_nats::NatsBroker;

    .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
        let router = routes::orders(b.broker());
        b.include_router(router);
    })
    ```

Each broker crate documents its own `Config`. Subscriptions that need broker-specific options
(consumer groups, durable names) use that broker's descriptor in the decorator, see
[Subscribers & publishers](../guides/subscribers.md#broker-specific-descriptors). The available
brokers are listed under [Brokers](../brokers/index.md).

## Next steps

- [Middleware](../guides/middleware.md) - cross-cutting logic around handlers.
- [Metrics](../guides/metrics.md) - Prometheus counters and histograms.
- [Testing](../guides/testing.md) - the in-memory test client.
