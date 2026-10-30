# The app: state, codecs, routers, descriptors, AsyncAPI

How the application is assembled. Component pages: [Application](https://powersemmi.github.io/ruststream/latest/components/application/), [State](https://powersemmi.github.io/ruststream/latest/components/application/state/), [Codecs](https://powersemmi.github.io/ruststream/latest/components/subscribers/codecs/), [Middleware](https://powersemmi.github.io/ruststream/latest/components/subscribers/middleware/), [AsyncAPI](https://powersemmi.github.io/ruststream/latest/components/asyncapi/).

## Contents

- State injection (dependency injection)
- Choosing a codec
- Routers
- Broker-specific subscriptions
- AsyncAPI metadata

## State injection (dependency injection)

Build the state once in `on_startup`; `#[derive(FromRef)]` makes every field injectable with
`State<FieldType>`.

```rust
use std::convert::Infallible;

use ruststream::memory::prelude::*;

#[derive(Clone)]
struct Database; // connection pool, etc.

#[derive(Clone, FromRef)]
struct AppState {
    db: Database,
}

#[subscriber("orders")]
async fn handle(order: &Order, State(db): State<Database>) -> HandlerOutcome {
    // db.store(order).await;
    let _ = db;
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(AppState { db: Database }))
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
```

## Choosing a codec

`include` uses `DefaultCodec`. No mounting call takes a codec argument; change the default for a
scope with `with_broker_codec`, or for a `Router` chain with `with_codec`.

```rust
use ruststream::codec::{CborCodec, JsonCodec};
use ruststream::prelude::*;

// every handler in this scope decodes with CborCodec
RustStream::new(info).with_broker_codec(broker, CborCodec, |b| b.include(handle));

// name the codec for one handler by mounting it through a router
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(Router::new().with_codec(JsonCodec).include(handle))
});
```

## Routers

Collect handlers in their own module, then mount the group. A `Router` is a consuming builder
(each call returns a new type), so the calls chain and the function returns
`Router<B, impl RouterDef<B>>`. Groups compose with `merge`. The app's global `.layer(..)` reaches
router handlers, so cross-cutting middleware (logging, metrics) goes on the app and applies
everywhere; that global layer must be a `BlanketLayer` (every bundled layer is). Publish-side
middleware (tracing propagation, broker envelopes such as Kafka's Schema Registry framing) is
added app-wide with `.publish_layer(..)` before `with_broker`; it wraps every publish a handler
makes, replies and `Out` slots alike, except in a router (its routes are typed before the app
that mounts them, so a router's slots carry their own `.transform(..)` steps only).

```rust
use ruststream::memory::prelude::*; // Router and RouterDef ride the prelude

fn orders() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    Router::new().include(accept)
}

fn shipping() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    Router::new().include(dispatch)
}

// in with_broker(broker, |b| ..):
b.include_router(orders().merge(shipping()))
```

A router carries its codec in its type, so a function returning one that called `with_codec` names
it as the third parameter:

```rust
use ruststream::codec::CborCodec;

fn metrics() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>, CborCodec> {
    Router::new()
        .with_codec(CborCodec)
        .include(write_samples.batch(nonzero!(64)))
}
```

A registration that carries an attachment (a reply publisher, an `Out` slot) goes on to the next
`.include(..)` directly, on a router and in a `with_broker` closure alike; a function that returns
a router ends the chain with `.build()`. A slot left unbound does not compile. With no
`.out_reply(..)` named, the broker's own default publish policy answers.

## Broker-specific subscriptions

For options the by-name form can't express (NATS JetStream, Kafka consumer groups / start offsets),
name the broker's descriptor in the attribute - the broker's prelude already carries it. Broker
crates layer their own extension trait over the mount builder for the rest, so the settings read in
the broker's own vocabulary.

```rust
use ruststream_nats::prelude::*; // brings SubscribeOptions

#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("worker"))]
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

## AsyncAPI metadata

Decoding only needs `serde::Deserialize`. The extra derives are **documentation only** for the
generated AsyncAPI spec and are optional: `MessageInfo` (in the prelude) names/describes the type,
and `JsonSchema` (feature `asyncapi`) emits its payload schema.

Neither selects a wire codec - that stays the default (`json` > `cbor` > `msgpack`) or whatever
`with_codec` / `with_broker_codec` names. `JsonSchema` describes the payload's logical shape in the
JSON Schema language (as in OpenAPI), independent of how bytes are encoded on the wire; you cannot
attach a codec to a type.

```rust
use ruststream::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::Deserialize;

/// An order placed on the orders channel.
#[derive(Debug, Deserialize, MessageInfo, JsonSchema)]
struct Order {
    id: u64,
}
```
