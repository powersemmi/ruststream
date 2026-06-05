# Subscribers and publishers

A subscriber binds a handler to one subscription. The `#[subscriber]` macro is the ergonomic way to
declare one; this guide covers the handler contract, the macro forms, how handlers are mounted, and
how to group them with a router.

## The handler contract

A handler is an `async fn` whose first parameter is a reference to the decoded payload:

```rust
use ruststream::runtime::HandlerResult;
use ruststream::subscriber;

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    HandlerResult::Ack
}
```

The macro turns the function into a value named after it (here `handle`) that implements the
mounting contract. You pass that value to `include`.

### Accepting the context

Declare an optional second parameter, `&mut Context`, to read headers, the subscription name, and
shared state, or to publish from inside the handler:

```rust
use ruststream::runtime::{Context, HandlerResult};

#[subscriber("orders")]
async fn handle(order: &Order, ctx: &mut Context<'_>) -> HandlerResult {
    if let Some(id) = ctx.headers().correlation_id() {
        // ...
    }
    HandlerResult::Ack
}
```

### Acking

The return type is anything that converts into a [`HandlerResult`]:

| Return value | Result |
|---|---|
| `HandlerResult::Ack` | acknowledge; the broker removes the message |
| `HandlerResult::retry()` | nack with requeue (redeliver later) |
| `HandlerResult::drop()` | nack without requeue (discard or dead-letter) |
| `()` | always `Ack` |
| `Result<_, E>` | `Ack` on `Ok`, `drop` on `Err` |

On the message itself, ack consumes `self`, so the type system prevents acking twice.

## Choosing the subscription source

### By name

`#[subscriber("orders")]` subscribes by name. It works with any broker that implements the
`Subscribe` capability, which covers the common case.

### Broker-specific descriptors

When a subscription needs broker-specific options (a consumer group, a durable name, a delivery
policy), the broker crate exposes a descriptor type. Use its constructor directly in the decorator:

```rust
#[subscriber(OrdersStream::new("orders", "workers"))]
async fn handle(order: &Order) -> HandlerResult {
    HandlerResult::Ack
}
```

The macro reads the descriptor type out of the constructor call, so the compiler checks the
descriptor against the broker it is mounted on. A descriptor is any type that implements
`SubscriptionSource<B>`; see [Broker authors](../broker-authors/index.md#subscription-sources).

## Mounting handlers

Inside `with_broker`, mount a definition with `include`. It decodes the payload with the default
codec - `json` if enabled, otherwise `cbor`, otherwise `msgpack`:

```rust
RustStream::new(info).with_broker(broker, |b| {
    b.include(handle);
});
```

### A scope default codec

To decode every handler in a scope with a specific codec instead, set it once with
`with_broker_codec`:

```rust
use ruststream::codec::CborCodec;

RustStream::new(info).with_broker_codec(broker, CborCodec, |b| {
    b.include(handle);          // decodes with CborCodec
    b.include(other_handler);
});
```

To override the codec for a single handler, mount it with `include_on` (naming its source and the
codec). On a standalone [`Router`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.Router.html),
the per-call form is `router.with_codec(codec).include(def)`.

### Codecs

| Codec | Feature | Wire format |
|---|---|---|
| `JsonCodec` | `json` | JSON |
| `MsgpackCodec` | `msgpack` | MessagePack |
| `CborCodec` | `cbor` | CBOR |

A codec is any type implementing the `Codec` trait, so you can supply your own.

When decoding fails, the message is dropped by default. The decode-failure behaviour is configurable
on the typed adapter when you build handlers by hand (see the API reference for `Typed` and
`DecodeFailure`).

## Macro or manual

`#[subscriber]` is sugar over a generic API. The macro generates a typed handler and its metadata;
you can write the same registration by hand with `typed` (which decodes the payload), a closure or
struct handler, and `HandlerMetadata`. Both forms below register the same handler.

=== "Macro"

    ```rust
    use ruststream::codec::JsonCodec;
    use ruststream::subscriber;

    #[subscriber("orders")]
    async fn handle(order: &Order) -> HandlerResult {
        HandlerResult::Ack
    }

    // inside with_broker(...):
    b.include(handle);
    ```

=== "Manual"

    ```rust
    use ruststream::Name;
    use ruststream::codec::JsonCodec;
    use ruststream::runtime::{Context, HandlerMetadata, HandlerResult, typed};

    // inside with_broker(...):
    b.subscribe(
        Name::new("orders"),
        typed(JsonCodec, |order: &Order, _ctx: &mut Context| async { HandlerResult::Ack }),
        HandlerMetadata::typed::<Order>("orders"),
    );
    ```

Reach for the manual form when a handler needs state the macro cannot express (a struct handler with
fields), or to set a non-default decode-failure policy. Otherwise the macro is less to maintain.

## Routers

Group handlers in their own module by collecting them into a `Router`, then mount the whole group
with `include_router`. The router-level `include` and `include_publishing` mirror the scope methods:

```rust title="routes.rs"
use ruststream::codec::JsonCodec;
use ruststream::runtime::Router;

pub fn orders() -> Router<MyBroker> {
    let mut router = Router::new();
    router.include(handle);
    router.include(other_handler);
    router
}
```

```rust title="main.rs"
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
});
```

The application's global middleware (added with `layer`) does not wrap router handlers, since a
router is finalized independently. Wrap handlers inside the router if you need that.

## Publishers

A handler that produces a reply is a publisher. See [Publishing and replies](publishing.md).
