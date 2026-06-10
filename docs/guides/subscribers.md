# Subscribers

A subscriber binds a handler to one subscription. The `#[subscriber]` macro is the ergonomic way to
declare one; this guide covers the handler contract, the macro forms, and how handlers are mounted.
Grouping handlers into modules is covered in [Routing](routing.md), and how payloads are decoded in
[Codecs](codecs.md).

## The handler contract

A handler is an `async fn` whose first parameter is a reference to the decoded payload:

```rust
use ruststream::runtime::HandlerResult;
use ruststream::subscriber;

--8<-- "examples/subscribers.rs:contract"
```

The macro turns the function into a value named after it (here `handle`) that implements the
mounting contract. You pass that value to `include`.

### Accepting the context

Declare an optional second parameter, `&mut Context`, to read headers, the subscription name, and
shared state, or to publish from inside the handler:

```rust
--8<-- "examples/subscribers.rs:context"
```

The macro resolves the context type itself, so the `Context` name needs no import when it appears
only in `#[subscriber]` signatures. The full context surface - the headers working copy, state
access, named publishers - is covered in [Context and state](context.md).

### Acking

The return type is anything that converts into a [`HandlerResult`]:

| Return value | Result |
|---|---|
| `HandlerResult::Ack` | acknowledge; the broker removes the message |
| `HandlerResult::retry()` | nack with requeue (redeliver later) |
| `HandlerResult::drop()` | nack without requeue (discard or dead-letter) |
| `()` | always `Ack` |
| `Result<(), E>` | `Ack` on `Ok`, `drop` on `Err` |
| `Result<HandlerResult, E>` | the inner result on `Ok`, `drop` on `Err` |

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

The source may also be a builder chain on that constructor, so fluent options stay inline. A NATS
JetStream consumer, for example:

```rust
use ruststream_nats::SubscribeOptions;

--8<-- "examples/nats_jetstream.rs:decorator"
```

The macro follows the chain down to the base `Type::new(..)` to name the source type, so each method
in the chain must return `Self`. Free functions are rejected, since their type is not visible to the
macro.

## Mounting handlers

Inside `with_broker`, mount a definition with `include`:

```rust
RustStream::new(info).with_broker(broker, |b| {
    b.include(handle);
});
```

`include` decodes the payload with the codec resolved from the most specific level you set - per
handler, per scope, or the feature-selected default. See
[where the codec comes from](codecs.md#where-the-decode-codec-comes-from).

To group handlers per module and mount them all at once, collect them into a `Router`; see
[Routing](routing.md).

## Macro or manual

`#[subscriber]` is sugar over a generic API. The macro generates a typed handler and its metadata;
you can write the same registration by hand with `typed` (which decodes the payload), a closure or
struct handler, and `HandlerMetadata`. Both forms below register the same handler.

=== "Macro"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/subscribers.rs:contract"

    // inside with_broker(...):
    b.include(handle);
    ```

=== "Manual"

    ```rust
    use ruststream::Name;
    use ruststream::codec::JsonCodec;
    use ruststream::runtime::{Context, HandlerMetadata, HandlerResult, typed};

    // inside with_broker(...):
    --8<-- "examples/subscribers.rs:manual"
    ```

Reach for the manual form when a handler needs state the macro cannot express (a struct handler with
fields), or to set a non-default [decode-failure policy](codecs.md#decode-failures). Otherwise the
macro is less to maintain.

## Publishers

A handler that produces a reply is a publisher. See [Publishing and replies](publishing.md).
