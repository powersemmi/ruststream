# Writing a broker

A broker is an independent crate that implements the core traits. It depends on `ruststream` with
default features off, so it pulls in the trait surface and runtime without the bundled JSON codec or
any other broker:

```toml
[dependencies]
ruststream = { version = "0.5", default-features = false }
```

This page is the contract. Implement the required traits, expose your own `Config`, add capability
traits for the features your broker supports, and prove the result with the
[conformance harness](conformance.md). For a complete implementation on a real client, see the
[worked NATS example](example-nats.md).

## The required traits

### `Broker`

The broker is pure lifecycle: connect and shut down. It carries no subscriber or publisher type, so a
single application can mix broker kinds.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/broker.rs (which carries Send bounds and rustdoc); a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Broker: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn connect(&self) -> Result<(), Self::Error>;
    async fn shutdown(&self) -> Result<(), Self::Error>;
}
```

`shutdown` must never block or panic; do all fallible teardown here and return a `Result`.

Construction is **synchronous and I/O-free**: `new(addrs)` only records configuration, all network
work happens in `connect` (idempotent, called once at startup by the runtime). Keep the live
connection behind interior mutability (for example `Arc<tokio::sync::OnceCell<_>>`) so a publisher
handed out before `connect` resolves the connection on first use; operations that need it earlier
return a clear "not connected" error. This lazy-startup contract is what lets a service compose
with the synchronous `#[ruststream::app]` builder; the
[conformance harness](conformance.md) proves it end to end.

### `Subscribe`

Implement `Subscribe` to support subscribing by name. This is what `#[subscriber("name")]` uses.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscription.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Subscribe: Broker {
    type Subscriber: Subscriber;
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error>;
}
```

### `Subscriber`

A subscriber is a `Stream` of incoming messages. Back-pressure comes for free from the stream.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscriber.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Subscriber: Send {
    type Message: IncomingMessage;
    type Error: std::error::Error + Send + Sync + 'static;
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_;
}
```

`stream` takes `&mut self`, so any state buffered between polls lives behind the mutable borrow,
which keeps it cancel-safe.

### `IncomingMessage`

A delivered message exposes its payload and headers, and is acked or nacked. Ack consumes `self`, so
double-ack is a compile error.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/message.rs, with the defaulted methods annotated inline for teaching; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait IncomingMessage: Send + Sync {
    fn payload(&self) -> &[u8];
    fn headers(&self) -> &Headers;
    async fn ack(self) -> Result<(), AckError>;
    async fn nack(self, requeue: bool) -> Result<(), AckError>;

    // Defaulted: a plain nack(true). Override when the transport has native
    // delayed redelivery (JetStream NAK with delay); handlers reach it through
    // HandlerResult::retry_after.
    async fn nack_after(self, delay: Duration) -> Result<(), AckError>;

    // Defaulted: None. Override (with the Partitioned capability) to feed the
    // runtime's keyed worker lanes, workers(n, by_key).
    fn partition_key(&self) -> Option<&[u8]>;
}
```

The two defaulted methods are how optional broker behaviour degrades gracefully: a broker that
overrides neither still works with every runtime feature, with `retry_after` falling back to an
immediate requeue and keyed lanes rotating keyless messages.

### `Publisher`

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Publisher: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error>;
}
```

`OutgoingMessage` borrows its name and payload, so publishing does not force an allocation.

## Subscription sources

`Subscribe` covers the by-name case. When a subscription needs broker-specific options (a consumer
group, a durable name, a delivery policy), expose a descriptor type that implements
`SubscriptionSource`:

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscription.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait SubscriptionSource<B: Broker> {
    type Subscriber: Subscriber;
    fn name(&self) -> &str;
    fn subscribe(self, broker: &B) -> impl Future<Output = Result<Self::Subscriber, B::Error>> + Send;
}
```

Give the descriptor an associated constructor (`OrdersStream::new(..)`) rather than a free function,
so users can name it directly in the decorator: `#[subscriber(OrdersStream::new("orders", "workers"))]`.
The macro reads the type out of the constructor call, and also accepts a builder chain on it
(`#[subscriber(OrdersStream::new("orders").durable("workers"))]`) as long as each method returns
`Self`. Because `type Subscriber` lives on the source, one broker can offer several subscription
kinds (pub/sub versus streams) with different subscriber types - or, as the
[NATS example](example-nats.md) does, serve them all from one descriptor that branches internally.

## Capability traits

Implement only the capabilities your broker supports; none are part of the mandatory interface.

| Trait | For brokers that support |
|---|---|
| `BatchSubscriber` | receiving messages in batches |
| `TransactionalPublisher` | begin / commit / abort around publishes |
| `RequestReply` | native request-reply (NATS yes, Kafka no) |
| `Partitioned` | a partition key on outgoing messages |
| `DescribeServer` | reporting a `ServerSpec` for AsyncAPI |

## Per-delivery context and `Ctx` keys

A broker with native delivery metadata (a partition, an offset, a stream sequence) exposes it as a
typed per-delivery context: a `#[non_exhaustive]` struct the subscriber names, plus `ContextField`
key types so handlers can bind single fields as parameters with the
[`Ctx<K>` extractor](../guides/context.md#per-delivery-context). Keys are unit structs; values are
owned. No type-map and no heap on the delivery path.

<!-- inline-rust: sketch; the real trait lives in src/field.rs -->
```rust
/// Per-delivery context of this broker.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct MyContext {
    pub partition: i32,
}

/// `Ctx<Partition>` in a handler binds the delivery's partition.
#[derive(Debug, Default, Clone, Copy)]
pub struct Partition;

impl ContextField for Partition {
    type Context = MyContext;
    type Value = i32;
    fn read(self, src: &MyContext) -> i32 {
        src.partition
    }
}
```

A broker with no per-delivery fields uses `()` and skips all of this.

## Middleware on the async edges

Integrations that need async I/O around encode and decode (a schema registry, a wire-format
envelope) do not belong in a `Codec`: the core codec is synchronous and handlers should stay on the
default one. Put them on the async edges instead - transcode incoming payloads on the
subscription's delivery path (before the codec sees them), and frame outgoing ones with a core
`PublishLayer` added app-wide via `RustStream::publish_layer`. The publish layer is async and
fallible, and `Outgoing::payload_mut` exists exactly for envelope wrapping.

## Config and defaults

Your crate owns its `Config`. The core carries no broker-specific config, which is what keeps an
upstream change scoped to one broker crate. If a config field has no sane default, do not implement
`Default` for it; force the user to set it explicitly rather than shipping a default that might break
later.

## Errors

Use `thiserror` for a single crate-level error enum, with variants by source. Mark public error
enums `#[non_exhaustive]`. Never use `anyhow` in a library crate.

## Test support

Ship an in-process transport implementing `TestableBroker` under a `testing` feature (registered with
`register_testable_broker!`) so users can unit-test handlers against your broker with the `TestApp`
harness. The transport does **core routing only**: it dispatches published messages to matching
subscribers and treats ack/nack as effectively a no-op. Do not simulate broker-specific semantics
(durable cursors, redelivery timers, offsets, dead-letter routing) in it; those are verified end to
end against a real server.

The reference is `MemoryBroker`'s own implementation:

```rust
--8<-- "src/memory/mod.rs:testable"
```

The transport calls `Coordinator::enqueued` on every enqueue into a subscriber and
`Coordinator::consumed` when a delivery is settled or dropped (so the harness can tell when the
reaction has settled), and routes delayed redeliveries through `Coordinator::schedule_redelivery`.
That one type then works with both `TestApp` and the conformance suite. See
[Testing](../guides/testing.md) for the user-facing side, and [Conformance](conformance.md) to
prove the implementation with `run_suite` and the lazy-startup `lifecycle` check.
