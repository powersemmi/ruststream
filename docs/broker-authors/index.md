# Writing a broker

A broker is an independent crate that implements the core traits. It depends on `ruststream` with
default features off, so it pulls in the trait surface and runtime without the bundled JSON codec or
any other broker:

```toml
[dependencies]
ruststream = { version = "0.3", default-features = false }
```

This page is the contract. Implement the required traits, expose your own `Config`, add capability
traits for the features your broker supports, and prove the result with the
[conformance harness](conformance.md). For a complete implementation on a real client, see the
[worked NATS example](example-nats.md).

## The required traits

### `Broker`

The broker is pure lifecycle: connect and shut down. It carries no subscriber or publisher type, so a
single application can mix broker kinds.

```rust
pub trait Broker: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn connect(&self) -> Result<(), Self::Error>;
    async fn shutdown(&self) -> Result<(), Self::Error>;
}
```

`shutdown` must never block or panic; do all fallible teardown here and return a `Result`.

### `Subscribe`

Implement `Subscribe` to support subscribing by name. This is what `#[subscriber("name")]` uses.

```rust
pub trait Subscribe: Broker {
    type Subscriber: Subscriber;
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error>;
}
```

### `Subscriber`

A subscriber is a `Stream` of incoming messages. Back-pressure comes for free from the stream.

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

```rust
pub trait IncomingMessage: Send + Sync {
    fn payload(&self) -> &[u8];
    fn headers(&self) -> &Headers;
    async fn ack(self) -> Result<(), AckError>;
    async fn nack(self, requeue: bool) -> Result<(), AckError>;
}
```

### `Publisher`

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

## Config and defaults

Your crate owns its `Config`. The core carries no broker-specific config, which is what keeps an
upstream change scoped to one broker crate. If a config field has no sane default, do not implement
`Default` for it; force the user to set it explicitly rather than shipping a default that might break
later.

## Errors

Use `thiserror` for a single crate-level error enum, with variants by source. Mark public error
enums `#[non_exhaustive]`. Never use `anyhow` in a library crate.

## Test support

Ship a `TestClient` under a `testing` feature so users can unit-test handlers against your broker
in-process. The test client does **core routing only**: it dispatches published messages to matching
subscribers and treats ack/nack as effectively a no-op. Do not simulate broker-specific semantics
(durable cursors, redelivery timers, offsets, dead-letter routing) in it; those are verified end to
end against a real server. See [Testing](../guides/testing.md) for the user-facing side, and
[Conformance](conformance.md) to prove the implementation.
