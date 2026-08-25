# Writing a broker

A broker is an independent crate that implements the core traits. It depends on `ruststream` with
default features off, so it pulls in the trait surface and runtime without the bundled JSON codec or
any other broker:

```toml
[dependencies]
ruststream = { version = "0.7", default-features = false }
```

This page is the contract. Implement the required traits, expose your own `Config`, add capability
traits for the features your broker supports, and prove the result with the
[conformance harness](conformance.md). For a complete implementation on a real client, see the
[worked NATS example](example-nats.md).

## The required traits

### `Broker` and `ConnectedBroker`

The broker is pure lifecycle, and the lifecycle is a ladder of consuming transitions: each state
is a distinct type, so out-of-order calls do not compile. The broker carries no subscriber or
publisher type, so a single application can mix broker kinds.

<!-- inline-rust: simplified contract sketch of the real RPITIT traits in src/broker.rs (which carry Send bounds and rustdoc); a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Broker: Send + Sync + Sized {
    type Error: std::error::Error + Send + Sync + 'static;
    type Connected: ConnectedBroker;
    async fn connect(self) -> Result<Self::Connected, Self::Error>;
}

pub trait ConnectedBroker: Send + Sync + Sized + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Closed: Send;
    async fn shutdown(self) -> Result<Self::Closed, Self::Error>;
}
```

`shutdown` must never block or panic; do all fallible teardown here and return a `Result`. The
`Closed` witness has no publish or subscribe surface; carry teardown diagnostics (flush results,
drop counts) in it as plain data, or use `()`.

Construction is **synchronous and I/O-free**: `new(addrs)` only records configuration, all network
work happens in `connect` (called once at startup by the runtime), and the connected form holds
the live client directly - its own operations never check a "maybe connected" state. A broker may
additionally keep a shared cell that `connect` fills (or a shareable in-process state, as the
in-memory broker does) so publishers can be handed out while the app is still being assembled,
before `connect` runs; the cell serves those early handles, not the connected form. The
[NATS example](example-nats.md) shows the cell-backed variant. The
[conformance harness](conformance.md) proves the ladder end to end.

There is no publish or subscribe to call on a broker you already shut down, so owner-side misuse
does not compile. Aliasing stays a runtime rule: handles that alias the connection (publishers
handed out from the connected form, clones of a shareable broker) must surface an error when
used after shutdown - never a silent success against a dead connection. The lifecycle check
drives that path too.

### `Subscribe`

Implement `Subscribe` on the connected form to support subscribing by name. This is what
`#[subscriber("name")]` uses.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/capability.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Subscribe: ConnectedBroker {
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

A broker that overrides neither defaulted method still works with every runtime feature:
`retry_after` falls back to an immediate requeue, and keyed lanes rotate keyless messages.

### `Publisher`

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Publisher: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error>;

    /// Defaulted: headers this handle contributes under every publish.
    fn base_headers(&self) -> Option<&Headers> { None }
}
```

`OutgoingMessage` borrows its name and payload, so publishing does not force an allocation.

This is the publish interface, not the one a service writes: applications publish through the
builder (`publisher.message(&value).publish()`, `publisher.raw(&bytes).to(dest).publish()`),
which resolves the destination, the codec and the headers and then makes exactly one call to
this method. Implement `publish` and the whole builder follows; there is nothing else to
provide.

A handle that carries an argument for a run of messages - a tenant, a partition hint, a delivery
option your broker expresses as a header - returns it from `base_headers` rather than writing it
into the message inside `publish`. The builder starts the outgoing map from that base and writes
the call site's headers over it key by key, so the call site wins (see
[where the headers come from](../guides/publishing.md#where-the-headers-come-from)).
`Transaction` carries the same defaulted method, so a transaction opened from such a handle
behaves identically. A publisher with nothing to add implements neither.

### `PublishPolicy`

A broker publisher is a bundle of policy (an exchange, a queue timeout, a transactional id) plus
the live connection. Split it along that seam: ship a freely constructible **policy** type with
the builder options and no publish surface, and implement `PublishPolicy` to pair it with the
connected form into the live publisher. Pairing is async and fallible for brokers that do real
work when a publisher comes alive (initializing a transactional producer); for most it is a cheap
constructor call.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait PublishPolicy<C: ConnectedBroker> {
    type Live; // the live publisher (or live wiring form, for combinator stacks)
    async fn pair(self, connected: &C) -> Result<Self::Live, PairError>;
}
```

The error is the type-erased `PairError`: wrap your broker's failure with `PairError::new`.
Pairing runs once per publisher at startup, never on the hot path.

Ship one policy/live pair per genuine publishing **mode**, and make mode selection a policy type
transition rather than a runtime flag: a plain policy pairs into the plain publisher, and a
`transactional_id(..)` builder step moves to a distinct transactional policy type whose live form
implements `TransactionalPublisher` - so the plain publisher has no transactional surface at all.
The in-memory broker's `MemoryPublish` / `MemoryRequest` are the minimal reference (no options, so
they are unit markers); the core's typed combinators implement `PublishPolicy` functorially, so
users compose codecs and transforms over your policy before it pairs.

When the plain policy is usable with its defaults (most are), also implement `DefaultPublish` on
the connected form to name it. The runtime then builds the default reply publisher when a
`publish("dest")` handler is included without an explicit `.publisher(..)`: `b.include(def)`
alone compiles. Brokers whose publishers always need explicit options do not implement it, and
their users attach a policy at every registration.

<!-- inline-rust: simplified contract sketch of the real trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait DefaultPublish: ConnectedBroker {
    type Policy: PublishPolicy<Self> + Default + Send + 'static;
}
```

## Subscription sources

`Subscribe` covers the by-name case. When a subscription needs broker-specific options (a consumer
group, a durable name, a delivery policy), expose a descriptor type that implements
`SubscriptionSource`:

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscription.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait SubscriptionSource<C: ConnectedBroker> {
    type Subscriber: Subscriber;
    fn name(&self) -> &str;
    fn subscribe(self, connected: &C) -> impl Future<Output = Result<Self::Subscriber, C::Error>> + Send;
}
```

Give the descriptor an associated constructor (`OrdersStream::new(..)`) rather than a free function,
so users can name it directly in the decorator: `#[subscriber(OrdersStream::new("orders", "workers"))]`.
The macro reads the type out of the constructor call, and also accepts a builder chain on it
(`#[subscriber(OrdersStream::new("orders").durable("workers"))]`) as long as each method returns
`Self`. Because `type Subscriber` lives on the source, one broker can offer several subscription
kinds (pub/sub versus streams) with different subscriber types - or, as the
[NATS example](example-nats.md) does, serve them all from one descriptor that branches internally.

Derive `Clone` on the descriptor: it is configuration, and the mount rebuilds it per registration
so one definition can be mounted on two brokers.

### Naming a kind by one string

A kind identified by a name and nothing else also implements `FromName`, whose single
constructor builds it from that name:

<!-- inline-rust: one-impl sketch against a broker-crate descriptor that has no in-repo compiled home -->
```rust
impl FromName for OrdersStream {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name)
    }
}
```

`#[subscriber(OrdersStream)]` is then legal: the attribute fixes the kind, and the mount site
supplies the value. A kind that genuinely needs more than a name to exist (a topic
*and* a subscription name) does not implement it, and that form does not compile for it.

### Settings in your own vocabulary

Core cannot know that a subscription has a stream, a durable name or a consumer group, so it
exposes one hook - `map_source`, a transform over the source the mount site is building - and your
crate layers its own trait on top, bound to your source type:

<!-- inline-rust: the extension-trait shape against a broker-crate descriptor with no in-repo compiled home -->
```rust
pub trait NatsSubscriber {
    fn jetstream(self, stream: impl Into<String>) -> Self;
    fn durable(self, name: impl Into<String>) -> Self;
}

impl<Def, W, F, P> NatsSubscriber for SubscriberBuilder<Def, SubscribeOptions, (W, F, P)> {
    fn jetstream(self, stream: impl Into<String>) -> Self {
        self.map_source(|source| source.jetstream(stream))
    }
    // ..
}
```

The bound on the source type means the methods do not exist on a builder for another broker.
Users import the trait to reach them, as with any extension trait. This is the same extension
shape the `Out` slot vocabulary uses below.

## Capability traits

Implement only the capabilities your broker supports; none are part of the mandatory interface.

| Trait | For brokers that support |
|---|---|
| `BatchSubscriber` | receiving messages in batches |
| `TransactionalPublisher` | begin / commit / abort around publishes on the handle |
| `OwnedTransactions` / `Transaction` | transactions whose buffer lives in a value, any number open at once per handle |
| `RequestReply` | native request-reply (NATS yes, Kafka no) |
| `Partitioned` | a partition key on outgoing messages |
| `Seekable` / `Seeker` | repositioning a live subscription in a replayable log |
| `Positioned` | deliveries that report their own log position |
| `DescribeServer` | reporting a `ServerSpec` for AsyncAPI |

`Seekable` mints its `Seeker` handle before the stream borrows the subscriber, so a running
subscription can be repositioned from outside the dispatch loop. Positions are broker-owned
(`KafkaPosition`-style constructors on your own type); a position captured from a delivered
message via `Positioned::position` carries a pinned contract - seeking to it redelivers exactly
that message - while constructed positions keep the semantics your position type documents.
Document what one seek covers (a consumer instance, or a shared group cursor) and reset any ack
bookkeeping the reposition invalidates.

### Extending the `Out` slot vocabulary

An `Out<impl X, Marker>` handler parameter accepts any `X` the runtime's `SlotPublisher`
wrapper implements; the core delegates its own capability set (`Publisher`,
`TransactionalPublisher`, `OwnedTransactions`, `RequestReply`). When your paired value offers
more than that - or is not a publisher at all (a per-partition producer cache, a shard
router) - declare your own capability trait, implement it for the value, and graft it onto the
wrapper with one blanket impl delegating through `SlotPublisher::inner`. Handlers then bound
their slot with your trait, and the concrete type still never appears in application code:

```rust
--8<-- "tests/out_slots.rs:extension"
```

Publishes made through values obtained from `inner` bypass the harness's per-slot capture
(like a settled owned transaction's buffer); they stay visible in the broker's publish log.

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

## Middleware on the async edges { #middleware-on-the-async-edges }

Integrations that need async I/O around encode and decode (a schema registry, a wire-format
envelope) do not belong in a `Codec`: the core codec is synchronous and handlers should stay on the
default one. Put them on the async edges instead - transcode incoming payloads on the
subscription's delivery path (before the codec sees them), and frame outgoing ones with a core
`PublishLayer` added app-wide via `RustStream::publish_layer`. The publish layer is async and
fallible, and `Outgoing::payload_mut` exists exactly for envelope wrapping.

## Config and defaults

Your crate owns its `Config`; the core carries no broker-specific config. If a config field has no
sane default, do not implement `Default` for it; force the user to set it explicitly rather than
shipping a default that might break later.

## Errors

Use `thiserror` for a single crate-level error enum, with variants by source. Mark public error
enums `#[non_exhaustive]`. Never use `anyhow` in a library crate.

## Test support

Ship an in-process transport implementing `TestableBroker` on its **connected form** under a
`testing` feature (registered with `register_testable_broker!` for that connected type, since the
harness connects every broker before recovering its transport) so users can unit-test handlers
against your broker with the `TestApp` harness. The transport does **core routing only**: it dispatches published messages to matching
subscribers and treats ack/nack as effectively a no-op. Do not simulate broker-specific semantics
(durable cursors, redelivery timers, offsets, dead-letter routing) in it; those are verified end to
end against a real server.

The reference is the in-memory broker's own implementation (on `ConnectedMemoryBroker`):

```rust
--8<-- "src/memory/mod.rs:testable"
```

The transport calls `Coordinator::enqueued` on every enqueue into a subscriber and
`Coordinator::consumed` when a delivery is settled or dropped (so the harness can tell when the
reaction has settled), and routes delayed redeliveries through `Coordinator::schedule_redelivery`.
That one type then works with both `TestApp` and the conformance suite. See
[Testing](../guides/testing.md) for the user-facing side, and [Conformance](conformance.md) to
prove the implementation with `run_suite` and the `lifecycle` ladder check.
