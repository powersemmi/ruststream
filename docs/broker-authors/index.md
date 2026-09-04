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
[conformance harness](conformance.md) proves the ladder end to end, and the
[NATS example](example-nats.md) walks the whole ladder on a real client.

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
    fn headers(&self) -> &HeaderMap;
    async fn ack(self) -> Result<(), AckError>;
    async fn nack(self, requeue: bool) -> Result<(), AckError>;

    // Defaulted: a plain nack(true). Override when the transport has native
    // delayed redelivery (JetStream NAK with delay); handlers reach it through
    // HandlerOutcome::retry_after.
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
    fn base_headers(&self) -> Option<&HeaderMap> { None }
}
```

`OutgoingMessage` borrows its name and payload, so publishing does not force an allocation.

This is the publish interface, not the one a service writes: applications publish through the
builder (`publisher.message(&value).publish()`), which resolves the destination, the codec
(where the value's wire needs one) and the headers and then makes exactly one call to
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
`publish("dest")` handler is included without an explicit `.out(Reply, ..)`: `b.include(def)`
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
use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings};

pub trait NatsSubscriber {
    fn jetstream(self, stream: impl Into<String>) -> Self;
    fn durable(self, name: impl Into<String>) -> Self;
}

// The four state slots are (workers, failure policies, start position, batch size); `Codec` is
// the registration's own decode override, `()` until one is named. Both travel unchanged.
impl<Def, Workers, Failures, StartPosition, Batch, Codec> NatsSubscriber
    for SubscriberBuilder<Def, SubscribeOptions, (Workers, Failures, StartPosition, Batch), Codec>
where
    Def: Declared,
{
    fn jetstream(self, stream: impl Into<String>) -> Self {
        self.map_source(|source| source.jetstream(stream))
    }

    fn durable(self, name: impl Into<String>) -> Self {
        self.map_source(|source| source.durable(name))
    }
}
```

The bound on the source type means the methods do not exist on a builder for another broker.
Users import the trait to reach them, as with any extension trait. This is the same extension
shape the `Out` slot vocabulary uses below.

One core setting changes the source type rather than a state slot: `start_at(..)` wraps the
descriptor in `StartAt<SubscribeOptions, Position>`, so on exactly the subscriptions that named a
start position your methods are no longer in scope. Cover that with a second impl over the wrapped
source. `StartAt::map_inner` reaches the descriptor underneath and hands the position back
untouched, so each method stays one line:

<!-- inline-rust: the second extension impl against the same broker-crate descriptor, which has no in-repo compiled home -->
```rust
use ruststream::StartAt;
use ruststream::runtime::Fixed;

// The start-position slot is `Fixed` here by construction - `start_at(..)` is what produced the
// wrapper - and the source type is a different one, so this impl and the one above never overlap.
impl<Def, Workers, Failures, Batch, Codec, Position> NatsSubscriber
    for SubscriberBuilder<
        Def,
        StartAt<SubscribeOptions, Position>,
        (Workers, Failures, Fixed, Batch),
        Codec,
    >
where
    Def: Declared,
{
    fn jetstream(self, stream: impl Into<String>) -> Self {
        self.map_source(|source| source.map_inner(|inner| inner.jetstream(stream)))
    }

    fn durable(self, name: impl Into<String>) -> Self {
        self.map_source(|source| source.map_inner(|inner| inner.durable(name)))
    }
}
```

### Publisher settings in your own vocabulary

The publish side mirrors it. A mount site names a publish policy with `.out(marker, policy)` -
`Reply` for what a `publish("dest")` handler returns, an `Out` slot's marker for a slot - and
`MapPublisher` is the hook over the policy that position carries:

<!-- inline-rust: the extension-trait shape against a broker-crate policy with no in-repo compiled home -->
```rust
use ruststream::runtime::MapPublisher;

pub trait NatsPublish {
    fn stream(self, name: impl Into<String>) -> Self;
    fn expect_last_sequence(self, seq: u64) -> Self;
}

impl<T: MapPublisher<Policy = Publish>> NatsPublish for T {
    fn stream(self, name: impl Into<String>) -> Self {
        self.map_publisher(|policy| policy.stream(name))
    }

    fn expect_last_sequence(self, seq: u64) -> Self {
        self.map_publisher(|policy| policy.expect_last_sequence(seq))
    }
}
```

A service then reads:

<!-- inline-rust: the call shape against the broker policy sketched above -->
```rust
b.include(confirm).out(Reply, Publish).stream("ORDERS");
b.include(mirror).out(Audit, Publish).stream("AUDIT").build();
```

The bound is on the policy, not on the chain, so one impl covers the reply position and every
slot, on a router and on a broker scope alike. `map_publisher` replaces the policy with one of
the same type, which is what a publisher's own settings produce; a different policy type is a
different publish mode and belongs in the `.out(marker, policy)` call itself. Passing an
already-configured value (`.out(Reply, Publish::default().stream("ORDERS"))`) keeps working - the
hook is the ergonomic mirror, not a replacement.

## Capability traits

Implement only the capabilities your broker supports; none are part of the mandatory interface.
`BatchSubscriber` comes closest to one: [offer it wherever you can](#batches-batchsubscriber),
because every batch handler asks for one and a transport with no batching of its own can still
assemble batches on the client.

| Trait | For brokers that support |
|---|---|
| `BatchSubscriber` | receiving messages in batches (offer it wherever you can; see below) |
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
bookkeeping the reposition invalidates. To let handler bodies seek, carry the delivery's
position and the subscription's seeker as fields of your per-delivery context and publish
`ContextField` keys for them - the in-memory broker's `MemoryContext` with its `Position` /
`SeekHandle` keys is the model. The batch forms reach the seeker through the batch context
below, which carries the handle without the position.

These traits are the vocabulary a handler body writes. A body bounds its slot with the capability
it needs (`Out<impl TransactionalPublisher, Journal>`, or `where W: TransactionalPublisher` on the
manual path) and never with a type of yours, and the include site checks the bound policy's live
form against it once, at compile time. Under each of the four publisher capabilities the arena
entry also offers that capability's typed form over the include site's codec and the marker's
dictionary - the publish builder, a transaction scope, an owned transaction, a correlated request
- so implementing the trait on your live publisher is all a service needs to reach them.

### Batches: `BatchSubscriber`

A handler taking `&[T]` consumes a batch, and its mount site names one number - the batch size -
which the runtime passes straight to `BatchSubscriber::batches(size)`. The batch your subscriber
yields is the batch the body sees: the runtime never splits or merges one, so a batch must never
carry more than `size` messages, and it may carry fewer whenever that is all the transport had.

Translate `size` into whatever your client already speaks: `XREADGROUP COUNT`, a JetStream pull
batch, a Kafka poll limit. Everything else about how a batch forms - a block timeout, a consumer
group, a prefetch window - stays your own vocabulary, configured on your subscription source
through your settings extension trait, so a service writes `b.include(handler.batch(nonzero!(6))
.block(Duration::from_secs(5)))` with the core's word first and yours after it.

Put the capability on every subscriber a mount can reach, not only on the one your own descriptor
opens. `#[subscriber("topic")]` goes through `Subscribe`, so a `&[T]` body on that form asks for
`BatchSubscriber` on `Subscribe::Subscriber`; a crate that wired the capability onto its
descriptor's subscriber alone leaves the string-literal form failing to compile. Where the two are
the same type there is nothing to do, and where they differ both need it.

Where the transport delivers one message at a time, do not leave the capability out: assemble
the batches on the client with the core's `BufferedSubscriber`, whose `batches` honours the size
it is given. The deadline that closes a partial batch is your choice, and it need not be a
constant: expose it on your subscription descriptor (`.max_wait(Duration::from_millis(25))`) and
hand it to the wrapper as the subscription opens, so a service can tune it per subscription. A
network transport wants exactly that: the 10 ms default is sized for an in-process bus, and once a
round trip is in the way it closes most batches at a single delivery, so the broker crates that
ship the deadline as a descriptor option settle between 10 and 50 ms. The size is not yours to
choose. Everything else about the subscriber reaches through the wrapper unchanged:

```rust
--8<-- "tests/batch_subscriber.rs:buffered_capability"
```

Nothing in the mount site says which of the two you did, which is the point: a service names the
batch size and gets batches.

Declining the capability is still a legitimate answer where batching would break a guarantee the
transport carries. A ZeroMQ ROUTER is the case in practice: it answers each peer at that peer's
own `reply-to`, while a batch reaches its reply wiring with one `PublishContext` for the whole
batch, so batched replies would follow one peer's address and misroute the rest. Say so in your
crate's docs; a `&[T]` body then simply does not compile on that transport, which is the honest
outcome.

The `conformance` batch suite checks the contract - it opens a subscription at a size smaller
than the run and fails a broker whose batches come back larger. It is not part of
`harness::run_suite`: capability suites are yours to call, one per capability you implement.

### The prelude your crate ships { #broker-prelude }

Your types are named at the mount site, not in the body, and that is what your crate's prelude is
for. Ship a `prelude` module in three layers, in this order:

1. `pub use ruststream::prelude::*;` so one glob serves the whole file;
2. your own surface a service names: the broker, its subscription source, its error, the
   `ContextField` keys a body reads;
3. your publish policies under the uniform names every broker uses - `Publish`, and where you
   have them `TransactionalPublish` and `Request` (`pub use crate::KafkaTransactionalPublish as
   TransactionalPublish;`). Add the capability traits you implement on your live values as a
   manifest, so the glob that names the policies also puts their operations in scope.

Those three names are policy names, so the core prelude exports nothing under them: a mount site
reads the same whichever broker it is on, and swapping brokers swaps the glob. Never alias a
policy to a core trait name (`Publisher`, `TransactionalPublisher`, `OwnedTransactions`,
`RequestReply`) or re-export something else under one: a body that globs both preludes has to keep
resolving those to the core traits.

The manifest is what your glob adds, so it is the consumer-side traits a body reaches through your
broker - `Positioned`, `Seeker`, `Transaction` and the like. The four publisher capabilities are
already in the core prelude, so re-exporting them changes nothing. Leave out a trait whose method
would collide with a defaulted core method - `Partitioned::partition_key` against
`IncomingMessage::partition_key` is the case in practice - and let a service that needs it import
it explicitly. `BatchSubscriber` belongs in no manifest at all: the framework calls it, and no
body ever writes it as a bound. `ruststream::memory::prelude` is the worked example.

### Extending the `Out` slot vocabulary

An `Out<impl X, Marker>` handler parameter accepts any `X` the live value behind the slot
implements; on top of that the core delegates its own capability set (`Publisher`,
`TransactionalPublisher`, `OwnedTransactions`, `RequestReply`). When your live value offers more
than that - or is not a publisher at all (a per-partition producer cache, a shard router) -
declare your own capability trait and implement it for the live value.

What the body actually holds is the arena entry, `Slot<Marker, W, E, Pipe, Body>`, a transparent
window onto that value. Autoderef carries a method call through it, but not a trait bound: a helper
written as `fn issue<L: Lanes>(lanes: &L)` rejects the entry with `E0277`. Add one blanket impl
next to your trait - `impl<M, W: Lanes, E, Pipe, Body> Lanes for Slot<M, W, E, Pipe, Body>`,
delegating through the entry's `Deref` - and helpers and bodies generic over the capability take
the entry as it is. The concrete type still never appears in application code:

=== "Macros"

    ```rust
    --8<-- "tests/out_slots.rs:extension"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_out_slots.rs:extension"
    ```

Where the send happens is what shapes the trait, and there are two shapes.

A **router-shaped** capability hands out a publisher and never sends one itself: the per-partition
producer cache above picks the lane for a shard and returns it. What the handler publishes through
that lane leaves by the unwrapped value, so it bypasses the harness's per-slot capture (like a
settled owned transaction's buffer) and is asserted on the broker's publish log instead. That is
the attribution boundary, and it is the price of handing out the inner publisher.

A **step-shaped** capability sets one argument on a message and ends in a single publish: an
ordering key, a priority, a QoS. Do not put the send in the trait. A publish that leaves through
your own value is a publish the slot view stops seeing, and an argument like an ordering key is
exactly what a test wants to assert on. Ride the entry's typed publish path instead
(`out.message(&value).publish()`) and carry the argument as a header: a publisher holding it for a
run of messages returns it from `Publisher::base_headers`, a call site setting it per message
writes it with `.with_headers(..)`, and your `publish` reads it off the outgoing map and strips it
before the wire. A value your publisher cannot read is a publish error, never a silent fallback to
the default - the caller asked for an ordering it would not get.

### Your crate's prelude

Two files import different things, and the split is what keeps a service portable. A handler body
imports `ruststream::prelude::*` and nothing of yours: it bounds an injected slot with the broker
capability trait - `Out<impl Publisher>`, `Out<impl TransactionalPublisher>`,
`Out<impl OwnedTransactions>`, `Out<impl RequestReply>` - so the body says what it needs of a
publisher and never which broker provides it. A routes file imports your prelude, because mounting
is where a broker is named.

That makes your prelude the one import a service on your broker writes, so its shape is part of the
contract. Four layers, in this order:

- `pub use ruststream::prelude::*;` first, so everything a body already knows arrives unchanged;
- the crate surface your own examples name: the broker, its subscription descriptor, its config;
- your publish policies, aliased to the uniform mount-site names - `NatsPublish as Publish`,
  `KafkaTransactionalPublish as TransactionalPublish`, `LapinRequest as Request` - so a routes file
  reads the same whichever broker it mounts, and switching brokers is a change of import;
- the capability manifest: the core capability traits your broker actually implements, so what a
  service may bound a slot with is legible from that one import.

The core exports no trait under the policy names, so those aliases collide with nothing. The rule
that keeps it that way runs in both directions, and your half is that your prelude must not shadow
a core name with anything of yours. An explicit re-export beats a glob without a word, so a name
you spell like a core trait takes that trait away from every service writing the glob, and the
error surfaces in the service's file rather than in yours.

Pin both halves with a probe behind your own glob: the bound a body writes still has to arrive as
the core trait, and the mount-site name still has to be your policy.

<!-- inline-rust: a compile-time probe that belongs in a broker crate, behind that crate's own prelude glob -->
```rust
// in your crate, behind your own prelude glob
use crate::prelude::*;

// A capability bound a body states: the core trait, not something of yours.
fn _p<T: Publisher>() {}

// A mount-site name: your policy, constructible with no connection in sight.
fn _q() {
    let _: Publish = Publish::default();
}
```

## Per-delivery context and `Ctx` keys

A broker with native delivery metadata (a partition, an offset, a stream sequence) exposes it as a
typed per-delivery context: a `#[non_exhaustive]` struct the subscriber names, plus `ContextField`
key types so handlers can bind single fields as parameters with the
[`Ctx<K>` extractor](../guides/context.md#per-delivery-context). Keys are unit structs. No type-map
and no heap on the delivery path.

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

The sketch reads a `Copy` scalar, which owns and borrows alike. A position that is not `Copy` - a
Pulsar message id, a Kinesis shard plus its sequence string - is read by borrowing:
`Field::Value<'a>` is generic over the source's lifetime, so the key hands back `&'a MessageId`
and a body reading it with `ctx.context(..)` copies nothing. Only `ContextField::Value`, the value
behind the `Ctx<K>` extractor, has to be owned and `'static`, because extractor values bind before
the body runs; that key clones what the borrowing one returns. A key usually implements both
traits, one shape each.

A broker with no per-delivery fields uses `()` and skips all of this.

Batch subscriptions get a context of their own, because a batch spans many deliveries: build a
second struct out of what the whole *subscription* shares (a seek handle, a stream name, a
consumer group), implement `BuildBatchContext` on it - the runtime builds one per batch from the
batch's first delivery - and publish `Field` keys so a batch body reads it with `ctx.context(..)`.
Per-delivery fields stay out of it: a position belongs to one delivery, so a batch reads it off
the elements instead. Keeping the two structs apart is what enforces that at compile time,
since a per-delivery context does not implement `BuildBatchContext` and a batch body therefore
cannot name it. The in-memory broker's `MemoryBatchContext` - the subscription's seeker under
the same `SeekHandle` key its per-delivery context publishes - is the model, and a broker with
nothing subscription-scoped to offer implements nothing and leaves batches on the `()` default.

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
