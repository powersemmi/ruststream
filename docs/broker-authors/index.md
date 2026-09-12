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

The broker is pure lifecycle: each state is a distinct type, and a transition consumes `self` and
returns the next state, so calls made out of order do not compile. The broker names neither a
subscriber type nor a publisher type, so one application can mix brokers of different kinds.

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

`shutdown` must never block or panic: do all teardown that can return an error here, and return a
`Result`. `Closed` is the shutdown witness: carry teardown diagnostics (flush results, drop counts)
in it as plain data, or use `()`.

Construction is **synchronous and I/O-free**: `new(addrs)` only records the configuration. All
network work happens in `connect`, which the runtime calls once at startup. The connected form
holds the live client directly, so its operations never check a "maybe connected" state.

A broker may additionally keep a shared cell that `connect` fills, or shareable in-process state,
as the in-memory broker does. Publishers can then be handed out while the application is still
being assembled, before `connect` runs: the cell serves those early handles, not the connected
form.

The [conformance harness](conformance.md) proves the whole sequence of transitions, and the
[NATS example](example-nats.md) walks it on a real client.

A broker you already shut down has nothing left to call, neither publish nor subscribe, so misuse
by the owner does not compile. Sharing the connection is checked at run time: handles that share
it (publishers handed out from the connected form, clones of a shareable broker) must return an
error after shutdown and must never succeed silently against a dead connection. The `lifecycle`
check covers that path too.

The in-memory broker walks the whole lifecycle in a few lines, and every example of it on this
page is cut from that same file, so a contract that moves takes the page's code with it:

```rust
--8<-- "src/memory/mod.rs:ladder"
```

`ClosedMemoryBroker` is the witness with the teardown diagnostics described above: it reports how
many subscriber registrations the shutdown dropped.

### `Subscribe`

Implement `Subscribe` on the connected form so a service can subscribe by the name of a topic, a
subject or a queue. `#[subscriber("name")]` subscribes through it.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/capability.rs, with the defaulted method annotated inline for teaching; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Subscribe: ConnectedBroker {
    type Subscriber: Subscriber;
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error>;

    // Defaulted: None. Answer with the name itself where a publish to a subscribe name
    // reaches the subscription opened under it, which is what a subject, a topic, a
    // stream or a queue name usually is.
    fn redelivery_address(&self, name: &str) -> Option<RedeliveryAddress>;
}
```

Opening a subscription and saying where a publish reaches it is all it has to do:

```rust
--8<-- "src/memory/mod.rs:subscribe"
```

The second answer is the address the runtime publishes a deferred retry to, so it decides whether
`#[subscriber("orders")]` works with `BrokerScope::retry_via` on your broker. Keep the default
where a subscribe name is not a publish destination: a Google Pub/Sub subscription is subscribed to
by its own name and published to through its topic, and the descriptor answers there instead.

### `Subscriber`

A subscriber is a `Stream` of incoming messages. Back-pressure comes from the stream itself.

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

A delivered message exposes its payload and its headers, and is acknowledged with `ack` or rejected
with `nack`. `ack` consumes `self`, so a double ack is a compile error.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/message.rs, with the defaulted methods annotated inline for teaching; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait IncomingMessage: Send + Sync {
    fn payload(&self) -> &[u8];
    fn headers(&self) -> &HeaderMap;
    async fn ack(self) -> Result<(), AckError>;
    async fn nack(self, requeue: bool) -> Result<(), AckError>;

    // Defaulted: false. The runtime reads this first and never calls
    // nack_after without it, so override the pair together.
    fn supports_nack_after(&self) -> bool;

    // Defaulted: AckError::Unsupported. Override when the transport has native
    // delayed redelivery (JetStream NAK with delay); handlers reach it through
    // HandlerOutcome::retry_after.
    async fn nack_after(self, delay: Duration) -> Result<(), AckError>;

    // Defaulted: None. Override (with the Partitioned capability) to feed the
    // runtime's keyed worker lanes, workers(n, by_key).
    fn partition_key(&self) -> Option<&[u8]>;
}
```

Delayed redelivery is two methods, and the runtime asks `supports_nack_after`. Override
`nack_after` alone and the flag stays `false`, so the override is never called. By default
`nack_after` returns `AckError::Unsupported` instead of settling the delivery with a plain
`nack(true)`: a transport that cannot hold a message back has to say so, or the pause before a
retry turns into a storm of redeliveries.

A broker that overrides none of the three defaulted methods still works with every runtime feature.
Where there is no native delayed redelivery the runtime runs `retry_after` itself: it drops the
delivery and, after the delay, publishes a copy through the publisher the application wired with
`BrokerScope::retry_via`, with an incremented retry-count header. That copy goes to the address
[your subscription reports](#where-a-deferred-retry-is-published). Only with no such publisher does
the delay degrade to an immediate requeue. Keyed worker lanes hand out keyless messages
round-robin.

There is no broker to point at for "overrides nothing": every broker in this workspace overrides
these methods. So the core pins the behaviour with a test:

```rust
--8<-- "src/message.rs:incoming_defaults"
```

The `Unsupported` answer is what lets the runtime tell a transport with no delayed redelivery from
one that honoured the delay, and run its own fallback.

### `Publisher`

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Publisher: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Your broker's per-message settings. Every field optional; `()` when you have none.
    type Options: Send + Sync;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error>;

    /// Defaulted: headers this handle contributes under every publish.
    fn base_headers(&self) -> Option<&HeaderMap> { None }
}
```

`OutgoingMessage` borrows both its name and its payload, so publishing does not force an
allocation.

A service writes the builder, not this method: `publisher.message(&value).publish()` picks the
destination, the codec and the headers, and makes exactly one call to `publish`. Implement
`publish` and the whole builder works on top of it.

`Options` holds what belongs to the message rather than to the handle: a QoS, a priority, an
ordering key, an expiration. Every field is optional, because a call carries only what it adjusted.
What it left alone is what the policy fixed when it paired this publisher, so resolving the two is
the first thing your `publish` does. A broker with no per-message setting writes
`type Options = ();`.

`options` is `None` wherever there is no call site to adjust them - a reply, a deferred
redelivery - and the policy's settings are then the whole answer.

`base_headers` is for a constant of the publisher itself: a tenant, a producer name, a schema id
every message of this handle carries. The builder starts the outgoing headers from that base and
writes the call site's headers over it key by key, so on a shared key the call site's value stays
(see [where the headers come from](../guides/publishing.md#where-the-headers-come-from)).

`Transaction` names the same `Options` and carries the same defaulted `base_headers`, so a message
inside a transaction takes the settings a message outside one takes. A publisher with nothing to
add overrides neither.

### `PublishPolicy`

A broker publisher is a policy (an exchange, a queue timeout, a transactional id) and the live
connection. Ship a separate **policy** type: it is constructible anywhere and holds the builder
options.

Implement `PublishPolicy` on it: the policy constructs the live publisher on the connected form,
and `pair` is that constructor. It is async and can return an error, so a broker that has to
initialize a transactional producer does it here.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait PublishPolicy<C: ConnectedBroker> {
    type Live; // the live publisher (or live wiring form, for combinator stacks)
    async fn pair(self, connected: &C) -> Result<Self::Live, PairError>;
}
```

The error is the type-erased `PairError`: wrap your broker's error with `PairError::new`. The
policy instantiates the publisher once, at startup, so `pair` never reaches the hot path.

Ship one policy and live form per genuine publishing **mode**, and make the choice of mode a
transition of the policy type rather than a runtime flag. The plain policy constructs the plain
publisher, and a `transactional_id(..)` builder step moves it to a distinct transactional policy
type whose live form implements `TransactionalPublisher`. The plain publisher then has no
transactional surface at all.

The minimal reference is the in-memory broker's `MemoryPublish` and `MemoryRequest`: they have no
options, so they are empty structs.

The core's typed combinators implement `PublishPolicy` functorially, so users compose codecs and
transforms over your policy before it constructs the publisher.

When the plain policy is usable with its defaults (most are), also implement `DefaultPublish` on
the connected form and name the policy there. The runtime then instantiates the reply publisher
itself when a `publish("dest")` handler is mounted without an explicit `.out(Reply, ..)`, and
`b.include(def)` compiles on its own. Brokers whose publishers always need explicit options do not
implement it, and their users specify the policy at every handler registration.

<!-- inline-rust: simplified contract sketch of the real trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait DefaultPublish: ConnectedBroker {
    type Policy: PublishPolicy<Self> + Default + Send + 'static;
}
```

Both halves, on a broker whose policy carries no options at all:

```rust
--8<-- "src/memory/mod.rs:publish_policy"
```

## Subscription sources

`Subscribe` covers the case where a name is all a subscription needs. When it needs options of your
own broker (a consumer group, a durable name, a delivery policy), ship a descriptor type that
implements `SubscriptionSource`:

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscription.rs, with the defaulted method annotated inline for teaching; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait SubscriptionSource<C: ConnectedBroker> {
    type Subscriber: Subscriber;
    fn name(&self) -> &str;
    fn subscribe(self, connected: &C) -> impl Future<Output = Result<Self::Subscriber, C::Error>> + Send;

    // Defaulted: Ok(None). Answer where a publish reaches this subscription again,
    // asking the broker when only the live connection knows.
    async fn redelivery_address(&self, connected: &C) -> Result<Option<RedeliveryAddress>, C::Error>;
}
```

Give the descriptor an associated constructor (`OrdersStream::new(..)`) rather than a free function:
a user then names it directly in the attribute,
`#[subscriber(OrdersStream::new("orders", "workers"))]`.

The macro reads the type out of the constructor call, and accepts a builder chain on it as well
(`#[subscriber(OrdersStream::new("orders").durable("workers"))]`), as long as each method returns
`Self`.

`type Subscriber` is declared on the source, so one broker can offer several kinds of subscription
(pub/sub and streams) with different subscriber types, or serve them all from one descriptor that
branches inside, as the [NATS example](example-nats.md) does.

Derive `Clone` on the descriptor: the mount rebuilds the configuration per registration, so one
definition can be mounted on two brokers at once.

### Where a deferred retry is published

Without native delayed redelivery, the runtime honours `retry_after` by publishing a copy of the
message once the delay is over. Your descriptor says where that copy goes.

```rust
--8<-- "src/memory/mod.rs:source"
```

Answer with the name a publisher bound to your broker uses to reach this subscription again: the
subject on NATS, the topic on Kafka, the stream key on Redis. On Google Pub/Sub it is neither - a
subscription and a topic are separate resources there, so the answer is the topic the subscription
is bound to, and the descriptor asks the API for it. The runtime asks once at startup, so a request
here costs nothing per message.

Keep the default where publishing cannot reach your subscription at all. An application that wires
a retry publisher over such a subscription then does not start, naming the subscription and its
source, instead of publishing every delayed message to an address nobody reads.

`harness::lifecycle` checks the answer you give: a publish to the reported address must arrive at
the subscription that reported it.

### Naming a kind by one string

A kind identified by a name and nothing else also implements `FromName`: its single constructor
builds the value from that name.

```rust
--8<-- "src/memory/mod.rs:from_name"
```

`#[subscriber(OrdersStream)]` is then legal: the attribute names the kind, and the mount site
supplies the value. A kind that needs more than one name (a topic *and* a subscription name) does
not implement `FromName`, and that form does not compile for it.

### Settings in your own vocabulary

The core does not know that a subscription has a stream, a durable name or a consumer group, so it
gives you one hook: `map_source`, a transform over the source the mount site is building. You put
your own trait on top and bind it to your source type:

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

The bound on the source type means these methods do not exist on a builder for another broker. The
`Out` slot vocabulary below uses the same extension shape.

One core setting changes the source type rather than a state slot: `start_at(..)` wraps the
descriptor in `StartAt<SubscribeOptions, Position>`. On the subscriptions that named a start
position your methods fall out of scope, so cover that case with a second impl over the wrapped
source. `StartAt::map_inner` takes the descriptor out of the wrapper and hands the position back
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

The publish side is built the same way. The mount site names a policy with `.out(marker, policy)`:
the `Reply` marker for what a `publish("dest")` handler returns, a slot's marker for an `Out` slot.
`MapPublisher` is the hook over the policy in that position:

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

In a service it reads like this:

<!-- inline-rust: the call shape against the broker policy sketched above -->
```rust
b.include(confirm).out(Reply, Publish).stream("ORDERS");
b.include(mirror).out(Audit, Publish).stream("AUDIT").build();
```

The bound is on the policy, not on the chain, so one impl covers the reply position, every slot, a
router and a broker scope alike.

`map_publisher` replaces the policy with one of the same type, and a different policy type means a
different publish mode, which belongs in the `.out(marker, policy)` call itself. An
already-configured value can be passed there directly:
`.out(Reply, Publish::default().stream("ORDERS"))`.

### Per-message settings on the publish builder

A setting one message differs from the next in - a QoS, a priority, an ordering key, an expiration
- is a field of your `Publisher::Options`, and a call site adjusts it through a step you add to the
publish builder. Nothing wraps the publisher, so the publish still leaves through the mount site's
own entry, with the codec and the transforms that entry named.

The four pieces are an options type whose every field is optional, a policy that carries the
defaults, a live publisher resolving one against the other, and an extension trait over
`PublishBuilder` bounded on the options type. The bound is what keeps your steps off a builder
over another broker's publisher:

```rust
--8<-- "tests/publish_options.rs:broker_side"
```

The broker half is the same whichever way a service mounts, because it is ordinary trait impls
either way. Ship the extension trait from your prelude next to the policy aliases.

A step is the only shape a per-message setting takes. Do not put the send in the trait: a publish
that leaves through a value of yours is a publish the slot view stops seeing, and a setting like an
ordering key is exactly what a test wants to assert on. Do not carry one as a header either: it is
a protocol field, and a string round trip through the header map inside one process is not one.

A value your broker cannot honour is a publish error, never a silent fallback to the default: the
caller asked for an ordering it would not get.

## Capability traits

Implement only the capabilities your broker supports; none are part of the mandatory interface.
`BatchSubscriber` comes closest to one: [offer it wherever you can](#batches-batchsubscriber),
because every batch handler asks for one and a transport with no batching of its own can still
assemble batches on the client.

| Trait | For brokers that support |
|---|---|
| `BatchSubscriber` | receiving messages in batches |
| `TransactionalPublisher` | begin / commit / abort around publishes on the publisher handle |
| `OwnedTransactions` / `Transaction` | any number of transactions open at once per handle, each with its own buffer |
| `RequestReply` | native request-reply |
| `Partitioned` | a partition key on outgoing messages |
| `Seekable` / `Seeker` | repositioning a live subscription in a replayable log |
| `Positioned` | reporting a delivery's own position in the log |
| `DescribeServer` | reporting a `ServerSpec` for AsyncAPI |

`Seekable` hands out its `Seeker` handle before `stream` borrows the subscriber, so a running
subscription can be repositioned from outside the dispatch loop.

Positions are broker-owned: you declare the constructors, `KafkaPosition`-style, on your own type.
A position captured from a delivered message through `Positioned::position` pins the contract:
seeking to it redelivers exactly that message. Constructed positions keep the semantics your
position type documents.

Document what one seek covers (a consumer instance or a shared group cursor) and reset any ack
bookkeeping the reposition invalidates.

To let handler bodies seek, carry the delivery's position and the subscription's seeker as fields
of your per-delivery context and publish `ContextField` keys for them. The in-memory broker's
`MemoryContext`, with its `Position` and `SeekHandle` keys, is the model. The batch forms take the
seeker from the batch context below, which carries no position.

A `DescribeServer` description reports the host and port clients connect to. Credentials never
appear in it: the document is generated to be published. A broker configured from a URL therefore
builds its description with `ServerSpec::from_url`, which drops the user name and password, and not
by trimming the scheme off the URL and passing the rest on. A broker that configures several
addresses joins them from `ServerSpec::host_from_url`.

These traits are the vocabulary a handler body writes. A body bounds its slot with the capability
it needs (`Out<impl TransactionalPublisher, Journal>`, or `where W: TransactionalPublisher` on the
manual path) and never with a type of yours, and the mount site checks the bound policy's live form
against it once, at compile time.

Under each of the four publisher capabilities the arena entry also offers that capability's typed
form over the mount site's codec and the marker's dictionary: the publish builder, a transaction
scope, an owned transaction, a correlated request. Implementing the trait on your live publisher is
all a service needs to reach them.

### Batches: `BatchSubscriber`

A handler taking `&[T]` consumes a batch, and its mount site names one number, the batch size. The
runtime passes it straight to `BatchSubscriber::batches(size)`. The batch your subscriber yields is
the batch the body sees: the runtime never splits or merges one, so a batch never carries more than
`size` messages, and it carries fewer whenever that is all the transport had.

Translate `size` into whatever your client already speaks: `XREADGROUP COUNT`, a JetStream pull
batch, a Kafka poll limit. Everything else about how a batch forms (a block timeout, a consumer
group, a prefetch window) stays your own vocabulary, configured on your subscription source through
your settings extension trait. A service then writes
`b.include(handler.batch(nonzero!(6)).block(Duration::from_secs(5)))`, the core's word first and
yours after it.

Put the capability on every subscriber a mount can reach, not only on the one your own descriptor
opens. `#[subscriber("topic")]` goes through `Subscribe`, so a `&[T]` body on that form asks for
`BatchSubscriber` on `Subscribe::Subscriber`.

A crate that wired the capability onto its descriptor's subscriber alone leaves the string-literal
form failing to compile. Where the two are the same type there is nothing to do, and where they
differ both need it.

Where the transport delivers one message at a time, implement the capability anyway and assemble
the batches on the client with the core's `BufferedSubscriber`, whose `batches` honours the size it
is given. The size is not yours to choose; the deadline that closes a partial batch is, and it need
not be a constant.

Expose that deadline on your subscription descriptor (`.max_wait(Duration::from_millis(25))`) and
hand it to the wrapper as the subscription opens, so a service can tune it per subscription. The
10 ms default is sized for an in-process bus: once a network round trip is in the way it closes
most batches at a single delivery, so the broker crates that ship the deadline as a descriptor
option settle between 10 and 50 ms.

Everything else about the subscriber passes through the wrapper unchanged:

```rust
--8<-- "tests/batch_subscriber.rs:buffered_capability"
```

Nothing in the mount site says which of the two you did: a service names the batch size and gets
batches.

Declining the capability is still a legitimate answer where batching would break a guarantee the
transport carries. A ZeroMQ ROUTER is the case in practice: it answers each peer at that peer's own
`reply-to`, while a whole batch reaches its reply wiring with one `PublishContext`, so the replies
for the batch would all go to one peer's address. Say so in your crate's docs: a `&[T]` body then
does not compile on that transport.

The `conformance` batch suite checks the contract: it opens a subscription at a size smaller than
the run and fails a broker whose batches come back larger. It is not part of `harness::run_suite`;
capability suites are yours to call, one per capability you implement.

### The prelude your crate ships { #broker-prelude }

Your types are named at the mount site, not in the body, and that is what your crate's prelude is
for. Ship a `prelude` module in three layers, in this order:

1. `pub use ruststream::prelude::*;` so one glob serves the whole file;
2. your own surface a service names: the broker, its subscription source, its `Config`, its error,
   the `ContextField` keys a body reads;
3. your publish policies under the uniform names every broker uses - `Publish`, and where you
   have them `TransactionalPublish` and `Request` (`pub use crate::KafkaTransactionalPublish as
   TransactionalPublish;`). Add the capability traits you implement on your live values as a
   manifest, so the glob that names the policies also puts their operations in scope.

The core prelude exports nothing under those three names, so a mount site reads the same whichever
broker it is on, and swapping brokers swaps the glob. Never alias a policy to a core trait name
(`Publisher`, `TransactionalPublisher`, `OwnedTransactions`, `RequestReply`) or re-export something
else under one: a body that globs both preludes has to keep resolving those to the core traits.

The manifest is what your glob adds: the consumer-side traits a body reaches through your broker,
`Positioned`, `Seeker`, `Transaction` and the like. The four publisher capabilities are already in
the core prelude, so re-exporting them changes nothing.

Leave out a trait whose method would collide with a defaulted core method, in practice
`Partitioned::partition_key` against `IncomingMessage::partition_key`, and let a service that needs
it import it explicitly. `BatchSubscriber` belongs in no manifest at all: the framework calls it,
and no body ever writes it as a bound.

`ruststream::memory::prelude` is the worked example.

### Extending the `Out` slot vocabulary

An `Out<impl X, Marker>` handler parameter accepts any `X` the live value behind the slot
implements; on top of that the core delegates its own capability set (`Publisher`,
`TransactionalPublisher`, `OwnedTransactions`, `RequestReply`). When your live value offers more
than that, or is not a publisher at all (a per-partition producer cache, a shard router), declare
your own capability trait and implement it for the live value.

What the body holds is not that value but the arena entry, `Slot<Marker, W, E, Pipe, Body>`, a
transparent window onto it. Autoderef carries a method call through the window, but not a trait
bound: a helper written as `fn issue<L: Lanes>(lanes: &L)` rejects the entry with `E0277`.

Add one blanket impl next to your trait, `impl<M, W: Lanes, E, Pipe, Body> Lanes for Slot<M, W, E,
Pipe, Body>` delegating through the entry's `Deref`, and helpers and bodies generic over the
capability take the entry as it is. The concrete type still never appears in application code:

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
producer cache above picks the publisher for a shard and returns it. A publish through that
publisher passes outside the slot view, so the harness does not attribute it to the slot, no more
than it does a settled owned transaction's buffer. Assert it on the broker's publish log instead.
That is the attribution boundary and the price of handing out the inner publisher.

A **step-shaped** capability sets one argument on a message and ends in a single publish: an
ordering key, a priority, a QoS. That one is not a capability trait at all - it is a field of your
`Publisher::Options` and a step on the publish builder, which keeps the send on the entry's own
path and the setting out of the header map. See
[per-message settings on the publish builder](#per-message-settings-on-the-publish-builder).

### Your crate's prelude

Two files import different things, and the split is what keeps a service portable. A handler body
imports `ruststream::prelude::*` and nothing of yours: it bounds an injected slot with the core
capability trait it needs (`Out<impl Publisher>`, `Out<impl TransactionalPublisher>`,
`Out<impl OwnedTransactions>`, `Out<impl RequestReply>`), so the body says what it needs of a
publisher and never which broker provides it.

A routes file imports your prelude, because mounting is where a broker is named.

The one exception is a per-message setting. It is broker-specific by nature and the call site is in
the body, so a body that adjusts one imports your prelude for the step and names your options type
in its bound (`Out<impl Publisher<Options = MqttOptions>, Telemetry>`). That body is tied to your
broker, and it says so in its signature.

That makes your prelude the one import of yours a service writes, so its shape is part of the
contract. The policy aliases (`NatsPublish as Publish`, `KafkaTransactionalPublish as
TransactionalPublish`, `LapinRequest as Request`) make a routes file read the same whichever broker
it mounts, so switching brokers is a change of import.

Your half of the naming rule: an explicit re-export shadows a glob without a word, so a name you
spell like a core trait takes that trait away from every service writing the glob, and the error
surfaces in the service's file rather than in yours.

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
key types. A key binds a single field as a handler parameter through the
[`Ctx<K>` extractor](../guides/context.md#per-delivery-context). Keys are unit structs, and the
delivery path carries no type-map and no heap allocation.

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

The sketch reads a `Copy` scalar, where owning and borrowing are the same thing. A position that is
not `Copy`, a Pulsar message id or a Kinesis shard plus its sequence string, is read by borrowing:
`Field::Value<'a>` is generic over the source's lifetime, so the key hands back `&'a MessageId` and
a body reading it with `ctx.context(..)` copies nothing.

Only `ContextField::Value`, the value behind the `Ctx<K>` extractor, has to be owned and `'static`,
because extractor values bind before the body runs; that key clones what the borrowing one returns.
A key usually implements both traits, one shape each.

A broker with no per-delivery fields uses `()`.

Batch subscriptions get a context of their own, because a batch spans many deliveries. Build a
second struct out of what the whole *subscription* shares (a seek handle, a stream name, a consumer
group), implement `BuildBatchContext` on it, and publish `Field` keys so a batch body reads it with
`ctx.context(..)`. The runtime builds one value per batch from the batch's first delivery.

Per-delivery fields stay out of it: a position belongs to one delivery, so a batch reads it off the
elements. Keeping the two structs apart is what makes that a compile-time rule, since a
per-delivery context does not implement `BuildBatchContext` and a batch body therefore cannot name
it.

The in-memory broker's `MemoryBatchContext` is the model: the subscription's seeker sits under the
same `SeekHandle` key its per-delivery context publishes. A broker with nothing subscription-scoped
to offer implements nothing and leaves batches on the `()` default.

## Middleware on the async edges { #middleware-on-the-async-edges }

Integrations that need async I/O around encode and decode (a schema registry, a wire-format
envelope) do not belong in a `Codec`: the core codec is synchronous and handlers should stay on the
default one.

Put them on the async edges instead. Transcode incoming payloads on the subscription's delivery
path, before the codec sees them, and frame outgoing ones with a core `PublishLayer` added app-wide
via `RustStream::publish_layer`. The publish layer is async and can return an error, and
`Outgoing::payload_mut` exists exactly for envelope wrapping.

## Config and defaults

Your crate owns its `Config`: the core carries no broker-specific config. If a field has no sane
default, do not implement `Default`. The user then sets the value explicitly instead of inheriting
a default that breaks later.

## Errors

Use `thiserror` and one crate-level error enum, with variants by source. Mark public error enums
`#[non_exhaustive]`. Never use `anyhow` in a library crate.

## Test support

Ship an in-process transport implementing `TestableBroker` on its **connected form** under a
`testing` feature. Register it with `register_testable_broker!` for that connected type: the
harness connects every broker before recovering its transport. Users can then unit-test handlers
against your broker with the `TestApp` harness.

The transport does **core routing only**: it dispatches published messages to matching subscribers,
and it answers `ack` and `nack` the way the real transport answers. Where the real transport
acknowledges, the stand-in answers in memory: `nack(requeue = true)` puts the delivery back. Where
it cannot acknowledge at all (ZeroMQ, MQTT `QoS 0`, Redis pub/sub), the answer stays
`AckError::Unsupported`. A stand-in that claims a settlement its transport never performs is what
makes a handler's retry pass in a test and lose the message in production.

Do not simulate broker-specific semantics (durable cursors, redelivery timers, offsets,
dead-letter routing) in it; those are verified end to end against a real server.

The reference is the in-memory broker's own implementation (on `ConnectedMemoryBroker`):

```rust
--8<-- "src/memory/mod.rs:testable"
```

The transport calls `Coordinator::enqueued` on every enqueue into a subscriber and
`Coordinator::consumed` when a delivery is settled or dropped, so the harness can tell when the
reaction has settled. It routes delayed redeliveries through `Coordinator::schedule_redelivery`.

That one type works with both `TestApp` and the conformance suite. See
[Testing](../guides/testing.md) for the user-facing side, and [Conformance](conformance.md) to
prove the implementation with `run_suite` and the `lifecycle` ladder check.

### Writing one you can trust

A stand-in is the type a service's whole test suite runs against, so every difference between it
and the real transport is a green test for behaviour production does not have. The differences that
matter are not exotic ones, and each rule below costs about one test.

**Run the core's contract suites against the stand-in, not only against a server.** The suites are
written against the traits and do not care whether a real broker or the stand-in answers them. One
`#[tokio::test]` is enough:

```rust
--8<-- "tests/conformance_self.rs:run_suite"
```

Run `lifecycle` first. It walks `new` -> `connect` -> subscribe -> publish -> ack -> `shutdown` and
then asks what a stand-in almost never gets asked: does a publisher created before the shutdown
return an error afterwards? A real client answers "not connected". A stand-in whose publish is a
channel send has no reason to, and accepts the message instead.

```rust
--8<-- "tests/conformance_self.rs:lifecycle"
```

Add the `capabilities::*` suites the same way, one for each capability you implement.

**Offer the capability surface the real broker offers.** The `testing` feature is for tests, and a
release build turns it off, which is what makes the two directions unequal. Falling short is the
expensive direction: a capability the real broker has and the stand-in lacks cannot be mounted in
process at all, so the behaviour behind it goes untested. Going over is the cheap one: a
transaction or a request-reply that only the stand-in offers does not compile in your own release
build, which is annoying and caught at once.

**Settle the way the transport settles.** The real `ack` returns `AckError::Unsupported` where the
transport does not acknowledge: a fire-and-forget transport, an at-most-once quality of service.
The stand-in returns the same. Answering `Ok(())` to keep a suite quiet is how a handler returning
`HandlerOutcome::retry()` passes in process and loses the message in production. The suites accept
the honest answer.

**Reproduce what the client does; do not fake what the broker does.** The split is not about
effort, it is about which side the behaviour runs on. Competing consumers, group distribution,
correlation and reply routing, and buffering until commit are client-side or routing-level, and an
in-process copy of them is exact. Cluster atomicity, fencing, broker-held timeouts and
exactly-once are broker-side, and an in-process copy of them is fiction.

Competing consumers is the one to get right, because getting it wrong looks like success. Handing
every message of a queue to every subscriber of that queue is a fan-out, not a queue. Two workers
sharing one queue then each run the whole stream, and a test that counts what was processed sees
the work done and reports no error.

**Give every gap a comment naming the assertion it makes unsound.** Do not write that the feature
is missing; write which test a reader may no longer trust, and what covers it instead:

<!-- inline-rust: the shape of a gap comment, not code - the in-memory broker has no transactional id to be fenced on -->
```rust
// No fencing: a second producer claiming the same transactional id is not rejected here, so a
// test cannot assert the first one is fenced out. `capabilities::transactions` against a real
// server is what covers that.
```

**Pin the gap with a test as well.** A comment goes stale the first time someone "fixes" the
stand-in to route what it deliberately does not route. A test asserting the handler is *not*
reached fails that day and explains itself.

**Mount the stand-in with the production wiring.** Your own subscription sources and publish
policies have to work against it unchanged, so a service tests the routes file it ships. If a user
must swap `OrdersStream` for something else to get a test running, the test no longer covers the
mount.
