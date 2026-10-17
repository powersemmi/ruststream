# The main impls

The shapes of the impls a broker crate writes, against the core's 0.7 signatures. `MyBroker`, `MyOptions` and `Client` stand for the crate's own types.

## The lifecycle ladder

`new` is synchronous; `connect` consumes the broker and yields a distinct connected type.

```rust
use ruststream::{Broker, ConnectedBroker};

pub struct MyBroker {
    addrs: String,
}

impl MyBroker {
    /// Records the address; opens the connection later in `Broker::connect`.
    #[must_use]
    pub fn new(addrs: impl Into<String>) -> Self {
        Self { addrs: addrs.into() }
    }
}

/// The connected form owns the live client - no "maybe connected" check anywhere.
pub struct ConnectedMyBroker {
    client: Client,
}

impl Broker for MyBroker {
    type Error = MyError;
    type Connected = ConnectedMyBroker;

    async fn connect(self) -> Result<ConnectedMyBroker, MyError> {
        let client = Client::connect(&self.addrs).await.map_err(MyError::connect)?;
        Ok(ConnectedMyBroker { client })
    }
}

impl ConnectedBroker for ConnectedMyBroker {
    type Error = MyError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), MyError> {
        self.client.drain().await.map_err(MyError::shutdown)
    }
}
```

## Publisher: policy plus live form

The policy is pure declaration and constructible anywhere; `pair` turns it into the live handle
once, at startup.

```rust
use ruststream::{DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher};

/// The broker's per-message settings: every field optional, so a call carries only what it
/// adjusted. Derive `Debug` and `PartialEq` too, and a test asserts `with_options(&MyOptions { .. })`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MyOptions {
    priority: Option<u8>,
}

/// The publish policy: builder options only, no connection, no publish surface. This is where the
/// per-message defaults are configured, at the mount site.
#[derive(Debug, Clone, Default)]
pub struct MyPublish {
    exchange: Option<String>,
    priority: u8,
}

impl PublishPolicy<ConnectedMyBroker> for MyPublish {
    type Live = MyPublisher;

    async fn pair(self, connected: &ConnectedMyBroker) -> Result<MyPublisher, PairError> {
        Ok(MyPublisher {
            client: connected.client.clone(),
            exchange: self.exchange,
            default_priority: self.priority,
        })
    }
}

/// The plain policy works with its defaults, so the runtime can build a default reply publisher.
impl DefaultPublish for ConnectedMyBroker {
    type Policy = MyPublish;
}

impl Publisher for MyPublisher {
    /// The client writes the bytes into a frame of its own and keeps nothing, so this publisher
    /// is lent them; a client that keeps the payload declares `Take` and is handed a `BytesMut`.
    type Payload = Lend;
    type Error = MyError;
    type Options = MyOptions;

    /// Resolving the call's fields over the policy's is the first thing publish does.
    async fn publish(
        &self,
        msg: OutgoingMessage<'_, &[u8]>,
        options: Option<&MyOptions>,
    ) -> Result<(), MyError> {
        let priority = options
            .and_then(|options| options.priority)
            .unwrap_or(self.default_priority);
        self.client
            .send(msg.name(), msg.payload(), priority)
            .await
            .map_err(MyError::publish)
    }
}
```

`Publisher::Payload` is the declaration everything else follows. Declare `Take` when your client
keeps the payload past the call and you are handed the `BytesMut` the framework wrote
(`Vec::from` is free, `freeze()` costs one block); declare `Lend` when your transport only reads
the bytes, and inside a dispatch loop the framework lends you one buffer per loop, so a reply
through your broker allocates nothing per delivery.

Read the payload with `msg.payload()` and the headers with `msg.headers()` whichever form you
declared. A transport that consumes the message takes its parts with `msg.into_parts()` - the
destination, the payload and the header map in one move, nothing copied.

## Deliveries

The subscriber is a stream of deliveries; one delivery is an `IncomingMessage`, settled once.

```rust
use futures::{Stream, StreamExt};
use ruststream::{AckError, HeaderMap, IncomingMessage, Subscriber};

impl Subscriber for MySubscriber {
    type Message = MyDelivery;
    type Error = MyError;

    fn stream(&mut self) -> impl Stream<Item = Result<MyDelivery, MyError>> + Send + '_ {
        (&mut self.consumer).map(|delivery| Ok(MyDelivery::from(delivery)))
    }
}

impl IncomingMessage for MyDelivery {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The transport counts its own deliveries: the first one answers 1.
    fn redelivery_count(&self) -> Option<u64> {
        Some(u64::from(self.inner.attempt()))
    }

    async fn ack(self) -> Result<(), AckError> {
        self.inner.ack().await.map_err(|e| AckError::Broker(Box::new(e)))
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.inner.nack(requeue).await.map_err(|e| AckError::Broker(Box::new(e)))
    }
}
```

A transport with no acknowledgement answers `Err(AckError::Unsupported)` from both.

## Batches on a transport that delivers one at a time

The public subscriber holds the wire subscriber inside a `BufferedSubscriber` and forwards both
streams to it. The size is the registration's `batch(n)`; the deadline that closes a partial batch
is the crate's own, set from the descriptor.

```rust
use std::num::NonZeroUsize;

use futures::Stream;
use ruststream::{BatchSubscriber, BufferedSubscriber, Subscriber};

pub struct MySubscriber {
    buffered: BufferedSubscriber<WireSubscriber>,
}

impl MySubscriber {
    fn new(wire: WireSubscriber, max_wait: Duration) -> Self {
        Self { buffered: BufferedSubscriber::new(wire).max_wait(max_wait) }
    }
}

impl Subscriber for MySubscriber {
    type Message = MyDelivery;
    type Error = MyError;

    fn stream(&mut self) -> impl Stream<Item = Result<MyDelivery, MyError>> + Send + '_ {
        self.buffered.stream()
    }
}

impl BatchSubscriber for MySubscriber {
    type Batch = Vec<MyDelivery>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, MyError>> + Send + '_ {
        self.buffered.batches(size)
    }
}
```

`Subscribe::Subscriber` is this same type, so a `&[T]` body works on `#[subscriber("name")]` too.
`conformance::capabilities::batches` checks the capability.

## Per-message settings on the publish builder

A call site adjusts one field through a step you add to the publish builder. Nothing wraps the
publisher, so the publish still goes through the mount site's own entry, with the codec and the
transforms that entry named. The bound on the sink's options type is what keeps your steps off a
builder over another broker's publisher. Ship the trait from your prelude, next to the policy
aliases.

```rust
use ruststream::runtime::{PublishBuilder, PublishSink};

pub trait MyPublishSteps {
    /// Sends this one message at `priority`, whatever the mount site's default is.
    #[must_use]
    fn priority(self, priority: u8) -> Self;
}

impl<Sink, Body, Enc, Hdrs, Dest> MyPublishSteps for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
where
    Sink: PublishSink<Options = MyOptions>,
{
    fn priority(mut self, priority: u8) -> Self {
        self.options_mut()
            .get_or_insert_with(MyOptions::default)
            .priority = Some(priority);
        self
    }
}
```

A step is the only shape a per-message setting takes. Do not put the send in a trait of your own: a
publish that leaves through a value of yours is one the test harness no longer attributes to the
slot, and a setting like an ordering key is exactly what a test wants to assert on. Do not carry
one as a header either - it is a protocol field, not bytes your `publish` parses back inside one
process.

## Subscription source

`Subscribe` on the connected form covers `#[subscriber("topic")]`; a descriptor carries richer
options and resolves against the connected form at startup.

```rust
use ruststream::{
    AddressedCopies, FromName, RedeliveryAddress, RedeliveryAddressed, Subscribe,
    SubscriptionSource,
};

impl Subscribe for ConnectedMyBroker {
    type Subscriber = MySubscriber;
    /// A publish under a subscribe name reaches the subscription opened by it, so the name is
    /// the address retry copies go to.
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<MySubscriber, MyError> {
        self.open(MySubscribeOptions::new(name)).await
    }
}

impl SubscriptionSource<ConnectedMyBroker> for MySubscribeOptions {
    type Subscriber = MySubscriber;
    /// This process publishes the copies a retry of this subscription is made of, and this
    /// descriptor knows where they go.
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        &self.subject
    }

    async fn subscribe(self, connected: &ConnectedMyBroker) -> Result<MySubscriber, MyError> {
        connected.open(self).await
    }
}

/// Where a publisher reaches this subscription again. The runtime asks once, at startup, so a
/// descriptor may ask the live connection (a Pub/Sub subscription's topic).
impl RedeliveryAddressed<ConnectedMyBroker> for MySubscribeOptions {
    async fn redelivery_address(
        &self,
        _connected: &ConnectedMyBroker,
    ) -> Result<RedeliveryAddress, MyError> {
        Ok(RedeliveryAddress::new(self.subject.clone()))
    }
}

/// A kind identified by a name and nothing else; makes `#[subscriber(MySubscribeOptions)]` legal.
impl FromName for MySubscribeOptions {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name)
    }
}
```

Users then write the descriptor straight into the attribute, builder chain and all:
`#[subscriber(MySubscribeOptions::new("orders.*").durable("worker"))]`.

## Settings in your own vocabulary

Core exposes one hook, `map_source`, a transform over the source the mount site is building, and
your crate layers its own extension trait on top, bound to your source type, so the methods do not
exist on a builder for another broker.

```rust
use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings};

pub trait MySubscriberSteps {
    type Out;
    fn durable(self, name: impl Into<String>) -> Self::Out;
}

// `State` carries the core settings fixed so far; `DefCodec` is the registration's own decode
// codec, `()` until one is named.
impl<Def: Declared, State, DefCodec> MySubscriberSteps
    for SubscriberBuilder<Def, MySubscribeOptions, State, DefCodec>
{
    type Out = SubscriberBuilder<Def, MySubscribeOptions, State, DefCodec>;

    fn durable(self, name: impl Into<String>) -> Self::Out {
        self.map_source(|source| source.durable(name))
    }
}
```

The [`map_source` example on docs.rs](https://docs.rs/ruststream/latest/ruststream/runtime/trait.SubscriberSettings.html#method.map_source) is the compiled form of this shape.

A step that depends on a core setting reads it off the builder rather than taking it again:
`dispatch()` (a `Workers` with `count()`, `by_key()`, `placement()`), `batch_size()` (`None` until
`batch(n)`), `subscription_name()`. It sees each value as fixed when the step is called: the
attribute's settings always, a mount-site one only when chained before the step.

One core setting changes the source type rather than a state slot: `start_at(..)` wraps your
descriptor in `StartAt<MySubscribeOptions, Position>`, and your methods fall out of scope on every
subscription that named a start position. Cover it with a second impl over the wrapped source,
where the position slot reads `Fixed` by construction, so the two never overlap;
`StartAt::map_inner` takes the descriptor out and hands the position back untouched, so each method
stays one line.

The publish side mirrors it. A mount site names a publish policy with `.out(marker, policy)` -
`Reply` for what a `reply("dest")` handler returns, an `Out` slot's marker for a slot - and
`MapPublisher` is the hook over the policy that position carries. Bind the extension trait to the
policy, not to the chain, so one impl covers the reply, every slot, a router and a broker scope:

```rust
pub trait MyPublishSettings {
    fn ack_timeout(self, timeout: Duration) -> Self;
}

impl<T: MapPublisher<Policy = MyPublish>> MyPublishSettings for T {
    fn ack_timeout(self, timeout: Duration) -> Self {
        self.map_publisher(|policy| policy.ack_timeout(timeout))
    }
}
```

A policy setting is a property of the handle, never the destination: where a message goes is the
message type's declaration or the call site's `.to(..)`, so a policy method that names a subject, a
topic or a stream puts a second source of truth next to the one the document reports.

`map_publisher` replaces the policy with one of the same type, which is what a publisher's own
settings produce; a different policy type is a different publish mode and belongs in the
`.out(marker, policy)` call itself, where an already-configured value can be passed directly
(`.out_reply(Publish::default().ack_timeout(Duration::from_secs(5)))`).

## Extending the `Out` slot vocabulary

A body's `Out<impl X, Marker>` accepts any `X` the live value behind the slot implements, so a live
value that is more than a publisher (a per-partition producer cache, a shard router) carries a
capability trait of your own. What the body holds is not that value but the arena entry,
`Slot<M, W, E, Pipe, Body>`, a transparent window onto it: autoderef carries a method call through,
a trait bound does not. Graft one blanket impl next to your trait and helpers generic over the
capability take the entry as it is:

```rust
impl<M, W: Lanes, E, Pipe, Body> Lanes for Slot<M, W, E, Pipe, Body> {
    fn lane(&self, shard: u64) -> (&MyPublisher, &'static str) {
        (**self).lane(shard)
    }
}
```

A capability that hands out a publisher and lets the body send through it publishes outside the
slot view, so the harness attributes nothing to the slot and a test asserts on the broker's publish
log instead. That is the price of handing out the inner publisher. A capability that only sets one
per-message setting and ends in a single publish is not a capability trait at all - it is a field
of `Publisher::Options` and a step on the builder.

## Typed context and `Ctx` keys

The broker names its context type on the subscriber; `ContextField` keys make individual fields
injectable as handler parameters.

```rust
use ruststream::ContextField;

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

Batch subscriptions take a second, separate struct: what the whole *subscription* shares (a seek
handle, a stream name), with `BuildBatchContext` on it - the runtime builds one per batch from the
batch's first delivery, or from the batch itself on the carried lane - plus `Field` keys a batch
body reads with `ctx.context(..)`. Per-delivery fields stay off it (a batch spans many deliveries,
so a position rides the elements instead), and keeping the two structs apart is what enforces
that: a per-delivery context does not implement `BuildBatchContext`, so a batch body cannot name
it. Nothing subscription-scoped to offer means no impl at all - `()` is the default.

## Conformance

The suites are written against the traits, so they run unchanged in process (the broker's
`InProcess` transition, whose connected form is the `TestableBroker`) and against a real server
behind an env gate. Run both: the in-process mode is what a service's whole test suite rides on,
and the server is what the release runs against. `run_suite` covers the routing contract - ordering, ack and nack, headers,
the publish log, the `routes` answer and the harness counts; the `capabilities::*` suites are yours
to call, one per capability you implement, and they are not part of `run_suite`.
`settlement::matches_in_process` holds ack, nack, out-of-order settlement and an unsettled drop to
their meaning live and in process, and fails when the two answer differently; `lifecycle` holds a
`nack_after` delay as a floor. `conformance::in_process::{backlog_matches_server,
refuses_like_the_server}` hold the in-process transport's backlog declaration and its refusals to
the real server; run them with the live suites.

```rust
use ruststream::conformance::harness;

// sync Fn() -> B, where B: InProcess and B::Connected: Subscribe; the production broker, connected
// through connect_in_process
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routing_conformance() {
    harness::run_suite(|| MyBroker::new("my://localhost")).await;
}

// the ladder: sync new -> connect(self) -> subscribe via SubscriptionSource -> publish ->
// receive -> ack (accepting AckError::Unsupported) -> shutdown(self), then a publisher made
// before shutdown must error after it, and a publish to the address the subscription reported
// must arrive at that subscription
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_conformance() {
    let Ok(url) = std::env::var("MY_BROKER_TEST_URL") else { return };
    // the closures stay closures: their bounds are higher-ranked, so a bare method path
    // would bind one concrete lifetime and fail to type-check
    harness::lifecycle(
        || MyBroker::new(url.clone()),
        |name| MySubscribeOptions::new(name),
        |connected| connected.publisher(),
    )
    .await;
}
```

Run the live suites against a real server before a release: an in-process pass is not enough.
