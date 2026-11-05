//! The [`Broker`] / [`ConnectedBroker`] ladder: the entry point of any broker implementation.

use core::error::Error as StdError;

#[cfg(feature = "std")]
use crate::bindable::Bindable;

/// An unconnected broker: configuration captured, no I/O performed yet.
///
/// `Broker` is where a broker crate starts, and this page is the route through one: twelve steps
/// from an empty crate to a broker that passes conformance. Each step names what to implement, the
/// rule it keeps, and where a working crate does it: [`ruststream-nats`][nats] over a real client,
/// and the in-memory broker of [`ruststream-memory`][memory], the reference implementation of every
/// trait on the route.
///
/// The lifecycle is a ladder of consuming transitions, so each state is a type of its own:
///
/// ```text
/// B::new(config)                      unconnected: sync, I/O-free construction
/// broker.connect(self)  -> Connected  the live connection, a typed witness
/// connected.shutdown(self) -> Closed  the terminal witness (may carry diagnostics)
/// ```
///
/// # 1. The crate and its dependencies
///
/// A broker crate depends on the facade with the default features off and the tokio runner on. Its
/// in-process mode ships under a `testing` feature, and the conformance suites are a
/// dev-dependency:
///
/// ```toml
/// [dependencies]
/// ruststream = { version = "0.8", default-features = false, features = ["tokio"] }
///
/// [features]
/// testing = ["ruststream/testing"]
///
/// [dev-dependencies]
/// ruststream = { version = "0.8", features = ["conformance"] }
/// ```
///
/// The crate owns its `Config`, and a field with no sane default has no `Default`. Its errors are
/// one `thiserror` enum with variants by source, marked `#[non_exhaustive]`. Every error type of
/// the contract is `Send + Sync + 'static`, so the crate maps its client's error into its own.
///
/// Example: the [`ruststream-nats` manifest][nats-manifest].
///
/// # 2. The lifecycle
///
/// The broker's `new(config)` is synchronous and only records the configuration. The service's
/// synchronous `#[ruststream::app]` builder calls it, so it performs no I/O.
/// [`connect`](Self::connect) does the network setup, once, at startup.
/// [`ConnectedBroker::shutdown`] runs every teardown step that can fail and returns a
/// [`Closed`](ConnectedBroker::Closed) witness.
///
/// Each transition consumes its state, so a subscribe or a publish after shutdown does not compile
/// for the owner of the handle. A handle that aliases the connection (a publisher paired earlier,
/// a clone of a shareable connected form) returns an error after shutdown, never a silent success.
/// A task the broker starts for itself runs on the runtime `connect` ran on.
///
/// Example: `NatsBroker` and `ConnectedNatsBroker` in [`broker.rs`][nats-broker];
/// `MemoryBroker` in [`broker.rs`][memory-broker]. The skeleton is under
/// [Examples](#examples).
///
/// # 3. Subscriptions
///
/// [`Subscribe`](crate::Subscribe) on the connected form opens a subscription a name alone
/// identifies; `#[subscriber("orders")]` reaches it. A
/// [`SubscriptionSource`](crate::SubscriptionSource) descriptor carries the broker's own settings
/// (a stream, a consumer group) and resolves once, at startup, against the connected form. Every
/// broker ships one, even a broker with no settings. Both open a
/// [`Subscriber`](crate::Subscriber), the stream of deliveries.
///
/// Both declare `Copies`, who publishes the copies a retry is made of:
/// [`AddressedCopies`](crate::AddressedCopies) when this process publishes them to the address
/// the descriptor reports through [`RedeliveryAddressed`](crate::RedeliveryAddressed),
/// [`NamedCopies`](crate::NamedCopies) when the mount site names the destination, and
/// [`BrokerMoves`](crate::BrokerMoves) when the server or the client moves the delivery itself.
///
/// Example: `CoreSubject` and `JetStreamSubject` in [`subject.rs`][nats-subject];
/// `MemorySource` in [`subscriber.rs`][memory-subscriber].
///
/// # 4. The delivery
///
/// A delivery implements two traits. [`IncomingMessage`](crate::IncomingMessage) reads what it
/// carries: the payload, the headers, and where the transport knows them, the partition key, the
/// delivery count and a decode error. [`Settle`](crate::Settle) settles it: `ack(self)` and
/// `nack(self, requeue)` consume the delivery, so a second settlement does not compile.
///
/// A transport with no acknowledgement returns
/// [`AckError::Unsupported`](crate::AckError::Unsupported), and never claims a settlement it does
/// not perform. A delivery that offers a native delay overrides `nack_after`; without it, the
/// runtime publishes the delayed copy itself.
///
/// Example: `NatsMessage` in [`message.rs`][nats-message]; `MemoryMessage` in
/// [`subscriber.rs`][memory-subscriber].
///
/// # 5. Publishing
///
/// A [`PublishPolicy`](crate::PublishPolicy) is a declaration that holds no connection, so a
/// service builds it anywhere. Its [`pair`](crate::PublishPolicy::pair) constructs the live
/// [`Publisher`](crate::Publisher) from the connected form at startup. The publisher declares:
///
/// - [`Payload`](crate::Publisher::Payload): [`Lend`](crate::Lend) when the client only reads the
///   bytes, [`Take`](crate::Take) when it keeps them. A lending publisher is lent the dispatch
///   loop's buffer, so a reply allocates nothing; declare `Lend` unless the client keeps the
///   buffer.
/// - [`Options`](crate::Publisher::Options): the per-message settings of the broker's own, every
///   field optional, or `()`. A broker's publish-builder steps write them through an extension
///   trait over [`PublishBuilder`][publish-builder]. A setting never rides a header.
/// - [`publish`](crate::Publisher::publish), which returns a
///   [`PublishOutcome`](crate::PublishOutcome): on failure, the broker's error and the message it
///   did not send, handed back with
///   [`PublishMessageError::with_message`](crate::PublishMessageError::with_message).
///
/// [`DefaultPublish`](crate::DefaultPublish) on the connected form names the policy a reply and a
/// retry copy use when the mount site names none.
///
/// Example: `NatsPublish` and `NatsPublisher` in [`publisher.rs`][nats-publisher],
/// `JetStreamPublish` and its `JetStreamPublishSteps` in [`jetstream.rs`][nats-jetstream];
/// `MemoryPublish` and `MemoryPublisher` in [`broker.rs`][memory-broker].
///
/// # 6. Batches
///
/// [`BatchSubscriber`](crate::BatchSubscriber) delivers batches; offer it wherever you can. The
/// runtime owns one [`BatchSlot`](crate::BatchSlot) per worker and lends it to the
/// [`BatchStream`](crate::BatchStream), whose [`poll_fill`](crate::BatchStream::poll_fill) fills
/// it with at most the batch size. The runtime then takes the batch out with
/// [`hand_over`](crate::BatchSlot::hand_over). A `Vec` of the broker's deliveries is a slot, and it
/// hands a batch over by trading storage, so no delivery moves. A transport that delivers one
/// message at a time wraps its subscriber in [`Buffered`][buffered].
///
/// Example: `NatsSubscriber` in [`subscriber.rs`][nats-subscriber] and its fills in
/// [`subscriber/fills.rs`][nats-fills]; `MemorySubscriber` in [`capability.rs`][memory-capability].
///
/// # 7. Capabilities
///
/// A capability trait is implemented only where the transport supports it, and each has its own
/// suite in [`conformance::capabilities`][conformance-capabilities]:
///
/// | Trait | For a broker that supports |
/// |---|---|
/// | [`BatchSubscriber`](crate::BatchSubscriber) | receiving messages in batches (step 6) |
/// | [`TransactionalPublisher`](crate::TransactionalPublisher) | begin, commit and abort around publishes on the publisher handle |
/// | [`OwnedTransactions`](crate::OwnedTransactions) / [`Transaction`](crate::Transaction) | many transactions open at once per handle, each with its own buffer |
/// | [`RequestReply`](crate::RequestReply) | native request-reply |
/// | [`Partitioned`](crate::Partitioned) | a partition key on outgoing messages |
/// | [`Seekable`](crate::Seekable) / [`Seeker`](crate::Seeker) | repositioning a live subscription in a replayable log |
/// | [`Positioned`](crate::Positioned) | reporting a delivery's own position in the log |
/// | [`Carries`](crate::Carries) / [`CarriesBatch`](crate::CarriesBatch) | lending the decoded value a delivery already holds |
///
/// A handler body bounds its `Out` slot with the capability it needs (`Out<impl
/// TransactionalPublisher>`), never with a broker's type. Implementing the trait on the live
/// publisher is all a service needs to reach it. A live value that offers more than the core's
/// capabilities extends the vocabulary as [`OutSlot`][out-slot] describes. A body that seeks or
/// reads a broker's field takes it from the per-delivery context, through
/// [`ContextField`](crate::ContextField) keys of the crate's own.
///
/// Example: `RequestReply` in [`request_reply.rs`][nats-request-reply]; the transactions and
/// seeking of `MemoryPublisher` and `MemorySubscriber` in [`capability/`][memory-capabilities].
///
/// # 8. The `AsyncAPI` description
///
/// [`DescribeServer`](crate::DescribeServer) on the broker reports a
/// [`ServerSpec`](crate::ServerSpec): the host and port clients connect to, never credentials.
/// Build it with [`ServerSpec::from_url`](crate::ServerSpec::from_url), or with
/// [`ServerSpec::host_from_url`](crate::ServerSpec::host_from_url) for each of several
/// addresses. Under the crate's `asyncapi` feature, the descriptor and the policies add protocol
/// bindings to the document through their `*_bindings` methods, which return
/// [`Bindings`](crate::bindings::Bindings).
///
/// Example: `DescribeServer for NatsBroker` in [`broker.rs`][nats-broker].
///
/// # 9. The prelude and the mount-site steps
///
/// The crate ships a `prelude`: the core prelude, its own surface, and its policies under the names
/// every broker uses (`Publish`, `TransactionalPublish`, `Request`), as the facade's
/// [prelude][prelude] describes. A service then swaps brokers by swapping one glob.
///
/// Settings in the broker's own vocabulary are mount-site steps. A step is a trait bounded on
/// [`IntoDeclared<Mk>`][into-declared], so it chains on a `#[subscriber]` handler and on the
/// settings builder alike, and its method transforms the crate's descriptor through
/// [`map_source`][map-source]. A step reads what the chain fixed so far (`dispatch()`,
/// `batch_size()`, `subscription_name()`) instead of asking for it again:
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::Buffered;
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::{AsDeclared, Declared, IntoDeclared, SubscriberBuilder};
/// use serde::Deserialize;
///
/// /// The crate's mount-site steps: batches its client assembles close `wait` after they open.
/// pub trait LingerSettings<Mk = AsDeclared>: IntoDeclared<Mk> {
///     fn linger(self, wait: Duration) -> <<Self::Decl as Declared>::Settings as LingerStep>::Out
///     where
///         <Self::Decl as Declared>::Settings: LingerStep,
///     {
///         self.into_declared().declare().apply_linger(wait)
///     }
/// }
///
/// impl<Def: IntoDeclared<Mk>, Mk> LingerSettings<Mk> for Def {}
///
/// /// The step on a registration that subscribes through the crate's descriptor.
/// pub trait LingerStep {
///     type Out;
///     fn apply_linger(self, wait: Duration) -> Self::Out;
/// }
///
/// impl<Def: Declared, State, DefCodec> LingerStep
///     for SubscriberBuilder<Def, MemorySource, State, DefCodec>
/// {
///     type Out = SubscriberBuilder<Def, Buffered<MemorySource>, State, DefCodec>;
///
///     fn apply_linger(self, wait: Duration) -> Self::Out {
///         self.map_source(|source| Buffered::new(source).max_wait(wait))
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(MemorySource)]
/// async fn settle(orders: &[Order]) -> HandlerOutcome {
///     tracing::info!(count = orders.len(), "settled a batch");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(
///             settle
///                 .name("orders")
///                 .batch(nonzero!(64))
///                 .linger(Duration::from_millis(20)),
///         )
///     })
/// }
/// # }
/// # fn main() {}
/// ```
///
/// The publish-side twin over a policy is [`MapPublisher`][map-publisher].
///
/// Example: the [`ruststream-nats` prelude][nats-prelude].
///
/// # 10. The in-process mode
///
/// Under the `testing` feature, the broker implements [`InProcess`][in-process]: a transition
/// beside `connect` that yields the production connected form over a transport inside the process,
/// with no I/O. The connected form implements [`TestableBroker`][testable-broker], and
/// [`register_testable_broker!`][register] registers the broker, so `TestApp` runs a service's
/// production app on it. The transport reads every setting from the production broker, and never
/// succeeds where the real broker fails.
///
/// Example: `InProcess for NatsBroker` in [`broker.rs`][nats-broker] and its transport in
/// [`in_process/`][nats-in-process]; `MemoryBroker`, whose in-process transition is its own
/// `connect`, in [`broker.rs`][memory-broker].
///
/// # 11. Conformance
///
/// The suites of [`conformance`][conformance] prove the crate; each panics on the first
/// violation, naming the step:
///
/// - [`harness::run_suite`][run-suite]: the routing contract of the in-process mode, from a
///   factory of production brokers.
/// - [`harness::lifecycle`][lifecycle]: the ladder end to end, against a real server and in
///   process through [`harness::InProcessBroker`][in-process-broker].
/// - [`lifecycle::refused_publish_returns_message`][refused]: a refused publish hands the message
///   back.
/// - The checks a crate calls where they apply: the shutdown and shared-handle checks of
///   [`lifecycle`][conformance-lifecycle], [`settlement`][conformance-settlement], the retry
///   address and moves of [`retry`][conformance-retry], keys and per-message settings in
///   [`message_shape`][conformance-message-shape], the credentials check
///   [`harness::describes_without_credentials`][credentials] where the crate has an `asyncapi`
///   feature (it needs the facade's `asyncapi` beside `conformance`), and the suite of each
///   capability.
///
/// The [`conformance`][conformance] overview ends with the author checklist.
///
/// Example: [`conformance_nats.rs`][nats-conformance].
///
/// # 12. Benchmarks and the zero-cost floor
///
/// The delivery and publish paths add no allocation, no `dyn` call, no extra atomic and no copy of
/// a delivery per message. A crate shows it with code-cost benchmarks in a package of its own,
/// built the way it ships (fat LTO) and without `testing`: instructions and allocations per message
/// under callgrind and DHAT, and wall clock on a multi-threaded runtime. The method is the core's,
/// described on the [benchmark methodology][methodology] page.
///
/// Example: the [`ruststream-nats-bench`][nats-bench] package.
///
/// # Brokers whose types are not `Send`
///
/// The lifecycle traits (this one, [`ConnectedBroker`], [`Subscribe`](crate::Subscribe),
/// [`SubscriptionSource`](crate::SubscriptionSource),
/// [`RedeliveryAddressed`](crate::RedeliveryAddressed),
/// [`PublishPolicy`](crate::PublishPolicy) and [`DefaultPublish`](crate::DefaultPublish))
/// run once, while the app starts or stops, and carry no `Send` on their futures. A broker
/// whose client holds `Rc` or `RefCell` (a thread-per-core client, an embedded network
/// stack) implements them as they are, and the embedded runner runs it. The tokio and compio
/// runners ask for `Send + Sync` where they move a broker or spawn a subscription loop
/// ([`SendBroker`]).
///
/// The per-message traits ([`Subscriber`](crate::Subscriber), [`Settle`](crate::Settle),
/// [`Publisher`](crate::Publisher) and the capability traits) are one trait per concept for
/// every runtime. Their futures and supertraits ask for `Send` (and `Sync`) when the runtime
/// that moves work between threads is in the build, and for nothing otherwise: the same
/// `Publisher` is implemented by a broker on the tokio runner and by one on the embedded runner.
/// A build with both takes the stricter bound, so a type that holds `Rc` does not implement the
/// traits next to the tokio or the compio runner, and the compiler says so.
///
/// # Examples
///
/// A broker's lifecycle over a client that dials asynchronously:
///
/// ```
/// use std::io;
///
/// use ruststream::{Broker, ConnectedBroker};
/// # struct Client;
/// # impl Client {
/// #     async fn dial(_url: &str) -> io::Result<Self> {
/// #         Ok(Self)
/// #     }
/// #     async fn flush(&self) -> io::Result<usize> {
/// #         Ok(0)
/// #     }
/// # }
///
/// /// The broker a service builds: the address, nothing dialled yet.
/// pub struct WireBroker {
///     url: String,
/// }
///
/// impl WireBroker {
///     /// Records the address. No I/O.
///     #[must_use]
///     pub fn new(url: impl Into<String>) -> Self {
///         Self { url: url.into() }
///     }
/// }
///
/// /// The live connection.
/// pub struct ConnectedWire {
///     client: Client,
/// }
///
/// /// What the shutdown reports.
/// pub struct ClosedWire {
///     /// The messages the shutdown flushed.
///     pub flushed: usize,
/// }
///
/// impl Broker for WireBroker {
///     type Error = io::Error;
///     type Connected = ConnectedWire;
///
///     async fn connect(self) -> io::Result<ConnectedWire> {
///         let client = Client::dial(&self.url).await?;
///         Ok(ConnectedWire { client })
///     }
/// }
///
/// impl ConnectedBroker for ConnectedWire {
///     type Error = io::Error;
///     type Closed = ClosedWire;
///
///     async fn shutdown(self) -> io::Result<ClosedWire> {
///         let flushed = self.client.flush().await?;
///         Ok(ClosedWire { flushed })
///     }
/// }
/// ```
///
/// [nats]: https://docs.rs/ruststream-nats/latest/ruststream_nats/
/// [memory]: https://docs.rs/ruststream-memory/latest/ruststream_memory/
/// [nats-manifest]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/Cargo.toml
/// [nats-broker]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/src/broker.rs
/// [nats-subject]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/src/subject.rs
/// [nats-message]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/src/message.rs
/// [nats-publisher]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/src/publisher.rs
/// [nats-jetstream]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/src/jetstream.rs
/// [nats-subscriber]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/src/subscriber.rs
/// [nats-fills]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/src/subscriber/fills.rs
/// [nats-request-reply]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/src/request_reply.rs
/// [nats-prelude]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/src/prelude.rs
/// [nats-in-process]: https://github.com/powersemmi/ruststream-nats/tree/main/crates/ruststream-nats/src/in_process
/// [nats-conformance]: https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/tests/conformance_nats.rs
/// [nats-bench]: https://github.com/powersemmi/ruststream-nats/tree/main/crates/ruststream-nats-bench
/// [memory-broker]: https://github.com/powersemmi/ruststream/blob/main/crates/memory/src/broker.rs
/// [memory-subscriber]: https://github.com/powersemmi/ruststream/blob/main/crates/memory/src/subscriber.rs
/// [memory-capability]: https://github.com/powersemmi/ruststream/blob/main/crates/memory/src/capability.rs
/// [memory-capabilities]: https://github.com/powersemmi/ruststream/tree/main/crates/memory/src/capability
/// [publish-builder]: https://docs.rs/ruststream/latest/ruststream/runtime/struct.PublishBuilder.html
/// [buffered]: https://docs.rs/ruststream/latest/ruststream/struct.Buffered.html
/// [out-slot]: https://docs.rs/ruststream/latest/ruststream/runtime/trait.OutSlot.html
/// [prelude]: https://docs.rs/ruststream/latest/ruststream/prelude/index.html#a-broker-crates-prelude
/// [into-declared]: https://docs.rs/ruststream/latest/ruststream/runtime/trait.IntoDeclared.html
/// [map-source]: https://docs.rs/ruststream/latest/ruststream/runtime/trait.SubscriberSettings.html#method.map_source
/// [map-publisher]: https://docs.rs/ruststream/latest/ruststream/runtime/trait.MapPublisher.html
/// [in-process]: https://docs.rs/ruststream/latest/ruststream/testing/trait.InProcess.html
/// [testable-broker]: https://docs.rs/ruststream/latest/ruststream/testing/trait.TestableBroker.html
/// [register]: https://docs.rs/ruststream/latest/ruststream/macro.register_testable_broker.html
/// [conformance]: https://docs.rs/ruststream/latest/ruststream/conformance/index.html
/// [conformance-capabilities]: https://docs.rs/ruststream/latest/ruststream/conformance/capabilities/index.html
/// [conformance-lifecycle]: https://docs.rs/ruststream/latest/ruststream/conformance/lifecycle/index.html
/// [conformance-settlement]: https://docs.rs/ruststream/latest/ruststream/conformance/settlement/index.html
/// [conformance-retry]: https://docs.rs/ruststream/latest/ruststream/conformance/retry/index.html
/// [conformance-message-shape]: https://docs.rs/ruststream/latest/ruststream/conformance/message_shape/index.html
/// [run-suite]: https://docs.rs/ruststream/latest/ruststream/conformance/harness/fn.run_suite.html
/// [lifecycle]: https://docs.rs/ruststream/latest/ruststream/conformance/harness/fn.lifecycle.html
/// [in-process-broker]: https://docs.rs/ruststream/latest/ruststream/conformance/harness/struct.InProcessBroker.html
/// [refused]: https://docs.rs/ruststream/latest/ruststream/conformance/lifecycle/fn.refused_publish_returns_message.html
/// [credentials]: https://docs.rs/ruststream/latest/ruststream/conformance/harness/fn.describes_without_credentials.html
/// [methodology]: https://powersemmi.github.io/ruststream/latest/architecture/benchmarks/methodology/
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a `Broker`",
    label = "not a broker",
    note = "a broker implements `Broker`: `connect` takes the unconnected form to its \
                    connected one"
)]
pub trait Broker: Sized {
    /// The error type returned by broker-level operations.
    type Error: StdError + Send + Sync + 'static;

    /// The connected form of this broker: the typed witness that [`connect`](Self::connect)
    /// succeeded.
    type Connected: ConnectedBroker;

    /// Establishes the connection to the broker, consuming the unconnected form.
    ///
    /// The returned [`Connected`](Self::Connected) value is the only way to reach the
    /// connection-bound surface (subscriptions, and for shareable brokers the live handles), so
    /// "not connected" is not representable on the owner's path.
    ///
    /// The runtime calls it once, at startup, after the synchronous `#[ruststream::app]`
    /// builder has run. All network setup happens here: the broker's constructor only records
    /// the configuration, so a service is assembled the same way whatever its broker. A client
    /// that can only be built by connecting is built here, and its handle lives in the
    /// connected form.
    ///
    /// The connected form may also keep a shared cell that `connect` fills, or shareable
    /// in-process state, as the in-memory broker does: publishers can then be handed out while
    /// the application is still being assembled, and the cell serves those early handles.
    ///
    /// # Internal tasks run on the runtime the broker connected on
    ///
    /// A task the broker starts on its own behalf (a delayed-redelivery timer, a reply
    /// dispatcher, a commit window) runs on the runtime `connect` ran on. The connected form
    /// keeps `tokio::runtime::Handle::current()` from `connect` and spawns every such task
    /// through it, whichever thread publishes, settles or requests. A handler on a dedicated
    /// thread publishes and settles from that thread's own runtime: a task spawned there waits
    /// behind the handler's computation and stops with that runtime, while the connection it
    /// serves lives on.
    /// [`conformance::harness::lifecycle`](https://docs.rs/ruststream/latest/ruststream/conformance/harness/fn.lifecycle.html)
    /// publishes and settles with a delay from a runtime that stops at once, and
    /// [`conformance::capabilities::request_reply`](https://docs.rs/ruststream/latest/ruststream/conformance/capabilities/fn.request_reply.html)
    /// requests from one; both then expect the work to complete.
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use tokio::runtime::Handle;
    /// use tokio::sync::mpsc::UnboundedSender;
    /// use tokio::time::sleep;
    ///
    /// struct ConnectedExample {
    ///     // Captured in `connect`.
    ///     runtime: Handle,
    /// }
    ///
    /// impl ConnectedExample {
    ///     fn requeue_after(&self, back: UnboundedSender<Vec<u8>>, delivery: Vec<u8>, delay: Duration) {
    ///         self.runtime.spawn(async move {
    ///             sleep(delay).await;
    ///             let _ = back.send(delivery);
    ///         });
    ///     }
    /// }
    /// # let _ = ConnectedExample::requeue_after;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker is unreachable, authentication fails, or the
    /// configuration is invalid.
    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>>;

    /// Wraps the broker for publisher-token minting before registration: the returned
    /// [`Bindable`](crate::bindable::Bindable) hands out
    /// [`Bound`](crate::bindable::Bound) tokens via its `bind`, and is itself what
    /// [`with_broker`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.with_broker) takes.
    ///
    /// Provided for every broker; implementations do not override it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "orders.accepted")]
    /// struct Accepted {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("orders", reply)]
    /// async fn accept(order: &Order) -> Accepted {
    ///     Accepted { id: order.id }
    /// }
    ///
    /// fn app() -> RustStream {
    ///     // Orders arrive on one bus and their answers leave on another.
    ///     let egress = MemoryBroker::new().bindable();
    ///     let to_egress = egress.bind(Publish);
    ///     RustStream::new(AppInfo::new("bridge", "0.1.0"))
    ///         .with_broker_labeled("egress", egress, |b| b)
    ///         .with_broker_labeled("ingress", MemoryBroker::new(), |b| {
    ///             b.include(accept).out_reply(to_egress)
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[cfg(feature = "std")]
    #[must_use]
    fn bindable(self) -> Bindable<Self>
    where
        Self: 'static,
    {
        Bindable::new(self)
    }
}

/// A connected broker: the typed witness of a live connection.
///
/// Obtained only from [`Broker::connect`]. The `'static` supertrait keeps the connected
/// form an owned value the runtime can hold and erase.
///
/// # Examples
///
/// ```
/// use ruststream::ConnectedBroker;
///
/// async fn stop<C: ConnectedBroker>(connected: C) -> Result<C::Closed, C::Error> {
///     connected.shutdown().await
/// }
/// ```
pub trait ConnectedBroker: Sized + 'static {
    /// The error type returned by connected-broker operations.
    type Error: StdError + Send + Sync + 'static;

    /// The terminal witness returned by [`shutdown`](Self::shutdown).
    ///
    /// A closed broker has no publish or subscribe surface; the witness may carry teardown
    /// diagnostics (flush results, drop counts) as plain data.
    type Closed;

    /// Closes the broker connection, flushing in-flight publishes and stopping background
    /// tasks, consuming the connected form.
    ///
    /// Consuming `self` makes a second shutdown, or any operation after shutdown, a compile
    /// error for the owner of the handle. Aliased handles (publishers handed out earlier,
    /// clones of a shareable broker) return the transport's own error when used afterwards,
    /// never a silent success against the dead connection;
    /// [`conformance::harness::lifecycle`](https://docs.rs/ruststream/latest/ruststream/conformance/harness/fn.lifecycle.html)
    /// checks them.
    ///
    /// Every teardown step that can fail runs here and returns its error; `shutdown` never
    /// blocks or panics. The [`Closed`](Self::Closed) witness carries teardown diagnostics as
    /// plain data, or is `()`;
    /// [`ClosedMemoryBroker`](https://docs.rs/ruststream/latest/ruststream/memory/struct.ClosedMemoryBroker.html)
    /// reports how many subscriber registrations the shutdown dropped.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker rejects the disconnect or a background flush
    /// fails to complete.
    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>>;
}

/// Shorthand for a broker's connected form, so bounds read
/// `S: SubscriptionSource<Connected<B>>` instead of spelling the associated type projection.
pub type Connected<B> = <B as Broker>::Connected;

/// A broker the tokio and compio runners can run: [`Broker`] with `Send + Sync` on the broker and
/// on its connected form.
///
/// The lifecycle traits carry no `Send`, so a broker whose client holds `Rc` or `RefCell` can
/// implement them. The tokio and compio runners move a broker and its connected form across the
/// tasks of a multi-thread pool or the cores of a service, and ask for this bound where they do.
/// Every `Broker` whose types are `Send + Sync` and `'static` implements it, so a broker crate
/// writes nothing for it.
///
/// # Examples
///
/// ```
/// use ruststream::SendBroker;
///
/// fn spawn_connect<B: SendBroker>(broker: B) {
///     // The broker and its connected form may sit on any task.
///     std::thread::spawn(move || drop(broker));
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a broker the tokio and compio runners can run",
    label = "not a `Send + Sync + 'static` broker",
    note = "the tokio and compio runners move a broker and its connected form across threads, so \
            both must be `Send + Sync + 'static`",
    note = "a broker whose client holds `Rc` or `RefCell` runs on the embedded runner, or keeps \
            the client's shared state in `Arc` and `Mutex` to run here"
)]
pub trait SendBroker: Broker<Connected: Send + Sync> + Send + Sync + 'static {}

#[diagnostic::do_not_recommend]
impl<B> SendBroker for B where B: Broker<Connected: Send + Sync> + Send + Sync + 'static {}
