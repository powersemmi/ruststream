//! The [`Broker`] / [`ConnectedBroker`] ladder: the entry point of any broker implementation.

use std::error::Error as StdError;

use crate::runtime::Bindable;

/// An unconnected broker: configuration captured, no I/O performed yet.
///
/// `Broker` is the entry point of any broker crate (`ruststream-nats`, `ruststream-kafka`, ...).
/// The lifecycle is a ladder of consuming transitions, so each state is a distinct type and
/// out-of-order calls do not compile:
///
/// ```text
/// B::new(config)                      unconnected: sync, I/O-free construction
/// broker.connect(self)  -> Connected  the live connection, a typed witness
/// connected.shutdown(self) -> Closed  the terminal witness (may carry diagnostics)
/// ```
///
/// Subscribing is described separately by a [`SubscriptionSource`](crate::SubscriptionSource)
/// (or the [`Subscribe`](crate::Subscribe) capability for the by-name case), resolved against the
/// [`Connected`](Self::Connected) form. Publishers are likewise produced by broker-specific
/// constructors.
///
/// `Send + Sync` is required so the runtime can move the broker across tasks.
///
/// # Writing a broker crate
///
/// A broker is a crate of its own that implements the traits below. It depends on the core with
/// the default features off, so it takes the trait surface and the runtime without the bundled
/// JSON codec or any other broker:
///
/// ```toml
/// [dependencies]
/// ruststream = { version = "0.7", default-features = false }
/// ```
///
/// [`MemoryBroker`](crate::memory::MemoryBroker) is the reference implementation of every trait
/// on this list, and `ruststream-nats`
/// ([docs.rs](https://docs.rs/ruststream-nats/latest/ruststream_nats/)) is a worked example over
/// a real client.
///
/// The required surface:
///
/// - `Broker` and [`ConnectedBroker`]: the lifecycle ladder described here.
/// - [`Subscribe`](crate::Subscribe) on the connected form, for subscriptions a name alone
///   identifies, and a [`SubscriptionSource`](crate::SubscriptionSource) descriptor for the ones
///   that need settings of the broker's own.
/// - [`Subscriber`](crate::Subscriber), the stream of deliveries, and
///   [`IncomingMessage`](crate::IncomingMessage), one delivery and its settlement.
/// - [`Publisher`](crate::Publisher), the live publish handle, constructed from a
///   [`PublishPolicy`](crate::PublishPolicy); [`DefaultPublish`](crate::DefaultPublish) names the
///   policy a reply and a retry copy use when the mount site names none.
///
/// Settings in the broker's own vocabulary ride the mount chain:
/// [`SubscriberBuilder`](crate::runtime::SubscriberBuilder) for a subscription's descriptor,
/// [`MapPublisher`](crate::runtime::MapPublisher) for a publish policy, and
/// [`Publisher::Options`](crate::Publisher::Options) for one message.
///
/// Optional capabilities, each implemented only where the transport supports it and each checked
/// by its own suite in [`conformance::capabilities`](crate::conformance::capabilities):
///
/// | Trait | For a broker that supports |
/// |---|---|
/// | [`BatchSubscriber`](crate::BatchSubscriber) | receiving messages in batches (offer it wherever you can) |
/// | [`TransactionalPublisher`](crate::TransactionalPublisher) | begin, commit and abort around publishes on the publisher handle |
/// | [`OwnedTransactions`](crate::OwnedTransactions) / [`Transaction`](crate::Transaction) | many transactions open at once per handle, each with its own buffer |
/// | [`RequestReply`](crate::RequestReply) | native request-reply |
/// | [`Partitioned`](crate::Partitioned) | a partition key on outgoing messages |
/// | [`Seekable`](crate::Seekable) / [`Seeker`](crate::Seeker) | repositioning a live subscription in a replayable log |
/// | [`Positioned`](crate::Positioned) | reporting a delivery's own position in the log |
/// | [`Carries`](crate::Carries) / [`CarriesBatch`](crate::CarriesBatch) | lending the decoded value a delivery already holds |
/// | [`DescribeServer`](crate::DescribeServer) | reporting a [`ServerSpec`](crate::ServerSpec) for `AsyncAPI` |
///
/// A body bounds its `Out` slot with the capability it needs (`Out<impl TransactionalPublisher>`)
/// and never with a broker's type, and the mount site checks the bound policy's live form against
/// it at compile time. Implementing the trait on the live publisher is all a service needs to
/// reach the typed form of that capability. A live value that offers more than the core's
/// capabilities extends the vocabulary as [`OutSlot`](crate::runtime::OutSlot) describes.
///
/// Around the traits: a per-delivery context with [`ContextField`](crate::ContextField) keys and
/// a batch context through [`BuildBatchContext`](crate::BuildBatchContext); protocol bindings for
/// the `AsyncAPI` document on the descriptor and the policies
/// ([`asyncapi::Bindings`](crate::asyncapi::Bindings)); a prelude of the crate's own ([`prelude`](crate::prelude#a-broker-crates-prelude));
/// the in-process mode under a `testing` feature ([`InProcess`](crate::testing::InProcess),
/// [`TestableBroker`](crate::testing::TestableBroker),
/// [`register_testable_broker!`](crate::register_testable_broker)); and
/// [`conformance`](crate::conformance), whose suites prove the crate and whose overview ends with the author checklist. Async work around encode and decode
/// (a schema registry, an envelope) belongs on the async edges, not in a codec, as the
/// [`codec`](crate::codec) overview describes.
///
/// # Lazy startup contract
///
/// Implementations MUST be constructible **synchronously**, without performing I/O: expose a plain
/// `new(..)` constructor that only captures configuration (addresses, credentials). All network
/// setup happens in [`connect`], which the runtime calls once at startup, after the synchronous
/// `#[ruststream::app]` builder has run. This is what lets a service be assembled with the app
/// macro regardless of broker. A broker that can only be built by connecting (an `async` "connect
/// and return the handle" constructor) does not satisfy this contract. Each broker also ships a
/// [`SubscriptionSource`](crate::SubscriptionSource) for its subjects, resolved against the
/// connected form.
/// [`conformance::harness::lifecycle`](crate::conformance::harness::lifecycle) checks the whole
/// ladder: synchronous construction, `connect`, subscribe through the source, deliver, ack,
/// `shutdown`, and the post-shutdown behaviour of aliased handles below.
///
/// The connected form holds the live client directly, so its operations never check a "maybe
/// connected" state. A broker may also keep a shared cell that `connect` fills, or shareable
/// in-process state, as the in-memory broker does: publishers can then be handed out while the
/// application is still being assembled, and the cell serves those early handles.
///
/// # Internal tasks run on the runtime the broker connected on
///
/// A task the broker starts on its own behalf (a delayed-redelivery timer, a reply dispatcher,
/// a commit window) runs on the runtime [`connect`] ran on. The connected form keeps
/// `tokio::runtime::Handle::current()` from `connect` and spawns every such task through it,
/// whichever thread publishes, settles or requests. A handler on a dedicated thread publishes and
/// settles from that thread's own runtime: a task spawned there waits behind the handler's
/// computation and stops with that runtime, while the connection it serves lives on.
/// [`conformance::harness::lifecycle`](crate::conformance::harness::lifecycle) publishes and
/// settles with a delay from a runtime that stops at once, and
/// [`conformance::capabilities::request_reply`](crate::conformance::capabilities::request_reply)
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
/// # Shutdown is a type, not a flag
///
/// [`ConnectedBroker::shutdown`] consumes the connected broker, so misuse by the owner of the
/// handle (a publish or subscribe after shutdown) is a compile error, not a runtime one. What
/// remains dynamic is transport reality, not contract bookkeeping: handles aliasing the
/// connection (publishers created before shutdown, clones of a shareable broker) MUST surface an
/// error when used after the connection closed, never a silent success against a dead
/// connection. The conformance lifecycle check verifies that aliased-handle behaviour.
///
/// `shutdown` never blocks or panics: every teardown step that can fail runs there and returns
/// its error. The [`Closed`](ConnectedBroker::Closed) witness carries teardown diagnostics as
/// plain data, or is `()`; [`ClosedMemoryBroker`](crate::memory::ClosedMemoryBroker) reports how many
/// subscriber registrations the shutdown dropped.
///
/// # Configuration and errors
///
/// The crate owns its `Config`; the core carries no broker-specific configuration. A field with
/// no sane default gets no `Default`, so a user sets it explicitly instead of inheriting a value
/// that breaks later.
///
/// Errors use `thiserror`: one crate-level error enum with variants by source, marked
/// `#[non_exhaustive]`. A library crate never uses `anyhow`.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, ConnectedBroker};
///
/// async fn ladder<B: Broker>(broker: B) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let connected = broker.connect().await?;
///     let _closed = connected.shutdown().await?;
///     Ok(())
/// }
/// ```
///
/// [`connect`]: Self::connect
pub trait Broker: Send + Sync + Sized {
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
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker is unreachable, authentication fails, or the
    /// configuration is invalid.
    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send;

    /// Wraps the broker for publisher-token minting before registration: the returned
    /// [`Bindable`](crate::runtime::Bindable) hands out
    /// [`Bound`](crate::runtime::Bound) tokens via its `bind`, and is itself what
    /// [`with_broker`](crate::runtime::RustStream::with_broker) takes.
    ///
    /// Provided for every broker; implementations do not override it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
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
    ///         .with_broker_labeled("egress", egress, |_b| {})
    ///         .with_broker_labeled("ingress", MemoryBroker::new(), |b| {
    ///             b.include(accept).out_reply(to_egress);
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
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
/// Obtained only from [`Broker::connect`]. The `'static` supertrait keeps the connected form an
/// owned value the runtime can hold and erase.
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
pub trait ConnectedBroker: Send + Sync + Sized + 'static {
    /// The error type returned by connected-broker operations.
    type Error: StdError + Send + Sync + 'static;

    /// The terminal witness returned by [`shutdown`](Self::shutdown).
    ///
    /// A closed broker has no publish or subscribe surface; the witness may carry teardown
    /// diagnostics (flush results, drop counts) as plain data.
    type Closed: Send;

    /// Closes the broker connection, flushing in-flight publishes and stopping background tasks,
    /// consuming the connected form.
    ///
    /// Consuming `self` makes a second shutdown, or any operation after shutdown, a compile
    /// error for the owner of the handle. Aliased handles (publishers handed out earlier,
    /// clones of a shareable broker) surface the transport's own error when used afterwards;
    /// see the [`Broker`] contract.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker rejects the disconnect or a background flush
    /// fails to complete.
    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> + Send;
}

/// Shorthand for a broker's connected form, so bounds read
/// `S: SubscriptionSource<Connected<B>>` instead of spelling the associated type projection.
pub type Connected<B> = <B as Broker>::Connected;
