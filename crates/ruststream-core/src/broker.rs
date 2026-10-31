//! The [`Broker`] / [`ConnectedBroker`] ladder: the entry point of any broker implementation.

use core::error::Error as StdError;

#[cfg(feature = "std")]
use crate::bindable::Bindable;

crate::__contract_twin! {
    send {
        /// An unconnected broker: configuration captured, no I/O performed yet.
        ///
        /// `Broker` is the entry point of any broker crate (`ruststream-nats`, `ruststream-kafka`,
        /// ...). The lifecycle is a ladder of consuming transitions, so each state is a distinct
        /// type and out-of-order calls do not compile:
        ///
        /// ```text
        /// B::new(config)                      unconnected: sync, I/O-free construction
        /// broker.connect(self)  -> Connected  the live connection, a typed witness
        /// connected.shutdown(self) -> Closed  the terminal witness (may carry diagnostics)
        /// ```
        ///
        /// Subscribing is described separately by a
        /// [`SubscriptionSource`](crate::SubscriptionSource) (or the
        /// [`Subscribe`](crate::Subscribe) capability for the by-name case), resolved against the
        /// [`Connected`](Self::Connected) form. Publishers are likewise produced by broker-specific
        /// constructors.
        ///
        /// `Send + Sync` is required so the runtime can move the broker across tasks.
        ///
        /// # Writing a broker crate
        ///
        /// A broker is a crate of its own that implements the traits below. It depends on the core
        /// with the default features off, so it takes the trait surface and the runtime without the
        /// bundled JSON codec or any other broker:
        ///
        /// ```toml
        /// [dependencies]
        /// ruststream = { version = "0.7", default-features = false }
        /// ```
        ///
        /// [`MemoryBroker`](https://docs.rs/ruststream/latest/ruststream/memory/struct.MemoryBroker.html)
        /// is the reference implementation of every trait on this list, and `ruststream-nats`
        /// ([docs.rs](https://docs.rs/ruststream-nats/latest/ruststream_nats/)) is a worked example
        /// over a real client.
        ///
        /// The required surface:
        ///
        /// - `Broker` and [`ConnectedBroker`]: the lifecycle ladder described here.
        /// - [`Subscribe`](crate::Subscribe) on the connected form, for subscriptions a name alone
        ///   identifies, and a [`SubscriptionSource`](crate::SubscriptionSource) descriptor for the
        ///   ones that need settings of the broker's own.
        /// - [`Subscriber`](crate::Subscriber), the stream of deliveries, and
        ///   [`IncomingMessage`](crate::IncomingMessage), one delivery and its settlement.
        /// - [`Publisher`](crate::Publisher), the live publish handle, constructed from a
        ///   [`PublishPolicy`](crate::PublishPolicy); [`DefaultPublish`](crate::DefaultPublish)
        ///   names the policy a reply and a retry copy use when the mount site names none.
        ///
        /// Settings in the broker's own vocabulary ride the mount chain:
        /// [`SubscriberBuilder`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.SubscriberBuilder.html)
        /// for a subscription's descriptor,
        /// [`MapPublisher`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.MapPublisher.html)
        /// for a publish policy, and [`Publisher::Options`](crate::Publisher::Options) for one
        /// message.
        ///
        /// Optional capabilities, each implemented only where the transport supports it and each
        /// checked by its own suite in
        /// [`conformance::capabilities`](https://docs.rs/ruststream/latest/ruststream/conformance/capabilities/index.html):
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
        /// A body bounds its `Out` slot with the capability it needs (`Out<impl
        /// TransactionalPublisher>`) and never with a broker's type, and the mount site checks the
        /// bound policy's live form against it at compile time. Implementing the trait on the live
        /// publisher is all a service needs to reach the typed form of that capability. A live
        /// value that offers more than the core's capabilities extends the vocabulary as
        /// [`OutSlot`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.OutSlot.html)
        /// describes.
        ///
        /// Around the traits: a per-delivery context with [`ContextField`](crate::ContextField)
        /// keys and a batch context through [`BuildBatchContext`](crate::BuildBatchContext);
        /// protocol bindings for the `AsyncAPI` document on the descriptor and the policies
        /// ([`asyncapi::Bindings`](crate::bindings::Bindings)); a prelude of the crate's own
        /// ([`prelude`](https://docs.rs/ruststream/latest/ruststream/prelude/index.html#a-broker-crates-prelude));
        /// the in-process mode under a `testing` feature
        /// ([`InProcess`](https://docs.rs/ruststream/latest/ruststream/testing/trait.InProcess.html),
        /// [`TestableBroker`](https://docs.rs/ruststream/latest/ruststream/testing/trait.TestableBroker.html),
        /// [`register_testable_broker!`](https://docs.rs/ruststream/latest/ruststream/macro.register_testable_broker.html));
        /// and
        /// [`conformance`](https://docs.rs/ruststream/latest/ruststream/conformance/index.html),
        /// whose suites prove the crate and whose overview ends with the author checklist. Async
        /// work around encode and decode (a schema registry, an envelope) belongs on the async
        /// edges, not in a codec, as the
        /// [`codec`](https://docs.rs/ruststream/latest/ruststream/codec/index.html) overview
        /// describes.
        ///
        /// # Brokers whose types are not `Send`
        ///
        /// The traits above require `Send` and `Sync`, and their futures are `Send`: the runtime
        /// moves a broker, its deliveries and their futures across the threads of tokio's
        /// multi-thread pool. A broker whose client holds `Rc` or `RefCell` (a thread-per-core
        /// client, an embedded network stack) implements the `Local*` twins instead. A twin holds
        /// the same items as its Send trait, without the `Send` and `Sync` bounds:
        ///
        /// | Send trait | Local twin |
        /// |---|---|
        /// | `Broker`, [`ConnectedBroker`] | [`LocalBroker`](crate::LocalBroker), [`LocalConnectedBroker`](crate::LocalConnectedBroker) |
        /// | [`Subscriber`](crate::Subscriber), [`IncomingMessage`](crate::IncomingMessage) | [`LocalSubscriber`](crate::LocalSubscriber), [`LocalIncomingMessage`](crate::LocalIncomingMessage) |
        /// | [`SubscriptionSource`](crate::SubscriptionSource), [`RedeliveryAddressed`](crate::RedeliveryAddressed) | [`LocalSubscriptionSource`](crate::LocalSubscriptionSource), [`LocalRedeliveryAddressed`](crate::LocalRedeliveryAddressed) |
        /// | [`Publisher`](crate::Publisher), [`PublishPolicy`](crate::PublishPolicy), [`DefaultPublish`](crate::DefaultPublish) | [`LocalPublisher`](crate::LocalPublisher), [`LocalPublishPolicy`](crate::LocalPublishPolicy), [`LocalDefaultPublish`](crate::LocalDefaultPublish) |
        /// | [`Subscribe`](crate::Subscribe), [`BatchSubscriber`](crate::BatchSubscriber) | [`LocalSubscribe`](crate::LocalSubscribe), [`LocalBatchSubscriber`](crate::LocalBatchSubscriber) |
        /// | [`Seekable`](crate::Seekable), [`Seeker`](crate::Seeker), [`Positioned`](crate::Positioned) | [`LocalSeekable`](crate::LocalSeekable), [`LocalSeeker`](crate::LocalSeeker), [`LocalPositioned`](crate::LocalPositioned) |
        /// | [`TransactionalPublisher`](crate::TransactionalPublisher), [`OwnedTransactions`](crate::OwnedTransactions), [`Transaction`](crate::Transaction) | [`LocalTransactionalPublisher`](crate::LocalTransactionalPublisher), [`LocalOwnedTransactions`](crate::LocalOwnedTransactions), [`LocalTransaction`](crate::LocalTransaction) |
        /// | [`RequestReply`](crate::RequestReply), [`DescribeServer`](crate::DescribeServer) | [`LocalRequestReply`](crate::LocalRequestReply), [`LocalDescribeServer`](crate::LocalDescribeServer) |
        /// | [`InProcess`](https://docs.rs/ruststream/latest/ruststream/testing/trait.InProcess.html), [`TestableBroker`](https://docs.rs/ruststream/latest/ruststream/testing/trait.TestableBroker.html) | [`LocalInProcess`](https://docs.rs/ruststream/latest/ruststream/testing/trait.LocalInProcess.html), [`LocalTestableBroker`](https://docs.rs/ruststream/latest/ruststream/testing/trait.LocalTestableBroker.html) |
        ///
        /// Error types keep `Send + Sync + 'static` in both families, as do a publisher's
        /// `Payload` and `Options`: a broker maps its client's error into an error type of its
        /// own.
        ///
        /// Every implementation of a Send trait is an implementation of its twin through a blanket
        /// impl, which forwards each item statically. Code written over the local family takes
        /// every broker: the shared delivery body of `ruststream-runtime` is written over it, and
        /// the tokio runner, which spawns that body on tokio's pool, takes the Send family. A
        /// crate imports one
        /// family: with both in scope, a method call on a broker's own type matches two traits and
        /// does not compile (E0034). The prelude carries the Send family.
        ///
        /// # Lazy startup contract
        ///
        /// Implementations MUST be constructible **synchronously**, without performing I/O: expose
        /// a plain `new(..)` constructor that only captures configuration (addresses, credentials).
        /// All network setup happens in [`connect`], which the runtime calls once at startup, after
        /// the synchronous `#[ruststream::app]` builder has run. This is what lets a service be
        /// assembled with the app macro regardless of broker. A broker that can only be built by
        /// connecting (an `async` "connect and return the handle" constructor) does not satisfy
        /// this contract. Each broker also ships a
        /// [`SubscriptionSource`](crate::SubscriptionSource) for its subjects, resolved against the
        /// connected form.
        /// [`conformance::harness::lifecycle`](https://docs.rs/ruststream/latest/ruststream/conformance/harness/fn.lifecycle.html)
        /// checks the whole ladder: synchronous construction, `connect`, subscribe through the
        /// source, deliver, ack, `shutdown`, and the post-shutdown behaviour of aliased handles
        /// below.
        ///
        /// The connected form holds the live client directly, so its operations never check a
        /// "maybe connected" state. A broker may also keep a shared cell that `connect` fills, or
        /// shareable in-process state, as the in-memory broker does: publishers can then be handed
        /// out while the application is still being assembled, and the cell serves those early
        /// handles.
        ///
        /// # Internal tasks run on the runtime the broker connected on
        ///
        /// A task the broker starts on its own behalf (a delayed-redelivery timer, a reply
        /// dispatcher, a commit window) runs on the runtime [`connect`] ran on. The connected form
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
        /// # Shutdown is a type, not a flag
        ///
        /// [`ConnectedBroker::shutdown`] consumes the connected broker, so misuse by the owner of
        /// the handle (a publish or subscribe after shutdown) is a compile error, not a runtime
        /// one. What remains dynamic is transport reality, not contract bookkeeping: handles
        /// aliasing the connection (publishers created before shutdown, clones of a shareable
        /// broker) MUST surface an error when used after the connection closed, never a silent
        /// success against a dead connection. The conformance lifecycle check verifies that
        /// aliased-handle behaviour.
        ///
        /// `shutdown` never blocks or panics: every teardown step that can fail runs there and
        /// returns its error. The [`Closed`](ConnectedBroker::Closed) witness carries teardown
        /// diagnostics as plain data, or is `()`;
        /// [`ClosedMemoryBroker`](https://docs.rs/ruststream/latest/ruststream/memory/struct.ClosedMemoryBroker.html)
        /// reports how many subscriber registrations the shutdown dropped.
        ///
        /// # Configuration and errors
        ///
        /// The crate owns its `Config`; the core carries no broker-specific configuration. A field
        /// with no sane default gets no `Default`, so a user sets it explicitly instead of
        /// inheriting a value that breaks later.
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
        #[diagnostic::on_unimplemented(
            message = "`{Self}` is not a `Broker`",
            label = "not a broker this runtime runs",
            note = "this runtime moves a broker, its deliveries and their futures across threads, \
                    so it runs brokers whose types are `Send` and implement `Broker`",
            note = "a broker whose client holds `Rc` or `RefCell` implements `LocalBroker` alone; \
                    to run here, keep the client's shared state in `Arc` and `Mutex` and implement \
                    `Broker`"
        )]
        pub trait Broker: Send + Sync + Sized
    }
    local {
        #[doc = include_str!("local/broker.md")]
        #[diagnostic::on_unimplemented(
            message = "`{Self}` is not a broker of the local family",
            label = "not a `LocalBroker`",
            note = "a broker implements `Broker`, or `LocalBroker` when its client holds `Rc` or \
                    `RefCell`; every `Broker` is a `LocalBroker` through the blanket impl"
        )]
        pub trait LocalBroker: Sized
    }
    blanket { impl<T: Broker> LocalBroker for T }
    forward { <T as Broker> }
    docs { Broker }
    types {
        /// The error type returned by broker-level operations.
        type Error: [StdError + Send + Sync + 'static];

        /// The connected form of this broker: the typed witness that [`connect`](Self::connect)
        /// succeeded.
        type Connected: [@twin { ConnectedBroker } { LocalConnectedBroker }];
    }
    fns {
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
        fn connect(self)
            -> [impl Future<Output = Result<Self::Connected, Self::Error>> @send { + Send }];
    }
    send_only {
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
        Self: 'static, {
            Bindable::new(self)
        }
    }
}

crate::__contract_twin! {
    send {
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
        pub trait ConnectedBroker: Send + Sync + Sized + 'static
    }
    local {
        #[doc = include_str!("local/connected.md")]
        pub trait LocalConnectedBroker: Sized + 'static
    }
    blanket { impl<T: ConnectedBroker> LocalConnectedBroker for T }
    forward { <T as ConnectedBroker> }
    docs { ConnectedBroker }
    types {
        /// The error type returned by connected-broker operations.
        type Error: [StdError + Send + Sync + 'static];

        /// The terminal witness returned by [`shutdown`](Self::shutdown).
        ///
        /// A closed broker has no publish or subscribe surface; the witness may carry teardown
        /// diagnostics (flush results, drop counts) as plain data.
        type Closed: [@send { Send }];
    }
    fns {
        /// Closes the broker connection, flushing in-flight publishes and stopping background
        /// tasks, consuming the connected form.
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
        fn shutdown(self)
            -> [impl Future<Output = Result<Self::Closed, Self::Error>> @send { + Send }];
    }
}

/// Shorthand for a broker's connected form, so bounds read
/// `S: SubscriptionSource<Connected<B>>` instead of spelling the associated type projection.
pub type Connected<B> = <B as Broker>::Connected;
