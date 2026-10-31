//! The broker in its three states (unconnected, connected and closed), its publish policies,
//! its publisher and its error.

use std::{
    fmt,
    future::ready,
    marker::PhantomData,
    mem,
    sync::{Arc, Mutex},
};

#[cfg(feature = "testing")]
use bytes::Bytes;
use futures::future::lazy;
#[cfg(doc)]
use ruststream_core::IncomingMessage;
#[cfg(feature = "testing")]
use ruststream_core::RawMessage;
use ruststream_core::{
    AddressedCopies, Broker, BytesMut, ConnectedBroker, DefaultPublish, DescribeServer,
    OutgoingMessage, PairError, PublishPolicy, Publisher, ServerSpec, Subscribe, Take,
};
#[cfg(feature = "testing")]
use ruststream_runtime::testing::Backlog;
#[cfg(feature = "testing")]
use ruststream_runtime::testing::Coordinator;
#[cfg(feature = "testing")]
use ruststream_runtime::testing::InProcess;
#[cfg(feature = "testing")]
use ruststream_runtime::testing::TestableBroker;
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

#[cfg(feature = "testing")]
use crate::log::NameLog;
use crate::pattern::has_wildcard;
use crate::{
    Bus, Discarding, LogMode, MemoryOutbound, MemoryRequester, MemoryState, MemorySubscriber,
    PatternError, Registry, Retaining, Retention, Routing,
};
#[cfg(doc)]
use crate::{
    MemoryMessage, MemoryPattern, MemoryPosition, MemorySeeker, MemorySource, RequestError,
};

/// An in-memory reference broker. Cheap to clone.
///
/// The type parameter is the broker's log mode, and it decides what a subscription can do.
/// [`new`](Self::new) builds the default, [`Discarding`] form: nothing is kept, memory does not
/// grow with the message count, and there is no [`Seekable`](ruststream_core::Seekable) implementation to
/// reposition a subscription with. [`retaining`](Self::retaining) builds the [`Retaining`] form,
/// which keeps the newest messages of every name within its [`Retention`] bound and replays them
/// on demand.
///
/// Every subscription of a name receives a copy of every message published to that name after
/// it opened, and the whole state lives in the process's memory. The capabilities are native to
/// the bus: request and reply through
/// [`MemoryRequester`], batch consumption on [`MemorySubscriber`], transactions on
/// [`MemoryPublisher`], partition keys on [`MemoryMessage`], and repositioning through
/// [`MemorySeeker`] over the per-name publish log.
///
/// A [`retry_after`](ruststream_runtime::HandlerOutcome::retry_after) outcome is the broker's own
/// affair: the delivery returns to the same subscriber once the delay has elapsed, and nothing is
/// republished. Every delivery carries the count of what the broker has handed that subscriber,
/// the first delivery included, which [`MemoryMessage`] reports through
/// [`IncomingMessage::redelivery_count`]. A
/// registration's `max_attempts(..)` is therefore spent on these redeliveries, and the delivery
/// that spends it goes to the `dead_letter(..)` destination, or is terminated where the
/// registration named none.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// #[subscriber("orders", start_at(MemoryPosition::start()))]
/// async fn rebuild(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "replayed");
///     HandlerOutcome::ack()
/// }
///
/// /// A service that only routes messages keeps none of them.
/// fn router() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(accept)
///     })
/// }
///
/// /// A service that replays keeps the last 128 messages of every name.
/// fn projection() -> RustStream {
///     let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(128)));
///     RustStream::new(AppInfo::new("projection", "0.1.0")).with_broker(broker, |b| {
///         b.include(rebuild)
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct MemoryBroker<Log = Discarding> {
    pub(super) state: Arc<MemoryState>,
    mode: PhantomData<Log>,
}

impl<Log> Clone for MemoryBroker<Log> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            mode: PhantomData,
        }
    }
}

impl Default for MemoryBroker<Discarding> {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBroker<Discarding> {
    /// Creates a new empty broker that keeps no publish log. Equivalent to
    /// [`MemoryBroker::default`].
    ///
    /// Nothing a subscriber has consumed stays in memory, so a long-running service on this
    /// broker holds only what its subscribers have yet to read. Replay is not available: the
    /// [`Seekable`](ruststream_core::Seekable) capability is implemented for a
    /// [`retaining`](Self::retaining) broker's subscriptions alone.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(MemoryState::discarding()),
            mode: PhantomData,
        }
    }
}

impl MemoryBroker<Retaining> {
    /// Creates a broker keeping the newest messages of every name within `retention`.
    ///
    /// Its subscriptions are [`Seekable`](ruststream_core::Seekable): a seeker replays the retained
    /// messages from any position that has not been evicted. The bound holds per name, so the
    /// broker's footprint is the bound times the number of names it publishes under.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Entry {
    ///     id: u64,
    /// }
    ///
    /// /// Opens at the oldest retained message, so a restart rebuilds from what the broker kept.
    /// #[subscriber("audit", start_at(MemoryPosition::start()))]
    /// async fn rebuild(entry: &Entry) -> HandlerOutcome {
    ///     tracing::info!(entry.id, "replayed");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(8)));
    ///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(rebuild)
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn retaining(retention: Retention) -> Self {
        Self {
            state: Arc::new(MemoryState::retaining(retention)),
            mode: PhantomData,
        }
    }
}

impl<Log: LogMode> MemoryBroker<Log> {
    /// Sets how a publish is routed where pattern subscriptions match it: to every match
    /// ([`Routing::EveryMatch`], the default) or to the most specific one
    /// ([`Routing::MostSpecific`]).
    ///
    /// A clone of the broker is the same broker, so it routes by the same rule.
    ///
    /// # Examples
    ///
    /// ```
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// # #[derive(serde::Deserialize)]
    /// # struct Order { id: u64 }
    ///
    /// #[subscriber("orders.eu")]
    /// async fn europe(order: &Order) -> HandlerOutcome {
    ///     let _ = order.id;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// // Every region without a handler of its own.
    /// #[subscriber(MemoryPattern::new("orders.*"))]
    /// async fn elsewhere(order: &Order) -> HandlerOutcome {
    ///     let _ = order.id;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     let broker = MemoryBroker::new().routing(Routing::MostSpecific);
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(europe)
    ///         .include(elsewhere)
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn routing(self, routing: Routing) -> Self {
        self.state.set_routing(routing);
        self
    }

    /// Opens a subscription to `name`. The returned subscriber starts receiving messages
    /// published after this call; messages published earlier are not delivered, though a
    /// retaining broker's [`Seekable`](ruststream_core::Seekable) capability can replay them from its
    /// publish log.
    ///
    /// On a shut-down broker the registration is refused and the subscriber simply never
    /// receives anything, matching this constructor's infallible signature; the
    /// [`Subscribe`] path reports [`MemoryError::ShutDown`] instead. A name with a wildcard
    /// token (`*` or `>`) is a pattern, which this constructor does not open: the subscriber's
    /// stream ends at once, where the [`Subscribe`] path reports [`MemoryError::WildcardName`]
    /// and [`MemoryPattern`] opens the pattern.
    #[must_use]
    pub fn subscribe(&self, name: impl Into<String>) -> MemorySubscriber<Log> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let name = name.into();
        if has_wildcard(&name) {
            rx.close();
        } else {
            let _ = self.state.register(&name, tx.clone());
        }
        MemorySubscriber::new(name, rx, tx, &self.state, None)
    }

    /// Returns a publisher bound to this broker.
    #[must_use]
    pub fn publisher(&self) -> MemoryPublisher {
        MemoryPublisher {
            state: Arc::clone(&self.state),
            txn: Mutex::new(None),
        }
    }

    /// Returns a request / reply-capable publisher bound to this broker.
    ///
    /// Unlike [`MemoryBroker::publisher`], which reports [`MemoryError`], a requester awaits a
    /// correlated reply that may never arrive, so its operations report [`RequestError`]: the
    /// shut-down case plus a reply timeout.
    #[must_use]
    pub fn requester(&self) -> MemoryRequester {
        MemoryRequester::new(Arc::clone(&self.state))
    }
}

impl<Log> fmt::Debug for MemoryBroker<Log> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryBroker").finish_non_exhaustive()
    }
}

impl<Log: LogMode> Broker for MemoryBroker<Log> {
    type Error = MemoryError;
    type Connected = ConnectedMemoryBroker<Log>;

    /// Connecting is free for an in-process bus. A shut-down bus (a clone lineage may have shut
    /// the shared state down) is revived with a fresh, empty registration map, so the connected
    /// form always starts live; a live bus keeps its registrations.
    ///
    /// The connected form keeps the runtime it was connected on, and a delayed redelivery's timer
    /// runs there whichever thread settles the delivery.
    ///
    /// # Panics
    ///
    /// Panics when polled outside a Tokio runtime, the one thing it has to capture.
    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        // Run on the first poll, so the runtime captured is the one the connection runs on, not
        // the one the future happened to be built on.
        lazy(move |_| {
            {
                let mut bus = self
                    .state
                    .subscribers
                    .lock()
                    .expect("memory broker mutex poisoned");
                if let Bus::ShutDown(routing) = *bus {
                    *bus = Bus::Live(Registry::new(routing));
                }
            }
            Ok(ConnectedMemoryBroker {
                state: self.state,
                runtime: Handle::current(),
                mode: PhantomData,
            })
        })
    }
}

/// The connected form of [`MemoryBroker`]: the typed witness that [`Broker::connect`] ran.
///
/// Cheap to clone: the in-memory bus is shared state by nature, so the connected form is a
/// shareable handle on it, exactly like the unconnected broker. Subscriptions (the
/// [`Subscribe`] capability, [`MemorySource`]) resolve against this form, and carry over the
/// broker's log mode: only a [`Retaining`] one opens repositionable subscriptions.
pub struct ConnectedMemoryBroker<Log = Discarding> {
    pub(super) state: Arc<MemoryState>,
    /// The runtime `connect` ran on, which every task the broker starts runs on.
    pub(super) runtime: Handle,
    mode: PhantomData<Log>,
}

impl<Log> Clone for ConnectedMemoryBroker<Log> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            runtime: self.runtime.clone(),
            mode: PhantomData,
        }
    }
}

impl<Log> fmt::Debug for ConnectedMemoryBroker<Log> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedMemoryBroker")
            .finish_non_exhaustive()
    }
}

impl<Log: LogMode> ConnectedMemoryBroker<Log> {
    /// Returns a publisher bound to this broker.
    #[must_use]
    pub fn publisher(&self) -> MemoryPublisher {
        MemoryPublisher {
            state: Arc::clone(&self.state),
            txn: Mutex::new(None),
        }
    }

    /// Returns a request / reply-capable publisher bound to this broker.
    ///
    /// See [`MemoryBroker::requester`] for why its operations report [`RequestError`] rather
    /// than [`MemoryError`].
    #[must_use]
    pub fn requester(&self) -> MemoryRequester {
        MemoryRequester::new(Arc::clone(&self.state))
    }
}

impl<Log: LogMode> ConnectedBroker for ConnectedMemoryBroker<Log> {
    type Error = MemoryError;
    type Closed = ClosedMemoryBroker;

    /// Enters the terminal shut-down state: the bus itself flips to its `ShutDown` variant, so
    /// every aliased handle that would touch it (a publisher's publish or transaction commit, a
    /// request) errors with [`MemoryError::ShutDown`]. Consuming `self` makes any further use
    /// of this handle a compile error; the returned witness reports how many subscriber
    /// registrations the teardown dropped.
    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> {
        let dropped = {
            let mut bus = self
                .state
                .subscribers
                .lock()
                .expect("memory broker mutex poisoned");
            let routing = match &*bus {
                Bus::Live(registry) => registry.routing,
                Bus::ShutDown(routing) => *routing,
            };
            match mem::replace(&mut *bus, Bus::ShutDown(routing)) {
                Bus::Live(registry) => registry.len(),
                Bus::ShutDown(_) => 0,
            }
        };
        ready(Ok(ClosedMemoryBroker {
            subscribers_dropped: dropped,
        }))
    }
}

/// The publish policy of the in-memory broker: no options to carry, so it is a unit marker.
///
/// Pairs into a [`MemoryPublisher`] against a [`ConnectedMemoryBroker`], filling the same
/// [`PublishPolicy`] position where a richer broker carries real options (an exchange, a queue
/// timeout, a transactional id).
///
/// It adds nothing to the generated `AsyncAPI` document. The specification's protocol keys are a
/// closed list and `memory` is not one of them, so there is no lawful binding to fill; an `x-`
/// extension would describe a transport that never leaves the process.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::memory::MemoryPublish;
/// use ruststream::memory::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "orders.confirmed")]
/// struct Confirmed {
///     id: u64,
/// }
///
/// #[subscriber("orders", reply)]
/// async fn confirm(order: &Order) -> Confirmed {
///     Confirmed { id: order.id }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         // The prelude also names this policy `Publish`.
///         b.include(confirm).out_reply(MemoryPublish)
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct MemoryPublish;

impl<Log: LogMode> PublishPolicy<ConnectedMemoryBroker<Log>> for MemoryPublish {
    type Live = MemoryPublisher;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.publisher()))
    }
}

impl<Log: LogMode> DefaultPublish for ConnectedMemoryBroker<Log> {
    type Policy = MemoryPublish;
}

/// The request / reply policy of the in-memory broker; pairs into a [`MemoryRequester`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::OutgoingMessage;
/// use ruststream::memory::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize)]
/// struct Ask {
///     id: u64,
/// }
///
/// #[derive(Deserialize)]
/// struct Quote {
///     price: u64,
/// }
///
/// #[derive(OutSlot)]
/// struct Pricing;
///
/// #[subscriber("orders")]
/// async fn price(order: &Order, Out(pricing): Out<impl RequestReply, Pricing>) -> HandlerOutcome {
///     let Ok(ask) = serde_json::to_vec(&Ask { id: order.id }) else {
///         return HandlerOutcome::drop();
///     };
///     // The reply comes back on an inbox the request names in its `reply-to` header.
///     let request = OutgoingMessage::new("quotes.ask", ask.as_slice());
///     let Ok(reply) = pricing.request(request, Duration::from_secs(2)).await else {
///         return HandlerOutcome::retry();
///     };
///     match serde_json::from_slice::<Quote>(reply.payload()) {
///         Ok(quote) => {
///             tracing::info!(order.id, quote.price, "priced");
///             HandlerOutcome::ack()
///         }
///         Err(_) => HandlerOutcome::drop(),
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(price).out(Pricing, Request)
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct MemoryRequest;

impl<Log: LogMode> PublishPolicy<ConnectedMemoryBroker<Log>> for MemoryRequest {
    type Live = MemoryRequester;

    fn pair(
        self,
        connected: &ConnectedMemoryBroker<Log>,
    ) -> impl Future<Output = Result<Self::Live, PairError>> {
        ready(Ok(connected.requester()))
    }
}

/// The terminal witness returned by shutting down a [`ConnectedMemoryBroker`].
///
/// Has no publish or subscribe surface; it carries the teardown diagnostics as plain data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedMemoryBroker {
    subscribers_dropped: usize,
}

impl ClosedMemoryBroker {
    /// How many subscriber registrations were dropped when the bus shut down.
    #[must_use]
    pub fn subscribers_dropped(&self) -> usize {
        self.subscribers_dropped
    }
}

impl<Log: LogMode> DescribeServer for MemoryBroker<Log> {
    /// The in-memory broker has no network address, so it describes itself as an in-process server
    /// over the `"memory"` protocol. Registered with
    /// [`with_broker_labeled`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.with_broker_labeled), the label is its
    /// stable identity, letting a service mount several memory brokers with disjoint routing and
    /// address each one by name.
    fn describe_server(&self) -> ServerSpec {
        ServerSpec::in_process("memory")
    }
}

// The bus is in-process by nature, so the transition the harness connects through is the ordinary
// `connect`: the connected form a test drives is the one production gets.
#[cfg(feature = "testing")]
impl<Log: LogMode> InProcess for MemoryBroker<Log> {
    fn connect_in_process(
        self,
    ) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        self.connect()
    }
}

// The harness drives the connected form that transition produces, and run_suite scenarios
// receive it too.
#[cfg(feature = "testing")]
impl<Log: LogMode> TestableBroker for ConnectedMemoryBroker<Log> {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.state.install_coordinator(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let (name, payload, headers) = message.into_parts();
        // Injecting into a shut-down bus is a harness bug (both run_suite and TestApp drive
        // the bus strictly before shutdown), so fail loudly instead of losing the message.
        self.state
            .fanout(name, Bytes::copy_from_slice(payload), headers)
            .expect("inject on a shut-down broker: drive the harness before shutdown");
    }

    /// What the broker holds under `name`: everything published there on a discarding broker
    /// (which records for the length of a harness run), and the retained window on a retaining
    /// one, so an assertion never claims more than the broker keeps.
    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state
            .log
            .lock()
            .expect("memory broker mutex poisoned")
            .name(name)
            .map(|log| log.messages(name))
            .unwrap_or_default()
    }

    /// How many of the oldest messages under `name` a retaining broker's bound pushed out of its
    /// log; zero on a discarding broker, whose log records without a bound for a harness run.
    fn evicted(&self, name: &str) -> usize {
        self.state
            .log
            .lock()
            .expect("memory broker mutex poisoned")
            .name(name)
            .map_or(0, NameLog::first_seq)
    }

    /// The broker's own rule, read from the [`Routing`] it was built with: a name with a
    /// wildcard token is a [`MemoryPattern`] subscription, every other one an exact name.
    fn routes(&self, destination: &str, subscriptions: &[&str]) -> Vec<usize> {
        self.state.routing().routes(destination, subscriptions)
    }

    /// A subscription receives what is published after it opens, on either log mode: a retaining
    /// broker keeps its log for a seek to replay, not for a subscription to start from.
    fn backlog(&self) -> Backlog {
        Backlog::Missed
    }
}

// One registration per log mode: the harness finds a broker by the type the app was built on,
// and the two modes are two types.
#[cfg(feature = "testing")]
ruststream_runtime::register_testable_broker!(MemoryBroker<Discarding>);
#[cfg(feature = "testing")]
ruststream_runtime::register_testable_broker!(MemoryBroker<Retaining>);

impl<Log: LogMode> Subscribe for ConnectedMemoryBroker<Log> {
    type Subscriber = MemorySubscriber<Log>;
    // One subject is both ends of the bus here, so a publish under the name a subscription reads
    // reaches that subscription, and the name is the address.
    type Copies = AddressedCopies;

    /// A name with a wildcard token is refused with [`MemoryError::WildcardName`]: this form
    /// reads one name, and [`MemoryPattern`] is the one that reads a pattern.
    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        if has_wildcard(name) {
            return ready(Err(MemoryError::WildcardName {
                name: name.to_owned(),
            }));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let name = name.to_owned();
        if let Err(err) = self.state.register(&name, tx.clone()) {
            return ready(Err(err));
        }
        ready(Ok(MemorySubscriber::new(
            name,
            rx,
            tx,
            &self.state,
            Some(self.runtime.clone()),
        )))
    }
}

/// Publisher returned by [`MemoryBroker::publisher`]. Fanout copy to every subscriber of the
/// target name at publish time.
///
/// Also implements [`TransactionalPublisher`](ruststream_core::TransactionalPublisher): while a
/// transaction is active on this handle, publishes are buffered and fan out together on commit.
pub struct MemoryPublisher {
    pub(super) state: Arc<MemoryState>,
    // Active transaction buffer of this handle. `None` outside a transaction.
    pub(super) txn: Mutex<Option<Vec<MemoryOutbound>>>,
}

impl Clone for MemoryPublisher {
    /// A clone is an independent handle on the same broker: it does not join (or carry over)
    /// this handle's active transaction.
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            txn: Mutex::new(None),
        }
    }
}

impl fmt::Debug for MemoryPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryPublisher").finish_non_exhaustive()
    }
}

/// Error type of the in-memory broker: returned by [`MemoryPublisher`] and used as the error
/// type of the [`Broker`] / [`ConnectedBroker`] lifecycle and the [`Subscribe`] capability.
///
/// The bus is in-process, so there is no transport to fail: operations against a live bus
/// succeed, and a publish, subscription, or transaction commit through a handle aliasing a
/// shut-down bus reports [`ShutDown`](MemoryError::ShutDown). The transaction variants cover
/// misuse, which the [`TransactionalPublisher`](ruststream_core::TransactionalPublisher) contract requires
/// to surface as errors rather than silent no-ops. The subscription variants refuse a name or a
/// pattern when the subscription opens, so a service with a bad one does not start.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MemoryError {
    /// `begin_transaction` was called while a transaction is already open on this handle.
    #[error("a transaction is already open on this publisher handle")]
    TransactionBusy,
    /// `commit` or `abort` was called with no open transaction on this handle.
    #[error("no transaction is open on this publisher handle")]
    NoTransaction,
    /// The operation (a publish, subscribe, transaction commit, or request) ran through a handle
    /// aliasing a bus that was shut down ([`ConnectedBroker::shutdown`]) and not revived by a
    /// sibling clone's [`Broker::connect`].
    #[error("the memory broker is shut down")]
    ShutDown,
    /// A seek named a message the broker no longer keeps: the retention bound evicted it. The
    /// oldest position still replayable is reported, so a caller that wants what is left seeks
    /// to [`MemoryPosition::start`] instead.
    #[error("position {requested} is no longer retained; the log starts at {oldest}")]
    PositionEvicted {
        /// The position the seek asked for.
        requested: usize,
        /// The oldest position the name still retains.
        oldest: usize,
    },
    /// A [`MemoryPattern`] subscription named a pattern that does not parse.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use ruststream::memory::{MemoryBroker, MemoryError, MemoryPattern, MemorySource};
    /// use ruststream::{Broker, SubscriptionSource};
    ///
    /// let connected = MemoryBroker::new().connect().await?;
    /// let refused = MemoryPattern::new("orders..eu").subscribe(&connected).await;
    /// assert!(matches!(refused, Err(MemoryError::InvalidPattern { .. })));
    /// # Ok(())
    /// # }
    /// ```
    #[error("subscription pattern {pattern:?} is invalid: {reason}")]
    InvalidPattern {
        /// The pattern as the subscription wrote it.
        pattern: String,
        /// What is wrong with it.
        reason: PatternError,
    },
    /// A subscription by name ([`MemorySource`] or a bare `#[subscriber("..")]`) named a pattern.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use ruststream::memory::{MemoryBroker, MemoryError, MemoryPattern, MemorySource};
    /// use ruststream::{Broker, SubscriptionSource};
    ///
    /// let connected = MemoryBroker::new().connect().await?;
    /// let refused = MemorySource::new("orders.*").subscribe(&connected).await;
    /// assert!(matches!(refused, Err(MemoryError::WildcardName { .. })));
    /// # Ok(())
    /// # }
    /// ```
    #[error(
        "subscription {name:?} has a wildcard token and reads one name only; subscribe to a \
         pattern with `MemoryPattern::new({name:?})`"
    )]
    WildcardName {
        /// The name as the subscription wrote it.
        name: String,
    },
}

impl Publisher for MemoryPublisher {
    // The bus keeps every message it is handed until the last subscriber has read it, so the
    // publish hands the buffer over rather than lending it.
    type Payload = Take;

    type Error = MemoryError;
    // The in-memory bus has no protocol field a message can differ in, so there is nothing to
    // settle per message.
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        {
            let mut txn = self.txn.lock().expect("memory broker mutex poisoned");
            if let Some(buffered) = txn.as_mut() {
                // Buffering is local to this handle and never touches the bus; a commit against
                // a shut-down bus is what reports the error. The name is copied here because the
                // buffer outlives this call.
                buffered.push(MemoryOutbound::from(msg));
                return ready(Ok(()));
            }
        }
        let (name, payload, headers) = msg.into_parts();
        ready(self.state.fanout(name, payload.freeze(), headers))
    }
}

#[cfg(test)]
mod tests {
    use futures::{FutureExt, StreamExt};
    use ruststream_core::{IncomingMessage, Subscriber};
    use tokio::runtime::Builder;

    use super::*;

    #[tokio::test]
    async fn a_reconnect_revives_a_bus_that_was_shut_down() {
        let broker = MemoryBroker::new();
        let connected = broker.clone().connect().await.unwrap();
        connected.shutdown().await.unwrap();

        // The lazy-connect contract lets the same configuration open a fresh bus afterwards.
        let reconnected = broker.connect().await.unwrap();
        let mut subscriber = reconnected.subscribe("orders").await.unwrap();
        reconnected
            .publisher()
            .publish(OutgoingMessage::new("orders", b"after"), None)
            .await
            .unwrap();

        let mut stream = std::pin::pin!(subscriber.stream());
        let delivered = stream.next().await.unwrap().unwrap();
        assert_eq!(delivered.payload(), b"after");
    }

    #[tokio::test]
    async fn shutdown_reports_dropped_registrations() {
        let broker = MemoryBroker::new();
        let connected = broker
            .connect()
            .await
            .expect("memory connect is infallible");
        let _first = connected.subscribe("orders").await.unwrap();
        let _second = connected.subscribe("orders").await.unwrap();
        let _third = connected.subscribe("billing").await.unwrap();

        let closed = connected.shutdown().await.unwrap();
        assert_eq!(closed.subscribers_dropped(), 3);
    }

    #[tokio::test]
    async fn shutdown_after_a_sibling_shutdown_reports_nothing_dropped() {
        let broker = MemoryBroker::new();
        let first = broker.clone().connect().await.unwrap();
        let second = broker.connect().await.unwrap();
        let _sub = first.subscribe("orders").await.unwrap();

        assert_eq!(first.shutdown().await.unwrap().subscribers_dropped(), 1);
        // The sibling shares the bus, which is already terminal: nothing left to drop.
        assert_eq!(second.shutdown().await.unwrap().subscribers_dropped(), 0);
    }

    // Paused time needs the current-thread runtime; the redelivery timer auto-advances instead
    // of sleeping for real. The subject is the broker's own timer, which the harness replaces with
    // its coordinator's, so the test drives the broker without one.

    /// A connect future built outside any runtime connects on the runtime that polls it.
    #[test]
    fn connect_captures_the_runtime_that_polls_it() {
        let connecting = MemoryBroker::new().connect();
        let runtime = Builder::new_current_thread().build().unwrap();
        assert!(runtime.block_on(connecting).is_ok());
    }

    /// A publish to a name nothing reads is accepted and left nowhere: a broker that keeps no log
    /// has no subscription to hand the message to, and a subscription opened afterwards starts at
    /// the tip.
    #[tokio::test]
    async fn a_publish_nothing_reads_is_accepted_and_kept_nowhere() {
        let broker = MemoryBroker::new();
        broker
            .publisher()
            .publish(OutgoingMessage::new("unread", b"body"), None)
            .await
            .unwrap();

        let mut subscriber = broker.subscribe("unread");
        let mut stream = std::pin::pin!(subscriber.stream());
        assert!(stream.next().now_or_never().is_none());
    }

    /// The infallible constructor does not open a pattern: the stream ends at once instead of waiting
    /// for publishes that never reach a name it did not register.
    #[tokio::test]
    async fn a_wildcard_name_on_the_inherent_subscribe_ends_the_stream() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("orders.*");
        broker
            .publisher()
            .publish(OutgoingMessage::new("orders.*", b"body"), None)
            .await
            .unwrap();
        let mut stream = std::pin::pin!(subscriber.stream());
        assert!(stream.next().await.is_none());
    }
}
