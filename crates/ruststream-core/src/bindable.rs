//! Cross-broker publisher tokens: a [`PublishPolicy`] bound to a concrete broker instance.
//!
//! A bare policy attached at an include site pairs with that scope's own broker. When the
//! handler must publish to a *different* broker (consume Kafka, publish to Redis), or a sibling
//! task needs a publisher after start, wrap the target broker in [`Bindable`] and mint [`Bound`]
//! tokens from it before registration - tokens carry the instance identity a foreign scope
//! cannot provide, and the registration order stops mattering (a bidirectional bridge binds
//! both directions up front).

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::fmt;
use std::sync::Mutex;

#[cfg(feature = "asyncapi")]
use crate::bindings::Bindings;
#[cfg(feature = "testing")]
use crate::pairing::pairs_against;
use crate::{Broker, Connected, ConnectedBroker, PairError, PublishPolicy, SendBroker};

/// The channel between the erased `connect` and the typed starters: `connect` stores the typed
/// connected broker here, starters clone it out to open their subscriptions. The teardown takes
/// the slot's reference back so the connected broker can be consumed by value. Public only
/// because [`BrokerRegistration`](crate::bindable::BrokerRegistration) names it in a hidden
/// method; never constructed by users.
#[doc(hidden)]
pub type ConnectedSlot<B> = Arc<Mutex<Option<Arc<<B as Broker>::Connected>>>>;

/// A broker wrapped for token minting before registration.
///
/// `Bindable` owns the slot the runtime fills with the broker's connected form, so [`bind`]
/// hands out [`Bound`] tokens *before* `with_broker` runs - registration order stops mattering,
/// which is what a bidirectional bridge needs (each direction's scope takes the other broker's
/// token). Pass the wrapper itself to
/// [`with_broker`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.with_broker); a token whose broker never
/// registers fails fast at pairing with a clear error.
///
/// [`bind`]: Self::bind
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
/// /// An order relayed out of the east region, under a name the west region does not relay.
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "orders.from-east")]
/// struct FromEast {
///     id: u64,
/// }
///
/// /// An order relayed out of the west region, under a name the east region does not relay.
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "orders.from-west")]
/// struct FromWest {
///     id: u64,
/// }
///
/// #[subscriber("orders", reply)]
/// async fn east_to_west(order: &Order) -> FromEast {
///     FromEast { id: order.id }
/// }
///
/// #[subscriber("orders", reply)]
/// async fn west_to_east(order: &Order) -> FromWest {
///     FromWest { id: order.id }
/// }
///
/// fn app() -> RustStream {
///     // Each region relays to the other: both tokens exist before either scope is built.
///     let east = MemoryBroker::new().bindable();
///     let west = MemoryBroker::new().bindable();
///     let to_east = east.bind(Publish);
///     let to_west = west.bind(Publish);
///     RustStream::new(AppInfo::new("bridge", "0.1.0"))
///         .with_broker_labeled("east", east, |b| {
///             b.include(east_to_west).out_reply(to_west)
///         })
///         .with_broker_labeled("west", west, |b| {
///             b.include(west_to_east).out_reply(to_east)
///         })
/// }
/// # }
/// # fn main() {}
/// ```
pub struct Bindable<B: Broker> {
    pub(crate) broker: B,
    pub(crate) slot: ConnectedSlot<B>,
}

impl<B: Broker + fmt::Debug> fmt::Debug for Bindable<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bindable")
            .field("broker", &self.broker)
            .finish_non_exhaustive()
    }
}

impl<B: Broker + 'static> Bindable<B> {
    /// Wraps `broker` so publisher tokens can be minted before it registers.
    #[must_use]
    pub fn new(broker: B) -> Self {
        Self {
            broker,
            slot: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the wrapped broker, for its own constructors (publishers, subscribers).
    #[must_use]
    pub fn broker(&self) -> &B {
        &self.broker
    }

    /// Mints a token for `source`: a publisher source carrying this broker's instance identity,
    /// usable at any include site, in a sibling task via
    /// [`RunningApp::publisher`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RunningApp.html#method.publisher), or directly with
    /// [`Bound::live`] once the app started.
    #[must_use]
    pub fn bind<S>(&self, source: S) -> Bound<B, S>
    where
        S: PublishPolicy<Connected<B>>,
    {
        Bound {
            slot: Arc::clone(&self.slot),
            source,
        }
    }
}

/// What the `with_broker` family accepts.
///
/// Either a bare [`Broker`] (registered with a fresh internal slot) or a [`Bindable`] carrying
/// pre-minted tokens. Implemented by those two shapes only; you never implement or call it.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a broker this runtime runs",
    label = "not a `Broker`",
    note = "this runtime moves a broker and its connected form across threads, so it runs \
            brokers whose types are `Send + Sync + 'static` and implement `Broker`",
    note = "a broker whose client holds `Rc` or `RefCell` runs on the embedded runner; to run \
            here, keep the client's shared state in `Arc` and `Mutex`"
)]
pub trait BrokerRegistration {
    /// The broker being registered.
    type Broker: SendBroker;

    #[doc(hidden)]
    fn into_parts(self) -> (Self::Broker, ConnectedSlot<Self::Broker>);
}

impl<B: SendBroker> BrokerRegistration for B {
    type Broker = B;

    fn into_parts(self) -> (B, ConnectedSlot<B>) {
        (self, Arc::new(Mutex::new(None)))
    }
}

impl<B: SendBroker> BrokerRegistration for Bindable<B> {
    type Broker = B;

    fn into_parts(self) -> (B, ConnectedSlot<B>) {
        (self.broker, self.slot)
    }
}

/// A publisher source bound to a concrete broker instance, minted by [`Bindable::bind`].
///
/// The token shares the slot the runtime fills with that broker's connected form at startup, so
/// pairing needs no lookup and cannot pick a wrong instance. It implements [`PublishPolicy`]
/// against *any* connected broker by ignoring it and pairing against its own; that is what lets
/// a Kafka-scope registration accept a token for the Redis broker.
pub struct Bound<B2: Broker, S> {
    pub(crate) slot: ConnectedSlot<B2>,
    pub(crate) source: S,
}

impl<B2: Broker, S: fmt::Debug> fmt::Debug for Bound<B2, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bound")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl<B2: Broker + 'static, S: Clone> Clone for Bound<B2, S> {
    fn clone(&self) -> Self {
        Self {
            slot: Arc::clone(&self.slot),
            source: self.source.clone(),
        }
    }
}

impl<B2, S> Bound<B2, S>
where
    B2: Broker + 'static,
    S: PublishPolicy<Connected<B2>>,
{
    /// Pairs the token against its own broker, producing the live form.
    ///
    /// Available anywhere the application startup has already connected the broker: an
    /// `after_startup` hook (the documented home of a first publish), or a sibling task after
    /// [`start`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.start) returned (the
    /// [`RunningApp::publisher`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RunningApp.html#method.publisher) sugar calls this).
    ///
    /// # Errors
    ///
    /// Returns [`PairError`] when the broker is not connected yet (pairing before startup
    /// finished), or when the policy's own pairing fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use ruststream::memory::MemoryError;
    /// use ruststream::runtime::PublishError;
    /// use ruststream::PairError;
    /// use serde::Serialize;
    ///
    /// # #[derive(Debug, thiserror::Error)]
    /// # enum StartupError {
    /// #     #[error(transparent)]
    /// #     Pair(#[from] PairError),
    /// #     #[error(transparent)]
    /// #     Publish(#[from] PublishError<MemoryError>),
    /// # }
    /// #[derive(Serialize, Outgoing)]
    /// #[outgoing(name = "services.started")]
    /// struct Started {
    ///     service: &'static str,
    /// }
    ///
    /// fn app() -> impl App {
    ///     let broker = MemoryBroker::new().bindable();
    ///     let announce = broker.bind(Publish);
    ///     RustStream::new(AppInfo::new("orders", "0.1.0"))
    ///         .with_broker(broker, |b| b)
    ///         .after_startup(async move |_state| {
    ///             let publisher = announce.live().await?;
    ///             publisher.message(&Started { service: "orders" }).publish().await?;
    ///             Ok::<_, StartupError>(())
    ///         })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub async fn live(self) -> Result<S::Live, PairError> {
        pair_bound::<B2, S>(&self.slot, self.source).await
    }
}

/// Pairs `source` against the broker in `slot`. The runtime's own pairing entry for tokens.
#[doc(hidden)]
pub async fn pair_bound<B2, S>(slot: &ConnectedSlot<B2>, source: S) -> Result<S::Live, PairError>
where
    B2: Broker + 'static,
    S: PublishPolicy<Connected<B2>>,
{
    let connected = slot
        .lock()
        .expect("connected slot mutex poisoned")
        .clone()
        .ok_or_else(|| {
            PairError::from_boxed(Box::from(
                "the token's broker is not connected: pairing happens after startup connects \
                 every registered broker",
            ))
        })?;
    // The token's publisher publishes to its own broker, whatever scope pairs it.
    #[cfg(feature = "testing")]
    pairs_against(connected.as_ref());
    source.pair(connected.as_ref()).await
}

// The token is a policy for ANY connected broker: it ignores the scope's broker and pairs
// against its own.
impl<C, B2, S> PublishPolicy<C> for Bound<B2, S>
where
    C: ConnectedBroker,
    B2: Broker + 'static,
    S: PublishPolicy<Connected<B2>>,
{
    type Live = S::Live;

    async fn pair(self, _connected: &C) -> Result<Self::Live, PairError> {
        pair_bound::<B2, S>(&self.slot, self.source).await
    }

    // The token's channel is the other broker's, so what describes it is the policy bound to
    // that broker rather than the scope the token was included in. The channel's `servers` list
    // is the one thing that does not follow: it names the scope's server, because the
    // registration's label is what the runtime knows.
    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        <S as PublishPolicy<Connected<B2>>>::channel_bindings(&self.source, channel)
    }

    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self, channel: &str) -> Bindings {
        <S as PublishPolicy<Connected<B2>>>::operation_bindings(&self.source, channel)
    }

    #[cfg(feature = "asyncapi")]
    fn message_bindings(&self, channel: &str) -> Bindings {
        <S as PublishPolicy<Connected<B2>>>::message_bindings(&self.source, channel)
    }

    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        <S as PublishPolicy<Connected<B2>>>::reply_address_location(&self.source)
    }
}
