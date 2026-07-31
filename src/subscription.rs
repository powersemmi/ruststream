//! Subscription descriptors: how a handler is bound to one broker subscription.
//!
//! A [`SubscriptionSource`] is the value a broker crate exposes as its subscriber configuration:
//! it carries everything needed to open one subscription (subject / name, consumer group,
//! durable name, delivery policy, ...) and knows how to turn that into a live [`Subscriber`]
//! against a connected broker. The default [`Name`] source covers brokers that only need a name
//! string (those implementing [`Subscribe`]); richer brokers ship their own sources.
//!
//! This is the seam the `#[subscriber(..)]` macro and the application object build on: the macro
//! takes a source (a name string or a broker config value), the runtime resolves it once against
//! the [`ConnectedBroker`] form produced by [`Broker::connect`](crate::Broker::connect).

use std::{
    borrow::Cow,
    future::Future,
    sync::{Arc, OnceLock},
};

use thiserror::Error;

use crate::{ConnectedBroker, Seekable, Seeker, Subscribe, Subscriber};

/// A description of one subscription, resolved against a connected broker at startup.
///
/// The runtime calls [`subscribe`] once, against the [`ConnectedBroker`] witness produced by
/// [`Broker::connect`](crate::Broker::connect), to obtain the live [`Subscriber`]. The associated
/// [`Subscriber`](Self::Subscriber) type lives on the source rather than the broker, so a single
/// broker can offer several subscription kinds with different subscriber types (for example
/// `Redis` pub/sub versus streams).
///
/// [`subscribe`]: Self::subscribe
///
/// # Examples
///
/// ```
/// use ruststream::{ConnectedBroker, SubscriptionSource};
///
/// async fn open<C, S>(source: S, connected: &C) -> Result<S::Subscriber, C::Error>
/// where
///     C: ConnectedBroker,
///     S: SubscriptionSource<C>,
/// {
///     source.subscribe(connected).await
/// }
/// ```
pub trait SubscriptionSource<C: ConnectedBroker> {
    /// The subscriber type this source opens.
    type Subscriber: Subscriber;

    /// The name (subject / channel) this subscription binds to.
    ///
    /// Used for handler metadata and `AsyncAPI` generation; it need not be the only routing
    /// information the source carries.
    fn name(&self) -> &str;

    /// Opens the subscription against the connected broker. Called once at startup.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectedBroker::Error`] when the broker rejects the subscription or the
    /// transport fails.
    fn subscribe(
        self,
        connected: &C,
    ) -> impl Future<Output = Result<Self::Subscriber, C::Error>> + Send;
}

/// The default [`SubscriptionSource`]: subscribe by name string via the [`Subscribe`] capability.
///
/// Produced by `#[subscriber("name")]` and usable directly with any connected broker implementing
/// [`Subscribe`].
///
/// # Examples
///
/// ```
/// use ruststream::{Name, Subscribe, SubscriptionSource};
///
/// async fn open<C: Subscribe>(connected: &C) -> Result<C::Subscriber, C::Error> {
///     Name::new("orders").subscribe(connected).await
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Name(Cow<'static, str>);

impl Name {
    /// Creates a name source bound to `name`.
    #[must_use]
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        Self(name.into())
    }
}

impl<C: Subscribe> SubscriptionSource<C> for Name {
    type Subscriber = C::Subscriber;

    fn name(&self) -> &str {
        &self.0
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        connected.subscribe(&self.0).await
    }
}

/// A source decorator handing out the subscription's [`Seeker`] through a
/// [`SeekerToken`] minted before registration.
///
/// The runtime owns the subscriber for the life of the service, so application code cannot call
/// [`Seekable::seeker`] itself. `attach` wraps any [`SubscriptionSource`] whose subscriber is
/// [`Seekable`] and returns the token alongside it: mount the wrapped source anywhere a source
/// goes, and once the runtime opens the subscription at startup the token resolves to the live
/// seeker. On a broker without the [`Seekable`] capability the wrapped source does not implement
/// [`SubscriptionSource`], so the mount fails to compile instead of failing at runtime.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySource};
/// use ruststream::{Broker, Seeker, SubscriptionSource, WithSeeker};
///
/// let (source, token) = WithSeeker::attach(MemorySource::new("orders"));
///
/// // The runtime does this at startup; the token resolves once the subscription is open.
/// let connected = MemoryBroker::new().connect().await?;
/// let subscriber = source.subscribe(&connected).await?;
///
/// token.seeker()?.seek(MemoryPosition::start()).await?;
/// # let _ = subscriber;
/// # Ok(())
/// # }
/// ```
pub struct WithSeeker<S, K> {
    inner: S,
    slot: Arc<OnceLock<K>>,
}

impl<S, K> WithSeeker<S, K> {
    /// Wraps `source`, returning the decorated source and the token its seeker will resolve
    /// through once the subscription opens.
    #[must_use]
    pub fn attach(source: S) -> (Self, SeekerToken<K>) {
        let slot = Arc::new(OnceLock::new());
        (
            Self {
                inner: source,
                slot: Arc::clone(&slot),
            },
            SeekerToken { slot },
        )
    }
}

impl<S, K> std::fmt::Debug for WithSeeker<S, K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WithSeeker").finish_non_exhaustive()
    }
}

impl<C, S, K> SubscriptionSource<C> for WithSeeker<S, K>
where
    C: ConnectedBroker,
    // `Send` on the source and the (`Seeker`-implied) markers on `K` keep the returned future
    // `Send`, as the trait's RPITIT promises.
    S: SubscriptionSource<C> + Send,
    S::Subscriber: Seekable<Seeker = K>,
    K: Seeker,
{
    type Subscriber = S::Subscriber;

    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        let subscriber = self.inner.subscribe(connected).await?;
        // `subscribe` consumes the source, so the slot is filled at most once; the fill happens
        // before the subscriber leaves this call, so a token can never observe an open
        // subscription without its seeker.
        let _ = self.slot.set(subscriber.seeker());
        Ok(subscriber)
    }
}

/// A source decorator opening the subscription at a chosen position instead of the broker's
/// default.
///
/// Wraps any [`SubscriptionSource`] whose subscriber is [`Seekable`] and seeks to `position`
/// before the first delivery, so the handler never sees a message from before the chosen
/// point. The position is the broker's own type (its latest / earliest constructors, a
/// sequence number, a captured [`Positioned`](crate::Positioned) value), which makes "start
/// from the latest on deploy" or "replay the whole log into a fresh subscription" a
/// declaration at the mount site rather than an operational action afterwards. On a broker
/// without the [`Seekable`] capability the wrapped source does not implement
/// [`SubscriptionSource`], so the mount fails to compile.
///
/// This is a forced position: it applies on every startup. A conditional default (only when
/// the broker has no stored cursor for the group) remains the domain of the broker's own
/// subscription descriptor.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use futures::StreamExt;
/// use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySource};
/// use ruststream::{Broker, IncomingMessage, OutgoingMessage, Publisher, StartAt};
/// use ruststream::{Subscriber, SubscriptionSource};
///
/// let connected = MemoryBroker::new().connect().await?;
/// let publisher = connected.publisher();
/// publisher.publish(OutgoingMessage::new("audit", b"one".as_slice())).await?;
///
/// // A fresh subscription opened at the start of the log replays the earlier publish.
/// let mut subscriber = StartAt::new(MemorySource::new("audit"), MemoryPosition::start())
///     .subscribe(&connected)
///     .await?;
/// let mut stream = std::pin::pin!(subscriber.stream());
/// let replayed = stream.next().await.expect("replayed")?;
/// assert_eq!(replayed.payload(), b"one");
/// replayed.ack().await?;
/// # Ok(())
/// # }
/// ```
pub struct StartAt<S, P> {
    inner: S,
    position: P,
}

impl<S, P> StartAt<S, P> {
    /// Wraps `source` so its subscription opens at `position`.
    #[must_use]
    pub fn new(source: S, position: P) -> Self {
        Self {
            inner: source,
            position,
        }
    }
}

impl<S, P> std::fmt::Debug for StartAt<S, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartAt").finish_non_exhaustive()
    }
}

impl<C, S, P> SubscriptionSource<C> for StartAt<S, P>
where
    C: ConnectedBroker,
    // `Send` on the pieces keeps the returned future `Send`, as the trait's RPITIT promises.
    S: SubscriptionSource<C> + Send,
    S::Subscriber: Seekable,
    // A rejected reposition surfaces as this source's subscribe error, so the seeker must
    // report the connected broker's error type (broker crates use one error type for both).
    <S::Subscriber as Seekable>::Seeker: Seeker<Position = P, Error = C::Error>,
    P: Send,
{
    type Subscriber = S::Subscriber;

    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        let subscriber = self.inner.subscribe(connected).await?;
        // Sought before the subscriber leaves this call: per the `Seeker::seek` contract the
        // next delivery reflects the position, so the dispatch loop never observes a message
        // from before it.
        subscriber.seeker().seek(self.position).await?;
        Ok(subscriber)
    }
}

/// [`StartAt`] without an explicit position: opens the subscription at the broker's default
/// reposition point.
///
/// The position is the [`Default`] of the seeker's position type; a broker whose positions
/// have a natural default implements it and documents what it means (the in-memory broker's
/// default is the start of the log). A position type without a `Default` cannot be used here,
/// so the mount fails to compile instead of guessing.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use ruststream::memory::{MemoryBroker, MemorySource};
/// use ruststream::{Broker, StartAtDefault, SubscriptionSource};
///
/// let connected = MemoryBroker::new().connect().await?;
/// // For the memory broker the default position is the start of the log.
/// let subscriber = StartAtDefault::new(MemorySource::new("audit"))
///     .subscribe(&connected)
///     .await?;
/// # let _ = subscriber;
/// # Ok(())
/// # }
/// ```
pub struct StartAtDefault<S> {
    inner: S,
}

impl<S> StartAtDefault<S> {
    /// Wraps `source` so its subscription opens at the broker's default position.
    #[must_use]
    pub fn new(source: S) -> Self {
        Self { inner: source }
    }
}

impl<S> std::fmt::Debug for StartAtDefault<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StartAtDefault").finish_non_exhaustive()
    }
}

impl<C, S> SubscriptionSource<C> for StartAtDefault<S>
where
    C: ConnectedBroker,
    S: SubscriptionSource<C> + Send,
    S::Subscriber: Seekable,
    <S::Subscriber as Seekable>::Seeker: Seeker<Error = C::Error>,
    <<S::Subscriber as Seekable>::Seeker as Seeker>::Position: Default,
{
    type Subscriber = S::Subscriber;

    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        let subscriber = self.inner.subscribe(connected).await?;
        // Same ordering guarantee as `StartAt`: sought before the subscriber leaves this
        // call, so the dispatch loop never observes a message from before the position.
        subscriber.seeker().seek(Default::default()).await?;
        Ok(subscriber)
    }
}

/// Resolves the [`Seeker`] of a subscription mounted through [`WithSeeker::attach`].
///
/// Clonable and cheap: hand one copy to an admin endpoint, keep another in the application
/// state. The token resolves once the runtime has opened the subscription (startup), so redeem
/// it in an [`after_startup`](crate::runtime::RustStream::after_startup) hook, from
/// [`RunningApp`](crate::runtime::RunningApp)-scoped code, or inside a handler.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # fn demo() {
/// use ruststream::WithSeeker;
/// use ruststream::memory::{MemorySeeker, MemorySource};
///
/// let (source, token) = WithSeeker::<_, MemorySeeker>::attach(MemorySource::new("orders"));
/// // Not opened yet: redeeming before startup reports the pending state.
/// assert!(token.seeker().is_err());
/// # let _ = source;
/// # }
/// ```
pub struct SeekerToken<K> {
    slot: Arc<OnceLock<K>>,
}

impl<K> Clone for SeekerToken<K> {
    fn clone(&self) -> Self {
        Self {
            slot: Arc::clone(&self.slot),
        }
    }
}

impl<K> std::fmt::Debug for SeekerToken<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeekerToken")
            .field("resolved", &self.slot.get().is_some())
            .finish_non_exhaustive()
    }
}

impl<K: Seeker> SeekerToken<K> {
    /// Returns a clone of the live seeker.
    ///
    /// # Errors
    ///
    /// Returns [`SeekerPendingError`] before the runtime has opened the subscription (the app
    /// has not started yet, or the token's source was never mounted).
    pub fn seeker(&self) -> Result<K, SeekerPendingError> {
        self.slot.get().cloned().ok_or(SeekerPendingError)
    }
}

/// The subscription behind a [`SeekerToken`] has not been opened yet.
#[derive(Debug, Error)]
#[error("the subscription this seeker token belongs to has not been opened yet")]
#[non_exhaustive]
pub struct SeekerPendingError;
