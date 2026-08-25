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

use std::{borrow::Cow, fmt, future::Future, marker::PhantomData};

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
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not open a subscription on `{C}`",
    label = "not a subscription source for this broker",
    note = "a subscriber left unnamed carries `Unnamed<..>` until the mount site names it: \
            `b.include(handle.name(\"orders\"))`",
    note = "otherwise the source belongs to a different broker than the one being mounted on"
)]
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

/// A subscription kind identified by a name and nothing else.
///
/// Every source in the broker family is constructed from one string - `new(topic)` for Kafka,
/// `new(subject)` for NATS, `new(key)` or `new(channel)` for Redis - and this trait says so, so
/// the mount site can build the kind once the name arrives:
/// `#[subscriber(RedisStream)]` names the kind and leaves the value to
/// `b.include(handle.name(subject))`.
///
/// A kind that genuinely needs more than a name to exist (a Pulsar source takes a topic *and* a
/// subscription name) does not implement it, and the name-only attribute form does not compile
/// for that kind. That is the honest boundary: nothing is ever built from thin air, and no
/// source needs a `Default` that would make a stream without a key representable.
///
/// # Examples
///
/// ```
/// use ruststream::{FromName, Name};
///
/// fn build<S: FromName>(name: &'static str) -> S {
///     S::from_name(name)
/// }
///
/// let source: Name = build("orders");
/// # let _ = source;
/// ```
pub trait FromName {
    /// Builds the source bound to `name`.
    #[must_use]
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self;
}

impl FromName for Name {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name)
    }
}

/// The stand-in a definition carries while its subscription has no name yet.
///
/// `#[subscriber]` and `#[subscriber(Kind)]` fix the subscription *kind* and leave its value to
/// the mount site, so the definition's source starts as `Unnamed<Kind>`. It deliberately
/// implements no [`SubscriptionSource`]: mounting a definition that was never named is a compile
/// error, not a startup one. [`name`](crate::runtime::SubscriberSettings::name) replaces it with
/// the kind itself, built through [`FromName`].
///
/// # Examples
///
/// ```
/// use ruststream::{FromName, Name, Unnamed};
///
/// let placeholder: Unnamed<Name> = Unnamed::new();
/// let named: Name = placeholder.into_named("orders");
/// # let _ = named;
/// ```
pub struct Unnamed<S>(PhantomData<fn() -> S>);

impl<S> Unnamed<S> {
    /// The placeholder for a subscription of kind `S` whose name is still missing.
    #[must_use]
    pub const fn new() -> Self {
        Self(PhantomData)
    }

    /// Builds the subscription kind now that its name is known.
    #[must_use]
    pub fn into_named(self, name: impl Into<Cow<'static, str>>) -> S
    where
        S: FromName,
    {
        S::from_name(name)
    }
}

impl<S> Default for Unnamed<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Clone for Unnamed<S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S> Copy for Unnamed<S> {}

impl<S> fmt::Debug for Unnamed<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Unnamed").finish_non_exhaustive()
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
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Broker, IncomingMessage, StartAt};
/// use ruststream::{Subscriber, SubscriptionSource};
///
/// let connected = MemoryBroker::new().connect().await?;
/// let publisher = connected.publisher();
/// publisher.raw(b"one").to("audit").publish().await?;
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
#[derive(Clone)]
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

impl<S, P> fmt::Debug for StartAt<S, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

#[cfg(all(test, feature = "memory"))]
mod tests {
    use super::*;
    use crate::memory::{ConnectedMemoryBroker, MemoryPosition, MemorySource};

    #[test]
    fn a_start_position_decorates_the_source_it_wraps() {
        let source = StartAt::new(MemorySource::new("orders"), MemoryPosition::start());
        assert_eq!(
            SubscriptionSource::<ConnectedMemoryBroker>::name(&source),
            "orders"
        );
        assert!(format!("{source:?}").contains("StartAt"));
    }
}
