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
    fmt,
    future::{Future, ready},
    marker::PhantomData,
    num::NonZeroU32,
};

use crate::{ConnectedBroker, Seekable, Seeker, Subscribe, Subscriber};

/// Who publishes the copies a subscription's retries are made of.
///
/// Every descriptor answers with [`SubscriptionSource::Copies`], and the answer is a closed set
/// of two: [`RuntimeCopies`] where this process publishes them, [`BrokerMoves`] where the server
/// or the client library moves the delivery itself. It decides two things at the mount site -
/// whether `.out_retry(policy)` has a publisher to name, and whether the runtime pairs one of its
/// own for every registration on that descriptor.
///
/// # Examples
///
/// ```
/// use ruststream::{BrokerMoves, CopyPath, RuntimeCopies};
///
/// fn declared<P: CopyPath>() -> &'static str {
///     std::any::type_name::<P>()
/// }
///
/// assert!(declared::<RuntimeCopies>().ends_with("RuntimeCopies"));
/// assert!(declared::<BrokerMoves>().ends_with("BrokerMoves"));
/// ```
pub trait CopyPath: copy_path::Sealed {}

mod copy_path {
    /// Keeps the set of copy paths at the two the runtime knows how to act on.
    pub trait Sealed {}

    impl Sealed for super::RuntimeCopies {}
    impl Sealed for super::BrokerMoves {}
}

/// The copy path of a subscription whose retries this process publishes.
///
/// The runtime pairs a retry publisher for every registration on such a descriptor, from the
/// broker's [`DefaultPublish`](crate::DefaultPublish) policy, and `.out_retry(policy)` replaces
/// it. A descriptor that declares it must also answer
/// [`redelivery_address`](SubscriptionSource::redelivery_address): that is where a deferred copy
/// goes, and a registration whose subscription cannot say refuses to start rather than publish
/// into nothing.
///
/// # Examples
///
/// ```
/// use ruststream::{Name, RuntimeCopies, Subscribe, SubscriptionSource};
///
/// // The by-name source publishes its copies here, so a mount site may name their publisher.
/// fn declared<C: Subscribe>() -> &'static str {
///     std::any::type_name::<<Name as SubscriptionSource<C>>::Copies>()
/// }
/// # let _: RuntimeCopies = RuntimeCopies;
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct RuntimeCopies;

impl CopyPath for RuntimeCopies {}

/// The copy path of a subscription whose deliveries the broker moves itself.
///
/// A queue with a delivery limit and a dead-letter exchange, a Pub/Sub subscription with a
/// dead-letter policy, an SQS redrive policy: the server applies the registration's declaration
/// and this process publishes nothing. `.out_retry(policy)` is a compile error on such a
/// descriptor, because there is no publisher to customise, and the runtime pairs none.
///
/// # Examples
///
/// ```
/// use ruststream::{BrokerMoves, CopyPath};
///
/// fn moves_at_the_broker<P: CopyPath>(_: P) {}
/// moves_at_the_broker(BrokerMoves);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct BrokerMoves;

impl CopyPath for BrokerMoves {}

/// What one registration declared about its retries: how many attempts a delivery gets, and
/// where it goes when they run out.
///
/// Built by the mount site's `max_attempts(..)` and `dead_letter(..)` steps and handed to the
/// subscription descriptor through
/// [`declare_retry`](SubscriptionSource::declare_retry) before the subscription opens. A
/// descriptor whose broker moves the message itself reads it here and configures the queue, the
/// subscription or the consumer with it; everywhere else the runtime applies it on the retry
/// path.
///
/// # Examples
///
/// ```
/// use ruststream::{RetryDeclaration, nonzero};
///
/// let declared = RetryDeclaration::new()
///     .with_max_attempts(nonzero!(5u32))
///     .with_dead_letter("orders.dead");
///
/// assert_eq!(declared.max_attempts().map(|n| n.get()), Some(5));
/// assert_eq!(declared.dead_letter(), Some("orders.dead"));
/// assert!(!declared.declares_nothing());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct RetryDeclaration {
    max_attempts: Option<NonZeroU32>,
    dead_letter: Option<Cow<'static, str>>,
}

impl RetryDeclaration {
    /// The declaration of a registration that declared nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::RetryDeclaration;
    ///
    /// assert!(RetryDeclaration::new().declares_nothing());
    /// ```
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_attempts: None,
            dead_letter: None,
        }
    }

    /// Caps the deliveries one message of this registration gets.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{RetryDeclaration, nonzero};
    ///
    /// let declared = RetryDeclaration::new().with_max_attempts(nonzero!(3u32));
    /// assert_eq!(declared.max_attempts().map(|n| n.get()), Some(3));
    /// ```
    #[must_use]
    pub fn with_max_attempts(mut self, attempts: NonZeroU32) -> Self {
        self.max_attempts = Some(attempts);
        self
    }

    /// Names where a delivery goes once the cap is reached.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::RetryDeclaration;
    ///
    /// let declared = RetryDeclaration::new().with_dead_letter("orders.dead");
    /// assert_eq!(declared.dead_letter(), Some("orders.dead"));
    /// ```
    #[must_use]
    pub fn with_dead_letter(mut self, destination: impl Into<Cow<'static, str>>) -> Self {
        self.dead_letter = Some(destination.into());
        self
    }

    /// How many deliveries one message gets, counting the first. `None` when the registration
    /// declared no cap.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::RetryDeclaration;
    ///
    /// assert_eq!(RetryDeclaration::new().max_attempts(), None);
    /// ```
    #[must_use]
    pub const fn max_attempts(&self) -> Option<NonZeroU32> {
        self.max_attempts
    }

    /// Where a delivery goes when the cap is reached. `None` when the registration named none,
    /// in which case the delivery at the cap is rejected instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::RetryDeclaration;
    ///
    /// assert_eq!(RetryDeclaration::new().dead_letter(), None);
    /// ```
    #[must_use]
    pub fn dead_letter(&self) -> Option<&str> {
        self.dead_letter.as_deref()
    }

    /// Whether the registration declared neither a cap nor a destination, which is what lets a
    /// broker leave its own topology untouched.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{RetryDeclaration, nonzero};
    ///
    /// assert!(RetryDeclaration::new().declares_nothing());
    /// assert!(!RetryDeclaration::new().with_max_attempts(nonzero!(2u32)).declares_nothing());
    /// ```
    #[must_use]
    pub const fn declares_nothing(&self) -> bool {
        self.max_attempts.is_none() && self.dead_letter.is_none()
    }
}

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

    /// Who publishes the copies this subscription's retries are made of:
    /// [`RuntimeCopies`] where this process does, [`BrokerMoves`] where the server or the client
    /// library moves the delivery itself.
    ///
    /// Answer [`RuntimeCopies`] unless the broker applies a delivery limit and a dead-letter
    /// destination on its own (a quorum queue with `x-delivery-limit` and an
    /// `x-dead-letter-exchange`, a Pub/Sub dead-letter policy, an SQS redrive policy, a Pulsar
    /// `DeadLetterPolicy`); a descriptor that answers so must also report a
    /// [`redelivery_address`](Self::redelivery_address). [`BrokerMoves`] makes `.out_retry(..)`
    /// a compile error at every mount site of this descriptor, because there is no publisher of
    /// this process's for the mount site to name.
    type Copies: CopyPath;

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

    /// Takes the registration's retry declaration into the descriptor, before the subscription
    /// opens.
    ///
    /// Called once per registration, with what the mount site declared with `max_attempts(..)`
    /// and `dead_letter(..)`. A broker that applies a delivery limit and a dead-letter
    /// destination itself reads them here and configures the subscription with them - and only
    /// when both are declared, because a native dead-letter policy needs both. The default keeps
    /// the descriptor as it was, which leaves the declaration to the runtime.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::borrow::Cow;
    ///
    /// use ruststream::{RetryDeclaration, RuntimeCopies, Subscribe, SubscriptionSource};
    ///
    /// /// A queue this broker declares itself, so it takes the declaration into its topology.
    /// #[derive(Debug, Clone)]
    /// struct Queue {
    ///     name: Cow<'static, str>,
    ///     delivery_limit: Option<u32>,
    ///     dead_letter: Option<Cow<'static, str>>,
    /// }
    ///
    /// impl<C: Subscribe> SubscriptionSource<C> for Queue {
    ///     type Subscriber = C::Subscriber;
    ///     type Copies = RuntimeCopies;
    ///
    ///     fn name(&self) -> &str {
    ///         &self.name
    ///     }
    ///
    ///     async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
    ///         connected.subscribe(&self.name).await
    ///     }
    ///
    ///     fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
    ///         self.delivery_limit = declaration.max_attempts().map(|n| n.get());
    ///         self.dead_letter = declaration.dead_letter().map(|d| Cow::Owned(d.to_owned()));
    ///         self
    ///     }
    /// }
    /// ```
    #[must_use]
    fn declare_retry(self, declaration: &RetryDeclaration) -> Self
    where
        Self: Sized,
    {
        let _ = declaration;
        self
    }

    /// Where a publish reaches this subscription again, for the runtime's deferred `retry_after`
    /// fallback. `None` means the broker cannot say.
    ///
    /// A broker without native delayed redelivery gets the delay honoured by a copy the runtime
    /// publishes after it: this is the name that copy goes to. Answer with the name a publisher
    /// bound to the same broker must use, resolving it against `connected` when only the live
    /// connection knows it (a Pub/Sub subscription has to be looked up to learn its topic).
    /// Called once per subscription at startup, never on the delivery path.
    ///
    /// The default answers `None`, which suits a descriptor whose
    /// [`Copies`](Self::Copies) are [`BrokerMoves`]: nothing of this process's is published for
    /// it. A [`RuntimeCopies`] descriptor owes an address, and a registration over one that
    /// answers `None` refuses to start. A subscription's name is not an address: where a
    /// subscription and a publish destination are separate resources, answering with it would
    /// publish the copy into nothing and lose the message under load.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectedBroker::Error`] when the broker has to be asked and the request fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "memory")]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use ruststream::memory::{MemoryBroker, MemorySource};
    /// use ruststream::{Broker, RedeliveryAddress, SubscriptionSource};
    ///
    /// let connected = MemoryBroker::new().connect().await?;
    /// let source = MemorySource::new("orders");
    ///
    /// // The in-memory subject is both what a subscription reads and what a publish reaches.
    /// assert_eq!(
    ///     source.redelivery_address(&connected).await?,
    ///     Some(RedeliveryAddress::new("orders")),
    /// );
    /// # Ok(())
    /// # }
    /// ```
    fn redelivery_address(
        &self,
        connected: &C,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, C::Error>> + Send {
        let _ = connected;
        async { Ok(None) }
    }
}

/// The name a deferred redelivery of one subscription is published to.
///
/// Reported by [`SubscriptionSource::redelivery_address`]. It is a publish destination, not a
/// subscription name: the two coincide on a NATS subject or a Kafka topic and differ wherever a
/// subscription is a resource of its own, so the runtime never substitutes one for the other.
///
/// # Examples
///
/// ```
/// use ruststream::RedeliveryAddress;
///
/// let address = RedeliveryAddress::new("orders");
/// assert_eq!(address.as_str(), "orders");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RedeliveryAddress(Cow<'static, str>);

impl RedeliveryAddress {
    /// The address a deferred copy is published under.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::RedeliveryAddress;
    ///
    /// let address = RedeliveryAddress::new(String::from("orders"));
    /// assert_eq!(address.to_string(), "orders");
    /// ```
    #[must_use]
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        Self(name.into())
    }

    /// Borrows the address as a string.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::RedeliveryAddress;
    ///
    /// fn publishes_to(address: &RedeliveryAddress) -> &str {
    ///     address.as_str()
    /// }
    ///
    /// assert_eq!(publishes_to(&RedeliveryAddress::new("orders")), "orders");
    /// ```
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RedeliveryAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
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
/// for that kind.
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
/// the mount site, so the definition's source starts as `Unnamed<Kind>`. It implements no
/// [`SubscriptionSource`]: mounting a definition that was never named is a compile
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
    // A name is one string on every broker that takes one, and a publish to it reaches the
    // subscription: the runtime is what publishes a copy there.
    type Copies = RuntimeCopies;

    fn name(&self) -> &str {
        &self.0
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        connected.subscribe(&self.0).await
    }

    /// The broker answers for its own names: only it knows whether a publish to a name it
    /// subscribes by comes back to that subscription (see
    /// [`Subscribe::redelivery_address`]). The answer needs no I/O, so the future is ready.
    fn redelivery_address(
        &self,
        connected: &C,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, C::Error>> + Send {
        ready(Ok(connected.redelivery_address(&self.0)))
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
/// # #[cfg(all(feature = "memory", feature = "macros"))]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use futures::StreamExt;
/// use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySource, Retention};
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Broker, IncomingMessage, Outgoing, Serialized, StartAt, nonzero};
/// use ruststream::{Subscriber, SubscriptionSource};
///
/// // An audit entry is opaque bytes, so it declares itself a serialized type and no codec
/// // runs on it.
/// #[derive(Outgoing, Serialized)]
/// struct Entry(Vec<u8>);
///
/// // Opening at a position replays what the broker kept, so this one keeps a window.
/// let connected = MemoryBroker::retaining(Retention::Messages(nonzero!(8)))
///     .connect()
///     .await?;
/// let publisher = connected.publisher();
/// publisher.message(&Entry(b"one".to_vec())).to("audit").publish().await?;
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

    /// Replaces the wrapped source with `f`'s result, keeping the position.
    ///
    /// A broker's own settings trait is bound to its descriptor type, and a `start_at(..)` in the
    /// attribute (or at the mount site) puts this wrapper between the builder and that
    /// descriptor - so those methods are no longer in scope on the source the chain carries. This
    /// is how a second impl over `StartAt<Descriptor, P>` reaches them again, in one line per
    /// setting:
    /// `self.map_source(|source| source.map_inner(|inner| inner.durable("workers")))`.
    ///
    /// The source type may change, which is what a descriptor that wraps another one (the
    /// client-side [`Buffered`](crate::Buffered) batcher, a broker's own adapter) needs.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "memory")]
    /// # {
    /// use std::time::Duration;
    ///
    /// use ruststream::memory::{ConnectedMemoryBroker, MemoryPosition, MemorySource, Retaining};
    /// use ruststream::{Buffered, StartAt, SubscriptionSource};
    ///
    /// // What `start_at(..)` builds at the mount site: the broker's descriptor, wrapped.
    /// let source = StartAt::new(MemorySource::new("orders"), MemoryPosition::start());
    ///
    /// // The broker's own setting reaches the descriptor underneath - here the client-side
    /// // batch buffer a transport with no batching of its own wraps it in - and the position
    /// // the mount site named stays where it was.
    /// let buffered =
    ///     source.map_inner(|inner| Buffered::new(inner).max_wait(Duration::from_millis(25)));
    ///
    /// assert_eq!(
    ///     SubscriptionSource::<ConnectedMemoryBroker<Retaining>>::name(&buffered),
    ///     "orders",
    /// );
    /// # }
    /// ```
    #[must_use]
    pub fn map_inner<T>(self, f: impl FnOnce(S) -> T) -> StartAt<T, P> {
        StartAt {
            inner: f(self.inner),
            position: self.position,
        }
    }
}

impl<S, P> fmt::Debug for StartAt<S, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StartAt").finish_non_exhaustive()
    }
}

// Same as the buffering decorator: it wraps a source the mount site already has, so it is never
// the answer to "which source does this broker take" and stays out of the suggested list.
#[diagnostic::do_not_recommend]
impl<C, S, P> SubscriptionSource<C> for StartAt<S, P>
where
    C: ConnectedBroker,
    // `Send` on the pieces keeps the returned future `Send`, as the trait's RPITIT promises;
    // `Sync` on the wrapped source is what lets the redelivery address be asked for by reference.
    S: SubscriptionSource<C> + Send + Sync,
    S::Subscriber: Seekable,
    // A rejected reposition surfaces as this source's subscribe error, so the seeker must
    // report the connected broker's error type (broker crates use one error type for both).
    <S::Subscriber as Seekable>::Seeker: Seeker<Position = P, Error = C::Error>,
    P: Send,
{
    type Subscriber = S::Subscriber;
    type Copies = S::Copies;

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn declare_retry(self, declaration: &RetryDeclaration) -> Self {
        self.map_inner(|inner| inner.declare_retry(declaration))
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        let subscriber = self.inner.subscribe(connected).await?;
        // Sought before the subscriber leaves this call: per the `Seeker::seek` contract the
        // next delivery reflects the position, so the dispatch loop never observes a message
        // from before it.
        subscriber.seeker().seek(self.position).await?;
        Ok(subscriber)
    }

    /// A start position changes where the subscription opens, not where a publish reaches it.
    fn redelivery_address(
        &self,
        connected: &C,
    ) -> impl Future<Output = Result<Option<RedeliveryAddress>, C::Error>> + Send {
        self.inner.redelivery_address(connected)
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use super::*;
    use crate::memory::{
        ConnectedMemoryBroker, MemoryBroker, MemoryPosition, MemorySource, Retaining, Retention,
    };
    use crate::{Broker, Buffered, nonzero};

    /// The generic clone keeps the assertion honest: a `Copy` placeholder would otherwise make
    /// the call read as redundant at the call site.
    fn clone_of<T: Clone>(value: &T) -> T {
        value.clone()
    }

    #[test]
    fn an_unnamed_placeholder_builds_its_kind_once_the_name_arrives() {
        let placeholder = Unnamed::<Name>::default();
        assert!(format!("{placeholder:?}").contains("Unnamed"));

        let named: Name = clone_of(&placeholder).into_named("orders");
        assert_eq!(
            SubscriptionSource::<ConnectedMemoryBroker>::name(&named),
            "orders"
        );
    }

    #[test]
    fn a_start_position_decorates_the_source_it_wraps() {
        let source = StartAt::new(MemorySource::new("orders"), MemoryPosition::start());
        assert_eq!(
            SubscriptionSource::<ConnectedMemoryBroker<Retaining>>::name(&source),
            "orders"
        );
        assert!(format!("{source:?}").contains("StartAt"));
    }

    /// The name source has no address of its own: the broker's answer for that name is what it
    /// reports, and a decorator reports whatever it wraps.
    #[tokio::test]
    async fn a_source_reports_the_address_its_broker_gives_the_name() {
        let connected = MemoryBroker::new()
            .connect()
            .await
            .expect("the in-memory broker connects");
        let address = RedeliveryAddress::new("orders");
        assert_eq!(address.as_str(), "orders");
        assert_eq!(address.to_string(), "orders");

        assert_eq!(
            Name::new("orders")
                .redelivery_address(&connected)
                .await
                .expect("the in-memory broker answers without a lookup"),
            Some(address.clone()),
        );
        assert_eq!(
            Buffered::new(MemorySource::new("orders"))
                .redelivery_address(&connected)
                .await
                .expect("client-side batching changes no address"),
            Some(address.clone()),
        );

        // The start-position decorator only wraps a source whose subscriptions replay, so it is
        // asked on a retaining broker.
        let retaining = MemoryBroker::retaining(Retention::Messages(nonzero!(8)))
            .connect()
            .await
            .expect("the in-memory broker connects");
        assert_eq!(
            StartAt::new(MemorySource::new("orders"), MemoryPosition::start())
                .redelivery_address(&retaining)
                .await
                .expect("a start position changes no address"),
            Some(address),
        );
    }

    /// The wrapper hides the descriptor a broker's settings trait is bound to, so it hands it
    /// back: the mapped source is what the subscription opens on, and the position is untouched.
    #[test]
    fn a_start_position_hands_back_the_source_it_wraps() {
        let renamed =
            StartAt::new(MemorySource::new("orders"), MemoryPosition::start()).map_inner(|inner| {
                assert_eq!(
                    SubscriptionSource::<ConnectedMemoryBroker<Retaining>>::name(&inner),
                    "orders",
                );
                MemorySource::new("orders-7")
            });
        assert_eq!(
            SubscriptionSource::<ConnectedMemoryBroker<Retaining>>::name(&renamed),
            "orders-7",
        );
    }
}
