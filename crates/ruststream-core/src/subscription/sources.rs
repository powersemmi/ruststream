//! The subscription sources the core ships: a bare name, a kind left unnamed until the mount
//! site names it, and a start position wrapped around another source.

use std::{borrow::Cow, fmt, future::ready, marker::PhantomData};

use crate::{ConnectedBroker, DeclareRetryError, Seekable, Seeker, Subscribe};

use super::{
    AddressedCopies, RedeliveryAddress, RedeliveryAddressed, RetryDeclaration, SubscriptionSource,
};

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
/// use std::borrow::Cow;
///
/// use ruststream::FromName;
///
/// /// A broker crate's stream descriptor: one key names it, so `#[subscriber(RedisStream)]`
/// /// leaves the key to the mount site's `.name(..)`.
/// #[derive(Debug, Clone)]
/// pub struct RedisStream {
///     key: Cow<'static, str>,
///     group: Option<String>,
/// }
///
/// impl FromName for RedisStream {
///     fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
///         Self {
///             key: name.into(),
///             group: None,
///         }
///     }
/// }
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
/// error, not a startup one. [`name`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.SubscriberSettings.html#method.name) replaces it with
/// the kind itself, built through [`FromName`].
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// /// The kind is fixed here; its source stays `Unnamed<MemorySource>` until the mount names it.
/// #[subscriber(MemorySource)]
/// async fn audit(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "audited");
///     HandlerOutcome::ack()
/// }
///
/// fn app(region: &str) -> RustStream {
///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(audit.name(format!("orders.{region}")));
///     })
/// }
/// # }
/// # fn main() {}
/// ```
///
/// A definition mounted without a name does not compile:
///
/// ```compile_fail
/// # #[cfg(not(feature = "json"))]
/// # compile_error!("the example needs the macros, memory and json features");
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(MemorySource)]
/// async fn audit(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "audited");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         // Never named: there is no subscription to open.
///         b.include(audit);
///     })
/// }
/// # }
/// # fn main() {}
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
    // Only the broker knows whether a publish under a subscribe name reaches the subscription
    // opened by it: a subject or a topic is both, an MQTT filter is neither.
    type Copies = C::Copies;

    fn name(&self) -> &str {
        &self.0
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        connected.subscribe(&self.0).await
    }

    /// A name carries no topology of its own, so what the registration declared is the broker's
    /// to take: it answers for the subscription this name opens.
    fn declare_retry_on(
        &self,
        connected: &C,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        connected.declare_retry(&self.0, declaration)
    }
}

/// Where the broker says a subscribe name is also a publish destination, the name is the address,
/// and no lookup stands between the descriptor and the answer.
impl<C: Subscribe<Copies = AddressedCopies>> RedeliveryAddressed<C> for Name {
    fn redelivery_address(
        &self,
        _connected: &C,
    ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> + Send {
        ready(Ok(RedeliveryAddress::new(self.0.clone())))
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
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Entry {
///     id: u64,
/// }
///
/// #[subscriber]
/// async fn rebuild(entry: &Entry) -> HandlerOutcome {
///     tracing::info!(entry.id, "replayed");
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     // Opening at a position replays what the broker kept, so this one keeps a window.
///     let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(1024)));
///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
///         // Wraps the descriptor in `StartAt`: every start replays the whole retained log.
///         b.include(rebuild.name("audit").start_at(MemoryPosition::start()));
///     })
/// }
/// # }
/// # fn main() {}
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
    /// client-side [`Buffered`] batcher, a broker's own adapter) needs.
    ///
    /// [`Buffered`]: https://docs.rs/ruststream/latest/ruststream/struct.Buffered.html
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use ruststream::memory::prelude::*;
    /// use ruststream::runtime::{Declared, SubscriberBuilder};
    /// use ruststream::{Buffered, StartAt};
    /// use serde::Deserialize;
    ///
    /// /// A broker crate's setting that still reaches its descriptor once the mount site wrapped
    /// /// it in `StartAt`: the position stays where the mount site put it.
    /// pub trait Linger {
    ///     type Out;
    ///     fn linger(self, wait: Duration) -> Self::Out;
    /// }
    ///
    /// impl<Def: Declared, State, DefCodec> Linger
    ///     for SubscriberBuilder<Def, StartAt<MemorySource, MemoryPosition>, State, DefCodec>
    /// {
    ///     type Out = SubscriberBuilder<
    ///         Def,
    ///         StartAt<Buffered<MemorySource>, MemoryPosition>,
    ///         State,
    ///         DefCodec,
    ///     >;
    ///
    ///     fn linger(self, wait: Duration) -> Self::Out {
    ///         self.map_source(|source| {
    ///             source.map_inner(|inner| Buffered::new(inner).max_wait(wait))
    ///         })
    ///     }
    /// }
    ///
    /// #[derive(Deserialize)]
    /// struct Entry {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber(MemorySource)]
    /// async fn rebuild(entries: &[Entry]) -> HandlerOutcome {
    ///     tracing::info!(count = entries.len(), "replayed a batch");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(1024)));
    ///     RustStream::new(AppInfo::new("audit", "0.1.0")).with_broker(broker, |b| {
    ///         b.include(
    ///             rebuild
    ///                 .name("audit")
    ///                 .batch(nonzero!(64))
    ///                 .start_at(MemoryPosition::start())
    ///                 .linger(Duration::from_millis(25)),
    ///         );
    ///     })
    /// }
    /// # }
    /// # fn main() {}
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

    /// A start position changes where the subscription opens, not what the broker is told about
    /// its retries.
    fn declare_retry_on(
        &self,
        connected: &C,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        self.inner.declare_retry_on(connected, declaration)
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

/// A start position changes where the subscription opens, not where a publish reaches it.
impl<C, S, P> RedeliveryAddressed<C> for StartAt<S, P>
where
    Self: SubscriptionSource<C, Copies = AddressedCopies>,
    C: ConnectedBroker,
    S: RedeliveryAddressed<C> + Send + Sync,
{
    fn redelivery_address(
        &self,
        connected: &C,
    ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> + Send {
        self.inner.redelivery_address(connected)
    }
}
