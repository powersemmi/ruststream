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

#[cfg(feature = "asyncapi")]
use crate::asyncapi::Bindings;
use crate::{ConnectedBroker, DeclareRetryError, Seekable, Seeker, Subscribe, Subscriber};

/// Who publishes the copies a subscription's retries are made of, and who names where they go.
///
/// Every descriptor answers with [`SubscriptionSource::Copies`], and the answer is a closed set of
/// three: [`AddressedCopies`] where this process publishes them and the descriptor knows the
/// destination, [`NamedCopies`] where this process publishes them and the mount site names the
/// destination, [`BrokerMoves`] where the server or the client library moves the delivery itself.
/// It decides three things at the mount site - whether `.out_retry(policy)` has a publisher to
/// customise, whether the runtime pairs one of its own, and whether the registration owes a
/// destination.
///
/// # Examples
///
/// ```
/// use std::borrow::Cow;
///
/// use ruststream::{
///     AddressedCopies, RedeliveryAddress, RedeliveryAddressed, Subscribe, SubscriptionSource,
/// };
///
/// /// A broker crate's topic descriptor: the topic it reads is also where a copy reaches it again,
/// /// so this process publishes the copies and the descriptor knows where to.
/// #[derive(Debug, Clone)]
/// struct Topic {
///     name: Cow<'static, str>,
/// }
///
/// impl<C: Subscribe> SubscriptionSource<C> for Topic {
///     type Subscriber = C::Subscriber;
///     type Copies = AddressedCopies;
///
///     fn name(&self) -> &str {
///         &self.name
///     }
///
///     async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
///         connected.subscribe(&self.name).await
///     }
/// }
///
/// impl<C: Subscribe> RedeliveryAddressed<C> for Topic {
///     async fn redelivery_address(&self, _connected: &C) -> Result<RedeliveryAddress, C::Error> {
///         Ok(RedeliveryAddress::new(self.name.clone()))
///     }
/// }
/// ```
pub trait CopyPath: copy_path::Sealed {}

pub(crate) mod copy_path {
    use crate::DeclareRetryError;

    /// Keeps the set of copy paths at the three the runtime knows how to act on, and answers for
    /// each of them what a registration mounted by a bare subscription name may declare on a
    /// broker that maps no declaration of its own.
    pub trait Sealed {
        /// Whether a non-empty retry declaration survives on this copy path when nothing but the
        /// default takes it.
        fn declared_by_name(broker: &'static str) -> Result<(), DeclareRetryError>;
    }

    impl Sealed for super::AddressedCopies {
        /// The runtime publishes this subscription's copies, so it applies the declaration.
        fn declared_by_name(_broker: &'static str) -> Result<(), DeclareRetryError> {
            Ok(())
        }
    }

    impl Sealed for super::NamedCopies {
        /// The same: the mount site names where the copies go, and the runtime publishes them.
        fn declared_by_name(_broker: &'static str) -> Result<(), DeclareRetryError> {
            Ok(())
        }
    }

    impl Sealed for super::BrokerMoves {
        /// Nothing in this process moves a spent delivery here, so a declaration nobody maps is a
        /// declaration nobody applies.
        fn declared_by_name(broker: &'static str) -> Result<(), DeclareRetryError> {
            Err(DeclareRetryError::Unsupported { broker })
        }
    }
}

/// The copy path of a subscription whose retries this process publishes, to a destination the
/// descriptor knows: a subject, a topic, a queue, a stream key.
///
/// A descriptor that declares it implements [`RedeliveryAddressed`], which is where that
/// destination comes from - the address is a property of the type, not an answer checked at
/// startup. The runtime pairs a retry publisher for every registration on such a descriptor, from
/// the broker's [`DefaultPublish`](crate::DefaultPublish) policy; `.out_retry(policy)` replaces
/// it, and `.to(name)` on that position overrides the address.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn reconcile(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "not ready yet");
///     HandlerOutcome::retry_after(Duration::from_secs(30))
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         // The subject is the copies' address; `.to(..)` sends them to a queue of their own.
///         b.include(reconcile).out_retry(Publish).to("orders.retry");
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct AddressedCopies;

impl CopyPath for AddressedCopies {}

/// The copy path of a subscription whose retries this process publishes but cannot address: a
/// wildcard subject, an MQTT filter, a Pulsar pattern, a list of topics.
///
/// One subscription like this reads many destinations, so the descriptor has no single answer and
/// the mount site names one: `.out_retry(policy).to("orders")` for a fixed destination, or a
/// publish transform that names it per delivery. A registration that names neither does not
/// compile.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber(MemoryPattern::new("orders.*"))]
/// async fn reconcile(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "not ready yet");
///     HandlerOutcome::retry_after(Duration::from_secs(30))
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         // A pattern reads many subjects and names none, so the mount site says where copies go.
///         b.include(reconcile).out_retry(Publish).to("orders.retry");
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct NamedCopies;

impl CopyPath for NamedCopies {}

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
/// use std::borrow::Cow;
///
/// use ruststream::{BrokerMoves, ConnectedBroker, RetryDeclaration};
/// use ruststream::{Subscribe, SubscriptionSource};
///
/// /// The broker crate's own channel call: declares a queue with its limit and dead-letter
/// /// exchange.
/// trait DeclareQueue: ConnectedBroker {
///     fn declare_queue(
///         &self,
///         name: &str,
///         delivery_limit: Option<u32>,
///         dead_letter: Option<&str>,
///     ) -> impl Future<Output = Result<(), Self::Error>> + Send;
/// }
///
/// /// A broker crate's queue with a delivery limit and a dead-letter exchange: the server moves a
/// /// spent delivery itself, so this process publishes no copies.
/// #[derive(Debug, Clone)]
/// struct Queue {
///     name: Cow<'static, str>,
///     delivery_limit: Option<u32>,
///     dead_letter: Option<String>,
/// }
///
/// impl<C: Subscribe + DeclareQueue> SubscriptionSource<C> for Queue {
///     type Subscriber = C::Subscriber;
///     type Copies = BrokerMoves;
///
///     fn name(&self) -> &str {
///         &self.name
///     }
///
///     async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
///         connected
///             .declare_queue(&self.name, self.delivery_limit, self.dead_letter.as_deref())
///             .await?;
///         connected.subscribe(&self.name).await
///     }
///
///     fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
///         self.delivery_limit = declaration.max_attempts().map(|n| n.get());
///         self.dead_letter = declaration.dead_letter().map(str::to_owned);
///         self
///     }
/// }
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
/// use std::borrow::Cow;
///
/// use ruststream::{BrokerMoves, ConnectedBroker, RetryDeclaration};
/// use ruststream::{Subscribe, SubscriptionSource};
///
/// /// The broker crate's own channel call: declares a queue with its arguments.
/// trait DeclareQueue: ConnectedBroker {
///     fn declare_queue(
///         &self,
///         name: &str,
///         arguments: &[(&'static str, String)],
///     ) -> impl Future<Output = Result<(), Self::Error>> + Send;
/// }
///
/// /// A broker crate's queue: the server enforces the delivery limit and dead-letters the
/// /// delivery that exhausts it.
/// #[derive(Debug, Clone)]
/// struct Queue {
///     name: Cow<'static, str>,
///     arguments: Vec<(&'static str, String)>,
/// }
///
/// impl<C: Subscribe + DeclareQueue> SubscriptionSource<C> for Queue {
///     type Subscriber = C::Subscriber;
///     type Copies = BrokerMoves;
///
///     fn name(&self) -> &str {
///         &self.name
///     }
///
///     async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
///         connected.declare_queue(&self.name, &self.arguments).await?;
///         connected.subscribe(&self.name).await
///     }
///
///     fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
///         // The queue's own arguments carry what the mount site declared.
///         if let Some(limit) = declaration.max_attempts() {
///             self.arguments.push(("x-delivery-limit", limit.to_string()));
///         }
///         if let Some(exchange) = declaration.dead_letter() {
///             self.arguments
///                 .push(("x-dead-letter-exchange", exchange.to_owned()));
///         }
///         self
///     }
/// }
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
    /// use ruststream::{DeclareRetryError, RetryDeclaration, Subscribe, nonzero};
    ///
    /// /// What the runtime declares for
    /// /// `b.include(reconcile).max_attempts(nonzero!(5)).dead_letter("orders.dead")`.
    /// fn declare<C: Subscribe>(connected: &C) -> Result<(), DeclareRetryError> {
    ///     let declared = RetryDeclaration::new()
    ///         .with_max_attempts(nonzero!(5u32))
    ///         .with_dead_letter("orders.dead");
    ///     connected.declare_retry("orders", &declared)
    /// }
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
    /// use ruststream::{DeclareRetryError, RetryDeclaration, Subscribe, nonzero};
    ///
    /// /// What the runtime declares for `b.include(reconcile).max_attempts(nonzero!(5))`.
    /// fn declare<C: Subscribe>(connected: &C) -> Result<(), DeclareRetryError> {
    ///     let declared = RetryDeclaration::new().with_max_attempts(nonzero!(5u32));
    ///     connected.declare_retry("orders", &declared)
    /// }
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
    /// use ruststream::{DeclareRetryError, RetryDeclaration, Subscribe};
    ///
    /// /// What the runtime declares for `b.include(reconcile).dead_letter("orders.dead")`.
    /// fn declare<C: Subscribe>(connected: &C) -> Result<(), DeclareRetryError> {
    ///     let declared = RetryDeclaration::new().with_dead_letter("orders.dead");
    ///     connected.declare_retry("orders", &declared)
    /// }
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
    /// use std::borrow::Cow;
    ///
    /// use ruststream::{BrokerMoves, RetryDeclaration, Subscribe, SubscriptionSource};
    ///
    /// /// A broker crate's queue: the server enforces the delivery limit and dead-letters the
    /// /// delivery that exhausts it.
    /// #[derive(Debug, Clone)]
    /// struct Queue {
    ///     name: Cow<'static, str>,
    ///     arguments: Vec<(&'static str, String)>,
    /// }
    ///
    /// impl<C: Subscribe> SubscriptionSource<C> for Queue {
    ///     type Subscriber = C::Subscriber;
    ///     type Copies = BrokerMoves;
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
    ///         if let Some(limit) = declaration.max_attempts() {
    ///             self.arguments.push(("x-delivery-limit", limit.to_string()));
    ///         }
    ///         self
    ///     }
    /// }
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
    /// use std::borrow::Cow;
    ///
    /// use ruststream::{BrokerMoves, RetryDeclaration, Subscribe, SubscriptionSource};
    ///
    /// /// A broker crate's queue: the server enforces the delivery limit and dead-letters the
    /// /// delivery that exhausts it.
    /// #[derive(Debug, Clone)]
    /// struct Queue {
    ///     name: Cow<'static, str>,
    ///     arguments: Vec<(&'static str, String)>,
    /// }
    ///
    /// impl<C: Subscribe> SubscriptionSource<C> for Queue {
    ///     type Subscriber = C::Subscriber;
    ///     type Copies = BrokerMoves;
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
    ///         if let Some(exchange) = declaration.dead_letter() {
    ///             self.arguments
    ///                 .push(("x-dead-letter-exchange", exchange.to_owned()));
    ///         }
    ///         self
    ///     }
    /// }
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
    /// use std::borrow::Cow;
    ///
    /// use ruststream::{BrokerMoves, RetryDeclaration, Subscribe, SubscriptionSource};
    ///
    /// /// A broker crate's queue: the server enforces the delivery limit and dead-letters the
    /// /// delivery that exhausts it.
    /// #[derive(Debug, Clone)]
    /// struct Queue {
    ///     name: Cow<'static, str>,
    ///     arguments: Vec<(&'static str, String)>,
    /// }
    ///
    /// impl<C: Subscribe> SubscriptionSource<C> for Queue {
    ///     type Subscriber = C::Subscriber;
    ///     type Copies = BrokerMoves;
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
    ///         // An empty declaration leaves the queue as the operator configured it.
    ///         if declaration.declares_nothing() {
    ///             return self;
    ///         }
    ///         if let Some(limit) = declaration.max_attempts() {
    ///             self.arguments.push(("x-delivery-limit", limit.to_string()));
    ///         }
    ///         if let Some(exchange) = declaration.dead_letter() {
    ///             self.arguments
    ///                 .push(("x-dead-letter-exchange", exchange.to_owned()));
    ///         }
    ///         self
    ///     }
    /// }
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

    /// Who publishes the copies this subscription's retries are made of, and who names where they
    /// go: [`AddressedCopies`], [`NamedCopies`] or [`BrokerMoves`].
    ///
    /// Answer [`AddressedCopies`] where one subscription reads one destination this process can
    /// publish to, and implement [`RedeliveryAddressed`] beside it - the copy path requires it.
    /// Answer [`NamedCopies`] where the subscription reads many (a wildcard, a filter, a pattern,
    /// a list) and the mount site has to name one. Answer [`BrokerMoves`] where the broker applies
    /// a delivery limit and a dead-letter destination on its own (a quorum queue with
    /// `x-delivery-limit` and an `x-dead-letter-exchange`, a Pub/Sub dead-letter policy, an SQS
    /// redrive policy, a Pulsar `DeadLetterPolicy`); that makes `.out_retry(..)` a compile error at
    /// every mount site of this descriptor, because there is no publisher of this process's to
    /// customise.
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
    /// use ruststream::{AddressedCopies, ConnectedBroker, RedeliveryAddress};
    /// use ruststream::{RedeliveryAddressed, RetryDeclaration, Subscribe, SubscriptionSource};
    ///
    /// /// The broker crate's own channel call: declares a queue with its limit and dead-letter
    /// /// destination.
    /// trait DeclareQueue: ConnectedBroker {
    ///     fn declare_queue(
    ///         &self,
    ///         name: &str,
    ///         delivery_limit: Option<u32>,
    ///         dead_letter: Option<&str>,
    ///     ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    /// }
    ///
    /// /// A queue this broker declares itself, so it takes the declaration into its topology.
    /// #[derive(Debug, Clone)]
    /// struct Queue {
    ///     name: Cow<'static, str>,
    ///     delivery_limit: Option<u32>,
    ///     dead_letter: Option<Cow<'static, str>>,
    /// }
    ///
    /// impl<C: Subscribe + DeclareQueue> SubscriptionSource<C> for Queue {
    ///     type Subscriber = C::Subscriber;
    ///     type Copies = AddressedCopies;
    ///
    ///     fn name(&self) -> &str {
    ///         &self.name
    ///     }
    ///
    ///     async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
    ///         connected
    ///             .declare_queue(&self.name, self.delivery_limit, self.dead_letter.as_deref())
    ///             .await?;
    ///         connected.subscribe(&self.name).await
    ///     }
    ///
    ///     fn declare_retry(mut self, declaration: &RetryDeclaration) -> Self {
    ///         self.delivery_limit = declaration.max_attempts().map(|n| n.get());
    ///         self.dead_letter = declaration.dead_letter().map(|d| Cow::Owned(d.to_owned()));
    ///         self
    ///     }
    /// }
    ///
    /// // The queue is one destination, so it answers where a copy reaches it again.
    /// impl<C: Subscribe + DeclareQueue> RedeliveryAddressed<C> for Queue {
    ///     async fn redelivery_address(&self, _connected: &C) -> Result<RedeliveryAddress, C::Error> {
    ///         Ok(RedeliveryAddress::new(self.name.clone()))
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

    /// Carries the registration's declaration to the broker, at the same point and before the
    /// subscription opens.
    ///
    /// A descriptor of your own reads the declaration in [`declare_retry`](Self::declare_retry)
    /// and turns it into topology there, so it leaves this one at its default, which carries
    /// nothing. The core's [`Name`] source is the one descriptor that overrides it: a name alone
    /// says nothing about what a cap and a dead-letter destination mean for the subscription, so
    /// it hands both to [`Subscribe::declare_retry`], where the broker answers for the name it is
    /// about to open.
    ///
    /// # Errors
    ///
    /// Returns [`DeclareRetryError`] when the broker refuses the declaration, which fails the
    /// registration at startup, naming the subscription.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{ConnectedBroker, DeclareRetryError, RetryDeclaration, SubscriptionSource};
    ///
    /// fn declare<C, S>(
    ///     source: &S,
    ///     connected: &C,
    ///     declaration: &RetryDeclaration,
    /// ) -> Result<(), DeclareRetryError>
    /// where
    ///     C: ConnectedBroker,
    ///     S: SubscriptionSource<C>,
    /// {
    ///     source.declare_retry_on(connected, declaration)?;
    ///     Ok(())
    /// }
    /// ```
    fn declare_retry_on(
        &self,
        connected: &C,
        declaration: &RetryDeclaration,
    ) -> Result<(), DeclareRetryError> {
        let _ = (connected, declaration);
        Ok(())
    }

    /// What this subscription adds to its channel in the generated `AsyncAPI` document.
    ///
    /// This is where a broker's own vocabulary reaches the document: a `RabbitMQ` queue's
    /// durability and its exchange, a Kafka topic's name where it differs from the channel id, a
    /// Pulsar namespace. The core never names a field of yours - it carries what you build with
    /// [`Binding`](crate::asyncapi::Binding) and writes `bindingVersion` for you.
    ///
    /// Three rules bound what belongs here. The value is computed from this descriptor alone,
    /// because the document is built before anything connects: a Kafka topic's real partition
    /// count, the topic behind a Pub/Sub subscription and an SQS queue's ARN cannot be reported
    /// from here at all. A credential never goes in, for the reason
    /// [`DescribeServer`](crate::DescribeServer) gives: the document is published and shared. And
    /// a protocol the specification has no binding for goes in
    /// [`Binding::extension`](crate::asyncapi::Binding::extension), because the protocol keys are
    /// a closed list.
    ///
    /// The default says nothing, and a broker that says nothing changes no document.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "asyncapi", feature = "memory"))]
    /// # fn demo() -> Result<(), ruststream::asyncapi::BindingError> {
    /// use ruststream::asyncapi::{Binding, Bindings};
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::{Connected, SubscriptionSource};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct AmqpChannel {
    ///     queue: Queue,
    /// }
    ///
    /// #[derive(Serialize)]
    /// struct Queue {
    ///     name: String,
    ///     durable: bool,
    /// }
    ///
    /// # struct RabbitQueue { name: String, durable: bool }
    /// # impl RabbitQueue {
    /// fn channel_bindings(&self) -> Bindings {
    ///     let body = AmqpChannel {
    ///         queue: Queue { name: self.name.clone(), durable: self.durable },
    ///     };
    ///     // A binding that fails to build is a binding the document goes without: a broker
    ///     // never holds up a service over a description of itself.
    ///     match Binding::new("amqp", "0.3.0", &body) {
    ///         Ok(binding) => Bindings::new().with(binding),
    ///         Err(_) => Bindings::new(),
    ///     }
    /// }
    /// # }
    /// # let queue = RabbitQueue { name: "orders".into(), durable: true };
    /// # assert!(!queue.channel_bindings().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "asyncapi")]
    #[must_use]
    fn channel_bindings(&self) -> Bindings {
        Bindings::new()
    }

    /// What this subscription adds to its `receive` operation in the document.
    ///
    /// The consumer's own settings live here rather than on the channel: a NATS queue group, a
    /// Kafka consumer group, an MQTT `QoS`. The rules of
    /// [`channel_bindings`](Self::channel_bindings) apply unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "asyncapi")]
    /// # fn demo() -> Result<(), ruststream::asyncapi::BindingError> {
    /// use ruststream::asyncapi::{Binding, Bindings};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct NatsOperation {
    ///     queue: String,
    /// }
    ///
    /// # struct NatsQueue { group: String }
    /// # impl NatsQueue {
    /// fn operation_bindings(&self) -> Bindings {
    ///     let body = NatsOperation { queue: self.group.clone() };
    ///     match Binding::new("nats", "0.1.0", &body) {
    ///         Ok(binding) => Bindings::new().with(binding),
    ///         Err(_) => Bindings::new(),
    ///     }
    /// }
    /// # }
    /// # assert!(!NatsQueue { group: "workers".into() }.operation_bindings().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "asyncapi")]
    #[must_use]
    fn operation_bindings(&self) -> Bindings {
        Bindings::new()
    }

    /// What this subscription adds to the messages that arrive on it.
    ///
    /// A Kafka record's key schema and where its schema id sits, a Pub/Sub ordering key. The
    /// rules of [`channel_bindings`](Self::channel_bindings) apply unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "asyncapi")]
    /// # fn demo() -> Result<(), ruststream::asyncapi::BindingError> {
    /// use ruststream::asyncapi::{Binding, Bindings};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct KafkaMessage {
    ///     #[serde(rename = "schemaIdLocation")]
    ///     schema_id_location: &'static str,
    /// }
    ///
    /// # struct KafkaTopic;
    /// # impl KafkaTopic {
    /// fn message_bindings(&self) -> Bindings {
    ///     let body = KafkaMessage { schema_id_location: "payload" };
    ///     match Binding::new("kafka", "0.5.0", &body) {
    ///         Ok(binding) => Bindings::new().with(binding),
    ///         Err(_) => Bindings::new(),
    ///     }
    /// }
    /// # }
    /// # assert!(!KafkaTopic.message_bindings().is_empty());
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "asyncapi")]
    #[must_use]
    fn message_bindings(&self) -> Bindings {
        Bindings::new()
    }
}

/// A subscription descriptor that knows where a publish reaches it again.
///
/// The other half of [`AddressedCopies`]: a descriptor declaring that copy path implements this
/// too, so the destination of a retry copy is a property of the descriptor's type rather than an
/// answer the runtime has to check at startup. A descriptor that cannot name one destination
/// declares [`NamedCopies`] instead and the mount site names it.
///
/// Answer with the name a publisher bound to the same broker uses to reach this subscription
/// again, resolving it against `connected` when only the live connection knows it (a Pub/Sub
/// subscription has to be looked up to learn its topic). Called once per subscription at startup,
/// never on the delivery path.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use ruststream::memory::{MemoryBroker, MemorySource};
/// use ruststream::{Broker, RedeliveryAddress, RedeliveryAddressed};
///
/// let connected = MemoryBroker::new().connect().await?;
/// let source = MemorySource::new("orders");
///
/// // The in-memory subject is both what a subscription reads and what a publish reaches.
/// assert_eq!(
///     source.redelivery_address(&connected).await?,
///     RedeliveryAddress::new("orders"),
/// );
/// # Ok(())
/// # }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` declares `Copies = AddressedCopies` but says no address",
    label = "no redelivery address for this descriptor on `{C}`",
    note = "a descriptor whose retry copies this process publishes to one destination implements \
            `RedeliveryAddressed` beside `SubscriptionSource`; one that reads many destinations \
            declares `Copies = NamedCopies` and lets the mount site name one"
)]
pub trait RedeliveryAddressed<C: ConnectedBroker>:
    SubscriptionSource<C, Copies = AddressedCopies>
{
    /// Where a publish reaches this subscription again.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectedBroker::Error`] when the broker has to be asked and the request fails.
    fn redelivery_address(
        &self,
        connected: &C,
    ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> + Send;
}

/// The name a deferred redelivery of one subscription is published to.
///
/// Reported by [`RedeliveryAddressed::redelivery_address`]. It is a publish destination, not a
/// subscription name: the two coincide on a NATS subject or a Kafka topic and differ wherever a
/// subscription is a resource of its own, so the runtime never substitutes one for the other.
///
/// # Examples
///
/// ```
/// use std::borrow::Cow;
///
/// use ruststream::{
///     AddressedCopies, RedeliveryAddress, RedeliveryAddressed, Subscribe, SubscriptionSource,
/// };
///
/// /// A broker crate's topic descriptor: the topic it reads is also where a copy reaches it again,
/// /// so this process publishes the copies and the descriptor knows where to.
/// #[derive(Debug, Clone)]
/// struct Topic {
///     name: Cow<'static, str>,
/// }
///
/// impl<C: Subscribe> SubscriptionSource<C> for Topic {
///     type Subscriber = C::Subscriber;
///     type Copies = AddressedCopies;
///
///     fn name(&self) -> &str {
///         &self.name
///     }
///
///     async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
///         connected.subscribe(&self.name).await
///     }
/// }
///
/// impl<C: Subscribe> RedeliveryAddressed<C> for Topic {
///     async fn redelivery_address(&self, _connected: &C) -> Result<RedeliveryAddress, C::Error> {
///         Ok(RedeliveryAddress::new(self.name.clone()))
///     }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RedeliveryAddress(Cow<'static, str>);

impl RedeliveryAddress {
    /// The address a deferred copy is published under.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::borrow::Cow;
    ///
    /// use ruststream::{
    ///     AddressedCopies, RedeliveryAddress, RedeliveryAddressed, Subscribe, SubscriptionSource,
    /// };
    ///
    /// /// A broker crate's topic descriptor: the topic it reads is also where a copy reaches it
    /// /// again, so this process publishes the copies and the descriptor knows where to.
    /// #[derive(Debug, Clone)]
    /// struct Topic {
    ///     name: Cow<'static, str>,
    /// }
    ///
    /// impl<C: Subscribe> SubscriptionSource<C> for Topic {
    ///     type Subscriber = C::Subscriber;
    ///     type Copies = AddressedCopies;
    ///
    ///     fn name(&self) -> &str {
    ///         &self.name
    ///     }
    ///
    ///     async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
    ///         connected.subscribe(&self.name).await
    ///     }
    /// }
    ///
    /// impl<C: Subscribe> RedeliveryAddressed<C> for Topic {
    ///     async fn redelivery_address(
    ///         &self,
    ///         _connected: &C,
    ///     ) -> Result<RedeliveryAddress, C::Error> {
    ///         // A copy goes to the topic's retry companion, built from its name.
    ///         Ok(RedeliveryAddress::new(format!("{}.retry", self.name)))
    ///     }
    /// }
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
    /// use ruststream::{OutgoingMessage, Publisher, RedeliveryAddress};
    ///
    /// /// A broker crate's own redelivery: the copy goes where the descriptor said it reaches the
    /// /// subscription again.
    /// async fn send_copy<P: Publisher>(
    ///     publisher: &P,
    ///     address: &RedeliveryAddress,
    ///     payload: &[u8],
    /// ) -> Result<(), P::Error> {
    ///     publisher
    ///         .publish(OutgoingMessage::new(address.as_str(), payload), None)
    ///         .await
    /// }
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
/// error, not a startup one. [`name`](crate::runtime::SubscriberSettings::name) replaces it with
/// the kind itself, built through [`FromName`].
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
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
/// # #[cfg(not(all(feature = "macros", feature = "memory", feature = "json")))]
/// # compile_error!("the example needs the macros, memory and json features");
/// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
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
/// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
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
    /// client-side [`Buffered`](crate::Buffered) batcher, a broker's own adapter) needs.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
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
            address.clone(),
        );
        assert_eq!(
            Buffered::new(MemorySource::new("orders"))
                .redelivery_address(&connected)
                .await
                .expect("client-side batching changes no address"),
            address.clone(),
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
            address,
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
