//! What a subscription declares about its retries: who publishes the copies, and the cap and
//! dead-letter destination of one registration.

use alloc::borrow::Cow;
use core::num::NonZeroU32;

#[cfg(doc)]
use super::{RedeliveryAddressed, SubscriptionSource};

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
/// # #[cfg(feature = "json")]
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
///         b.include(reconcile).out_retry(Publish).to("orders.retry")
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
/// compile on a [`Router`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.Router.html) chain; mounted directly in a broker scope, it
/// compiles and the app refuses to start, with an error naming the subscription and the
/// descriptor.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
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
///         b.include(reconcile).out_retry(Publish).to("orders.retry")
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
