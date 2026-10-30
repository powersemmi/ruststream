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

use alloc::borrow::Cow;
use core::fmt;

#[cfg(doc)]
use crate::Subscribe;
#[cfg(feature = "asyncapi")]
use crate::bindings::Bindings;
use crate::{ConnectedBroker, DeclareRetryError, Subscriber};

mod retry;
mod sources;

pub(crate) use retry::copy_path;
pub use retry::{AddressedCopies, BrokerMoves, CopyPath, NamedCopies, RetryDeclaration};
pub use sources::{FromName, Name, StartAt, Unnamed};

/// A description of one subscription, resolved against a connected broker at startup.
///
/// The runtime calls [`subscribe`] once, against the [`ConnectedBroker`] witness produced by
/// [`Broker::connect`](crate::Broker::connect), to obtain the live [`Subscriber`]. The associated
/// [`Subscriber`](Self::Subscriber) type lives on the source rather than the broker, so a single
/// broker can offer several subscription kinds with different subscriber types (for example
/// `Redis` pub/sub versus streams).
///
/// # Implementing it
///
/// [`Subscribe`](crate::Subscribe) covers a subscription a name alone identifies. One that needs
/// settings of the broker's own (a consumer group, a durable name, a delivery policy) gets a
/// descriptor type implementing this trait.
///
/// - Give it an associated constructor, `OrdersStream::new("orders", "workers")`, not a free
///   function: a service names it in the attribute, `#[subscriber(OrdersStream::new(..))]`, and
///   the macro reads the type out of the call. A builder chain on it works too
///   (`OrdersStream::new("orders").durable("workers")`) as long as each method returns `Self`.
/// - Derive `Clone`: a mount rebuilds the configuration per registration, so one definition can
///   be mounted on two brokers at once.
/// - A kind identified by one name and nothing else also implements [`FromName`], which makes the
///   bare kind legal in the attribute.
/// - Answer [`Copies`](Self::Copies). [`AddressedCopies`] and [`NamedCopies`] pair a retry
///   publisher for every registration from the broker's
///   [`DefaultPublish`](crate::DefaultPublish) policy, so a descriptor declaring either over a
///   broker without one does not compile. Where native redelivery depends on a field's value
///   rather than on the type (a queue without a delay setting has none of its own), the
///   descriptor keeps a copy path open.
/// - Read the registration's cap and dead-letter destination in
///   [`declare_retry`](Self::declare_retry) where the broker has a mechanism for them.
///
/// `conformance::harness::redelivery_address` checks an [`AddressedCopies`] descriptor's answer,
/// and `conformance::retry::broker_moves` a [`BrokerMoves`] descriptor's mapping of the cap.
/// Settings a service adds at the mount site reach the descriptor through an extension trait over
/// [`SubscriberBuilder`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.SubscriberBuilder.html), and protocol bindings for the
/// `AsyncAPI` document come from its `*_bindings` methods.
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
    /// [`Binding`](crate::bindings::Binding) and writes `bindingVersion` for you.
    ///
    /// Three rules bound what belongs here. The value is computed from this descriptor alone,
    /// because the document is built before anything connects: a Kafka topic's real partition
    /// count, the topic behind a Pub/Sub subscription and an SQS queue's ARN cannot be reported
    /// from here at all. A credential never goes in, for the reason
    /// [`DescribeServer`](crate::DescribeServer) gives: the document is published and shared. And
    /// a protocol the specification has no binding for goes in
    /// [`Binding::extension`](crate::bindings::Binding::extension), because the protocol keys are
    /// a closed list.
    ///
    /// The default says nothing, and a broker that says nothing changes no document.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "asyncapi")]
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
/// A `.to(name)` on the retry position overrides the answer. The generated document reports such
/// a name with a `send` operation; the answered address is the subscription's own channel, which
/// the document already carries.
/// [`conformance::harness::redelivery_address`](https://docs.rs/ruststream/latest/ruststream/conformance/retry/fn.redelivery_address.html)
/// holds the answer to its promise: a copy published there arrives at the subscription that
/// reported it, once per group, with its headers.
///
/// # Examples
///
/// ```
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
