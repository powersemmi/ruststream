//! The [`Publisher`] trait and its declaration-side counterpart, [`PublishPolicy`].

use std::{error::Error as StdError, future::Future};

use bytes::BytesMut;
use serde::Serialize;
use thiserror::Error;

#[cfg(feature = "asyncapi")]
use crate::asyncapi::Bindings;
use crate::codec::{Codec, CodecError};
use crate::runtime::{Outgoing, OutgoingName, Serialized};
use crate::{ConnectedBroker, HeaderMap, OutgoingMessage};

/// How a transport consumes the payload of a publish: [`Lend`] where it reads the bytes,
/// [`Take`] where its client keeps them.
///
/// Every [`Publisher`] names one as [`Publisher::Payload`], and the message its `publish`
/// receives follows the declaration: a lending publisher is handed `&[u8]`, a taking one a
/// [`BytesMut`] it owns. There is no third form and no arm that never arrives, so a transport
/// matches on nothing and the runtime branches on nothing: it produces the payload the way the
/// type said before the first message was ever published.
///
/// The declaration is what the framework's own buffers follow. A lending publisher inside a
/// dispatch loop is lent one encode buffer the loop reuses, so a reply through it allocates
/// nothing per message; a taking one is handed a fresh buffer per publish, which it keeps.
///
/// Sealed: the two forms are the whole set.
///
/// # Examples
///
/// ```
/// use std::convert::Infallible;
///
/// use ruststream::{Lend, OutgoingMessage, Publisher};
///
/// // A transport that packs the payload into a frame of its own reads it and keeps nothing.
/// struct Framed;
///
/// impl Publisher for Framed {
///     type Payload = Lend;
///     type Error = Infallible;
///     type Options = ();
///
///     async fn publish(
///         &self,
///         msg: OutgoingMessage<'_, &[u8]>,
///         _options: Option<&()>,
///     ) -> Result<(), Infallible> {
///         let (name, payload, _headers) = msg.into_parts();
///         let _frame = (name.len(), payload.len());
///         Ok(())
///     }
/// }
/// ```
pub trait PayloadForm: sealed::Sealed {
    /// The payload as this form carries it: `&'a [u8]` for [`Lend`], [`BytesMut`] for [`Take`].
    ///
    /// The bounds are what every publish position needs of it: the bytes are readable, the
    /// message crosses a task boundary, and bytes the caller holds can be handed over in this
    /// form (free where the transport lends, one copy where it keeps them).
    type Form<'a>: AsRef<[u8]> + Send + From<&'a [u8]>;

    /// The codec's output in this form: written into `buf` and lent, or a buffer of its own.
    ///
    /// `buf` is the publish path's scratch - inside a dispatch loop, the one buffer that loop
    /// reuses - and is emptied here, so what it held for the previous message never leaves with
    /// this one.
    ///
    /// The framework's own side of the declaration, and the reason it is on this trait: a form
    /// is one decision with two ends, what the transport is handed and what the runtime writes
    /// into, and splitting them would let the two drift. Sealed, so nothing outside implements
    /// it; you never call it.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when the codec rejects the value.
    #[doc(hidden)]
    fn encoded<'b, C, T>(
        codec: &C,
        value: &T,
        buf: &'b mut BytesMut,
    ) -> Result<Self::Form<'b>, CodecError>
    where
        C: Codec,
        T: Serialize;

    /// The bytes a [`Serialized`] value publishes, in this form: the ones it holds, or the ones
    /// it writes into `buf`.
    ///
    /// # Errors
    ///
    /// Returns the value's own error when its encoder rejects it.
    #[doc(hidden)]
    fn serialized<'v, T>(value: &'v T, buf: &'v mut BytesMut) -> Result<Self::Form<'v>, T::Error>
    where
        T: Serialized;

    /// A message in this form, put back on the publish pipeline for the stages above the leaf.
    #[doc(hidden)]
    fn rebuilt<'a>(
        name: OutgoingName<'a>,
        payload: Self::Form<'a>,
        headers: HeaderMap,
    ) -> Outgoing<'a>;

    /// The message that leaves the pipeline, in this form.
    ///
    /// The broker is the last reader, so a taking transport is handed the buffer the stages
    /// wrote and a lending one the bytes where they are; either way the map the transforms
    /// filled moves into the message rather than being cloned into it.
    #[doc(hidden)]
    fn leaving<'a>(out: &'a mut Outgoing<'a>) -> OutgoingMessage<'a, Self::Form<'a>>;
}

/// The declaration of a transport that reads the payload and keeps nothing: it is handed
/// `&[u8]`.
///
/// What every transport that writes the payload into a frame of its own declares. See
/// [`PayloadForm`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Lend;

/// The declaration of a transport whose client keeps the payload: it is handed a [`BytesMut`]
/// of its own.
///
/// The buffer arrives as it was written, so `Vec::from` and [`BytesMut::freeze`] turn it into
/// what the client speaks without copying it. See [`PayloadForm`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Take;

/// The message a publisher of form `Form` is handed: [`OutgoingMessage`] over that form's
/// payload.
///
/// A broker names the form itself (`OutgoingMessage<'_, &[u8]>`, `OutgoingMessage<'_, BytesMut>`)
/// and never needs this alias; a wrapper generic over the publisher underneath it does, because
/// the form is not known there.
///
/// # Examples
///
/// ```
/// use ruststream::{HeaderMap, OutgoingFor, Publisher};
///
/// // A handle that forwards every publish to the publisher it wraps, whichever form that one
/// // declared.
/// struct Tagged<P>(P, HeaderMap);
///
/// impl<P: Publisher> Publisher for Tagged<P> {
///     type Payload = P::Payload;
///     type Error = P::Error;
///     type Options = P::Options;
///
///     async fn publish(
///         &self,
///         msg: OutgoingFor<'_, Self::Payload>,
///         options: Option<&Self::Options>,
///     ) -> Result<(), Self::Error> {
///         self.0.publish(msg, options).await
///     }
///
///     fn base_headers(&self) -> Option<&HeaderMap> {
///         Some(&self.1)
///     }
/// }
/// ```
pub type OutgoingFor<'a, Form> = OutgoingMessage<'a, <Form as PayloadForm>::Form<'a>>;

mod sealed {
    /// Seals [`PayloadForm`](super::PayloadForm): a transport reads the payload or keeps it,
    /// and a third answer would be one the runtime has no buffer strategy for.
    pub trait Sealed {}

    impl Sealed for super::Lend {}
    impl Sealed for super::Take {}
}

/// A producer that sends messages into the broker.
///
/// `Publisher` is `Send + Sync` so a single instance can be shared across tasks. Implementations
/// are expected to be cheap to clone; expensive shared state (connection pool, batch buffers)
/// should live behind an [`Arc`].
///
/// # Examples
///
/// ```
/// use ruststream::{OutgoingMessage, Publisher};
///
/// async fn emit<P: Publisher>(publisher: &P) -> Result<(), P::Error> {
///     // Bytes the caller holds, in whichever form this publisher declared.
///     let msg = OutgoingMessage::new("orders.created", b"{}".as_slice());
///     publisher.publish(msg, None).await
/// }
/// ```
///
/// [`Arc`]: std::sync::Arc
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a publisher",
    note = "for an `Out<impl Publisher, _>` slot, the type comes from the policy attached at \
            the include site: attach one whose live form publishes"
)]
pub trait Publisher: Send + Sync {
    /// How this transport consumes the payload: [`Lend`] where it reads the bytes and keeps
    /// nothing, [`Take`] where its client keeps them.
    ///
    /// The declaration decides what [`publish`] receives - `&[u8]` under `Lend`, a
    /// [`BytesMut`] of its own under `Take` - and what the framework does with its own buffers
    /// above it: a lending publisher inside a dispatch loop is lent the one encode buffer that
    /// loop reuses, so a reply through it allocates nothing per message, while a taking one is
    /// handed a fresh buffer it may keep. Declare `Lend` unless the client keeps the bytes past
    /// the call.
    ///
    /// [`publish`]: Self::publish
    type Payload: PayloadForm;

    /// The error type returned by [`publish`].
    ///
    /// [`publish`]: Self::publish
    type Error: StdError + Send + Sync + 'static;

    /// The broker's per-message settings: a `QoS`, a priority, an ordering key, whatever this
    /// transport lets one message differ from the next in.
    ///
    /// Every field is optional, because a publish carries only what its call site adjusted: the
    /// rest keeps what the [`PublishPolicy`] fixed when it paired this publisher. A broker with
    /// no per-message setting writes `type Options = ();`.
    ///
    /// The type is the broker's own, and so are the builder steps that fill it: a broker ships an
    /// extension trait over [`PublishBuilder`](crate::runtime::PublishBuilder) bounded on this
    /// type, so its steps appear on a builder over its own publisher and nowhere else.
    ///
    /// `Clone` and `'static` are what the test harness needs: it copies the options a slot
    /// publish carried and hands them back to the test as this type
    /// (`tb.out::<Marker>().with_options(..)` under the `testing` feature).
    type Options: Clone + Send + Sync + 'static;

    /// Publishes a message to the broker, with the per-message settings the call site adjusted.
    ///
    /// This is the contract a broker implements, and the direct call a broker crate used on its
    /// own - without this one - is written against. Inside a service built on `ruststream` it is
    /// the layer underneath: what a handler sends goes through the publish builder
    /// ([`message`](crate::runtime::PublishExt::message)), which resolves the destination, the
    /// codec and the headers and assembles the
    /// [`OutgoingMessage`] itself. Reach for this one where the message is already built: a
    /// publish transform, a middleware, a post-settle hook.
    ///
    /// `options` is `None` on every path with no call site to adjust them - a reply, a deferred
    /// redelivery - and the policy's own settings apply.
    ///
    /// The message arrives by value, and so do its payload and its header map. Read them with
    /// [`OutgoingMessage::payload`] and [`OutgoingMessage::headers`]; a transport that consumes
    /// the message takes its parts with [`OutgoingMessage::into_parts`] - the destination, the
    /// payload and the map in one move, nothing copied.
    ///
    /// What the payload is follows [`Payload`](Self::Payload). A [`Lend`] publisher is handed
    /// `&[u8]` valid for the length of the call: read it, write it into your frame, and hold
    /// nothing afterwards. A [`Take`] publisher is handed the [`BytesMut`] the framework wrote,
    /// as it was written, so `Vec::from(payload)` and [`BytesMut::freeze`] turn it into what the
    /// client speaks with no copy of the body; only a publish lending bytes the framework does
    /// not own is copied into that buffer.
    ///
    /// # Cancel safety
    ///
    /// Cancel safety is implementation-defined: most brokers will leave a message in an
    /// indeterminate state if the future is dropped mid-flight. Implementors must document the
    /// guarantees their broker provides.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker rejects the message, the connection is lost, or
    /// the operation times out.
    fn publish(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// The headers this publisher contributes to every message it sends, underneath whatever the
    /// message itself names.
    ///
    /// `None` by default: a plain broker publisher contributes nothing and the outgoing map starts
    /// empty. This is the place for a constant of the publisher itself - a tenant, a producer
    /// name, a schema id every message of this handle carries - laid down first, with the
    /// message's own headers written over it key by key, so the call site wins over the handle.
    ///
    /// A delivery setting is not one of those: it belongs to the message, and a broker carries it
    /// as a field of [`Options`](Self::Options) that the policy defaults and the call site
    /// adjusts.
    ///
    /// Every message the runtime sends through this publisher starts from that base: a publish
    /// through the builder, and the reply a `publish("dest")` handler returns (whose
    /// [`PublishTransform`](crate::runtime::PublishTransform) stack then writes over it, like any
    /// other call site). A publish a handler body issues on a value it obtained outside the
    /// framework is that body's own call and reaches nothing here.
    ///
    /// The map is borrowed, never rebuilt per publish, so a handle that has one keeps it in its
    /// own state.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros"))]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{HeaderMap, Outgoing, OutgoingFor, Publisher, Serialized};
    ///
    /// // A handle that tags every message it sends, without touching the message itself.
    /// struct Tenanted<P>(P, HeaderMap);
    ///
    /// impl<P: Publisher> Publisher for Tenanted<P> {
    ///     type Payload = P::Payload;
    ///     type Error = P::Error;
    ///     type Options = P::Options;
    ///
    ///     async fn publish(
    ///         &self,
    ///         msg: OutgoingFor<'_, Self::Payload>,
    ///         options: Option<&Self::Options>,
    ///     ) -> Result<(), Self::Error> {
    ///         self.0.publish(msg, options).await
    ///     }
    ///
    ///     fn base_headers(&self) -> Option<&HeaderMap> {
    ///         Some(&self.1)
    ///     }
    /// }
    ///
    /// // Bytes that are already the payload, so this example needs no codec feature.
    /// #[derive(Outgoing, Serialized)]
    /// struct Order(Vec<u8>);
    ///
    /// let broker = MemoryBroker::new();
    /// let base = [("tenant", "acme")].into_iter().collect();
    /// let publisher = Tenanted(broker.publisher(), base);
    /// publisher.message(&Order(b"{}".to_vec())).to("orders").publish().await?;
    /// # Ok(())
    /// # }
    /// ```
    fn base_headers(&self) -> Option<&HeaderMap> {
        None
    }
}

/// The declaration half of a publisher: pure policy, no connection, no publish surface.
///
/// A broker publisher is a bundle of policy (an exchange, a queue timeout, a transactional id)
/// paired with the live connection. `PublishPolicy` is that bundle alone, freely constructible
/// anywhere - before startup, in router definitions, in configuration - because it holds no
/// connection and no broker instance identity. [`pair`](Self::pair) joins it with a
/// [`ConnectedBroker`] witness to produce the live [`Publisher`], so "not connected" is not
/// representable on this path: a publisher exists only after the connection does.
///
/// The policy is also where a broker's per-message settings ([`Publisher::Options`]) get their
/// defaults: the mount site configures the policy, pairing hands the live publisher whatever it
/// fixed, and a call site adjusts single fields over that.
///
/// This is the publish-side mirror of [`SubscriptionSource`](crate::SubscriptionSource). The
/// reply wiring a mount site's chain builds over a policy is itself a `PublishPolicy`, resolved
/// functorially: pairing swaps the leaf for its live publisher and keeps the codec and transform
/// stacks the chain named, fully monomorphized.
///
/// `pair` is async and fallible because some brokers do real work when a publisher comes alive
/// (a transactional producer initializing its transactions); for most it is a cheap constructor
/// call. It reports the type-erased [`PairError`].
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros"))]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::memory::{MemoryBroker, MemoryPublish};
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Broker, Outgoing, PublishPolicy, Serialized};
///
/// // Bytes that are already the payload, so this example needs no codec feature.
/// #[derive(Outgoing, Serialized)]
/// struct Order(Vec<u8>);
///
/// let policy = MemoryPublish; // no broker in sight
/// let connected = MemoryBroker::new().connect().await?;
/// let publisher = policy.pair(&connected).await?; // live only past this point
/// publisher.message(&Order(b"{}".to_vec())).to("orders").publish().await?;
/// # Ok(())
/// # }
/// ```
pub trait PublishPolicy<C: ConnectedBroker> {
    /// The live form this policy pairs into: a [`Publisher`] for a leaf policy, or the live
    /// wiring form for a combinator stack (a typed publisher over a policy pairs into the same
    /// typed publisher over the live leaf).
    type Live;

    /// Pairs the policy with a connected broker, producing the live publisher.
    ///
    /// # Errors
    ///
    /// Returns [`PairError`] when bringing the publisher alive requires broker work and that
    /// work fails (most policies pair infallibly).
    fn pair(self, connected: &C) -> impl Future<Output = Result<Self::Live, PairError>> + Send;

    /// What a publish through this policy adds to its channel in the generated `AsyncAPI`
    /// document.
    ///
    /// The publish-side mirror of
    /// [`SubscriptionSource::channel_bindings`](crate::SubscriptionSource::channel_bindings), and
    /// the same three rules bound it. The value is computed from the policy and the name it is
    /// handed, because the document is built before anything connects, so a Kafka topic's real
    /// partition count cannot come from here. A credential never goes in, for the reason
    /// [`DescribeServer`](crate::DescribeServer) gives. And a protocol the specification has no
    /// binding for goes in [`Binding::extension`](crate::asyncapi::Binding::extension).
    ///
    /// The policy answers for every position it is bound on: the reply of a `publish(..)`
    /// registration, an [`Out`](crate::runtime::Out) slot, and the publisher a dead-lettered
    /// delivery leaves through. A [`Bound`](crate::runtime::Bound) token answers with the policy
    /// it carries, so a cross-broker publish is described by the broker it reaches. The channel's
    /// `servers` list does not follow it: that comes from the label the registration's own broker
    /// was registered under, which is the only broker the runtime can name for a channel.
    ///
    /// `channel` is the destination the mount site resolved for that position - the reply type's
    /// own name or the `publish("dest")` clause, the slot entry's name, the `dead_letter("dlq")`
    /// declaration - and it is what the document reports as the channel's `address`. An SNS topic
    /// and an SQS queue are named by their binding's required `name` field, and this is where
    /// that name comes from: a policy declares a broker's settings and never a destination. Where
    /// a naming transform decides the destination per delivery, the document reports no address
    /// and the mount site's fallback name arrives here instead.
    ///
    /// The default says nothing, and a policy that says nothing changes no document.
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
    /// struct SnsChannel {
    ///     name: String,
    /// }
    ///
    /// # struct SnsPublish;
    /// # impl SnsPublish {
    /// fn channel_bindings(&self, channel: &str) -> Bindings {
    ///     let body = SnsChannel { name: channel.to_owned() };
    ///     // A binding that fails to build is a binding the document goes without: a broker
    ///     // never holds up a service over a description of itself.
    ///     match Binding::new("sns", "0.1.0", &body) {
    ///         Ok(binding) => Bindings::new().with(binding),
    ///         Err(_) => Bindings::new(),
    ///     }
    /// }
    /// # }
    /// # assert!(!SnsPublish.channel_bindings("orders").is_empty());
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "asyncapi")]
    #[must_use]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        let _ = channel;
        Bindings::new()
    }

    /// What a publish through this policy adds to its `send` operation in the document.
    ///
    /// The producer's own settings live here rather than on the channel: a Kafka client id, an
    /// MQTT `QoS`, an SQS delay. The rules of [`channel_bindings`](Self::channel_bindings) apply
    /// unchanged, `channel` included: it is the same resolved destination.
    ///
    /// A reply has no `send` operation - it is the `reply` of the operation it answers, and the
    /// specification gives that object no bindings - so what this returns reaches the document
    /// from a slot and from a dead-letter destination only.
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
    /// struct SqsOperation {
    ///     queues: Vec<SqsQueue>,
    /// }
    ///
    /// #[derive(Serialize)]
    /// struct SqsQueue {
    ///     name: String,
    /// }
    ///
    /// # struct SqsPublish;
    /// # impl SqsPublish {
    /// fn operation_bindings(&self, channel: &str) -> Bindings {
    ///     let body = SqsOperation { queues: vec![SqsQueue { name: channel.to_owned() }] };
    ///     match Binding::new("sqs", "0.3.0", &body) {
    ///         Ok(binding) => Bindings::new().with(binding),
    ///         Err(_) => Bindings::new(),
    ///     }
    /// }
    /// # }
    /// # assert!(!SqsPublish.operation_bindings("orders").is_empty());
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "asyncapi")]
    #[must_use]
    fn operation_bindings(&self, channel: &str) -> Bindings {
        let _ = channel;
        Bindings::new()
    }

    /// What a publish through this policy adds to the messages that leave through it.
    ///
    /// A Kafka record's key schema, a Pub/Sub ordering key. The rules of
    /// [`channel_bindings`](Self::channel_bindings) apply unchanged, `channel` included: it is
    /// the same resolved destination.
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
    /// struct GooglePubSubMessage {
    ///     #[serde(rename = "orderingKey")]
    ///     ordering_key: &'static str,
    /// }
    ///
    /// # struct PubSubPublish;
    /// # impl PubSubPublish {
    /// fn message_bindings(&self, _channel: &str) -> Bindings {
    ///     let body = GooglePubSubMessage { ordering_key: "tenant" };
    ///     match Binding::new("googlepubsub", "0.2.0", &body) {
    ///         Ok(binding) => Bindings::new().with(binding),
    ///         Err(_) => Bindings::new(),
    ///     }
    /// }
    /// # }
    /// # assert!(!PubSubPublish.message_bindings("orders").is_empty());
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "asyncapi")]
    #[must_use]
    fn message_bindings(&self, channel: &str) -> Bindings {
        let _ = channel;
        Bindings::new()
    }

    /// Where a client finds the address of a reply this policy publishes, as a runtime
    /// expression.
    ///
    /// A broker that answers a request through a reply-to header names that header here:
    /// `"$message.header#/reply-to"` is the specification's form, and the part after the `#` is
    /// a JSON Pointer into the request's headers. The document then reports the reply channel
    /// with `address: null` and puts the expression in the operation's `reply.address.location`,
    /// so a reader knows the destination is decided per delivery and where to read it.
    ///
    /// It reaches the document only where a [`PublishTransform`](crate::runtime::PublishTransform)
    /// on the reply position declares
    /// [`Destination = Names`](crate::runtime::PublishTransform::Destination): that transform is
    /// what actually redirects the reply, and without it the reply goes to the name the mount
    /// site declared, which is what the document reports.
    ///
    /// The default is `None`: a broker whose replies carry no such address says nothing, and the
    /// document keeps the declared name.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "asyncapi")]
    /// # {
    /// # struct NatsPublish;
    /// # impl NatsPublish {
    /// fn reply_address_location(&self) -> Option<&'static str> {
    ///     Some("$message.header#/reply-to")
    /// }
    /// # }
    /// assert_eq!(NatsPublish.reply_address_location(), Some("$message.header#/reply-to"));
    /// # }
    /// ```
    #[cfg(feature = "asyncapi")]
    #[must_use]
    fn reply_address_location(&self) -> Option<&'static str> {
        None
    }
}

/// The error of [`PublishPolicy::pair`]: whatever the broker reported while bringing a publisher
/// alive, type-erased.
///
/// A cross-broker token pairs against a broker other than the including scope's, so the error
/// cannot be typed to one broker.
#[derive(Debug, Error)]
#[error("pairing a publisher failed: {0}")]
pub struct PairError(#[source] Box<dyn StdError + Send + Sync>);

impl PairError {
    /// Wraps a broker's pairing failure.
    #[must_use]
    pub fn new(source: impl StdError + Send + Sync + 'static) -> Self {
        Self(Box::new(source))
    }

    /// Wraps an already-boxed failure, or a plain message.
    #[must_use]
    pub fn from_boxed(source: Box<dyn StdError + Send + Sync>) -> Self {
        Self(source)
    }
}

/// A connected broker that names its plain publish policy, so the runtime can build a default
/// reply publisher when a `publish("dest")` handler is included without an explicit one.
///
/// Implement it alongside [`ConnectedBroker`](crate::ConnectedBroker) when the broker has a
/// publish policy whose default configuration is usable as-is (most are). Brokers whose
/// publishers always need explicit options simply do not implement it, and their users attach a
/// policy at every registration.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # fn demo() {
/// use ruststream::DefaultPublish;
/// use ruststream::memory::{ConnectedMemoryBroker, MemoryPublish};
///
/// fn default_policy<C: DefaultPublish>() -> C::Policy {
///     C::Policy::default()
/// }
/// let _: MemoryPublish = default_policy::<ConnectedMemoryBroker>();
/// # }
/// ```
pub trait DefaultPublish: ConnectedBroker {
    /// The broker's plain publish policy, constructible with its defaults.
    type Policy: PublishPolicy<Self> + Default + Send + 'static;
}
