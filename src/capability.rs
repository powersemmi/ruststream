//! Optional capability traits implemented by brokers that support specific semantics.
//!
//! Brokers implement only the capabilities they natively support. Generic runtime code that
//! depends on a capability adds it as a bound, leaving brokers that do not support it free of
//! emulation cost.

use std::{error::Error as StdError, future::Future, time::Duration};

use futures::Stream;

use crate::{
    Broker, ConnectedBroker, Headers, IncomingMessage, OutgoingMessage, Publisher, Subscriber,
};

/// A subscriber that natively delivers messages in batches.
///
/// Brokers that batch on the wire (`Kafka`, `JetStream` pull consumers) implement this so the
/// runtime can dispatch a whole batch through middleware in one go. Brokers without native
/// batching simply do not implement it.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not deliver messages in batches",
    label = "this subscription has no batching of its own",
    note = "a handler taking `&[T]` needs batches: add the framework's buffer at the mount site, \
            `b.include(handle.buffered(nonzero!(64), Duration::from_millis(10)))`, or mount the \
            definition on a subscription kind that batches on the wire"
)]
pub trait BatchSubscriber: Subscriber {
    /// Container yielded by [`batches`]. Implementations choose between [`Vec`], custom
    /// iterators, or anything else that yields the underlying [`Subscriber::Message`].
    ///
    /// [`batches`]: Self::batches
    type Batch: IntoIterator<Item = <Self as Subscriber>::Message> + Send;

    /// Returns a stream of batches.
    ///
    /// # Cancel safety
    ///
    /// Same guarantees as [`Subscriber::stream`]: cancel-safe between polls.
    fn batches(
        &mut self,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_;
}

/// A subscriber whose position in a replayable log can be moved.
///
/// Implemented only by brokers whose transport can replay (`Kafka` seeks per partition, `Redis`
/// streams move a group cursor, file-backed logs seek by offset); brokers without a replayable
/// log simply do not implement it. [`Subscriber::stream`] borrows the subscriber mutably for the
/// life of the returned stream, and the runtime holds that stream for the life of the service,
/// so repositioning goes through a [`Seeker`] handle minted before the stream is opened, the
/// same way publishers are handed out at build time.
///
/// # Examples
///
/// ```
/// use ruststream::{Seekable, Seeker};
///
/// async fn rewind<S: Seekable>(
///     subscriber: &S,
///     to: <S::Seeker as Seeker>::Position,
/// ) -> Result<(), <S::Seeker as Seeker>::Error> {
///     subscriber.seeker().seek(to).await
/// }
/// ```
pub trait Seekable: Subscriber {
    /// The handle usable while this subscriber's stream is running.
    type Seeker: Seeker;

    /// Mints a handle for repositioning this subscription.
    fn seeker(&self) -> Self::Seeker;
}

/// A clonable handle that repositions one subscription, minted by [`Seekable::seeker`].
///
/// What one seek covers differs between brokers: a broker whose position lives on the consumer
/// instance (`Kafka`) moves that instance only, while a broker whose position is a shared group
/// cursor (`Redis` streams) moves the whole consumer group. Repositioning also invalidates any
/// acknowledgement bookkeeping the broker keeps for the subscription (a contiguous-watermark
/// commit tracker must reset). Broker implementations document both.
pub trait Seeker: Clone + Send + Sync + 'static {
    /// The broker's own position type (`Kafka` partition offsets, a `Redis` entry id, a byte
    /// offset), constructed by the broker crate or captured from a delivered message via
    /// [`Positioned::position`].
    type Position: Send;

    /// The error returned when the broker rejects the reposition.
    type Error: StdError + Send + Sync + 'static;

    /// Moves the subscription to `to`; subsequent deliveries resume from there.
    ///
    /// Once the returned future resolves, the next delivery yielded by the subscription
    /// reflects the new position. For a position captured from a delivered message the resume
    /// point is fixed by the [`Positioned`] contract: that message is delivered again. For a
    /// position built by a broker constructor (earliest, a timestamp) the resume point is
    /// defined by the broker's own position documentation.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` when the broker rejects the reposition or the transport fails.
    fn seek(&self, to: Self::Position) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A delivered message that knows its own position in the broker's replayable log.
///
/// The captured half of the seek capability, next to broker-constructed positions:
/// [`position`](Self::position) returns a value [`Seeker::seek`] accepts, with pinned
/// semantics - seeking to it redelivers this exact message. Generic replay code and the
/// conformance suite rely on that contract; positions built by broker constructors keep
/// broker-documented semantics instead.
///
/// # Examples
///
/// ```
/// use ruststream::{Positioned, Seekable, Seeker};
///
/// async fn replay_from<S>(
///     subscriber: &S,
///     msg: &S::Message,
/// ) -> Result<(), <S::Seeker as Seeker>::Error>
/// where
///     S: Seekable,
///     S::Message: Positioned<Position = <S::Seeker as Seeker>::Position>,
/// {
///     subscriber.seeker().seek(msg.position()).await
/// }
/// ```
pub trait Positioned: IncomingMessage {
    /// The position type, matching the subscription's [`Seeker::Position`].
    type Position: Send;

    /// Returns the position of this delivery; seeking to it redelivers this message.
    fn position(&self) -> Self::Position;
}

/// A publisher that supports broker-side transactions.
///
/// Implementations must guarantee that messages published between [`begin_transaction`] and
/// [`commit`] either all become visible to subscribers or none of them do.
///
/// Misuse is an error, never a silent no-op: a [`commit`] or [`abort`] with no open transaction,
/// and a [`begin_transaction`] while one is open, must return `Self::Error`. A caller can
/// therefore trust that `Ok` from `commit` means "an open transaction committed", not "there was
/// nothing to commit". The [`conformance`](crate::conformance) transactional suite checks these
/// paths.
///
/// This is the borrowed kind of the two transaction capabilities: the handle carries at most one
/// broker-side transaction, and [`begin_transaction`] takes an exclusive claim on it - which is
/// why a second begin while one is open must error. Kafka-like brokers, whose client object
/// holds exactly one transaction per producer, implement only this kind. Brokers whose
/// transactions are client buffers can additionally implement [`OwnedTransactions`], the owned
/// kind: every call there opens an independent buffer-owning [`Transaction`] value, so
/// concurrent transactions on one handle are legal.
///
/// [`begin_transaction`]: Self::begin_transaction
/// [`commit`]: Self::commit
/// [`abort`]: Self::abort
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not support broker-side transactions",
    note = "for an `Out<impl TransactionalPublisher, _>` slot, attach a policy whose live \
            publisher is transactional (a transactional producer configuration)"
)]
pub trait TransactionalPublisher: Publisher {
    /// Begins a new transaction on this publisher.
    ///
    /// Messages published from the same publisher handle after this call are part of the
    /// transaction until [`commit`] or [`abort`] is called.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` when a transaction is already open on this handle (a second begin
    /// must not silently join or restart it), or when the broker refuses to start one. A
    /// rejected begin must leave an already-open transaction untouched.
    ///
    /// [`commit`]: Self::commit
    /// [`abort`]: Self::abort
    fn begin_transaction(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Commits the active transaction, making all buffered messages visible atomically.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` when no transaction is open on this handle, or when the broker
    /// rejects the commit. After a failed commit the transaction is closed: the implementation
    /// discards or aborts the broker-side transaction rather than leaving it open, and a
    /// subsequent [`begin_transaction`](Self::begin_transaction) either starts a fresh
    /// transaction or returns an error - the handle must never wedge permanently. Messages of a
    /// failed transaction are lost to this handle; redelivery of the inputs, not resubmission of
    /// the buffer, is the recovery path.
    fn commit(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Aborts the active transaction, discarding all buffered messages.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` when no transaction is open on this handle, or when the broker
    /// fails to abort.
    fn abort(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A client-buffered transaction that owns its buffer, opened by
/// [`OwnedTransactions::transaction`].
///
/// [`publish`] appends to the buffer; nothing is visible to subscribers until [`commit`] flushes
/// the whole buffer atomically, and [`abort`] discards it. Both settle the transaction by
/// consuming `self`, so a double commit, a commit after an abort, or a publish after settling
/// are compile errors, not runtime checks. Dropping an unsettled transaction discards the buffer
/// like an abort (destructors cannot run async work); implementations log a warning, because a
/// silently vanishing buffer is almost always a missing `commit`.
///
/// This is the transaction value of the owned kind; see [`OwnedTransactions`] for the contrast
/// with the borrowed [`TransactionalPublisher`] kind.
///
/// # Examples
///
/// ```
/// use ruststream::{OutgoingMessage, Transaction};
///
/// async fn settle_pair<T: Transaction>(mut txn: T) -> Result<(), T::Error> {
///     txn.publish(OutgoingMessage::new("orders", b"{}".as_slice())).await?;
///     txn.publish(OutgoingMessage::new("audit", b"{}".as_slice())).await?;
///     txn.commit().await
/// }
/// ```
///
/// [`publish`]: Self::publish
/// [`commit`]: Self::commit
/// [`abort`]: Self::abort
#[must_use = "a transaction does nothing until settled with commit() or abort()"]
pub trait Transaction: Send {
    /// The error type returned by transaction operations.
    type Error: StdError + Send + Sync + 'static;

    /// Publishes `msg` into the transaction: buffered, not visible before [`commit`](Self::commit).
    ///
    /// A failed publish does not settle the transaction; the caller decides between retrying
    /// and [`abort`](Self::abort).
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the message cannot be buffered. For a pure client buffer
    /// this is infallible in practice; implementations that stage broker-side state may reject
    /// here.
    fn publish(
        &mut self,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Commits the transaction: the whole buffer becomes visible atomically, in publish order.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the flush fails. A failed commit has still consumed the
    /// transaction and its buffer is lost; redelivery of the inputs, not resubmission of the
    /// buffer, is the recovery path (the same rule as [`TransactionalPublisher::commit`]).
    fn commit(self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Aborts the transaction, discarding the buffer.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the implementation fails to discard staged broker-side
    /// state; for a pure client buffer this is infallible in practice.
    fn abort(self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// The headers this transaction contributes to every message published through the publish
    /// builder, underneath whatever the call site names.
    ///
    /// The transaction-side twin of [`Publisher::base_headers`], with the same default (`None`)
    /// and the same precedence: the call site wins over the transaction, the transaction wins
    /// over nothing. A transaction opened from a handle that carries an argument passes that
    /// argument on here, so a publish behaves the same inside a transaction as outside one.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{Headers, OutgoingMessage, Transaction};
    ///
    /// struct Tagged<T>(T, Headers);
    ///
    /// impl<T: Transaction> Transaction for Tagged<T> {
    ///     type Error = T::Error;
    ///
    ///     async fn publish(&mut self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
    ///         self.0.publish(msg).await
    ///     }
    ///
    ///     async fn commit(self) -> Result<(), Self::Error> {
    ///         self.0.commit().await
    ///     }
    ///
    ///     async fn abort(self) -> Result<(), Self::Error> {
    ///         self.0.abort().await
    ///     }
    ///
    ///     fn base_headers(&self) -> Option<&Headers> {
    ///         Some(&self.1)
    ///     }
    /// }
    ///
    /// async fn settle<T: Transaction>(txn: T) -> Result<(), T::Error> {
    ///     let base = [("tenant", "acme")].into_iter().collect();
    ///     Tagged(txn, base).commit().await
    /// }
    /// ```
    ///
    /// [`Publisher::base_headers`]: crate::Publisher::base_headers
    fn base_headers(&self) -> Option<&Headers> {
        None
    }
}

/// A publisher that opens caller-owned, client-buffered transactions.
///
/// This is the owned kind of the two transaction capabilities, the counterpart of the borrowed
/// [`TransactionalPublisher`]:
///
/// * owned (this trait): every [`transaction`](Self::transaction) call opens its own independent
///   transaction, and the returned [`Transaction`] value owns the buffer. Double-begin is
///   unrepresentable - there is no shared "the transaction" to collide on - and concurrent
///   transactions on one handle are legal.
/// * borrowed ([`TransactionalPublisher`]): the handle carries the broker's single transaction
///   and a begin claims it exclusively, so a second begin while one is open errors.
///
/// Implement it when the broker's transactions are client buffers flushed at commit (an AMQP
/// confirms buffer, a Redis pipeline, the in-memory broker). Kafka-like brokers, whose client
/// object holds exactly one transaction per producer, implement only the borrowed kind.
///
/// # Examples
///
/// ```
/// use ruststream::{OutgoingMessage, OwnedTransactions, Transaction};
///
/// async fn dual_write<P: OwnedTransactions>(
///     publisher: &P,
/// ) -> Result<(), Box<dyn std::error::Error>> {
///     let mut orders = publisher.transaction().await?;
///     let mut audit = publisher.transaction().await?; // concurrent with `orders`
///     orders.publish(OutgoingMessage::new("orders", b"{}".as_slice())).await?;
///     audit.publish(OutgoingMessage::new("audit", b"{}".as_slice())).await?;
///     orders.commit().await?;
///     audit.commit().await?;
///     Ok(())
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not open caller-owned transactions",
    note = "for an `Out<impl OwnedTransactions, _>` slot, attach a policy whose live publisher \
            buffers client-side transactions; Kafka-like brokers offer only the borrowed \
            `TransactionalPublisher` kind"
)]
pub trait OwnedTransactions: Publisher {
    /// The buffer-owning transaction opened by [`transaction`](Self::transaction).
    type Transaction: Transaction;

    /// Opens a new transaction owned by the returned value.
    ///
    /// Every call opens its own independent transaction: settling one never affects another,
    /// and the handle keeps publishing directly ([`Publisher::publish`]) while any number of
    /// them are open.
    ///
    /// # Errors
    ///
    /// Returns [`Publisher::Error`] when the broker refuses to open a transaction; pure
    /// client-buffer implementations are infallible in practice.
    fn transaction(&self) -> impl Future<Output = Result<Self::Transaction, Self::Error>> + Send;
}

/// A publisher that supports synchronous request / reply messaging.
///
/// Naturally implemented by `NATS` core and `NATS` `JetStream`'s `req` pattern. Brokers without
/// native reply correlation (`Kafka`, `RabbitMQ` classic queues) do not implement this; users that
/// need request / reply on those transports must emulate it themselves.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not support request / reply messaging",
    note = "for an `Out<impl RequestReply, _>` slot, attach a policy whose live publisher \
            correlates replies natively (NATS-style); Kafka and classic queues do not"
)]
pub trait RequestReply: Publisher {
    /// The reply message type.
    type Reply: IncomingMessage;

    /// Publishes `msg` and awaits a single correlated reply, or fails after `timeout`.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` when the broker rejects the publish, the reply times out, or
    /// the underlying transport fails before a reply arrives.
    fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> impl Future<Output = Result<Self::Reply, Self::Error>> + Send;
}

/// Messages or publishers that carry a routing key for broker-side partitioning.
///
/// Implemented by message types whose broker assigns partitions / shards based on a key
/// (`Kafka`, `NATS` partitioned streams). The router uses this to preserve per-key ordering when
/// dispatching to handlers.
pub trait Partitioned {
    /// Returns the partition key for this item, or `None` if the broker should pick a partition.
    fn partition_key(&self) -> Option<&[u8]>;
}

/// A connected broker whose subscriptions are fully determined by a name string.
///
/// A broker implements this when a name alone identifies one of its subscriptions: it maps the
/// name onto the subscription kind it treats as its default (a `NATS` core subject, a `Redis`
/// stream). Whatever that kind needs beyond a name, such as a consumer group or a durable
/// subscription name, is configured once on the broker itself; a broker left without that setting
/// rejects the subscription with an error naming the fix rather than inventing a default. Kinds
/// outside that default are described with a broker-specific
/// [`SubscriptionSource`](crate::SubscriptionSource) instead.
///
/// Implemented on the [`ConnectedBroker`](crate::ConnectedBroker) form: a subscription needs a
/// live connection, and the ladder makes that requirement a type, not a runtime check.
///
/// # Examples
///
/// ```
/// use ruststream::Subscribe;
///
/// async fn open<C: Subscribe>(connected: &C) -> Result<C::Subscriber, C::Error> {
///     connected.subscribe("orders").await
/// }
/// ```
pub trait Subscribe: ConnectedBroker {
    /// The subscriber type opened by a by-name subscription.
    type Subscriber: Subscriber;

    /// Opens a subscription to `name`, producing this broker's [`Subscriber`](Self::Subscriber).
    ///
    /// # Errors
    ///
    /// Returns [`ConnectedBroker::Error`](crate::ConnectedBroker::Error) when the broker rejects
    /// the subscription or the transport fails.
    fn subscribe(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> + Send;
}

/// How to reach a broker, for the `servers` section of an `AsyncAPI` document.
///
/// Each broker a service connects to is one `AsyncAPI` server. Construct it directly, or let a
/// broker that implements [`DescribeServer`] build it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ServerSpec {
    /// The host (and optional port) clients connect to, e.g. `"nats.example.com:4222"`. `None` for
    /// an in-process broker with no network address (the in-memory broker), reachable only within
    /// the running service; such a server carries no `host` in the `AsyncAPI` document.
    pub host: Option<String>,
    /// The messaging protocol, e.g. `"nats"`, `"kafka"`, `"amqp"`, or `"memory"` for the in-process
    /// broker.
    pub protocol: String,
    /// An optional human description of this server.
    pub description: Option<String>,
    /// How clients authenticate to this server, emitted as the `AsyncAPI` server's `security`
    /// list. Empty by default: authentication is a property of the described deployment (the same
    /// broker is deployed publicly and internally with different schemes), so the service author
    /// states it at registration ([`with_security`](Self::with_security)); brokers never set it.
    pub security: Vec<SecurityScheme>,
}

impl ServerSpec {
    /// Describes a server reachable at `host` over `protocol`.
    #[must_use]
    pub fn new(host: impl Into<String>, protocol: impl Into<String>) -> Self {
        Self {
            host: Some(host.into()),
            protocol: protocol.into(),
            description: None,
            security: Vec::new(),
        }
    }

    /// Describes an in-process server with no network address (the in-memory broker), reachable only
    /// within the running service.
    ///
    /// It still has a stable identity (its label / server name) so a multi-broker service can route
    /// to and distinguish it, but the generated `AsyncAPI` server carries no `host`.
    #[must_use]
    pub fn in_process(protocol: impl Into<String>) -> Self {
        Self {
            host: None,
            protocol: protocol.into(),
            description: None,
            security: Vec::new(),
        }
    }

    /// Builder-style setter for the server description.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Adds a security scheme clients use to authenticate to this server. Call repeatedly to
    /// declare alternatives; without any call the generated document carries no security
    /// sections, exactly as before.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// let spec = ServerSpec::new("kafka.example.com:9093", "kafka")
    ///     .with_security(SecurityScheme::scram_sha512().with_description("SASL over TLS"));
    /// assert_eq!(spec.security.len(), 1);
    /// ```
    #[must_use]
    pub fn with_security(mut self, scheme: SecurityScheme) -> Self {
        self.security.push(scheme);
        self
    }
}

/// How clients authenticate to an [`AsyncAPI` server](ServerSpec), per the `AsyncAPI` 3.0
/// security scheme types.
///
/// Constructed with the per-kind constructors ([`scram_sha512`](Self::scram_sha512),
/// [`user_password`](Self::user_password), ...) and attached to a server with
/// [`ServerSpec::with_security`]. For a scheme shape the constructors do not model, use
/// [`custom`](Self::custom) with the raw `AsyncAPI` security scheme object.
///
/// # Examples
///
/// ```
/// use ruststream::SecurityScheme;
///
/// let scheme = SecurityScheme::user_password().with_description("service credentials");
/// # let _ = scheme;
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityScheme {
    pub(crate) kind: SecuritySchemeKind,
    pub(crate) description: Option<String>,
}

/// The scheme kind and its type-specific fields. Raw JSON payloads (`oauth2` flows, `custom`)
/// are stored as serialized text so the containing types stay `Eq`.
// Only the asyncapi module reads the fields (into the generated document); without that feature
// they are write-only, which is fine - the data still travels through ServerSpec equality.
#[cfg_attr(not(feature = "asyncapi"), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SecuritySchemeKind {
    UserPassword,
    ApiKey {
        location: ApiKeyLocation,
    },
    X509,
    Plain,
    ScramSha256,
    ScramSha512,
    Gssapi,
    Http {
        scheme: String,
    },
    HttpApiKey {
        name: String,
        location: HttpApiKeyLocation,
    },
    OpenIdConnect {
        url: String,
    },
    Oauth2 {
        flows: String,
    },
    Custom {
        object: String,
    },
}

/// Where an `apiKey` scheme carries the key, per `AsyncAPI` 3.0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKeyLocation {
    /// The key rides in the user field of the transport's credentials.
    User,
    /// The key rides in the password field of the transport's credentials.
    Password,
}

impl ApiKeyLocation {
    /// The `AsyncAPI` document value; read by the asyncapi module only.
    #[cfg_attr(not(feature = "asyncapi"), allow(dead_code))]
    pub(crate) fn as_api(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Password => "password",
        }
    }
}

/// Where an `httpApiKey` scheme carries the key, per `AsyncAPI` 3.0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpApiKeyLocation {
    /// A query parameter.
    Query,
    /// An HTTP header.
    Header,
    /// A cookie.
    Cookie,
}

impl HttpApiKeyLocation {
    /// The `AsyncAPI` document value; read by the asyncapi module only.
    #[cfg_attr(not(feature = "asyncapi"), allow(dead_code))]
    pub(crate) fn as_api(self) -> &'static str {
        match self {
            Self::Query => "query",
            Self::Header => "header",
            Self::Cookie => "cookie",
        }
    }
}

impl SecurityScheme {
    fn of(kind: SecuritySchemeKind) -> Self {
        Self {
            kind,
            description: None,
        }
    }

    /// Username and password credentials (`userPassword`).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    /// let scheme = SecurityScheme::user_password();
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn user_password() -> Self {
        Self::of(SecuritySchemeKind::UserPassword)
    }

    /// An API key carried in the transport credentials (`apiKey`).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{ApiKeyLocation, SecurityScheme};
    /// let scheme = SecurityScheme::api_key(ApiKeyLocation::User);
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn api_key(location: ApiKeyLocation) -> Self {
        Self::of(SecuritySchemeKind::ApiKey { location })
    }

    /// Mutual TLS with client certificates (`X509`).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    /// let scheme = SecurityScheme::x509();
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn x509() -> Self {
        Self::of(SecuritySchemeKind::X509)
    }

    /// SASL PLAIN (`plain`).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    /// let scheme = SecurityScheme::plain();
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn plain() -> Self {
        Self::of(SecuritySchemeKind::Plain)
    }

    /// SASL SCRAM-SHA-256 (`scramSha256`).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    /// let scheme = SecurityScheme::scram_sha256();
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn scram_sha256() -> Self {
        Self::of(SecuritySchemeKind::ScramSha256)
    }

    /// SASL SCRAM-SHA-512 (`scramSha512`).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    /// let scheme = SecurityScheme::scram_sha512();
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn scram_sha512() -> Self {
        Self::of(SecuritySchemeKind::ScramSha512)
    }

    /// SASL GSSAPI / Kerberos (`gssapi`).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    /// let scheme = SecurityScheme::gssapi();
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn gssapi() -> Self {
        Self::of(SecuritySchemeKind::Gssapi)
    }

    /// An HTTP authentication scheme (`http`), e.g. `"basic"` or `"bearer"` - for brokers with
    /// HTTP-based control or connection endpoints.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    /// let scheme = SecurityScheme::http("bearer");
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn http(scheme: impl Into<String>) -> Self {
        Self::of(SecuritySchemeKind::Http {
            scheme: scheme.into(),
        })
    }

    /// An API key in an HTTP query parameter, header, or cookie (`httpApiKey`).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{HttpApiKeyLocation, SecurityScheme};
    /// let scheme = SecurityScheme::http_api_key("X-Api-Key", HttpApiKeyLocation::Header);
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn http_api_key(name: impl Into<String>, location: HttpApiKeyLocation) -> Self {
        Self::of(SecuritySchemeKind::HttpApiKey {
            name: name.into(),
            location,
        })
    }

    /// `OpenID Connect` discovery (`openIdConnect`), pointing at the provider's discovery URL.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    /// let scheme = SecurityScheme::open_id_connect("https://idp.example.com/.well-known/openid-configuration");
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn open_id_connect(url: impl Into<String>) -> Self {
        Self::of(SecuritySchemeKind::OpenIdConnect { url: url.into() })
    }

    /// `OAuth2` (`oauth2`) with the given `AsyncAPI` flows object, passed as raw JSON (the flows
    /// shape is deep and deployment-specific, so it is not modeled field by field).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    ///
    /// let scheme = SecurityScheme::oauth2(serde_json::json!({
    ///     "clientCredentials": {
    ///         "tokenUrl": "https://idp.example.com/token",
    ///         "availableScopes": { "kafka:write": "produce" },
    ///     }
    /// }));
    /// # let _ = scheme;
    /// ```
    #[cfg(feature = "json")]
    #[must_use]
    // By-value keeps the natural `oauth2(json!(..))` call shape; the value is serialized on the
    // spot, so borrowing would only force callers to hold a temporary.
    #[allow(clippy::needless_pass_by_value)]
    pub fn oauth2(flows: serde_json::Value) -> Self {
        Self::of(SecuritySchemeKind::Oauth2 {
            flows: flows.to_string(),
        })
    }

    /// An arbitrary `AsyncAPI` security scheme object, emitted as-is - the escape hatch for
    /// kinds or fields the constructors do not model.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    ///
    /// let scheme = SecurityScheme::custom(serde_json::json!({ "type": "symmetricEncryption" }));
    /// # let _ = scheme;
    /// ```
    #[cfg(feature = "json")]
    #[must_use]
    // By-value keeps the natural `custom(json!(..))` call shape; the value is serialized on the
    // spot, so borrowing would only force callers to hold a temporary.
    #[allow(clippy::needless_pass_by_value)]
    pub fn custom(object: serde_json::Value) -> Self {
        Self::of(SecuritySchemeKind::Custom {
            object: object.to_string(),
        })
    }

    /// Builder-style setter for the scheme description.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::SecurityScheme;
    /// let scheme = SecurityScheme::plain().with_description("SASL over TLS");
    /// # let _ = scheme;
    /// ```
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// A broker that describes itself as an `AsyncAPI` server.
///
/// Broker crates implement this so their connection coordinates land in the generated `AsyncAPI`
/// document and the broker carries a stable identity when registered with
/// [`with_broker_labeled`](crate::runtime::RustStream::with_broker_labeled); it can also be wired on
/// manually with [`RustStream::server`](crate::runtime::RustStream::server). A broker without a
/// network address (the in-memory broker) describes itself with
/// [`ServerSpec::in_process`], so it still gets a label / identity for multi-broker routing.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, DescribeServer, ServerSpec};
///
/// fn describe<B: DescribeServer>(broker: &B) -> ServerSpec {
///     broker.describe_server()
/// }
/// ```
pub trait DescribeServer: Broker {
    /// Returns the server coordinates for this broker.
    fn describe_server(&self) -> ServerSpec;
}

#[cfg(test)]
mod tests;
