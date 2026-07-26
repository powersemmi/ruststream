//! Optional capability traits implemented by brokers that support specific semantics.
//!
//! Brokers implement only the capabilities they natively support. Generic runtime code that
//! depends on a capability adds it as a bound, leaving brokers that do not support it free of
//! emulation cost.

use std::{future::Future, time::Duration};

use futures::Stream;

use crate::{Broker, IncomingMessage, OutgoingMessage, Publisher, Subscriber};

/// A subscriber that natively delivers messages in batches.
///
/// Brokers that batch on the wire (`Kafka`, `JetStream` pull consumers) implement this so the
/// runtime can dispatch a whole batch through middleware in one go. Brokers without native
/// batching simply do not implement it.
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
/// [`begin_transaction`]: Self::begin_transaction
/// [`commit`]: Self::commit
/// [`abort`]: Self::abort
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

/// A publisher that supports synchronous request / reply messaging.
///
/// Naturally implemented by `NATS` core and `NATS` `JetStream`'s `req` pattern. Brokers without
/// native reply correlation (`Kafka`, `RabbitMQ` classic queues) do not implement this; users that
/// need request / reply on those transports must emulate it themselves.
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

/// A broker whose subscriptions are fully determined by a name string.
///
/// This is the common case (`NATS` core subjects, the in-memory broadcast broker, `Redis` pub/sub
/// channels): no consumer group, partition, or durable-consumer configuration is needed to open a
/// subscription, so the runtime can subscribe given just a name. Brokers whose subscriptions
/// require richer options (`Kafka` consumer groups, `JetStream` durable consumers) do not
/// implement `Subscribe`; callers describe those with a broker-specific
/// [`SubscriptionSource`](crate::SubscriptionSource) instead.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, Subscribe};
///
/// async fn open<B: Subscribe>(broker: &B) -> Result<B::Subscriber, B::Error> {
///     broker.subscribe("orders").await
/// }
/// ```
pub trait Subscribe: Broker {
    /// The subscriber type opened by a by-name subscription.
    type Subscriber: Subscriber;

    /// Opens a subscription to `name`, producing this broker's [`Subscriber`](Self::Subscriber).
    ///
    /// Called after [`Broker::connect`]; implementations may assume a live connection.
    ///
    /// # Errors
    ///
    /// Returns [`Broker::Error`] when the broker rejects the subscription or the transport fails.
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
mod tests {
    use super::*;

    #[test]
    fn with_security_accumulates_schemes_in_order() {
        let spec = ServerSpec::new("kafka.example.com:9093", "kafka")
            .with_security(SecurityScheme::scram_sha512())
            .with_security(SecurityScheme::x509());
        assert_eq!(spec.security.len(), 2);
        assert_eq!(spec.security[0].kind, SecuritySchemeKind::ScramSha512);
        assert_eq!(spec.security[1].kind, SecuritySchemeKind::X509);
    }

    #[test]
    fn in_process_spec_starts_without_security() {
        let spec = ServerSpec::in_process("memory");
        assert!(spec.security.is_empty());
    }

    #[test]
    fn constructors_produce_their_kind() {
        let cases = [
            (
                SecurityScheme::user_password(),
                SecuritySchemeKind::UserPassword,
            ),
            (
                SecurityScheme::api_key(ApiKeyLocation::User),
                SecuritySchemeKind::ApiKey {
                    location: ApiKeyLocation::User,
                },
            ),
            (SecurityScheme::x509(), SecuritySchemeKind::X509),
            (SecurityScheme::plain(), SecuritySchemeKind::Plain),
            (
                SecurityScheme::scram_sha256(),
                SecuritySchemeKind::ScramSha256,
            ),
            (
                SecurityScheme::scram_sha512(),
                SecuritySchemeKind::ScramSha512,
            ),
            (SecurityScheme::gssapi(), SecuritySchemeKind::Gssapi),
            (
                SecurityScheme::http("bearer"),
                SecuritySchemeKind::Http {
                    scheme: "bearer".into(),
                },
            ),
            (
                SecurityScheme::http_api_key("X-Api-Key", HttpApiKeyLocation::Header),
                SecuritySchemeKind::HttpApiKey {
                    name: "X-Api-Key".into(),
                    location: HttpApiKeyLocation::Header,
                },
            ),
            (
                SecurityScheme::open_id_connect("https://idp.example.com/.well-known"),
                SecuritySchemeKind::OpenIdConnect {
                    url: "https://idp.example.com/.well-known".into(),
                },
            ),
        ];
        for (scheme, kind) in cases {
            assert_eq!(scheme.kind, kind);
            assert_eq!(scheme.description, None);
        }
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_backed_constructors_serialize_their_payload() {
        let oauth2 = SecurityScheme::oauth2(serde_json::json!({ "clientCredentials": {} }));
        assert_eq!(
            oauth2.kind,
            SecuritySchemeKind::Oauth2 {
                flows: r#"{"clientCredentials":{}}"#.into(),
            }
        );

        let custom = SecurityScheme::custom(serde_json::json!({ "type": "symmetricEncryption" }));
        assert_eq!(
            custom.kind,
            SecuritySchemeKind::Custom {
                object: r#"{"type":"symmetricEncryption"}"#.into(),
            }
        );
    }

    #[test]
    fn with_description_sets_the_description() {
        let scheme = SecurityScheme::plain().with_description("SASL over TLS");
        assert_eq!(scheme.description.as_deref(), Some("SASL over TLS"));
    }

    #[test]
    fn api_key_locations_map_to_document_values() {
        assert_eq!(ApiKeyLocation::User.as_api(), "user");
        assert_eq!(ApiKeyLocation::Password.as_api(), "password");
    }

    #[test]
    fn http_api_key_locations_map_to_document_values() {
        assert_eq!(HttpApiKeyLocation::Query.as_api(), "query");
        assert_eq!(HttpApiKeyLocation::Header.as_api(), "header");
        assert_eq!(HttpApiKeyLocation::Cookie.as_api(), "cookie");
    }
}
