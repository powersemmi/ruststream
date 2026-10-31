//! How a broker describes the server it connects to, for the `servers` section of an `AsyncAPI`
//! document: its coordinates and the security schemes it accepts.

use alloc::borrow::ToOwned;
use alloc::string::String;
#[cfg(any(feature = "asyncapi", feature = "json"))]
use alloc::string::ToString;
use alloc::vec::Vec;
#[cfg(any(feature = "asyncapi", feature = "json"))]
use serde_json::Value;
#[cfg(feature = "asyncapi")]
use serde_json::json;

use crate::Broker;
#[cfg(feature = "asyncapi")]
use crate::bindings::Bindings;

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
    ///
    /// Credentials never belong here. A broker configured from a URL takes its host through
    /// [`from_url`](Self::from_url), which drops the userinfo a URL like
    /// `amqp://user:password@host` carries.
    pub host: Option<String>,
    /// The messaging protocol, e.g. `"nats"`, `"kafka"`, `"amqp"`, or `"memory"` for the in-process
    /// broker.
    pub protocol: String,
    /// The version of that protocol, when the protocol has versions a client has to match:
    /// `"0.9.1"` against `"1.0"` for AMQP, `"5"` for MQTT. `None` where the protocol name already
    /// says everything, which is the usual case.
    pub protocol_version: Option<String>,
    /// An optional human description of this server.
    pub description: Option<String>,
    /// How clients authenticate to this server, emitted as the `AsyncAPI` server's `security`
    /// list. Empty by default: authentication is a property of the described deployment, so the
    /// service author states it at registration ([`security`](method@Self::security)); brokers
    /// never set it.
    pub security: Vec<SecurityScheme>,
    /// The server binding the broker contributes, emitted as the `AsyncAPI` server's `bindings`
    /// object. Empty by default.
    ///
    /// This is the broker's own vocabulary, which the core never names: a `MQTT` client id and
    /// keep-alive, a Kafka schema-registry URL. A credential has no place here for the same
    /// reason it has none in [`host`](Self::host): the document is published.
    #[cfg(feature = "asyncapi")]
    pub bindings: Bindings,
}

impl ServerSpec {
    /// Describes a server reachable at `host` over `protocol`.
    #[must_use]
    pub fn new(host: impl Into<String>, protocol: impl Into<String>) -> Self {
        Self {
            host: Some(host.into()),
            protocol: protocol.into(),
            protocol_version: None,
            description: None,
            security: Vec::new(),
            #[cfg(feature = "asyncapi")]
            bindings: Bindings::new(),
        }
    }

    /// Describes a server reachable at the host and port `url` names, over `protocol`.
    ///
    /// This is how a broker configured from a URL describes itself. Broker URLs carry credentials
    /// (`amqp://user:password@host:5672`) and a server description is published in the service's
    /// `AsyncAPI` document, so the userinfo is dropped here rather than left to each broker crate
    /// to remember. The scheme goes with it, as does anything after the host: a path, a vhost, a
    /// query.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{DescribeServer, ServerSpec};
    /// # use ruststream::{Broker, ConnectedBroker};
    /// # struct ConnectedRabbit;
    /// # impl Broker for RabbitBroker {
    /// #     type Error = std::io::Error;
    /// #     type Connected = ConnectedRabbit;
    /// #     async fn connect(self) -> Result<ConnectedRabbit, Self::Error> { Ok(ConnectedRabbit) }
    /// # }
    /// # impl ConnectedBroker for ConnectedRabbit {
    /// #     type Error = std::io::Error;
    /// #     type Closed = ();
    /// #     async fn shutdown(self) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    ///
    /// /// Configured from `amqp://svc:secret@broker.example.com:5672/prod`.
    /// struct RabbitBroker {
    ///     url: String,
    /// }
    ///
    /// impl DescribeServer for RabbitBroker {
    ///     fn describe_server(&self) -> ServerSpec {
    ///         // The document carries `broker.example.com:5672`, never the password.
    ///         ServerSpec::from_url(&self.url, "amqp")
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn from_url(url: &str, protocol: impl Into<String>) -> Self {
        Self::new(Self::host_from_url(url), protocol)
    }

    /// The host and port `url` names, without the scheme, the userinfo, or anything after the
    /// host.
    ///
    /// [`from_url`](Self::from_url) is the whole job for a broker configured from one URL. This is
    /// the piece for a broker that configures several addresses and joins them into one host
    /// string itself.
    ///
    /// Never fails: a server description must not hold up startup over a URL the connection
    /// itself will reject.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{DescribeServer, ServerSpec};
    /// # use ruststream::{Broker, ConnectedBroker};
    /// # struct ConnectedNats;
    /// # impl Broker for NatsBroker {
    /// #     type Error = std::io::Error;
    /// #     type Connected = ConnectedNats;
    /// #     async fn connect(self) -> Result<ConnectedNats, Self::Error> { Ok(ConnectedNats) }
    /// # }
    /// # impl ConnectedBroker for ConnectedNats {
    /// #     type Error = std::io::Error;
    /// #     type Closed = ();
    /// #     async fn shutdown(self) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    ///
    /// /// Configured with every seed of the cluster, each with its own credentials.
    /// struct NatsBroker {
    ///     seeds: Vec<String>,
    /// }
    ///
    /// impl DescribeServer for NatsBroker {
    ///     fn describe_server(&self) -> ServerSpec {
    ///         let hosts: Vec<String> =
    ///             self.seeds.iter().map(|url| ServerSpec::host_from_url(url)).collect();
    ///         ServerSpec::new(hosts.join(","), "nats")
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn host_from_url(url: &str) -> String {
        let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
        // The authority ends at the first '/', '?' or '#', so an '@' past that point belongs to a
        // path or a query and separates nothing: cutting on '@' first reads `nats://host/a@b` as a
        // host of `b`.
        let authority = after_scheme
            .split_once(['/', '?', '#'])
            .map_or(after_scheme, |(authority, _)| authority);
        // Inside the authority the last '@' is the boundary, because a password may contain one.
        authority
            .rsplit_once('@')
            .map_or(authority, |(_, host)| host)
            .to_owned()
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
            protocol_version: None,
            description: None,
            security: Vec::new(),
            #[cfg(feature = "asyncapi")]
            bindings: Bindings::new(),
        }
    }

    /// Builder-style setter for the server description.
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Names the version of the protocol clients speak to this server.
    ///
    /// Worth filling in wherever one protocol name covers incompatible versions: a reader cannot
    /// tell AMQP 0.9.1 from AMQP 1.0 by the server's host.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{DescribeServer, ServerSpec};
    /// # use ruststream::{Broker, ConnectedBroker};
    /// # struct ConnectedRabbit;
    /// # impl Broker for RabbitBroker {
    /// #     type Error = std::io::Error;
    /// #     type Connected = ConnectedRabbit;
    /// #     async fn connect(self) -> Result<ConnectedRabbit, Self::Error> { Ok(ConnectedRabbit) }
    /// # }
    /// # impl ConnectedBroker for ConnectedRabbit {
    /// #     type Error = std::io::Error;
    /// #     type Closed = ();
    /// #     async fn shutdown(self) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    ///
    /// struct RabbitBroker {
    ///     url: String,
    /// }
    ///
    /// impl DescribeServer for RabbitBroker {
    ///     fn describe_server(&self) -> ServerSpec {
    ///         // The client speaks AMQP 0-9-1, not the 1.0 the protocol key alone would suggest.
    ///         ServerSpec::from_url(&self.url, "amqp").protocol_version("0.9.1")
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn protocol_version(mut self, version: impl Into<String>) -> Self {
        self.protocol_version = Some(version.into());
        self
    }

    /// Adds a security scheme clients use to authenticate to this server. Call repeatedly to
    /// declare alternatives; without any call the generated document carries no security
    /// sections, exactly as before.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     let production = ServerSpec::new("kafka.example.com:9093", "kafka")
    ///         .security(SecurityScheme::scram_sha512().description("SASL over TLS"));
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
    /// ```
    #[must_use]
    pub fn security(mut self, scheme: SecurityScheme) -> Self {
        self.security.push(scheme);
        self
    }

    /// Sets the server binding this broker contributes (see [`bindings`](Self::bindings)).
    ///
    /// Computed from the broker's configuration alone: the document is built before anything
    /// connects, so a value only the live connection knows has no place in it. A credential has
    /// no place in it either.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "asyncapi")]
    /// # mod demo {
    /// use ruststream::ServerSpec;
    /// use ruststream::asyncapi::{Binding, BindingError, Bindings};
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct MqttServer {
    ///     #[serde(rename = "clientId")]
    ///     client_id: &'static str,
    /// }
    ///
    /// pub fn app() -> Result<impl App, BindingError> {
    ///     let binding = Binding::new(
    ///         "mqtt",
    ///         "0.2.0",
    ///         &MqttServer {
    ///             client_id: "orders",
    ///         },
    ///     )?;
    ///     let production = ServerSpec::new("mqtt.example.com:1883", "mqtt")
    ///         .bindings(Bindings::new().with(binding));
    ///     Ok(RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production))
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[cfg(feature = "asyncapi")]
    #[must_use]
    pub fn bindings(mut self, bindings: Bindings) -> Self {
        self.bindings = bindings;
        self
    }
}

/// How clients authenticate to an [`AsyncAPI` server](ServerSpec), per the `AsyncAPI`
/// security scheme types.
///
/// Constructed with the per-kind constructors ([`scram_sha512`](Self::scram_sha512),
/// [`user_password`](Self::user_password), ...) and attached to a server with
/// [`ServerSpec::security`](method@ServerSpec::security). For a scheme shape the constructors do
/// not model, use [`custom`](Self::custom) with the raw `AsyncAPI` security scheme object.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{App, AppInfo, RustStream};
/// use ruststream::{SecurityScheme, ServerSpec};
///
/// fn app() -> impl App {
///     let production = ServerSpec::new("rabbit.example.com:5672", "amqp")
///         .security(SecurityScheme::user_password().description("service credentials"));
///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityScheme {
    pub(crate) kind: SecuritySchemeKind,
    pub(crate) description: Option<String>,
}

/// The scheme kind and its type-specific fields. Raw JSON payloads (`oauth2` flows, `custom`)
/// are stored as serialized text so the containing types stay `Eq`.
// Only the asyncapi module reads the fields (into the generated document); without that feature
// they are write-only.
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

/// Where an `apiKey` scheme carries the key, per `AsyncAPI`.
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

/// Where an `httpApiKey` scheme carries the key, per `AsyncAPI`.
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
    /// The scheme as its `AsyncAPI` security scheme object, as the generated document renders
    /// it.
    #[cfg(feature = "asyncapi")]
    #[doc(hidden)]
    #[must_use]
    pub fn document_object(&self) -> Value {
        use SecuritySchemeKind as Kind;

        // Raw payloads round-trip through the string the constructor serialized, so parsing them
        // back cannot fail; Null is the unreachable fallback, not an error path.
        let parse = |raw: &str| serde_json::from_str::<Value>(raw).unwrap_or(Value::Null);
        let mut object = match &self.kind {
            Kind::UserPassword => json!({ "type": "userPassword" }),
            Kind::ApiKey { location } => {
                json!({ "type": "apiKey", "in": location.as_api() })
            }
            Kind::X509 => json!({ "type": "X509" }),
            Kind::Plain => json!({ "type": "plain" }),
            Kind::ScramSha256 => json!({ "type": "scramSha256" }),
            Kind::ScramSha512 => json!({ "type": "scramSha512" }),
            Kind::Gssapi => json!({ "type": "gssapi" }),
            Kind::Http { scheme } => json!({ "type": "http", "scheme": scheme }),
            Kind::HttpApiKey { name, location } => json!({
                "type": "httpApiKey",
                "name": name,
                "in": location.as_api(),
            }),
            Kind::OpenIdConnect { url } => json!({
                "type": "openIdConnect",
                "openIdConnectUrl": url,
            }),
            Kind::Oauth2 { flows } => json!({ "type": "oauth2", "flows": parse(flows) }),
            Kind::Custom { object } => parse(object),
        };
        if let (Some(description), Some(fields)) = (&self.description, object.as_object_mut()) {
            fields.insert("description".to_owned(), Value::String(description.clone()));
        }
        object
    }

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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     let production = ServerSpec::new("rabbit.example.com:5672", "amqp")
    ///         .security(SecurityScheme::user_password());
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{ApiKeyLocation, SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     // The token rides in the user field of the connection credentials.
    ///     let production = ServerSpec::new("pulsar.example.com:6651", "pulsar")
    ///         .security(SecurityScheme::api_key(ApiKeyLocation::User));
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     // Clients present a certificate the broker trusts.
    ///     let production =
    ///         ServerSpec::new("rabbit.example.com:5671", "amqp").security(SecurityScheme::x509());
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     let production = ServerSpec::new("kafka.example.com:9093", "kafka")
    ///         .security(SecurityScheme::plain());
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     let production = ServerSpec::new("kafka.example.com:9093", "kafka")
    ///         .security(SecurityScheme::scram_sha256());
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     let production = ServerSpec::new("kafka.example.com:9093", "kafka")
    ///         .security(SecurityScheme::scram_sha512());
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     // Kerberos, through the cluster's KDC.
    ///     let production = ServerSpec::new("kafka.example.com:9093", "kafka")
    ///         .security(SecurityScheme::gssapi());
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     let production = ServerSpec::new("gateway.example.com:443", "websockets")
    ///         .security(SecurityScheme::http("bearer"));
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{HttpApiKeyLocation, SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     let production = ServerSpec::new("gateway.example.com:443", "websockets").security(
    ///         SecurityScheme::http_api_key("X-Api-Key", HttpApiKeyLocation::Header),
    ///     );
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     let production = ServerSpec::new("gateway.example.com:443", "websockets").security(
    ///         SecurityScheme::open_id_connect(
    ///             "https://idp.example.com/.well-known/openid-configuration",
    ///         ),
    ///     );
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    /// use serde_json::json;
    ///
    /// fn app() -> impl App {
    ///     let oauth = SecurityScheme::oauth2(json!({
    ///         "clientCredentials": {
    ///             "tokenUrl": "https://idp.example.com/token",
    ///             "availableScopes": { "kafka:write": "produce" },
    ///         }
    ///     }));
    ///     let production = ServerSpec::new("kafka.example.com:9093", "kafka").security(oauth);
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
    /// ```
    #[cfg(feature = "json")]
    #[must_use]
    // The value is serialized on the spot; by-value keeps the `oauth2(json!(..))` call shape.
    #[allow(clippy::needless_pass_by_value)]
    pub fn oauth2(flows: Value) -> Self {
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
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    /// use serde_json::json;
    ///
    /// fn app() -> impl App {
    ///     // Every frame on this bus is encrypted with a key the operators hand out.
    ///     let encrypted = SecurityScheme::custom(json!({ "type": "symmetricEncryption" }));
    ///     let production = ServerSpec::new("bus.example.com:5555", "zmq").security(encrypted);
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
    /// ```
    #[cfg(feature = "json")]
    #[must_use]
    // The value is serialized on the spot; by-value keeps the `custom(json!(..))` call shape.
    #[allow(clippy::needless_pass_by_value)]
    pub fn custom(object: Value) -> Self {
        Self::of(SecuritySchemeKind::Custom {
            object: object.to_string(),
        })
    }

    /// Builder-style setter for the scheme description.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{SecurityScheme, ServerSpec};
    ///
    /// fn app() -> impl App {
    ///     let production = ServerSpec::new("kafka.example.com:9093", "kafka")
    ///         .security(SecurityScheme::plain().description("SASL over TLS"));
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).server("production", production)
    /// }
    /// ```
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// A broker that describes itself as an `AsyncAPI` server.
///
/// Broker crates implement this so their connection coordinates land in the generated
/// `AsyncAPI` document and the broker carries a stable identity when registered with
/// [`with_broker_labeled`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.with_broker_labeled);
/// it can also be wired on manually with
/// [`RustStream::server`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.server).
/// A broker without a network address (the in-memory broker) describes itself with
/// [`ServerSpec::in_process`], so it still gets a label / identity for multi-broker
/// routing.
///
/// # The host is a coordinate, not the configuration
///
/// A description carries the host and port clients connect to, and nothing else the broker
/// was configured with. Credentials in particular never appear in it: the document is
/// generated to be published and shared, so a password that reaches it has left the
/// service.
///
/// A broker configured from a URL therefore describes itself with
/// [`ServerSpec::from_url`], which drops the userinfo, rather than trimming the scheme off
/// the URL and passing the rest to [`ServerSpec::new`]. A broker that configures several
/// addresses joins them from [`ServerSpec::host_from_url`].
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, ConnectedBroker, DescribeServer, ServerSpec};
/// # struct AmqpBroker { url: String }
/// # struct ConnectedAmqp;
/// # impl Broker for AmqpBroker {
/// #     type Error = std::io::Error;
/// #     type Connected = ConnectedAmqp;
/// #     async fn connect(self) -> Result<ConnectedAmqp, Self::Error> { Ok(ConnectedAmqp) }
/// # }
/// # impl ConnectedBroker for ConnectedAmqp {
/// #     type Error = std::io::Error;
/// #     type Closed = ();
/// #     async fn shutdown(self) -> Result<(), Self::Error> { Ok(()) }
/// # }
///
/// impl DescribeServer for AmqpBroker {
///     fn describe_server(&self) -> ServerSpec {
///         // `amqp://svc:secret@broker:5672/prod` is described as `broker:5672`.
///         ServerSpec::from_url(&self.url, "amqp")
///     }
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
    fn security_accumulates_schemes_in_order() {
        let spec = ServerSpec::new("kafka.example.com:9093", "kafka")
            .security(SecurityScheme::scram_sha512())
            .security(SecurityScheme::x509());
        assert_eq!(spec.security.len(), 2);
        assert_eq!(spec.security[0].kind, SecuritySchemeKind::ScramSha512);
        assert_eq!(spec.security[1].kind, SecuritySchemeKind::X509);
    }

    #[test]
    fn in_process_spec_starts_without_security() {
        let spec = ServerSpec::in_process("memory");
        assert_eq!(spec.security, Vec::new());
    }

    #[test]
    fn the_host_survives_a_scheme_userinfo_a_path_and_a_query() {
        let cases = [
            ("amqp://localhost:5672", "localhost:5672"),
            ("amqp://user:pass@broker:5672", "broker:5672"),
            ("amqps://broker:5671/vhost", "broker:5671"),
            ("nats://broker:4222/?tls=true", "broker:4222"),
            ("redis://:secret@cache:6379", "cache:6379"),
            // No scheme at all: a bare address is already the coordinate.
            ("broker:5672", "broker:5672"),
            // A password may contain '@', so inside the authority the boundary is the last one.
            ("amqp://user:p@ss@broker:5672", "broker:5672"),
            // A host without a port stays a host.
            ("mqtt://user:pass@broker", "broker"),
            // The authority ends before the path, the query and the fragment, so an '@' past it
            // separates nothing. Cutting on '@' before cutting the path reports a host of "b".
            ("nats://broker:4222/a@b", "broker:4222"),
            ("nats://broker:4222/?token=a@b", "broker:4222"),
            ("nats://broker:4222#a@b", "broker:4222"),
            // A token carries no colon and no user name, and is userinfo all the same.
            ("nats://s3cret-token@broker:4222", "broker:4222"),
        ];
        for (url, expected) in cases {
            assert_eq!(ServerSpec::host_from_url(url), expected, "url {url:?}");
        }
    }

    /// The document a service generates is meant to be published, so a URL that authenticates the
    /// connection must not describe the server it reaches.
    #[test]
    fn a_url_carrying_credentials_describes_a_server_without_them() {
        let spec = ServerSpec::from_url("amqp://svc:secret@broker.example.com:5672/prod", "amqp");

        assert_eq!(spec.host.as_deref(), Some("broker.example.com:5672"));
        assert_eq!(spec.protocol, "amqp");

        let host = spec.host.expect("a networked broker describes a host");
        assert!(!host.contains("secret"), "the description leaked {host:?}");
        assert!(!host.contains("svc"), "the description leaked {host:?}");
        assert!(!host.contains('@'), "the description leaked {host:?}");
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
    fn the_description_reaches_the_scheme() {
        let scheme = SecurityScheme::plain().description("SASL over TLS");
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

    #[cfg(all(feature = "asyncapi", feature = "json"))]
    #[test]
    fn every_scheme_kind_renders_its_document_object() {
        let cases = [
            (
                SecurityScheme::user_password(),
                json!({ "type": "userPassword" }),
            ),
            (
                SecurityScheme::api_key(ApiKeyLocation::Password),
                json!({ "type": "apiKey", "in": "password" }),
            ),
            (SecurityScheme::x509(), json!({ "type": "X509" })),
            (SecurityScheme::plain(), json!({ "type": "plain" })),
            (
                SecurityScheme::scram_sha256(),
                json!({ "type": "scramSha256" }),
            ),
            (
                SecurityScheme::scram_sha512(),
                json!({ "type": "scramSha512" }),
            ),
            (SecurityScheme::gssapi(), json!({ "type": "gssapi" })),
            (
                SecurityScheme::http("bearer"),
                json!({ "type": "http", "scheme": "bearer" }),
            ),
            (
                SecurityScheme::http_api_key("X-Api-Key", HttpApiKeyLocation::Header),
                json!({ "type": "httpApiKey", "name": "X-Api-Key", "in": "header" }),
            ),
            (
                SecurityScheme::open_id_connect("https://idp.example.com/.well-known"),
                json!({
                    "type": "openIdConnect",
                    "openIdConnectUrl": "https://idp.example.com/.well-known",
                }),
            ),
            (
                SecurityScheme::oauth2(json!({ "clientCredentials": {} })),
                json!({ "type": "oauth2", "flows": { "clientCredentials": {} } }),
            ),
            (
                SecurityScheme::custom(json!({ "type": "symmetricEncryption" })),
                json!({ "type": "symmetricEncryption" }),
            ),
        ];
        for (scheme, expected) in cases {
            assert_eq!(scheme.document_object(), expected);
        }
    }
}
