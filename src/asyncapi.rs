//! `AsyncAPI` 3.0 document generation from a [`RustStream`](crate::runtime::RustStream) service.
//!
//! [`build_spec`] turns a service's registered handlers and metadata into a [`Spec`] that
//! serializes to an `AsyncAPI` 3.0 document ([`to_json`](Spec::to_json) / [`to_yaml`](Spec::to_yaml)).
//! Hosting it over HTTP is the user's concern; [`render_viewer_html`] produces a ready-to-serve HTML
//! page that renders the document with the `AsyncAPI` React component from a CDN.
//!
//! The document covers info, servers, channels, operations, and per-message payload JSON schemas
//! (for message types that implement [`schemars::JsonSchema`]).

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;
use tracing::warn;

use crate::runtime::App;

/// An `AsyncAPI` 3.0 document.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Spec {
    /// The `AsyncAPI` specification version (always `"3.0.0"`).
    pub asyncapi: String,
    /// Service metadata.
    pub info: Info,
    /// Servers (one per broker the service connects to), keyed by server name.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub servers: BTreeMap<String, Server>,
    /// Channels, keyed by channel id (the name).
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub channels: BTreeMap<String, Channel>,
    /// Operations, keyed by operation id.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub operations: BTreeMap<String, Operation>,
    /// Reusable components (message definitions).
    pub components: Components,
}

impl Spec {
    /// Serializes the document to pretty-printed JSON.
    ///
    /// # Errors
    ///
    /// Returns [`serde_json::Error`] if serialization fails (not expected for a well-formed spec).
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// The message components carrying no payload schema, excluding the deliberately
    /// schema-free raw-bytes messages: the models that would document better with a
    /// [`schemars::JsonSchema`] derive.
    ///
    /// [`build_spec`] logs a `WARN` per gap when the document is generated; this accessor makes
    /// the same list assertable, so a service can gate schema coverage in its test suite.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "memory")]
    /// # fn demo() {
    /// use ruststream::asyncapi::build_spec;
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{AppInfo, RustStream};
    ///
    /// let app = RustStream::new(AppInfo::new("orders", "1.0.0"))
    ///     .with_broker(MemoryBroker::new(), |b| { let _ = b; });
    /// let spec = build_spec(&app);
    /// assert!(spec.messages_without_schema().is_empty());
    /// # }
    /// ```
    #[must_use]
    pub fn messages_without_schema(&self) -> Vec<&str> {
        self.components
            .messages
            .values()
            .filter(|message| message.payload.is_none() && message.name != "bytes")
            .map(|message| message.name.as_str())
            .collect()
    }

    /// Serializes the document to YAML.
    ///
    /// # Errors
    ///
    /// Returns [`serde_norway::Error`] if serialization fails (not expected for a well-formed spec).
    pub fn to_yaml(&self) -> Result<String, serde_norway::Error> {
        serde_norway::to_string(self)
    }
}

/// An `AsyncAPI` server: where and how clients reach a broker.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Server {
    /// The host (and optional port), e.g. `"nats.example.com:4222"`. Absent for an in-process
    /// broker with no network address (the in-memory broker).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// The messaging protocol, e.g. `"nats"`.
    pub protocol: String,
    /// Optional human description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// References into `components.securitySchemes` describing how clients authenticate. Empty
    /// (and absent from the document) unless the service author attached schemes with
    /// [`ServerSpec::with_security`](crate::ServerSpec::with_security).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub security: Vec<Reference>,
}

/// `AsyncAPI` `Info` object: service title, version, and optional description.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Info {
    /// Service title.
    pub title: String,
    /// Service version.
    pub version: String,
    /// Optional description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// An `AsyncAPI` channel: an address plus the messages that flow over it.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Channel {
    /// The channel address (the broker name / subject). A templated address keeps its
    /// `{placeholder}` segments, and every one of them is declared in
    /// [`parameters`](Self::parameters).
    pub address: String,
    /// Messages on this channel, keyed by message name, referencing component definitions.
    pub messages: BTreeMap<String, Reference>,
    /// The address's parameters, one per `{placeholder}` segment; empty (and omitted) for a
    /// fixed address.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, Parameter>,
}

/// An `AsyncAPI` channel parameter: one `{placeholder}` segment of a templated address.
///
/// The declaration names the segments; what a service puts in them is a run-time value.
#[derive(Debug, Clone, Default, Serialize)]
#[non_exhaustive]
pub struct Parameter {
    /// Optional human description of the segment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// An `AsyncAPI` operation: an action on a channel.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Operation {
    /// The action: `"receive"` for subscribers, `"send"` for declared outgoing messages.
    pub action: String,
    /// Reference to the channel this operation acts on.
    pub channel: Reference,
    /// The messages this operation handles.
    pub messages: Vec<Reference>,
    /// Optional human description, typically from the handler's doc comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Reusable `AsyncAPI` components.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Components {
    /// Message definitions, keyed by message name.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub messages: BTreeMap<String, MessageObject>,
    /// Security scheme definitions the servers reference, keyed by scheme name (the server's
    /// name, `-N`-suffixed when a server declares several).
    #[serde(rename = "securitySchemes", skip_serializing_if = "BTreeMap::is_empty")]
    pub security_schemes: BTreeMap<String, Value>,
}

/// An `AsyncAPI` message definition.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct MessageObject {
    /// The message name.
    pub name: String,
    /// Optional human description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The JSON Schema of the payload, when the message type implements
    /// [`schemars::JsonSchema`]. Absent for raw-bytes handlers and types without a schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// The JSON Schema of the application headers, when the handler declares a typed header
    /// contract (a `Headers<T>` parameter). The schema describes the logical contract; on
    /// the wire, header values are string-encoded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<Value>,
}

/// A JSON `$ref` pointer.
#[derive(Debug, Clone, Serialize)]
pub struct Reference {
    /// The reference target, e.g. `#/components/messages/Order`.
    #[serde(rename = "$ref")]
    pub reference: String,
}

impl Reference {
    fn new(target: impl Into<String>) -> Self {
        Self {
            reference: target.into(),
        }
    }
}

/// Builds an [`AsyncAPI`](Spec) 3.0 document from a service's handlers and metadata.
///
/// Each registered subscriber becomes a channel (addressed by its name), a `receive` operation,
/// and a message component named after the handler's input type.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # fn demo() -> Result<(), serde_json::Error> {
/// use ruststream::asyncapi::build_spec;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{AppInfo, Context, HandlerMetadata, HandlerOutcome, RustStream};
///
/// let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
///     MemoryBroker::new(),
///     |b| {
///         let subscriber = b.broker().subscribe("orders");
///         b.handle(
///             subscriber,
///             |_msg: &_, _ctx: &mut Context| async { HandlerOutcome::ack() },
///             HandlerMetadata::raw("orders"),
///         );
///     },
/// );
///
/// let spec = build_spec(&app);
/// assert_eq!(spec.info.title, "orders");
/// let json = spec.to_json()?;
/// assert!(json.contains("\"asyncapi\""));
/// # Ok(())
/// # }
/// ```
#[must_use]
pub fn build_spec<A: App>(app: &A) -> Spec {
    let info = Info {
        title: app.info().title.clone(),
        version: app.info().version.clone(),
        description: app.info().description.clone(),
    };

    let mut security_schemes = BTreeMap::new();
    let servers = app
        .servers()
        .iter()
        .map(|(name, spec)| {
            let security = spec
                .security
                .iter()
                .enumerate()
                .map(|(index, scheme)| {
                    let key = if index == 0 {
                        name.clone()
                    } else {
                        format!("{name}-{index}")
                    };
                    security_schemes.insert(key.clone(), security_scheme_object(scheme));
                    Reference::new(format!("#/components/securitySchemes/{key}"))
                })
                .collect();
            (
                name.clone(),
                Server {
                    host: spec.host.clone(),
                    protocol: spec.protocol.clone(),
                    description: spec.description.clone(),
                    security,
                },
            )
        })
        .collect();

    let mut channels = BTreeMap::new();
    let mut operations = BTreeMap::new();
    let mut messages = BTreeMap::new();

    for handler in app.handlers() {
        add_receive(handler, &mut channels, &mut operations, &mut messages);
        // One `send` operation per declared outgoing message: the reply of a publish(..) form
        // and every Out slot dictionary entry. A channel several messages flow through lists
        // them all; the message component is shared with any handler that receives it.
        for outgoing in &handler.outgoing {
            add_outgoing(
                outgoing,
                handler.name.as_ref(),
                &mut channels,
                &mut operations,
                &mut messages,
            );
        }
    }

    Spec {
        asyncapi: "3.0.0".to_owned(),
        info,
        servers,
        channels,
        operations,
        components: Components {
            messages,
            security_schemes,
        },
    }
}

/// Renders a [`SecurityScheme`](crate::SecurityScheme) as its `AsyncAPI` security scheme object.
fn security_scheme_object(scheme: &crate::SecurityScheme) -> Value {
    use crate::capability::SecuritySchemeKind as Kind;

    // Raw payloads round-trip through the string the constructor serialized, so parsing them
    // back cannot fail; Null is the unreachable fallback, not an error path.
    let parse = |raw: &str| serde_json::from_str::<Value>(raw).unwrap_or(Value::Null);
    let mut object = match &scheme.kind {
        Kind::UserPassword => serde_json::json!({ "type": "userPassword" }),
        Kind::ApiKey { location } => {
            serde_json::json!({ "type": "apiKey", "in": location.as_api() })
        }
        Kind::X509 => serde_json::json!({ "type": "X509" }),
        Kind::Plain => serde_json::json!({ "type": "plain" }),
        Kind::ScramSha256 => serde_json::json!({ "type": "scramSha256" }),
        Kind::ScramSha512 => serde_json::json!({ "type": "scramSha512" }),
        Kind::Gssapi => serde_json::json!({ "type": "gssapi" }),
        Kind::Http { scheme } => serde_json::json!({ "type": "http", "scheme": scheme }),
        Kind::HttpApiKey { name, location } => serde_json::json!({
            "type": "httpApiKey",
            "name": name,
            "in": location.as_api(),
        }),
        Kind::OpenIdConnect { url } => serde_json::json!({
            "type": "openIdConnect",
            "openIdConnectUrl": url,
        }),
        Kind::Oauth2 { flows } => serde_json::json!({ "type": "oauth2", "flows": parse(flows) }),
        Kind::Custom { object } => parse(object),
    };
    if let (Some(description), Some(fields)) = (&scheme.description, object.as_object_mut()) {
        fields.insert("description".to_owned(), Value::String(description.clone()));
    }
    object
}

/// Renders a self-contained HTML page that displays `spec_url` using the `AsyncAPI` React component.
///
/// The component and its styles load from a CDN (jsDelivr) by default; override
/// [`cdn_base`](ViewerOptions::cdn_base) to pin a version or self-host for offline / locked-down
/// deployments. Serve the returned HTML from your own HTTP stack alongside the spec document.
///
/// # Examples
///
/// ```
/// use ruststream::asyncapi::{render_viewer_html, ViewerOptions};
///
/// let html = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
/// assert!(html.contains("/asyncapi.json"));
/// ```
#[must_use]
pub fn render_viewer_html(spec_url: &str, opts: &ViewerOptions<'_>) -> String {
    let title = opts.title;
    let cdn = opts.cdn_base.trim_end_matches('/');
    let spec = spec_url.replace('"', "&quot;");
    format!(
        "<!DOCTYPE html>\n\
<html lang=\"en\">\n\
<head>\n\
  <meta charset=\"utf-8\" />\n\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n\
  <title>{title}</title>\n\
  <link rel=\"stylesheet\" href=\"{cdn}/styles/default.min.css\" />\n\
</head>\n\
<body>\n\
  <div id=\"asyncapi\"></div>\n\
  <script src=\"{cdn}/browser/standalone/index.js\"></script>\n\
  <script>\n\
    AsyncApiStandalone.render(\n\
      {{ schema: {{ url: \"{spec}\" }}, config: {{ show: {{ sidebar: true }} }} }},\n\
      document.getElementById(\"asyncapi\"),\n\
    );\n\
  </script>\n\
</body>\n\
</html>\n"
    )
}

/// Options for [`render_viewer_html`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ViewerOptions<'a> {
    /// The HTML page title.
    pub title: &'a str,
    /// Base URL the `AsyncAPI` React assets load from (no trailing slash required).
    pub cdn_base: &'a str,
}

impl<'a> ViewerOptions<'a> {
    /// Sets the HTML page title.
    #[must_use]
    pub const fn with_title(mut self, title: &'a str) -> Self {
        self.title = title;
        self
    }

    /// Sets the base URL the `AsyncAPI` React assets load from.
    #[must_use]
    pub const fn with_cdn_base(mut self, cdn_base: &'a str) -> Self {
        self.cdn_base = cdn_base;
        self
    }
}

impl Default for ViewerOptions<'_> {
    fn default() -> Self {
        Self {
            title: "AsyncAPI",
            cdn_base: "https://cdn.jsdelivr.net/npm/@asyncapi/react-component@2.6.4",
        }
    }
}

/// Takes the final path segment of a type name as the message name (`a::b::Order` -> `Order`).
fn message_name(type_name: &str) -> String {
    type_name
        .rsplit("::")
        .next()
        .unwrap_or(type_name)
        .to_owned()
}

/// Adds one registered subscriber to the document: its channel entry, its `receive` operation,
/// and its message component (with the payload and headers schemas the handler declared).
fn add_receive(
    handler: &crate::runtime::HandlerMetadata,
    channels: &mut BTreeMap<String, Channel>,
    operations: &mut BTreeMap<String, Operation>,
    messages: &mut BTreeMap<String, MessageObject>,
) {
    let name = handler.name.as_ref();
    let payload = handler
        .payload_schema
        .as_deref()
        .and_then(|json| serde_json::from_str::<Value>(json).ok());
    // A raw handler is deliberately schema-free; a typed model without a schema is a
    // documentation gap worth flagging at generation time.
    if payload.is_none() && handler.input_type != "bytes" {
        warn!(
            target: "ruststream::asyncapi",
            subscription = %name,
            message_type = handler.input_type,
            "message type has no JSON Schema; its AsyncAPI message carries no payload schema - \
             derive schemars::JsonSchema on it",
        );
    }
    let headers = handler
        .headers_schema
        .as_deref()
        .and_then(|json| serde_json::from_str::<Value>(json).ok());
    // The JsonSchema derive captures the type's own doc comment (and a schemars title /
    // rename), so a documented payload type feeds the component without a Message impl.
    let schema_str = |key: &str| {
        payload
            .as_ref()
            .and_then(|schema| schema.get(key))
            .and_then(Value::as_str)
            .map(str::to_owned)
    };
    // A `Message` impl on the input type names the component; the schema title is next; the
    // type name is the fallback.
    let message_name = handler
        .message_name
        .as_ref()
        .map(ToString::to_string)
        .or_else(|| schema_str("title"))
        .unwrap_or_else(|| message_name(handler.input_type));

    channels
        .entry(name.to_owned())
        .or_insert_with(|| Channel {
            address: name.to_owned(),
            messages: BTreeMap::new(),
            parameters: BTreeMap::new(),
        })
        .messages
        .insert(
            message_name.clone(),
            Reference::new(format!("#/components/messages/{message_name}")),
        );

    operations.insert(
        receive_operation_id(operations, name),
        Operation {
            action: "receive".to_owned(),
            channel: Reference::new(format!("#/channels/{name}")),
            messages: vec![Reference::new(format!(
                "#/channels/{name}/messages/{message_name}"
            ))],
            description: handler.description.as_ref().map(ToString::to_string),
        },
    );

    // Message::DESCRIPTION wins, then the type's own doc comment from the schema, then the
    // handler doc (already on the operation) so plain types keep their description.
    let message_description = handler
        .message_description
        .as_ref()
        .map(ToString::to_string)
        .or_else(|| schema_str("description"))
        .or_else(|| handler.description.as_ref().map(ToString::to_string));

    merge_message(
        messages,
        message_name,
        message_description,
        payload,
        headers,
        name,
    );
}

/// Merges one contribution into a shared message component: an absent description, payload, or
/// headers schema fills in from a later contributor (component identity is the message name,
/// and several handlers may carry different slices of its metadata), while a conflicting
/// headers schema keeps the first one with a WARN - one component cannot carry two contracts,
/// and the conflict usually means two handlers read the same type with different `Headers`
/// declarations.
fn merge_message(
    messages: &mut BTreeMap<String, MessageObject>,
    name: String,
    description: Option<String>,
    payload: Option<Value>,
    headers: Option<Value>,
    context: &str,
) {
    let entry = messages
        .entry(name.clone())
        .or_insert_with(|| MessageObject {
            name,
            description: None,
            payload: None,
            headers: None,
        });
    if entry.description.is_none() {
        entry.description = description;
    }
    if entry.payload.is_none() {
        entry.payload = payload;
    }
    match (&entry.headers, headers) {
        (None, Some(headers)) => entry.headers = Some(headers),
        (Some(existing), Some(headers)) if *existing != headers => {
            warn!(
                target: "ruststream::asyncapi",
                message = %entry.name,
                context = %context,
                "conflicting headers schemas for one message component; keeping the first",
            );
        }
        _ => {}
    }
}

/// Adds one declared outgoing message to the document: its channel entry, one `send` operation
/// (id derived from the publishing handler's subscription name and the channel), and its
/// message component (shared with any handler that receives the same type). The message name
/// falls back like the receive side: `Message` impl, then schema title, then the type name.
fn add_outgoing(
    outgoing: &crate::runtime::OutgoingMessageMetadata,
    handler_name: &str,
    channels: &mut BTreeMap<String, Channel>,
    operations: &mut BTreeMap<String, Operation>,
    messages: &mut BTreeMap<String, MessageObject>,
) {
    let payload = outgoing
        .payload_schema
        .as_deref()
        .and_then(|json| serde_json::from_str::<Value>(json).ok());
    // A publish_raw reply is deliberately schema-free; a typed model without a schema is a
    // documentation gap worth flagging at generation time.
    if payload.is_none() && outgoing.message_type != "bytes" {
        warn!(
            target: "ruststream::asyncapi",
            channel = %outgoing.channel,
            message_type = outgoing.message_type,
            "outgoing message type has no JSON Schema; its AsyncAPI message carries no payload \
             schema - derive schemars::JsonSchema on it",
        );
    }
    let headers = outgoing
        .headers_schema
        .as_deref()
        .and_then(|json| serde_json::from_str::<Value>(json).ok());
    let title = payload
        .as_ref()
        .and_then(|schema| schema.get("title"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let name = outgoing
        .message_name
        .map(str::to_owned)
        .or(title)
        .unwrap_or_else(|| message_name(outgoing.message_type));
    let description = outgoing.message_description.map(str::to_owned).or_else(|| {
        payload
            .as_ref()
            .and_then(|schema| schema.get("description"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    });
    let channel = outgoing.channel.as_ref();

    channels
        .entry(channel.to_owned())
        .or_insert_with(|| Channel {
            address: channel.to_owned(),
            messages: BTreeMap::new(),
            // A templated address declares its placeholders, so the document says what the
            // segments are instead of showing an address nothing describes.
            parameters: outgoing
                .parameters
                .iter()
                .map(|segment| ((*segment).to_owned(), Parameter::default()))
                .collect(),
        })
        .messages
        .insert(
            name.clone(),
            Reference::new(format!("#/components/messages/{name}")),
        );

    operations.insert(
        send_operation_id(operations, handler_name, channel),
        Operation {
            action: "send".to_owned(),
            channel: Reference::new(format!("#/channels/{channel}")),
            messages: vec![Reference::new(format!(
                "#/channels/{channel}/messages/{name}"
            ))],
            description: None,
        },
    );

    merge_message(messages, name, description, payload, headers, channel);
}

/// Derives a stable `receive` operation id from a subscription name.
///
/// Several handlers may share one channel (each opens its own subscription), so the name alone
/// does not identify an operation: a collision takes a deterministic numeric suffix instead of
/// overwriting the operation already there, as on the send side.
fn receive_operation_id(operations: &BTreeMap<String, Operation>, name: &str) -> String {
    let base = format!("receive_{}", sanitize_id(name));
    if !operations.contains_key(&base) {
        return base;
    }
    let mut suffix = 2;
    loop {
        let candidate = format!("{base}_{suffix}");
        if !operations.contains_key(&candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

/// Derives a stable `send` operation id from the publishing handler's subscription name and
/// the target channel: `send_<name>_<channel>`.
fn send_operation_id(
    operations: &BTreeMap<String, Operation>,
    name: &str,
    channel: &str,
) -> String {
    let base = format!("send_{}_{}", sanitize_id(name), sanitize_id(channel));
    if !operations.contains_key(&base) {
        return base;
    }
    // A residual collision (several handlers on one subject publishing to one channel, or a
    // lossy sanitization) gets a deterministic numeric suffix instead of overwriting.
    let mut suffix = 2;
    loop {
        let candidate = format!("{base}_{suffix}");
        if !operations.contains_key(&candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

/// Replaces every non-alphanumeric character with `_` for use in an operation id.
fn sanitize_id(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::{ApiKeyLocation, HttpApiKeyLocation, SecurityScheme};

    use super::security_scheme_object;

    #[test]
    fn every_scheme_kind_renders_its_document_object() {
        let cases = [
            (
                SecurityScheme::user_password(),
                serde_json::json!({ "type": "userPassword" }),
            ),
            (
                SecurityScheme::api_key(ApiKeyLocation::Password),
                serde_json::json!({ "type": "apiKey", "in": "password" }),
            ),
            (
                SecurityScheme::x509(),
                serde_json::json!({ "type": "X509" }),
            ),
            (
                SecurityScheme::plain(),
                serde_json::json!({ "type": "plain" }),
            ),
            (
                SecurityScheme::scram_sha256(),
                serde_json::json!({ "type": "scramSha256" }),
            ),
            (
                SecurityScheme::scram_sha512(),
                serde_json::json!({ "type": "scramSha512" }),
            ),
            (
                SecurityScheme::gssapi(),
                serde_json::json!({ "type": "gssapi" }),
            ),
            (
                SecurityScheme::http("bearer"),
                serde_json::json!({ "type": "http", "scheme": "bearer" }),
            ),
            (
                SecurityScheme::http_api_key("X-Api-Key", HttpApiKeyLocation::Header),
                serde_json::json!({ "type": "httpApiKey", "name": "X-Api-Key", "in": "header" }),
            ),
            (
                SecurityScheme::open_id_connect("https://idp.example.com/.well-known"),
                serde_json::json!({
                    "type": "openIdConnect",
                    "openIdConnectUrl": "https://idp.example.com/.well-known",
                }),
            ),
            (
                SecurityScheme::oauth2(serde_json::json!({ "clientCredentials": {} })),
                serde_json::json!({ "type": "oauth2", "flows": { "clientCredentials": {} } }),
            ),
            (
                SecurityScheme::custom(serde_json::json!({ "type": "symmetricEncryption" })),
                serde_json::json!({ "type": "symmetricEncryption" }),
            ),
        ];
        for (scheme, expected) in cases {
            assert_eq!(security_scheme_object(&scheme), expected);
        }
    }

    #[test]
    fn description_lands_in_the_rendered_object() {
        let object = security_scheme_object(&SecurityScheme::plain().with_description("over TLS"));
        assert_eq!(object["description"], "over TLS");
    }
}
