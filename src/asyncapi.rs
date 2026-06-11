//! `AsyncAPI` 3.0 document generation from a [`RustStream`] service.
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

use crate::runtime::RustStream;

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
    /// The host (and optional port), e.g. `"nats.example.com:4222"`.
    pub host: String,
    /// The messaging protocol, e.g. `"nats"`.
    pub protocol: String,
    /// Optional human description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
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
    /// The channel address (the broker name / subject).
    pub address: String,
    /// Messages on this channel, keyed by message name, referencing component definitions.
    pub messages: BTreeMap<String, Reference>,
}

/// An `AsyncAPI` operation: an action on a channel.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Operation {
    /// The action; `"receive"` for subscribers.
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
/// use ruststream::runtime::{AppInfo, Context, HandlerMetadata, HandlerResult, RustStream};
///
/// let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
///     MemoryBroker::new(),
///     |b| {
///         let subscriber = b.broker().subscribe("orders");
///         b.handle(
///             subscriber,
///             |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
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
pub fn build_spec<L>(app: &RustStream<L>) -> Spec {
    let info = Info {
        title: app.info().title.clone(),
        version: app.info().version.clone(),
        description: app.info().description.clone(),
    };

    let servers = app
        .servers()
        .iter()
        .map(|(name, spec)| {
            (
                name.clone(),
                Server {
                    host: spec.host.clone(),
                    protocol: spec.protocol.clone(),
                    description: spec.description.clone(),
                },
            )
        })
        .collect();

    let mut channels = BTreeMap::new();
    let mut operations = BTreeMap::new();
    let mut messages = BTreeMap::new();

    for handler in app.handlers() {
        let name = handler.name.as_ref();
        // A `Message` impl on the input type names the component; the type name is the fallback.
        let message_name = handler
            .message_name
            .as_ref()
            .map_or_else(|| message_name(handler.input_type), ToString::to_string);

        channels.entry(name.to_owned()).or_insert_with(|| Channel {
            address: name.to_owned(),
            messages: BTreeMap::from([(
                message_name.clone(),
                Reference::new(format!("#/components/messages/{message_name}")),
            )]),
        });

        operations.insert(
            operation_id(name),
            Operation {
                action: "receive".to_owned(),
                channel: Reference::new(format!("#/channels/{name}")),
                messages: vec![Reference::new(format!(
                    "#/channels/{name}/messages/{message_name}"
                ))],
                description: handler.description.as_ref().map(ToString::to_string),
            },
        );

        messages
            .entry(message_name.clone())
            .or_insert_with(|| MessageObject {
                name: message_name,
                // The `Message` description documents the type itself; the handler doc (already
                // on the operation) doubles as a fallback so plain types keep their description.
                description: handler
                    .message_description
                    .as_ref()
                    .or(handler.description.as_ref())
                    .map(ToString::to_string),
                payload: handler
                    .payload_schema
                    .as_deref()
                    .and_then(|json| serde_json::from_str(json).ok()),
            });
    }

    Spec {
        asyncapi: "3.0.0".to_owned(),
        info,
        servers,
        channels,
        operations,
        components: Components { messages },
    }
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

/// Derives a stable operation id from a name.
fn operation_id(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    format!("receive_{sanitized}")
}
