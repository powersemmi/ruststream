//! `AsyncAPI` 3.0 document generation from a [`RustStream`] service.
//!
//! [`build_spec`] turns a service's registered handlers and metadata into a [`Spec`] that
//! serializes to an `AsyncAPI` 3.0 document. Hosting it over HTTP is the user's concern; this
//! module only produces the document.
//!
//! Message payload JSON schemas are not yet emitted (they require `schemars` integration); the
//! current output covers info, channels, operations, and message names / descriptions.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::runtime::RustStream;

/// An `AsyncAPI` 3.0 document.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Spec {
    /// The `AsyncAPI` specification version (always `"3.0.0"`).
    pub asyncapi: String,
    /// Service metadata.
    pub info: Info,
    /// Channels, keyed by channel id (the topic).
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
    /// The channel address (the broker topic / subject).
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
/// Each registered subscriber becomes a channel (addressed by its topic), a `receive` operation,
/// and a message component named after the handler's input type.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # fn demo() -> Result<(), serde_json::Error> {
/// use ruststream::asyncapi::build_spec;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{AppInfo, HandlerMetadata, HandlerResult, RustStream};
///
/// let app = RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(
///     MemoryBroker::new(),
///     |b| {
///         let subscriber = b.broker().subscribe("orders");
///         b.handle(
///             subscriber,
///             |_msg: &_| async { HandlerResult::Ack },
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

    let mut channels = BTreeMap::new();
    let mut operations = BTreeMap::new();
    let mut messages = BTreeMap::new();

    for handler in app.handlers() {
        let topic = handler.topic.as_ref();
        let message_name = message_name(handler.input_type);

        channels.entry(topic.to_owned()).or_insert_with(|| Channel {
            address: topic.to_owned(),
            messages: BTreeMap::from([(
                message_name.clone(),
                Reference::new(format!("#/components/messages/{message_name}")),
            )]),
        });

        operations.insert(
            operation_id(topic),
            Operation {
                action: "receive".to_owned(),
                channel: Reference::new(format!("#/channels/{topic}")),
                messages: vec![Reference::new(format!(
                    "#/channels/{topic}/messages/{message_name}"
                ))],
            },
        );

        messages
            .entry(message_name.clone())
            .or_insert_with(|| MessageObject {
                name: message_name,
                description: handler.description.as_ref().map(ToString::to_string),
            });
    }

    Spec {
        asyncapi: "3.0.0".to_owned(),
        info,
        channels,
        operations,
        components: Components { messages },
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

/// Derives a stable operation id from a topic.
fn operation_id(topic: &str) -> String {
    let sanitized: String = topic
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    format!("receive_{sanitized}")
}
