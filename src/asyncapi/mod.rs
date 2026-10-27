//! `AsyncAPI` 3.1 document generation from a [`RustStream`](crate::runtime::RustStream) service.
//!
//! [`build_spec`] turns the app `main` runs into a [`Spec`]: each subscriber becomes a channel
//! and a `receive` operation, a reply rides that operation, every `Out` slot message gets a `send`
//! operation, and payload and header types contribute schemas.
//!
//! # Generation
//!
//! - [AsyncAPI][asyncapi]: what the `asyncapi` feature adds to a service.
//! - [Generation][generation]: how the document is produced, serialized and served.
//! - [build_spec][build-spec]: [`build_spec`] reads the document from the app value.
//! - [Formats][formats]: [`Spec::to_json`] and [`Spec::to_yaml`].
//! - [Command][command]: `ruststream asyncapi gen` prints the document from the generated `main`
//!   (see [`cli`](crate::runtime::cli)).
//! - [Viewer][viewer]: [`render_viewer_html`] and [`ViewerOptions`] serve a page that renders it.
//! - [describe][describe]: the operation description of a manual registration.
//! - [undocumented][undocumented]: a manual registration left out of the document.
//!
//! # Schemas
//!
//! - [Schemas][schemas]: a payload type that derives `JsonSchema` (re-exported as [`schemars`])
//!   gets a payload schema.
//! - [MessageInfo][message-info]: [`MessageInfo`](crate::MessageInfo) names and describes a
//!   message component whatever the Rust type is called.
//! - [messages_without_schema][without-schema]: [`Spec::messages_without_schema`] lists the gaps
//!   for a test to fail on.
//!
//! # What the document reports
//!
//! - [What the document reports][reports]: what it says beyond the channels.
//! - [Replies][replies]: a reply is the `reply` of the operation it answers.
//! - [Parameters][parameters]: a name template becomes a channel with `parameters`.
//! - [Headers][headers]: a header contract becomes the message's headers schema.
//! - [Content][content]: each message names its codec's
//!   [`CONTENT_TYPE`](crate::codec::Codec::CONTENT_TYPE).
//! - [Message sets][message-sets]: a subscription over an
//!   [`InMessages`](crate::runtime::InMessages) set lists one message per variant, each with
//!   its own format, and the header rule as a `const` in its headers schema.
//! - [Retry][retry]: the attempt cap and the retry destinations as `x-ruststream-retry`.
//!
//! # Servers
//!
//! - [Servers][servers]: the brokers a service connects to, how clients authenticate, and the
//!   protocol bindings.
//! - [with_broker_labeled][with-broker-labeled]:
//!   [`with_broker_labeled`](crate::runtime::RustStream::with_broker_labeled) registers a broker
//!   under its server name, and a broker implementing [`DescribeServer`](crate::DescribeServer)
//!   describes the server.
//! - [Server][server]: [`server`](crate::runtime::RustStream::server) declares one from a
//!   [`ServerSpec`](crate::ServerSpec).
//! - [Security][security]: [`ServerSpec::security`](crate::ServerSpec::security) with a
//!   [`SecurityScheme`](crate::SecurityScheme).
//! - [Bindings][bindings]: [`Bindings`] carry what one protocol says about a server, a channel,
//!   an operation or a message.
//!
//! # Service info
//!
//! - [Service info][service-info]: the `info` section, what the service is and who owns it.
//! - [Info][app-info]: [`AppInfo`](crate::runtime::AppInfo), the title, the version and the
//!   description.
//! - [AppId][app-id]: [`AppId`], the service's identifier, parsed as a URI.
//! - [Contact][contact]: [`Contact`], who answers for the service.
//! - [License][license]: [`License`].
//! - [Tag][tag]: [`Tag`], a label that groups services.
//! - [ExternalDocs][external-docs]: [`ExternalDocs`], documentation kept
//!   outside the document.
//!
//! [asyncapi]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/
//! [generation]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/
//! [reports]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/
//! [build-spec]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/build-spec/
//! [formats]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/to-json-yaml/
//! [command]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/cli-gen/
//! [viewer]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/viewer/
//! [describe]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/describe/
//! [undocumented]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/undocumented/
//! [schemas]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/schemas/
//! [message-info]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/message-info/
//! [without-schema]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/messages-without-schema/
//! [replies]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/replies/
//! [parameters]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/channel-parameters/
//! [headers]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/header-schemas/
//! [content]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/content-type/
//! [message-sets]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/message-sets/
//! [retry]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/retry-extension/
//! [servers]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/servers/
//! [with-broker-labeled]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/servers/with-broker-labeled/
//! [server]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/servers/server/
//! [security]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/servers/security-schemes/
//! [bindings]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/servers/bindings/
//! [service-info]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/
//! [app-info]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/app-info/
//! [app-id]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/app-id/
//! [contact]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/contact/
//! [license]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/license/
//! [tag]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/tag/
//! [external-docs]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/external-docs/

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use schemars::{JsonSchema, generate::SchemaSettings};
use serde::Serialize;
use serde_json::{Map, Value, json};
use tracing::warn;

mod bindings;
mod viewer;

pub use bindings::{Binding, BindingError, Bindings, PublishBindings, SubscriptionBindings};
pub use viewer::{ViewerOptions, render_viewer_html};

use crate::describe::{AppId, Contact, ExternalDocs, License, Tag};
use crate::runtime::{
    App, HandlerMetadata, HeaderDiscriminator, IncomingMessageMetadata, OutgoingKind,
    OutgoingMessageMetadata,
};

/// The specification version every generated document declares.
const ASYNCAPI_VERSION: &str = "3.1.0";

/// An `AsyncAPI` 3.1 document.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Spec {
    /// The `AsyncAPI` specification version (always `"3.1.0"`).
    pub asyncapi: String,
    /// The service's own identifier, when it declared one
    /// ([`AppInfo::id`](method@crate::runtime::AppInfo::id)).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<AppId>,
    /// Service metadata.
    pub info: Info,
    /// Servers (one per broker the service connects to), keyed by server name.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub servers: BTreeMap<String, Server>,
    /// The media type every message names, when all of them name the same one. A second format,
    /// or a message no codec reads or writes, leaves it out; each codec-encoded message still
    /// states its own.
    #[serde(rename = "defaultContentType", skip_serializing_if = "Option::is_none")]
    pub default_content_type: Option<String>,
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
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "asyncapi", feature = "macros", feature = "memory", feature = "json"))]
    /// # mod demo {
    /// use ruststream::asyncapi::build_spec;
    /// use ruststream::memory::prelude::*;
    /// use ruststream::schemars::JsonSchema;
    /// use serde::{Deserialize, Serialize};
    ///
    /// /// An order placed by a customer.
    /// #[derive(Deserialize, JsonSchema)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// /// The confirmation an order gets back.
    /// #[derive(Serialize, Outgoing, JsonSchema)]
    /// struct Confirmed {
    ///     id: u64,
    /// }
    ///
    /// /// Answers every request on `requests` with a confirmation on `responses`.
    /// #[subscriber("requests", reply("responses"))]
    /// async fn confirm(order: &Order) -> Confirmed {
    ///     Confirmed { id: order.id }
    /// }
    ///
    /// fn document() -> Result<String, serde_json::Error> {
    ///     let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker_labeled(
    ///         "in-process",
    ///         MemoryBroker::new(),
    ///         |b| {
    ///             b.include(confirm);
    ///         },
    ///     );
    ///     build_spec(&app).to_json()
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// The message components carrying no payload schema, excluding the deliberately
    /// schema-free ones (a [`Deserialized`](crate::runtime::Deserialized) input, a
    /// [`Serialized`](crate::runtime::Serialized) reply - their bytes are their own wire
    /// format): the models that would document better with a [`schemars::JsonSchema`] derive.
    ///
    /// [`build_spec`] logs a `WARN` per gap when the document is generated; this accessor makes
    /// the same list assertable, so a service can gate schema coverage in its test suite.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
    /// # mod demo {
    /// use ruststream::asyncapi::build_spec;
    /// use ruststream::memory::prelude::*;
    /// use ruststream::schemars::JsonSchema;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize, JsonSchema)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("orders")]
    /// async fn accept(order: &Order) -> HandlerOutcome {
    ///     tracing::info!(order.id, "accepted");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// pub fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(accept);
    ///     })
    /// }
    ///
    /// /// A test in the service's suite: every model in the document carries a schema.
    /// pub fn every_message_has_a_schema() {
    ///     let spec = build_spec(&app());
    ///     assert_eq!(spec.messages_without_schema(), Vec::<&str>::new());
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn messages_without_schema(&self) -> Vec<&str> {
        self.components
            .messages
            .values()
            // The "bytes" name stays excluded next to the flag: a hand-rolled metadata entry
            // spells its schema-free intent through the label alone.
            .filter(|message| {
                message.payload.is_none() && !message.schemaless && message.name != "bytes"
            })
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
    /// The version of that protocol, when the broker names one: `"0.9.1"` against `"1.0"` for
    /// AMQP, `"5"` for MQTT.
    #[serde(rename = "protocolVersion", skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<String>,
    /// Optional human description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// References into `components.securitySchemes` describing how clients authenticate. Empty
    /// (and absent from the document) unless the service author attached schemes with
    /// [`ServerSpec::security`](method@crate::ServerSpec::security).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub security: Vec<Reference>,
    /// What the broker says about this server in its own protocol's vocabulary.
    #[serde(skip_serializing_if = "Bindings::is_empty")]
    pub bindings: Bindings,
}

/// `AsyncAPI` `Info` object: what the service is, who owns it, and under what terms it runs.
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
    /// Where the terms of service are published.
    #[serde(rename = "termsOfService", skip_serializing_if = "Option::is_none")]
    pub terms_of_service: Option<String>,
    /// Who to contact about the service.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contact: Option<Contact>,
    /// The licence the service is published under.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<License>,
    /// Labels grouping the service among its neighbours.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<Tag>,
    /// Where the prose about this service lives.
    #[serde(rename = "externalDocs", skip_serializing_if = "Option::is_none")]
    pub external_docs: Option<ExternalDocs>,
}

/// An `AsyncAPI` channel: an address plus the messages that flow over it.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Channel {
    /// The channel address (the broker name / subject). A templated address keeps its
    /// `{placeholder}` segments, and every one of them is declared in
    /// [`parameters`](Self::parameters).
    ///
    /// `None` - rendered as `null`, which the specification reads as "decided at runtime" - on
    /// the reply channel of a registration whose transform names the destination per delivery.
    /// The operation's `reply.address.location` then says where a client reads it.
    pub address: Option<String>,
    /// Messages on this channel, keyed by message name, referencing component definitions.
    pub messages: BTreeMap<String, Reference>,
    /// The address's parameters, one per `{placeholder}` segment; empty (and omitted) for a
    /// fixed address.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, Parameter>,
    /// The servers this channel exists on, as references into the document's `servers` map.
    ///
    /// Filled from the label the registration's broker was registered under
    /// ([`with_broker_labeled`](crate::runtime::RustStream::with_broker_labeled)), and from the
    /// single server of a one-broker document. Empty where neither applies, which per the
    /// specification means the channel is available on every server: that is the honest answer
    /// when the registration named no broker, and it is also the limit of what the core can say
    /// about a publish that leaves through a [`Bound`](crate::runtime::Bound) token, since such a
    /// publish reaches the token's own broker rather than the registration's.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub servers: Vec<Reference>,
    /// What the broker says about this channel in its own protocol's vocabulary.
    #[serde(skip_serializing_if = "Bindings::is_empty")]
    pub bindings: Bindings,
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
    /// What answers this operation, for a request-reply registration: the channel the reply goes
    /// to and the messages it carries. Absent on an operation nothing answers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply: Option<OperationReply>,
    /// Labels grouping the operation. The core contributes the protocol of the server the
    /// operation's channel lives on, so a multi-broker document reads by transport.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<Tag>,
    /// What the registration declared about retrying a failed delivery, under the extension key
    /// `x-ruststream-retry`. Absent unless the mount site declared a cap or a dead-letter
    /// destination.
    ///
    /// It is an extension because no binding carries it: the specification models a dead-letter
    /// queue only for `sqs` and `sns`, and nothing at all for the attempt cap.
    #[serde(rename = "x-ruststream-retry", skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryExtension>,
    /// What the broker says about this operation in its own protocol's vocabulary.
    #[serde(skip_serializing_if = "Bindings::is_empty")]
    pub bindings: Bindings,
}

/// What answers a `receive` operation: the `reply` of an `AsyncAPI` operation.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct OperationReply {
    /// Where the reply goes when the registration decides that per delivery, as a runtime
    /// expression. Absent where the reply goes to the channel's own address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<ReplyAddress>,
    /// Reference to the channel the reply is published to.
    pub channel: Reference,
    /// The messages the reply carries.
    pub messages: Vec<Reference>,
}

/// Where a client finds the address a reply is published to: the `AsyncAPI` operation reply
/// address object.
///
/// Filled from the publish policy's
/// [`reply_address_location`](crate::PublishPolicy::reply_address_location), which a broker
/// answers with the header carrying the address (`$message.header#/reply-to`).
///
/// # Examples
///
/// ```
/// use ruststream::asyncapi::Spec;
///
/// /// Where a client generator finds the address to read an operation's reply from.
/// fn reply_location<'a>(spec: &'a Spec, operation: &str) -> Option<&'a str> {
///     let reply = spec.operations.get(operation)?.reply.as_ref()?;
///     reply
///         .address
///         .as_ref()
///         .map(|address| address.location.as_str())
/// }
/// ```
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct ReplyAddress {
    /// The runtime expression naming the address, per the specification's grammar.
    pub location: String,
}

impl ReplyAddress {
    /// The address a reply is published to, as a runtime expression.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::asyncapi::{ReplyAddress, Spec};
    ///
    /// /// The gateway in front of the service answers on the inbox a request names, which the
    /// /// broker underneath cannot say, so the published document is completed here.
    /// fn add_gateway_reply(spec: &mut Spec) {
    ///     if let Some(reply) = spec
    ///         .operations
    ///         .get_mut("receive_requests")
    ///         .and_then(|operation| operation.reply.as_mut())
    ///     {
    ///         reply.address = Some(ReplyAddress::new("$message.header#/reply-to"));
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn new(location: impl Into<String>) -> Self {
        Self {
            location: location.into(),
        }
    }
}

/// What a registration declared about retrying a failed delivery, rendered under
/// `x-ruststream-retry`.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct RetryExtension {
    /// How many times a delivery is handed to the handler before it is given up on.
    #[serde(rename = "maxAttempts", skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    /// Where a delivery out of attempts goes. The channel is in the document too, with a `send`
    /// operation: the traffic is real.
    #[serde(rename = "deadLetter", skip_serializing_if = "Option::is_none")]
    pub dead_letter: Option<String>,
    /// Where the registration publishes a deferred copy of a delivery, when the mount site named
    /// a destination for its retry copies. That channel is in the document too, with a `send`
    /// operation. Absent where the copies go back to the subscription's own address, which is a
    /// channel the document already carries.
    #[serde(rename = "retryDestination", skip_serializing_if = "Option::is_none")]
    pub retry_destination: Option<String>,
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
    /// The human-readable title, from the payload schema's own, when it says something the
    /// machine name does not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional human description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The media type the payload is encoded in, from the codec that decodes this message.
    #[serde(rename = "contentType", skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// The JSON Schema of the payload, when the message type implements
    /// [`schemars::JsonSchema`]. Absent for raw-bytes handlers and types without a schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    /// The JSON Schema of the application headers, when the handler declares a typed header
    /// contract (a `Headers<T>` parameter). The schema describes the logical contract; on
    /// the wire, header values are string-encoded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<Value>,
    /// True when the message rides a self-carrying lane
    /// ([`Deserialized`](crate::runtime::Deserialized) input /
    /// [`Serialized`](crate::runtime::Serialized) reply): the bytes are their own wire format,
    /// so the missing payload schema is by design. Generation bookkeeping, not part of the
    /// rendered document.
    #[serde(skip)]
    pub schemaless: bool,
    /// What the broker says about this message in its own protocol's vocabulary.
    #[serde(skip_serializing_if = "Bindings::is_empty")]
    pub bindings: Bindings,
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

/// Builds an [`AsyncAPI`](Spec) 3.1.0 document from a service's handlers and metadata.
///
/// Each registered subscriber becomes a channel (addressed by its name), a `receive` operation,
/// and a message component named after the handler's input type.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
/// # mod demo {
/// use std::error::Error;
/// use std::fs;
///
/// use ruststream::asyncapi::build_spec;
/// use ruststream::memory::prelude::*;
/// use ruststream::schemars::JsonSchema;
/// use serde::Deserialize;
///
/// #[derive(Deserialize, JsonSchema)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(accept);
///     })
/// }
///
/// /// Writes the document the service publishes next to its code, from the app `main` runs.
/// pub fn write_document() -> Result<(), Box<dyn Error>> {
///     let spec = build_spec(&app());
///     fs::write("asyncapi.json", spec.to_json()?)?;
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
#[must_use]
pub fn build_spec<A: App>(app: &A) -> Spec {
    let contact = app.info().contact.clone();
    let info = Info {
        title: app.info().title.clone(),
        version: app.info().version.clone(),
        description: app.info().description.clone(),
        terms_of_service: app.info().terms_of_service.clone(),
        contact: (!contact.is_empty()).then_some(contact),
        license: app.info().license.clone(),
        tags: app.info().tags.clone(),
        external_docs: app.info().external_docs.clone(),
    };

    let (servers, security_schemes) = build_servers(app);

    let mut channels = BTreeMap::new();
    let mut operations = BTreeMap::new();
    let mut messages = BTreeMap::new();

    for handler in app.handlers() {
        let server = resolve_server(handler.server.as_deref(), &servers);
        let on_servers = server
            .map(|name| vec![Reference::new(format!("#/servers/{name}"))])
            .unwrap_or_default();
        let tags = server
            .and_then(|name| servers.get(name))
            .map(|server| vec![Tag::new(server.protocol.clone())])
            .unwrap_or_default();

        // The reply contributes its channel and its message before the `receive` operation
        // points at them, and it takes no `send` operation of its own: what answers a request
        // is the request operation's `reply`.
        let reply = handler
            .outgoing
            .iter()
            .find(|outgoing| outgoing.kind == OutgoingKind::Answer)
            .map(|outgoing| {
                let refs = add_outgoing(outgoing, &on_servers, &mut channels, &mut messages);
                OperationReply {
                    address: outgoing.reply_address_location.map(ReplyAddress::new),
                    channel: refs.channel_ref(),
                    messages: vec![refs.message_ref()],
                }
            });

        add_receive(
            handler,
            reply,
            &on_servers,
            &tags,
            &mut channels,
            &mut operations,
            &mut messages,
        );

        // One `send` operation per remaining declared outgoing message: every Out slot
        // dictionary entry, the dead-letter destination of a `dead_letter(..)` declaration, and
        // the destination a `.to(name)` gave the registration's retry copies.
        // A channel several messages flow through lists them all; the message component is
        // shared with any handler that receives it.
        for outgoing in &handler.outgoing {
            if outgoing.kind == OutgoingKind::Answer {
                continue;
            }
            let refs = add_outgoing(outgoing, &on_servers, &mut channels, &mut messages);
            operations.insert(
                send_operation_id(&operations, handler.name.as_ref(), &refs.channel),
                Operation {
                    action: "send".to_owned(),
                    channel: refs.channel_ref(),
                    messages: vec![refs.message_ref()],
                    description: None,
                    reply: None,
                    tags: tags.clone(),
                    retry: None,
                    bindings: outgoing.bindings.operation.clone(),
                },
            );
        }
    }

    let default_content_type = agreed_content_type(&messages);

    Spec {
        asyncapi: ASYNCAPI_VERSION.to_owned(),
        id: app.info().id.clone(),
        info,
        servers,
        default_content_type,
        channels,
        operations,
        components: Components {
            messages,
            security_schemes,
        },
    }
}

/// Renders the service's registered servers, collecting the security schemes they reference into
/// the components map as it goes.
fn build_servers<A: App>(app: &A) -> (BTreeMap<String, Server>, BTreeMap<String, Value>) {
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
                    protocol_version: spec.protocol_version.clone(),
                    description: spec.description.clone(),
                    security,
                    bindings: spec.bindings.clone(),
                },
            )
        })
        .collect();
    (servers, security_schemes)
}

/// The server a registration's channels live on: the label the broker was registered under, or
/// the single server of a one-broker document.
///
/// A document with several servers and an unlabeled registration gets no answer, and the channel
/// then says nothing rather than claiming a server it cannot name.
fn resolve_server<'a>(
    label: Option<&'a str>,
    servers: &'a BTreeMap<String, Server>,
) -> Option<&'a str> {
    match label {
        Some(label) if servers.contains_key(label) => Some(label),
        _ if servers.len() == 1 => servers.keys().next().map(String::as_str),
        _ => None,
    }
}

/// The media type every message in the document names, for the root `defaultContentType`.
///
/// A reader applies the root value to the whole document, and `AsyncAPI` gives it to each message
/// that names no media type of its own. So it stands only while every message names the same one:
/// a second format, or a message no codec reads, would be misdescribed by it.
fn agreed_content_type(messages: &BTreeMap<String, MessageObject>) -> Option<String> {
    let mut agreed: Option<&str> = None;
    for message in messages.values() {
        let content_type = message.content_type.as_deref()?;
        match agreed {
            None => agreed = Some(content_type),
            Some(existing) if existing == content_type => {}
            Some(_) => return None,
        }
    }
    agreed.map(str::to_owned)
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

/// Serializes the JSON Schema of `T` in the draft the document's Schema Object is a superset of.
///
/// The `AsyncAPI` Schema Object extends JSON Schema Draft 07, while `schemars` generates
/// 2020-12 by default: a payload generated with the default settings claims one draft inside a
/// document that says another. Every schema in the document goes through here.
pub(crate) fn schema_json<T: JsonSchema + ?Sized>() -> Option<String> {
    let schema = SchemaSettings::draft07()
        .into_generator()
        .into_root_schema_for::<T>();
    serde_json::to_string(&schema).ok()
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
    handler: &HandlerMetadata,
    reply: Option<OperationReply>,
    on_servers: &[Reference],
    tags: &[Tag],
    channels: &mut BTreeMap<String, Channel>,
    operations: &mut BTreeMap<String, Operation>,
    messages: &mut BTreeMap<String, MessageObject>,
) {
    let name = handler.name.as_ref();
    // One message for an input that is one message; one per model of a message set.
    let received: Vec<MessageContribution> = if handler.incoming.is_empty() {
        vec![Received::of_handler(handler).contribution(handler)]
    } else {
        handler
            .incoming
            .iter()
            .map(|model| Received::of_model(model).contribution(handler))
            .collect()
    };

    let channel = channels.entry(name.to_owned()).or_insert_with(|| Channel {
        address: Some(name.to_owned()),
        messages: BTreeMap::new(),
        parameters: BTreeMap::new(),
        servers: on_servers.to_vec(),
        bindings: Bindings::new(),
    });
    // A subscription reads a real address, whatever a publish that reached this channel first
    // said about its own destination.
    channel.address = Some(name.to_owned());
    // A channel a publish created first carries no binding: the descriptor that reads it is what
    // describes it, and it may reach the channel second.
    channel.bindings.fill_from(&handler.bindings.channel);
    let mut refs = Vec::with_capacity(received.len());
    for contribution in received {
        let message_name = &contribution.name;
        channel.messages.insert(
            message_name.clone(),
            Reference::new(format!("#/components/messages/{message_name}")),
        );
        refs.push(Reference::new(format!(
            "#/channels/{name}/messages/{message_name}"
        )));
        merge_message(messages, contribution, name);
    }

    operations.insert(
        receive_operation_id(operations, name),
        Operation {
            action: "receive".to_owned(),
            channel: Reference::new(format!("#/channels/{name}")),
            messages: refs,
            description: handler.description.as_ref().map(ToString::to_string),
            reply,
            tags: tags.to_vec(),
            retry: retry_extension(handler),
            bindings: handler.bindings.operation.clone(),
        },
    );
}

/// What one received message is described by: the handler's own input, or one model of its
/// message set. Both carry the same slices of metadata, so one routine renders either.
struct Received<'a> {
    message_type: &'a str,
    payload_schema: Option<&'a str>,
    headers_schema: Option<&'a str>,
    message_name: Option<&'a str>,
    message_description: Option<&'a str>,
    content_type: Option<&'a str>,
    deserialized: bool,
    discriminator: Option<HeaderDiscriminator>,
}

impl<'a> Received<'a> {
    fn of_handler(handler: &'a HandlerMetadata) -> Self {
        Self {
            message_type: handler.input_type,
            payload_schema: handler.payload_schema.as_deref(),
            headers_schema: handler.headers_schema.as_deref(),
            message_name: handler.message_name.as_deref(),
            message_description: handler.message_description.as_deref(),
            content_type: handler.content_type,
            deserialized: handler.deserialized,
            discriminator: None,
        }
    }

    fn of_model(model: &'a IncomingMessageMetadata) -> Self {
        Self {
            message_type: model.message_type,
            payload_schema: model.payload_schema.as_deref(),
            headers_schema: model.headers_schema.as_deref(),
            message_name: model.message_name,
            message_description: model.message_description,
            content_type: model.content_type,
            deserialized: model.deserialized,
            discriminator: model.discriminator,
        }
    }

    /// Renders the message as its contribution to the shared component.
    fn contribution(&self, handler: &HandlerMetadata) -> MessageContribution {
        let payload = self
            .payload_schema
            .and_then(|json| serde_json::from_str::<Value>(json).ok());
        // A self-deserializing input is deliberately schema-free (its bytes are its own wire
        // format); a typed model without a schema is a documentation gap worth flagging at
        // generation time.
        if payload.is_none() && !self.deserialized && self.message_type != "bytes" {
            warn!(
                target: "ruststream::asyncapi",
                subscription = %handler.name,
                message_type = self.message_type,
                "message type has no JSON Schema; its AsyncAPI message carries no payload \
                 schema - derive schemars::JsonSchema on it",
            );
        }
        let headers = self
            .headers_schema
            .and_then(|json| serde_json::from_str::<Value>(json).ok());
        // The header a set reads its variant from is part of the message's contract: the value
        // that names this model rides its headers schema as a `const`.
        let headers = match self.discriminator {
            Some(rule) => Some(with_header_const(headers, rule)),
            None => headers,
        };
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
        let name = self
            .message_name
            .map(str::to_owned)
            .or_else(|| schema_str("title"))
            .unwrap_or_else(|| message_name(self.message_type));
        // Message::DESCRIPTION wins, then the type's own doc comment from the schema, then the
        // handler doc (already on the operation) so plain types keep their description.
        let description = self
            .message_description
            .map(str::to_owned)
            .or_else(|| schema_str("description"))
            .or_else(|| handler.description.as_ref().map(ToString::to_string));
        MessageContribution {
            name,
            title: schema_str("title"),
            description,
            content_type: self.content_type.map(str::to_owned),
            payload,
            headers,
            schemaless: self.deserialized,
            bindings: handler.bindings.message.clone(),
        }
    }
}

/// Adds the header a message set reads, with the value that names this model, to the model's
/// headers schema as a required `const` property: the `AsyncAPI` counterpart of an `OpenAPI`
/// discriminator mapping. A model without a header contract gets an object schema of that one
/// property.
fn with_header_const(headers: Option<Value>, rule: HeaderDiscriminator) -> Value {
    let mut schema = match headers {
        Some(Value::Object(schema)) => schema,
        _ => Map::from_iter([("type".to_owned(), Value::String("object".to_owned()))]),
    };
    let mut properties = match schema.remove("properties") {
        Some(Value::Object(properties)) => properties,
        _ => Map::new(),
    };
    properties.insert(rule.header.to_owned(), json!({ "const": rule.value }));
    schema.insert("properties".to_owned(), Value::Object(properties));
    let mut required = match schema.remove("required") {
        Some(Value::Array(required)) => required,
        _ => Vec::new(),
    };
    if !required.iter().any(|name| name == rule.header) {
        required.push(Value::String(rule.header.to_owned()));
    }
    schema.insert("required".to_owned(), Value::Array(required));
    Value::Object(schema)
}

/// Renders what a registration declared about retrying as the `x-ruststream-retry` extension, or
/// nothing when it declared nothing: the cap, where a spent delivery is dead-lettered, and where
/// the deferred copies go when the mount site named a destination for them.
fn retry_extension(handler: &HandlerMetadata) -> Option<RetryExtension> {
    let retry_destination = handler
        .outgoing
        .iter()
        .find(|outgoing| outgoing.kind == OutgoingKind::RetryCopy)
        .map(|outgoing| outgoing.channel.as_ref().to_owned());
    let declaration = &handler.retry;
    if declaration.declares_nothing() && retry_destination.is_none() {
        return None;
    }
    Some(RetryExtension {
        max_attempts: declaration.max_attempts().map(NonZeroU32::get),
        dead_letter: declaration.dead_letter().map(str::to_owned),
        retry_destination,
    })
}

/// One side's contribution to a shared message component.
struct MessageContribution {
    name: String,
    title: Option<String>,
    description: Option<String>,
    content_type: Option<String>,
    payload: Option<Value>,
    headers: Option<Value>,
    schemaless: bool,
    bindings: Bindings,
}

/// Merges one contribution into a shared message component: an absent title, description,
/// media type, payload, or headers schema fills in from a later contributor (component identity
/// is the message name, and several handlers may carry different slices of its metadata), while
/// a conflicting headers schema keeps the first one with a WARN - one component cannot carry two
/// contracts, and the conflict usually means two handlers read the same type with different
/// `Headers` declarations.
fn merge_message(
    messages: &mut BTreeMap<String, MessageObject>,
    contribution: MessageContribution,
    context: &str,
) {
    let MessageContribution {
        name,
        title,
        description,
        content_type,
        payload,
        headers,
        schemaless,
        bindings,
    } = contribution;
    let entry = messages
        .entry(name.clone())
        .or_insert_with(|| MessageObject {
            name,
            title: None,
            description: None,
            content_type: None,
            payload: None,
            headers: None,
            schemaless: false,
            bindings: Bindings::new(),
        });
    entry.bindings.fill_from(&bindings);
    // A title repeating the machine name says nothing the reader does not already see.
    if entry.title.is_none() && title.as_deref().is_some_and(|title| title != entry.name) {
        entry.title = title;
    }
    if entry.description.is_none() {
        entry.description = description;
    }
    if entry.content_type.is_none() {
        entry.content_type = content_type;
    }
    // One schema-free-by-design contributor is enough: the shared component's missing schema
    // is then explained, whichever side contributed it.
    entry.schemaless |= schemaless;
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

/// Where an operation points to reach one declared outgoing message.
struct OutgoingRefs {
    channel: String,
    message: String,
}

impl OutgoingRefs {
    fn channel_ref(&self) -> Reference {
        Reference::new(format!("#/channels/{}", self.channel))
    }

    fn message_ref(&self) -> Reference {
        Reference::new(format!(
            "#/channels/{}/messages/{}",
            self.channel, self.message
        ))
    }
}

/// Adds one declared outgoing message to the document: its channel entry and its message
/// component (shared with any handler that receives the same type), answering the references an
/// operation uses to point at them. The message name falls back like the receive side: `Message`
/// impl, then schema title, then the type name.
///
/// What operation the caller then writes is the entry's kind: a slot publish and a dead-letter
/// destination each get a `send` operation, while a reply becomes the `reply` of the `receive`
/// operation it answers.
fn add_outgoing(
    outgoing: &OutgoingMessageMetadata,
    on_servers: &[Reference],
    channels: &mut BTreeMap<String, Channel>,
    messages: &mut BTreeMap<String, MessageObject>,
) -> OutgoingRefs {
    let payload = outgoing
        .payload_schema
        .as_deref()
        .and_then(|json| serde_json::from_str::<Value>(json).ok());
    // A serialized-wire message is deliberately schema-free (its bytes are its own wire
    // format); a typed model without a schema is a documentation gap worth flagging at
    // generation time.
    if payload.is_none() && !outgoing.serialized && outgoing.message_type != "bytes" {
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
        .or_else(|| title.clone())
        .unwrap_or_else(|| message_name(outgoing.message_type));
    let description = outgoing.message_description.map(str::to_owned).or_else(|| {
        payload
            .as_ref()
            .and_then(|schema| schema.get("description"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    });
    let channel = outgoing.channel.as_ref();

    let entry = channels
        .entry(channel.to_owned())
        .or_insert_with(|| Channel {
            // A destination named per delivery has no address to report: the operation's reply
            // says where a client reads it instead.
            address: outgoing
                .reply_address_location
                .is_none()
                .then(|| channel.to_owned()),
            messages: BTreeMap::new(),
            // A templated address declares its placeholders, so the document says what the
            // segments are instead of showing an address nothing describes.
            parameters: outgoing
                .parameters
                .iter()
                .map(|segment| ((*segment).to_owned(), Parameter::default()))
                .collect(),
            servers: on_servers.to_vec(),
            bindings: Bindings::new(),
        });
    entry.bindings.fill_from(&outgoing.bindings.channel);
    entry.messages.insert(
        name.clone(),
        Reference::new(format!("#/components/messages/{name}")),
    );

    merge_message(
        messages,
        MessageContribution {
            name: name.clone(),
            title,
            description,
            // The media type comes from the codec the mount chain bound on this position; a
            // message also received somewhere contributes the receiving side's too, and the
            // first contributor wins.
            content_type: outgoing.content_type.map(str::to_owned),
            payload,
            headers,
            schemaless: outgoing.serialized,
            bindings: outgoing.bindings.message.clone(),
        },
        channel,
    );

    OutgoingRefs {
        channel: channel.to_owned(),
        message: name,
    }
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
    use serde_json::json;

    use crate::runtime::HeaderDiscriminator;
    use crate::{ApiKeyLocation, HttpApiKeyLocation, SecurityScheme};

    use super::with_header_const;

    /// A model with no header contract documents the header the set reads as the one property
    /// of its headers schema.
    #[test]
    fn the_header_rule_makes_a_headers_schema_of_its_own() {
        let schema = with_header_const(None, HeaderDiscriminator::new("event", "created"));
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": { "event": { "const": "created" } },
                "required": ["event"],
            })
        );
    }

    /// A model with a header contract keeps it, and the rule's header joins its properties; a
    /// header the contract already requires is listed once.
    #[test]
    fn the_header_rule_joins_an_existing_contract() {
        let contract = json!({
            "type": "object",
            "properties": { "tenant": { "type": "string" }, "event": { "type": "string" } },
            "required": ["tenant", "event"],
        });
        let schema =
            with_header_const(Some(contract), HeaderDiscriminator::new("event", "created"));
        assert_eq!(schema["properties"]["tenant"], json!({ "type": "string" }));
        assert_eq!(schema["properties"]["event"], json!({ "const": "created" }));
        assert_eq!(schema["required"], json!(["tenant", "event"]));
    }

    /// A contract whose `properties` or `required` is not the shape a schema gives them is
    /// rewritten around the rule rather than trusted.
    #[test]
    fn the_header_rule_replaces_malformed_parts_of_a_contract() {
        let odd = json!({ "type": "object", "properties": 7, "required": "event" });
        let schema = with_header_const(Some(odd), HeaderDiscriminator::new("event", "created"));
        assert_eq!(
            schema["properties"],
            json!({ "event": { "const": "created" } })
        );
        assert_eq!(schema["required"], json!(["event"]));
        let scalar = with_header_const(
            Some(json!(true)),
            HeaderDiscriminator::new("event", "created"),
        );
        assert_eq!(scalar["type"], json!("object"));
    }

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
}
