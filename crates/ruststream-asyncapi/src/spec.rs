//! The objects of an `AsyncAPI` 3.1 document: what [`build_spec`](super::build_spec) fills and
//! the document serializes.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::{AppId, Contact, ExternalDocs, License, Tag};

use crate::bindings::Bindings;

/// The specification version every generated document declares.
pub(super) const ASYNCAPI_VERSION: &str = "3.1.0";

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
    /// [`build_spec`](super::build_spec) logs a `WARN` per gap when the document is generated; this accessor makes
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
    pub(super) fn new(target: impl Into<String>) -> Self {
        Self {
            reference: target.into(),
        }
    }
}
