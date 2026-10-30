//! How one registration becomes channels, operations and messages: the subscriber's `receive`
//! operation with what it declared, and a `send` operation (or the reply of the `receive`) per
//! declared outgoing message.

use std::collections::BTreeMap;
use std::num::NonZeroU32;

use serde_json::Value;
use tracing::warn;

use ruststream_core::Tag;
use ruststream_runtime::{
    HandlerMetadata, HeaderDiscriminator, IncomingMessageMetadata, OutgoingKind,
    OutgoingMessageMetadata,
};

use super::messages::{MessageContribution, merge_message, message_name, with_header_const};
use super::spec::{
    Channel, MessageObject, Operation, OperationReply, Parameter, Reference, RetryExtension,
};
use ruststream_core::bindings::Bindings;

/// Adds one registered subscriber to the document: its channel entry, its `receive` operation,
/// and its message component (with the payload and headers schemas the handler declared).
pub(super) fn add_receive(
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

/// Where an operation points to reach one declared outgoing message.
pub(super) struct OutgoingRefs {
    pub(super) channel: String,
    message: String,
}

impl OutgoingRefs {
    pub(super) fn channel_ref(&self) -> Reference {
        Reference::new(format!("#/channels/{}", self.channel))
    }

    pub(super) fn message_ref(&self) -> Reference {
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
pub(super) fn add_outgoing(
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
    // A copy of a model of a message set carries the rule that named it, as the receive side
    // documents it, so the shared component sees one contract from both sides.
    let headers = match outgoing.discriminator {
        Some(rule) => Some(with_header_const(headers, rule)),
        None => headers,
    };
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
pub(super) fn receive_operation_id(operations: &BTreeMap<String, Operation>, name: &str) -> String {
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
pub(super) fn send_operation_id(
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
