//! Handler metadata collected by the router and consumed by `ruststream-asyncapi`.

use std::{any::type_name, borrow::Cow, marker::PhantomData};

/// One message a handler publishes, as declared for the `AsyncAPI` document: the reply of a
/// `publish("dest")` form, or one entry of an `Out` slot's `#[publishes(..)]` dictionary.
///
/// Constructed by generated code through [`new`](Self::new) plus the builder-style setters;
/// consumed by `build_spec`, which renders each entry as a `send` operation on its channel.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OutgoingMessageMetadata {
    /// The channel / subject the message is published to.
    pub channel: Cow<'static, str>,
    /// Type name of the published value, as captured at registration time.
    pub message_type: &'static str,
    /// The type's [`Message`](crate::Message) name, when it implements that trait.
    pub message_name: Option<&'static str>,
    /// The type's [`Message`](crate::Message) description, when it implements that trait.
    pub message_description: Option<&'static str>,
    /// The type's serialized JSON Schema, when available (`asyncapi` feature).
    pub payload_schema: Option<String>,
    /// The serialized JSON Schema of the type's `#[message(headers(..))]` contract, when it
    /// declares one.
    pub headers_schema: Option<String>,
}

impl OutgoingMessageMetadata {
    /// Constructs an entry for a message type published to `channel`.
    #[must_use]
    pub fn new(channel: impl Into<Cow<'static, str>>, message_type: &'static str) -> Self {
        Self {
            channel: channel.into(),
            message_type,
            message_name: None,
            message_description: None,
            payload_schema: None,
            headers_schema: None,
        }
    }

    /// Builder-style setter for the [`Message`](crate::Message) name.
    #[must_use]
    pub fn with_message_name(mut self, name: Option<&'static str>) -> Self {
        self.message_name = name;
        self
    }

    /// Builder-style setter for the [`Message`](crate::Message) description.
    #[must_use]
    pub fn with_message_description(mut self, description: Option<&'static str>) -> Self {
        self.message_description = description;
        self
    }

    /// Builder-style setter for the serialized payload schema.
    #[must_use]
    pub fn with_payload_schema(mut self, schema: Option<String>) -> Self {
        self.payload_schema = schema;
        self
    }

    /// Builder-style setter for the serialized headers schema.
    #[must_use]
    pub fn with_headers_schema(mut self, schema: Option<String>) -> Self {
        self.headers_schema = schema;
        self
    }
}

/// Descriptive metadata for a registered subscriber handler.
///
/// Collected by the router so downstream tools (`AsyncAPI` generator, dashboards, CLI) can
/// describe the service without re-parsing source code.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HandlerMetadata {
    /// Broker name / subject the handler is bound to.
    pub name: Cow<'static, str>,
    /// Optional broker-provided routing key, when the broker distinguishes from `name`.
    pub routing_key: Option<Cow<'static, str>>,
    /// Type name of the decoded input value, as captured at registration time.
    pub input_type: &'static str,
    /// Type name of the response value, when the handler produces one (e.g. request / reply).
    pub output_type: Option<&'static str>,
    /// Free-form human description, typically pulled from a doc comment on the handler.
    pub description: Option<Cow<'static, str>>,
    /// The input type's JSON Schema, serialized, when the type implements
    /// [`schemars::JsonSchema`] (captured under the `asyncapi` feature). Feeds the `AsyncAPI`
    /// message payload schema.
    pub payload_schema: Option<String>,
    /// The typed header contract's JSON Schema, serialized, when the handler declares one (a
    /// `FromHeaders<T>` parameter, captured under the `asyncapi` feature). Feeds the `AsyncAPI`
    /// message headers schema.
    pub headers_schema: Option<String>,
    /// The input type's [`Message`](crate::Message) name, when it implements that trait. Overrides
    /// the `input_type`-derived name in the `AsyncAPI` document.
    pub message_name: Option<Cow<'static, str>>,
    /// The input type's [`Message`](crate::Message) description, when it implements that trait.
    /// Feeds the `AsyncAPI` message description.
    pub message_description: Option<Cow<'static, str>>,
    /// The messages this handler publishes, when it declares them: the reply of a
    /// `publish("dest")` form, and every entry of an `Out` slot's `#[publishes(..)]`
    /// dictionary. Feeds the `AsyncAPI` `send` operations.
    pub outgoing: Vec<OutgoingMessageMetadata>,
}

impl HandlerMetadata {
    /// Constructs metadata for a raw-bytes handler bound to a name.
    #[must_use]
    pub fn raw(name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            routing_key: None,
            input_type: "bytes",
            output_type: None,
            description: None,
            payload_schema: None,
            headers_schema: None,
            message_name: None,
            message_description: None,
            outgoing: Vec::new(),
        }
    }

    /// Constructs metadata for a typed handler. The input type name is captured via
    /// [`std::any::type_name`].
    #[must_use]
    pub fn typed<T>(name: impl Into<Cow<'static, str>>) -> Self {
        let _ = PhantomData::<T>;
        Self {
            name: name.into(),
            routing_key: None,
            input_type: type_name::<T>(),
            output_type: None,
            description: None,
            payload_schema: None,
            headers_schema: None,
            message_name: None,
            message_description: None,
            outgoing: Vec::new(),
        }
    }

    /// Builder-style setter for the handler description.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Builder-style setter for the broker routing key.
    #[must_use]
    pub fn with_routing_key(mut self, key: impl Into<Cow<'static, str>>) -> Self {
        self.routing_key = Some(key.into());
        self
    }

    /// Builder-style setter for the response type name.
    #[must_use]
    pub fn with_output_type(mut self, name: &'static str) -> Self {
        self.output_type = Some(name);
        self
    }

    /// Builder-style setter for the serialized input payload schema.
    #[must_use]
    pub fn with_payload_schema(mut self, schema: impl Into<String>) -> Self {
        self.payload_schema = Some(schema.into());
        self
    }

    /// Builder-style setter for the serialized header contract schema.
    #[must_use]
    pub fn with_headers_schema(mut self, schema: impl Into<String>) -> Self {
        self.headers_schema = Some(schema.into());
        self
    }

    /// Builder-style setter for the [`Message`](crate::Message) name of the input type.
    #[must_use]
    pub fn with_message_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.message_name = Some(name.into());
        self
    }

    /// Builder-style setter for the [`Message`](crate::Message) description of the input type.
    #[must_use]
    pub fn with_message_description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.message_description = Some(description.into());
        self
    }

    /// Attaches the optional descriptive fields that every generated definition trait exposes
    /// with identical signatures (`description`, `input_schema`, `headers_schema`,
    /// `message_name`, `message_description`). Shared tail of the per-definition metadata
    /// builders.
    #[must_use]
    pub(crate) fn with_def_details(
        mut self,
        description: Option<&str>,
        input_schema: Option<String>,
        headers_schema: Option<String>,
        message_name: Option<&'static str>,
        message_description: Option<&'static str>,
    ) -> Self {
        if let Some(description) = description {
            self = self.with_description(description.to_owned());
        }
        if let Some(schema) = input_schema {
            self = self.with_payload_schema(schema);
        }
        if let Some(schema) = headers_schema {
            self = self.with_headers_schema(schema);
        }
        if let Some(name) = message_name {
            self = self.with_message_name(name);
        }
        if let Some(description) = message_description {
            self = self.with_message_description(description);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_tail_attaches_every_optional_detail() {
        let meta = HandlerMetadata::raw("orders")
            .with_routing_key("orders.eu")
            .with_def_details(
                Some("handles orders"),
                Some("{}".to_owned()),
                Some("{\"type\":\"object\"}".to_owned()),
                Some("Order"),
                Some("an order event"),
            );
        assert_eq!(meta.routing_key.as_deref(), Some("orders.eu"));
        assert_eq!(meta.description.as_deref(), Some("handles orders"));
        assert_eq!(meta.payload_schema.as_deref(), Some("{}"));
        assert_eq!(
            meta.headers_schema.as_deref(),
            Some("{\"type\":\"object\"}")
        );
        assert_eq!(meta.message_name.as_deref(), Some("Order"));
        assert_eq!(meta.message_description.as_deref(), Some("an order event"));

        // The all-None tail changes nothing.
        let plain = HandlerMetadata::raw("orders").with_def_details(None, None, None, None, None);
        assert!(plain.description.is_none());
        assert!(plain.payload_schema.is_none());
        assert!(plain.headers_schema.is_none());
    }
}
