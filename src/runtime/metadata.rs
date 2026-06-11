//! Handler metadata collected by the router and consumed by `ruststream-asyncapi`.

use std::{any::type_name, borrow::Cow, marker::PhantomData};

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
    /// The input type's [`Message`](crate::Message) name, when it implements that trait. Overrides
    /// the `input_type`-derived name in the `AsyncAPI` document.
    pub message_name: Option<Cow<'static, str>>,
    /// The input type's [`Message`](crate::Message) description, when it implements that trait.
    /// Feeds the `AsyncAPI` message description.
    pub message_description: Option<Cow<'static, str>>,
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
            message_name: None,
            message_description: None,
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
            message_name: None,
            message_description: None,
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
}
