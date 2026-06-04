//! Handler metadata collected by the router and consumed by `ruststream-asyncapi`.

use std::{any::type_name, borrow::Cow, marker::PhantomData};

/// Descriptive metadata for a registered subscriber handler.
///
/// Collected by the router so downstream tools (`AsyncAPI` generator, dashboards, CLI) can
/// describe the service without re-parsing source code.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HandlerMetadata {
    /// Broker topic / subject the handler is bound to.
    pub topic: Cow<'static, str>,
    /// Optional broker-provided routing key, when the broker distinguishes from `topic`.
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
}

impl HandlerMetadata {
    /// Constructs metadata for a raw-bytes handler bound to a topic.
    #[must_use]
    pub fn raw(topic: impl Into<Cow<'static, str>>) -> Self {
        Self {
            topic: topic.into(),
            routing_key: None,
            input_type: "bytes",
            output_type: None,
            description: None,
            payload_schema: None,
        }
    }

    /// Constructs metadata for a typed handler. The input type name is captured via
    /// [`std::any::type_name`].
    #[must_use]
    pub fn typed<T>(topic: impl Into<Cow<'static, str>>) -> Self {
        let _ = PhantomData::<T>;
        Self {
            topic: topic.into(),
            routing_key: None,
            input_type: type_name::<T>(),
            output_type: None,
            description: None,
            payload_schema: None,
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
}
