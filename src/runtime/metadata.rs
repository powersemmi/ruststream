//! Handler metadata collected by the router and consumed by `ruststream-asyncapi`.

use std::{any::type_name, borrow::Cow, marker::PhantomData};

#[cfg(feature = "asyncapi")]
use crate::asyncapi::{PublishBindings, SubscriptionBindings};
use crate::runtime::input::DecodeWith;
use crate::{ConnectedBroker, PublishPolicy, RetryDeclaration, SubscriptionSource};

/// What a declared outgoing message is to the registration declaring it.
///
/// The three kinds document differently: an answer replies to the delivery being handled, a slot
/// entry is a destination the handler body writes to, and a dead-lettered delivery is the input
/// giving up. `build_spec` reads this to put a reply on the `receive` operation instead of a
/// `send` operation of its own, and to tell a dead-letter channel from a business destination.
///
/// The variants are named apart from the mount-position markers [`Reply`](crate::runtime::Reply)
/// and [`Slot`](crate::runtime::Slot): a position is where a policy is bound, a kind is what the
/// document makes of the message that leaves through it.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{OutgoingKind, OutgoingMessageMetadata};
///
/// let slot = OutgoingMessageMetadata::new("events.progress", "Progress");
/// assert_eq!(slot.kind, OutgoingKind::SlotEntry);
/// assert_eq!(slot.with_kind(OutgoingKind::Answer).kind, OutgoingKind::Answer);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum OutgoingKind {
    /// One entry of an `Out` slot's `#[publishes(..)]` dictionary: a destination the handler
    /// body publishes to. The default, because a bare declaration is exactly that.
    #[default]
    SlotEntry,
    /// The reply of a `publish(..)` registration: the value the handler returns, answering the
    /// delivery it was given.
    Answer,
    /// The destination a `dead_letter(..)` declaration names: a delivery out of attempts leaves
    /// the service there.
    DeadLetter,
}

/// One message a handler publishes, as declared for the `AsyncAPI` document.
///
/// The declaration is the reply of a `publish("dest")` form, one entry of an `Out` slot's
/// `#[publishes(..)]` dictionary, or the destination of a `dead_letter(..)` declaration.
///
/// Constructed by generated code through [`new`](Self::new) plus the builder-style setters;
/// consumed by `build_spec`, which renders each entry according to its [`kind`](Self::kind).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OutgoingMessageMetadata {
    /// The channel / subject the message is published to.
    pub channel: Cow<'static, str>,
    /// Type name of the published value, as captured at registration time.
    pub message_type: &'static str,
    /// The type's [`Message`](crate::MessageInfo) name, when it implements that trait.
    pub message_name: Option<&'static str>,
    /// The type's [`Message`](crate::MessageInfo) description, when it implements that trait.
    pub message_description: Option<&'static str>,
    /// The type's serialized JSON Schema, when available (`asyncapi` feature).
    pub payload_schema: Option<String>,
    /// The serialized JSON Schema of the type's `#[message(headers(..))]` contract, when it
    /// declares one.
    pub headers_schema: Option<String>,
    /// The placeholder names of a templated destination (`orders.{tenant}.v1`), in the order
    /// they appear in [`channel`](Self::channel). Empty for a fixed one; the generated document
    /// turns them into the channel's parameters block, so the declaration and the call site
    /// cannot drift apart.
    pub parameters: &'static [&'static str],
    /// True when the type rides the serialized wire
    /// ([`Serialized`](crate::runtime::Serialized)): its bytes are its own wire format, so the
    /// missing payload schema is by design rather than a documentation gap.
    pub serialized: bool,
    /// What this entry is to its registration: a slot publish, a reply, or a dead-letter
    /// destination.
    pub kind: OutgoingKind,
    /// The media type the codec bound at the mount site encodes this message in
    /// ([`Codec::CONTENT_TYPE`](crate::codec::Codec::CONTENT_TYPE)). `None` where nothing
    /// encodes: a [`Serialized`](crate::runtime::Serialized) reply and a dead-lettered delivery
    /// both carry bytes that are their own wire format.
    pub content_type: Option<&'static str>,
    /// The runtime expression naming where a reply of this entry actually goes, when the
    /// registration answers per delivery: the publish policy's
    /// [`reply_address_location`](crate::PublishPolicy::reply_address_location), carried only
    /// where a transform on the reply position names the destination. The document then reports
    /// the channel with `address: null` and puts the expression in the operation's reply.
    pub reply_address_location: Option<&'static str>,
    /// What the publish policy bound on this position adds to the generated document at each
    /// level. Empty unless the broker's policy fills it in.
    #[cfg(feature = "asyncapi")]
    pub bindings: PublishBindings,
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
            parameters: &[],
            serialized: false,
            kind: OutgoingKind::SlotEntry,
            content_type: None,
            reply_address_location: None,
            #[cfg(feature = "asyncapi")]
            bindings: PublishBindings::default(),
        }
    }

    /// Builder-style setter for what this entry is to its registration (see
    /// [`kind`](Self::kind)).
    #[must_use]
    pub const fn with_kind(mut self, kind: OutgoingKind) -> Self {
        self.kind = kind;
        self
    }

    /// Builder-style setter for a templated destination's placeholder names.
    #[must_use]
    pub fn with_parameters(mut self, parameters: &'static [&'static str]) -> Self {
        self.parameters = parameters;
        self
    }

    /// Builder-style setter for the [`Message`](crate::MessageInfo) name.
    #[must_use]
    pub fn with_message_name(mut self, name: Option<&'static str>) -> Self {
        self.message_name = name;
        self
    }

    /// Builder-style setter for the [`Message`](crate::MessageInfo) description.
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

    /// Builder-style setter for the serialized-wire marker (see [`serialized`](Self::serialized)).
    #[must_use]
    pub fn with_serialized(mut self, serialized: bool) -> Self {
        self.serialized = serialized;
        self
    }

    /// Builder-style setter for the media type this message leaves in (see
    /// [`content_type`](Self::content_type)).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::OutgoingMessageMetadata;
    ///
    /// let entry = OutgoingMessageMetadata::new("events.progress", "Progress")
    ///     .with_content_type(Some("application/json"));
    ///
    /// assert_eq!(entry.content_type, Some("application/json"));
    /// ```
    #[must_use]
    pub const fn with_content_type(mut self, content_type: Option<&'static str>) -> Self {
        self.content_type = content_type;
        self
    }
}

/// What one bound publish position says about itself, read off its policy at mount time.
///
/// The three publish positions - a reply, an [`Out`](crate::runtime::Out) slot, the publisher a
/// dead-lettered delivery leaves through - each resolve one of these and write it onto the
/// [`OutgoingMessageMetadata`] entries they own. Nothing here runs per message.
#[derive(Debug, Clone, Default)]
pub(crate) struct PublishDescription {
    /// The media type the position's codec encodes in, where one does.
    pub(crate) content_type: Option<&'static str>,
    /// The policy's reply address expression, carried only where a transform on the position
    /// names the destination per delivery.
    pub(crate) reply_address_location: Option<&'static str>,
    /// What the policy adds to the document at each level.
    #[cfg(feature = "asyncapi")]
    pub(crate) bindings: PublishBindings,
}

impl PublishDescription {
    /// Reads the description off `policy`, against the broker it pairs with.
    ///
    /// `names_destination` is what the mount site's transform stack declared: only a position
    /// that names the destination per delivery has a reply address to report, because every
    /// other one publishes to the name the document already carries.
    pub(crate) fn of<C, P>(
        policy: &P,
        content_type: Option<&'static str>,
        names_destination: bool,
    ) -> Self
    where
        C: ConnectedBroker,
        P: PublishPolicy<C> + ?Sized,
    {
        let _ = (policy, names_destination);
        #[cfg(not(feature = "asyncapi"))]
        let description = Self {
            content_type,
            reply_address_location: None,
        };
        #[cfg(feature = "asyncapi")]
        let description = Self {
            content_type,
            reply_address_location: names_destination
                .then(|| policy.reply_address_location())
                .flatten(),
            bindings: PublishBindings {
                channel: policy.channel_bindings(),
                operation: policy.operation_bindings(),
                message: policy.message_bindings(),
            },
        };
        description
    }

    /// Writes what the position says onto one entry.
    ///
    /// A message riding the serialized wire carries bytes that are their own wire format, so it
    /// takes no media type from the position's codec: nothing encoded it.
    fn apply(&self, entry: &mut OutgoingMessageMetadata) {
        entry.content_type = (!entry.serialized).then_some(self.content_type).flatten();
        entry.reply_address_location = self.reply_address_location;
        #[cfg(feature = "asyncapi")]
        {
            entry.bindings = self.bindings.clone();
        }
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
    /// `Headers<T>` parameter, captured under the `asyncapi` feature). Feeds the `AsyncAPI`
    /// message headers schema.
    pub headers_schema: Option<String>,
    /// The input type's [`Message`](crate::MessageInfo) name, when it implements that trait. Overrides
    /// the `input_type`-derived name in the `AsyncAPI` document.
    pub message_name: Option<Cow<'static, str>>,
    /// The input type's [`Message`](crate::MessageInfo) description, when it implements that trait.
    /// Feeds the `AsyncAPI` message description.
    pub message_description: Option<Cow<'static, str>>,
    /// The messages this handler publishes, when it declares them: the reply of a
    /// `publish("dest")` form, and every entry of an `Out` slot's `#[publishes(..)]`
    /// dictionary. Feeds the `AsyncAPI` `send` operations.
    pub outgoing: Vec<OutgoingMessageMetadata>,
    /// True when the input rides the self-deserializing lane
    /// ([`Deserialized`](crate::runtime::Deserialized)): the payload has no serde model, so
    /// the missing schema is by design rather than a documentation gap.
    pub deserialized: bool,
    /// The name of the `AsyncAPI` server this handler's broker was registered under, when the
    /// registration carries one: the label of
    /// [`with_broker_labeled`](crate::runtime::RustStream::with_broker_labeled). Feeds the
    /// channel's `servers` list, so a multi-broker document says which broker a channel lives
    /// on instead of showing every channel on every server.
    ///
    /// A cross-broker publish is outside what this can answer: a
    /// [`Bound`](crate::runtime::Bound) token publishes against its own broker, which the
    /// registration's label does not name.
    pub server: Option<Cow<'static, str>>,
    /// The media type the codec decoding this subscription produces, when a codec decodes it
    /// at all ([`Codec::CONTENT_TYPE`](crate::codec::Codec::CONTENT_TYPE)). `None` on the
    /// self-deserializing lane, whose bytes are their own wire format.
    pub content_type: Option<&'static str>,
    /// What the registration declared about retrying a failed delivery: the attempt cap and the
    /// dead-letter destination. Empty unless the mount site declared one.
    pub retry: RetryDeclaration,
    /// What the subscription descriptor adds to the generated document at each level. Empty
    /// unless the broker's descriptor fills it in.
    #[cfg(feature = "asyncapi")]
    pub bindings: SubscriptionBindings,
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
            deserialized: false,
            server: None,
            content_type: None,
            retry: RetryDeclaration::new(),
            #[cfg(feature = "asyncapi")]
            bindings: SubscriptionBindings::default(),
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
            deserialized: false,
            server: None,
            content_type: None,
            retry: RetryDeclaration::new(),
            #[cfg(feature = "asyncapi")]
            bindings: SubscriptionBindings::default(),
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

    /// Builder-style setter for the [`Message`](crate::MessageInfo) name of the input type.
    #[must_use]
    pub fn with_message_name(mut self, name: impl Into<Cow<'static, str>>) -> Self {
        self.message_name = Some(name.into());
        self
    }

    /// Builder-style setter for the [`Message`](crate::MessageInfo) description of the input type.
    #[must_use]
    pub fn with_message_description(mut self, description: impl Into<Cow<'static, str>>) -> Self {
        self.message_description = Some(description.into());
        self
    }

    /// Records the media type the mounted codec decodes this subscription with, taken from the
    /// input kind so a byte input (which mounts with no codec) reports none.
    #[must_use]
    pub(crate) fn decoded_with<Input, DecodeCodec>(mut self) -> Self
    where
        Input: DecodeWith<DecodeCodec>,
    {
        self.content_type = <Input as DecodeWith<DecodeCodec>>::CONTENT_TYPE;
        self
    }

    /// Writes what the reply position's policy says onto the entry the reply declared.
    pub(crate) fn describe_reply(&mut self, description: &PublishDescription) {
        for entry in &mut self.outgoing {
            if entry.kind == OutgoingKind::Answer {
                description.apply(entry);
            }
        }
    }

    /// Writes what one slot's policy says onto the entries its marker declared, matched by the
    /// destination each entry names.
    pub(crate) fn describe_slot(
        &mut self,
        channels: &[Cow<'static, str>],
        description: &PublishDescription,
    ) {
        for entry in &mut self.outgoing {
            if entry.kind == OutgoingKind::SlotEntry && channels.contains(&entry.channel) {
                description.apply(entry);
            }
        }
    }

    /// Writes what the retry publisher's policy says onto the dead-letter entry.
    ///
    /// The copy carries the delivery's own bytes, so the media type stays the one the
    /// subscription decodes rather than anything the retry publisher would encode.
    pub(crate) fn describe_dead_letter(&mut self, description: &PublishDescription) {
        let content_type = self.content_type;
        for entry in &mut self.outgoing {
            if entry.kind == OutgoingKind::DeadLetter {
                description.apply(entry);
                entry.content_type = content_type;
            }
        }
    }

    /// Records what the subscription descriptor adds to the generated document.
    ///
    /// Without the `asyncapi` feature there is no document and no descriptor to ask, so the call
    /// carries the metadata through unchanged.
    #[must_use]
    pub(crate) fn describing<C, S>(self, source: &S) -> Self
    where
        C: ConnectedBroker,
        S: SubscriptionSource<C>,
    {
        let _ = source;
        #[cfg(not(feature = "asyncapi"))]
        let this = self;
        #[cfg(feature = "asyncapi")]
        let this = Self {
            bindings: SubscriptionBindings {
                channel: source.channel_bindings(),
                operation: source.operation_bindings(),
                message: source.message_bindings(),
            },
            ..self
        };
        this
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

    /// A hand-written definition states the media type of what it publishes the same way the
    /// mount chain does.
    #[test]
    fn an_outgoing_entry_takes_a_media_type_from_its_builder() {
        let entry = OutgoingMessageMetadata::new("events.progress", "Progress")
            .with_content_type(Some("application/json"));

        assert_eq!(entry.content_type, Some("application/json"));
        assert_eq!(
            OutgoingMessageMetadata::new("events.progress", "Progress").content_type,
            None,
        );
    }
}
