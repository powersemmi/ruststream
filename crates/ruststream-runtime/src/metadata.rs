//! Handler metadata collected by the router and consumed by `ruststream-asyncapi`.

use std::{any::type_name, borrow::Cow, marker::PhantomData};

#[cfg(feature = "asyncapi")]
use crate::bindings::{PublishBindings, SubscriptionBindings};
use crate::input::{DecodeWith, Materialize};
#[cfg(feature = "asyncapi")]
use crate::join_headers;
use crate::{ConnectedBroker, PublishPolicy, RetryDeclaration, SubscriptionSource};

/// What a declared outgoing message is to the registration declaring it.
///
/// The kinds document differently: an answer replies to the delivery being handled, a slot entry
/// is a destination the handler body writes to, a dead-lettered delivery is the input giving up,
/// and a retry copy is the input coming back after a delay. `build_spec` reads this to put a
/// reply on the `receive` operation instead of a `send` operation of its own, and to tell a
/// dead-letter channel from a business destination.
///
/// The variants are named apart from the mount-position markers [`Reply`](crate::Reply)
/// and [`Slot`](crate::Slot): a position is where a policy is bound, a kind is what the
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
    /// The reply of a `reply(..)` registration: the value the handler returns, answering the
    /// delivery it was given.
    Answer,
    /// The destination a `dead_letter(..)` declaration names: a delivery out of attempts leaves
    /// the service there.
    DeadLetter,
    /// The destination a `.to(name)` after `out_retry(policy)` names: every deferred copy of a
    /// delivery is published there. A subscription that takes its copies back at its own address
    /// declares nothing and gets no entry - that address is not a channel of the service.
    RetryCopy,
}

/// One message a handler publishes, as declared for the `AsyncAPI` document.
///
/// The declaration is the reply of a `reply("dest")` form, one entry of an `Out` slot's
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
    /// ([`Serialized`](crate::Serialized)): its bytes are its own wire format, so the
    /// missing payload schema is by design rather than a documentation gap.
    pub serialized: bool,
    /// What this entry is to its registration: a slot publish, a reply, or a dead-letter
    /// destination.
    pub kind: OutgoingKind,
    /// The media type the codec bound at the mount site encodes this message in
    /// ([`Codec::CONTENT_TYPE`](crate::codec::Codec::CONTENT_TYPE)). `None` where nothing
    /// encodes: a [`Serialized`](crate::Serialized) reply and a dead-lettered delivery
    /// both carry bytes that are their own wire format.
    pub content_type: Option<&'static str>,
    /// The runtime expression naming where a reply of this entry actually goes, when the
    /// registration answers per delivery: the publish policy's
    /// [`reply_address_location`](crate::PublishPolicy::reply_address_location), carried only
    /// where a transform on the reply position names the destination. The document then reports
    /// the channel with `address: null` and puts the expression in the operation's reply.
    pub reply_address_location: Option<&'static str>,
    /// The header value that names this message, when it is a model of a message set that reads
    /// its variant from a header: the copy of a delivery carries the same rule as the delivery.
    pub discriminator: Option<HeaderDiscriminator>,
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
            discriminator: None,
            #[cfg(feature = "asyncapi")]
            bindings: PublishBindings::default(),
        }
    }

    /// Builder-style setter for the header value that names this message (see
    /// [`discriminator`](Self::discriminator)).
    #[must_use]
    pub const fn with_discriminator(mut self, discriminator: HeaderDiscriminator) -> Self {
        self.discriminator = Some(discriminator);
        self
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

/// The header a message set reads its variant from, with the value one variant declares.
///
/// `#[in_messages(header = "event")]` on the set names the header and
/// `#[in_messages(value = "created")]` on the variant its value. The generated document carries
/// the pair as a `const` in the message's headers schema.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::HeaderDiscriminator;
///
/// let rule = HeaderDiscriminator::new("event", "created");
/// assert_eq!((rule.header, rule.value), ("event", "created"));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct HeaderDiscriminator {
    /// The header the set reads.
    pub header: &'static str,
    /// The value that names this variant.
    pub value: &'static str,
}

impl HeaderDiscriminator {
    /// Pairs the header a set reads with the value one of its variants declares.
    #[must_use]
    pub const fn new(header: &'static str, value: &'static str) -> Self {
        Self { header, value }
    }
}

/// One model of a message set a handler receives, as declared for the `AsyncAPI` document: the
/// receiving counterpart of [`OutgoingMessageMetadata`].
///
/// `#[derive(InMessages)]` builds one per variant through [`new`](Self::new) plus the
/// builder-style setters; the mount fills the media type in from its codec, and `build_spec`
/// renders each entry as a message of the subscription's channel.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct IncomingMessageMetadata {
    /// Type name of the model, as captured where the set is declared.
    pub message_type: &'static str,
    /// The model's [`Message`](crate::MessageInfo) name.
    pub message_name: Option<&'static str>,
    /// The model's [`Message`](crate::MessageInfo) description.
    pub message_description: Option<&'static str>,
    /// The model's serialized JSON Schema, when available (`asyncapi` feature).
    pub payload_schema: Option<String>,
    /// The serialized JSON Schema of the model's `#[message(headers(..))]` contract, when it
    /// declares one.
    pub headers_schema: Option<String>,
    /// True when the model reads its own bytes ([`Deserialized`](crate::Deserialized)):
    /// no codec decodes it, so the missing schema and media type are by design.
    pub deserialized: bool,
    /// The media type the codec of the mount decodes this model in; `None` for a model that
    /// reads its own bytes.
    pub content_type: Option<&'static str>,
    /// The header value that names this model, when the set reads its variant from a header.
    pub discriminator: Option<HeaderDiscriminator>,
}

impl IncomingMessageMetadata {
    /// Constructs an entry for one model of a set.
    #[must_use]
    pub const fn new(message_type: &'static str) -> Self {
        Self {
            message_type,
            message_name: None,
            message_description: None,
            payload_schema: None,
            headers_schema: None,
            deserialized: false,
            content_type: None,
            discriminator: None,
        }
    }

    /// Builder-style setter for the [`Message`](crate::MessageInfo) name.
    #[must_use]
    pub const fn with_message_name(mut self, name: Option<&'static str>) -> Self {
        self.message_name = name;
        self
    }

    /// Builder-style setter for the [`Message`](crate::MessageInfo) description.
    #[must_use]
    pub const fn with_message_description(mut self, description: Option<&'static str>) -> Self {
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

    /// Builder-style setter for the self-decoding marker (see
    /// [`deserialized`](Self::deserialized)).
    #[must_use]
    pub const fn with_deserialized(mut self, deserialized: bool) -> Self {
        self.deserialized = deserialized;
        self
    }

    /// Builder-style setter for the header value that names this model.
    #[must_use]
    pub const fn with_discriminator(mut self, discriminator: HeaderDiscriminator) -> Self {
        self.discriminator = Some(discriminator);
        self
    }

    /// The copy of this model a retry or a dead-letter destination receives: the delivery, byte
    /// for byte, so a model that reads its own bytes stays schema-free there too.
    fn copied_to(
        &self,
        destination: Cow<'static, str>,
        kind: OutgoingKind,
    ) -> OutgoingMessageMetadata {
        let copy = OutgoingMessageMetadata::new(destination, self.message_type)
            .with_message_name(self.message_name)
            .with_message_description(self.message_description)
            .with_payload_schema(self.payload_schema.clone())
            .with_headers_schema(self.headers_schema.clone())
            .with_serialized(self.deserialized)
            .with_kind(kind);
        match self.discriminator {
            Some(rule) => copy.with_discriminator(rule),
            None => copy,
        }
    }
}

/// What one bound publish position says about itself, read off its policy at mount time.
///
/// The three publish positions - a reply, an [`Out`](crate::Out) slot, the publisher a
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
    /// Reads the description off `policy`, against the broker it pairs with, for the one channel
    /// the position publishes to.
    ///
    /// `channel` is the destination the mount site resolved, which is what the document reports
    /// as the channel's address; a position whose transform names the destination per delivery
    /// reports none, and the mount site's fallback name is what the policy is asked about.
    ///
    /// `names_destination` is what the mount site's transform stack declared: only a position
    /// that names the destination per delivery has a reply address to report, because every
    /// other one publishes to the name the document already carries.
    pub(crate) fn of<C, P>(
        policy: &P,
        content_type: Option<&'static str>,
        names_destination: bool,
        channel: &str,
    ) -> Self
    where
        C: ConnectedBroker,
        P: PublishPolicy<C> + ?Sized,
    {
        let _ = (policy, names_destination, channel);
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
            bindings: PublishBindings::new(
                policy.channel_bindings(channel),
                policy.operation_bindings(channel),
                policy.message_bindings(channel),
            ),
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
    /// `reply("dest")` form, and every entry of an `Out` slot's `#[publishes(..)]`
    /// dictionary. Feeds the `AsyncAPI` `send` operations.
    pub outgoing: Vec<OutgoingMessageMetadata>,
    /// The models of the input's message set (`#[derive(InMessages)]`), one per variant, in
    /// the order of the variants. Empty for an input that is one message, which the fields above
    /// describe. Feeds the messages of the `AsyncAPI` `receive` operation.
    pub incoming: Vec<IncomingMessageMetadata>,
    /// True when the input rides the self-deserializing lane
    /// ([`Deserialized`](crate::Deserialized)): the payload has no serde model, so
    /// the missing schema is by design rather than a documentation gap.
    pub deserialized: bool,
    /// The name of the `AsyncAPI` server this handler's broker was registered under, when the
    /// registration carries one: the label of
    /// [`with_broker_labeled`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.with_broker_labeled). Feeds the
    /// channel's `servers` list, so a multi-broker document says which broker a channel lives
    /// on instead of showing every channel on every server.
    ///
    /// A cross-broker publish is outside what this can answer: a
    /// [`Bound`](crate::Bound) token publishes against its own broker, which the
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
    /// The same metadata on the AsyncAPI server `server`: the label of the broker scope that
    /// mounted the handler.
    #[doc(hidden)]
    #[must_use]
    pub fn at_server(mut self, server: Option<Cow<'static, str>>) -> Self {
        self.server = server;
        self
    }

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
            incoming: Vec::new(),
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
            incoming: Vec::new(),
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
        self.incoming = <Input as DecodeWith<DecodeCodec>>::incoming();
        self.describe_incoming();
        self
    }

    /// The same for a single-delivery input, which materializes from the subscription's
    /// delivery type `M`: a carried value is read by no codec, so it reports none either.
    #[must_use]
    pub(crate) fn materialized_with<Input, Decoder, M>(mut self) -> Self
    where
        Input: Materialize<Decoder, M>,
    {
        self.content_type = <Input as Materialize<Decoder, M>>::CONTENT_TYPE;
        self.incoming = <Input as Materialize<Decoder, M>>::incoming();
        self.describe_incoming();
        self
    }

    /// Writes what the mount knows onto the models of a message set: the media type of its codec
    /// for the models it decodes (a model that reads its own bytes keeps none), and the header
    /// contract of the subscription (a `Headers<T>` parameter), which every model's message
    /// carries next to its own.
    fn describe_incoming(&mut self) {
        for entry in &mut self.incoming {
            entry.content_type = (!entry.deserialized).then_some(self.content_type).flatten();
            #[cfg(feature = "asyncapi")]
            if let Some(shared) = self.headers_schema.as_deref() {
                entry.headers_schema = join_headers(shared, entry.headers_schema.as_deref());
            }
        }
    }

    /// Writes what the reply position's policy says onto the entry the reply declared.
    ///
    /// The policy is asked about the destination that entry names, so a broker whose binding
    /// carries the channel's own name fills it from the mount site rather than from itself.
    pub(crate) fn describe_reply(&mut self, describe: impl Fn(&str) -> PublishDescription) {
        for entry in &mut self.outgoing {
            if entry.kind == OutgoingKind::Answer {
                let description = describe(entry.channel.as_ref());
                description.apply(entry);
            }
        }
    }

    /// Writes what one slot's policy says onto the entries its marker declared, matched by the
    /// destination each entry names.
    ///
    /// A slot's dictionary names several destinations and one policy answers for each of them,
    /// so `described` carries the policy's answer per destination.
    pub(crate) fn describe_slot(&mut self, described: &[(Cow<'static, str>, PublishDescription)]) {
        for entry in &mut self.outgoing {
            if entry.kind == OutgoingKind::SlotEntry
                && let Some((_, description)) = described
                    .iter()
                    .find(|(channel, _)| *channel == entry.channel)
            {
                description.apply(entry);
            }
        }
    }

    /// Writes what the retry publisher's policy says onto the entries that publisher owns: the
    /// dead-letter destination, and the destination a `.to(name)` gave the retry copies.
    ///
    /// Both carry the delivery's own bytes, so the media type stays the one the subscription
    /// decodes rather than anything the retry publisher would encode.
    pub(crate) fn describe_copies(&mut self, describe: impl Fn(&str) -> PublishDescription) {
        let content_type = self.content_type;
        for entry in &mut self.outgoing {
            if matches!(
                entry.kind,
                OutgoingKind::DeadLetter | OutgoingKind::RetryCopy
            ) {
                let description = describe(entry.channel.as_ref());
                description.apply(entry);
                entry.content_type = (!entry.serialized).then_some(content_type).flatten();
            }
        }
    }

    /// Declares a channel this registration republishes a delivery to, under the input's own
    /// name and schema: the copy is the delivery, byte for byte.
    ///
    /// Both destinations a retry can name come through here - where a spent delivery is
    /// dead-lettered, and where a deferred copy is published - so the two are documented alike.
    pub(crate) fn declare_copy(&mut self, destination: Cow<'static, str>, kind: OutgoingKind) {
        // A set's copy carries whichever of its models the delivery was, so the destination lists
        // every one of them.
        if !self.incoming.is_empty() {
            let copies = self
                .incoming
                .iter()
                .map(|model| model.copied_to(destination.clone(), kind));
            self.outgoing.extend(copies);
            return;
        }
        let entry = OutgoingMessageMetadata::new(destination, self.input_type)
            .with_payload_schema(self.payload_schema.clone())
            .with_headers_schema(self.headers_schema.clone())
            .with_kind(kind);
        self.outgoing.push(entry);
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
            bindings: SubscriptionBindings::new(
                source.channel_bindings(),
                source.operation_bindings(),
                source.message_bindings(),
            ),
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

    fn set_of_two() -> Vec<IncomingMessageMetadata> {
        vec![
            IncomingMessageMetadata::new("app::OrderCreated")
                .with_message_name(Some("OrderCreated"))
                .with_message_description(Some("An order was placed."))
                .with_payload_schema(Some("{}".to_owned()))
                .with_headers_schema(Some("{\"type\":\"object\"}".to_owned()))
                .with_discriminator(HeaderDiscriminator::new("event", "created")),
            IncomingMessageMetadata::new("app::OrderCancelled")
                .with_message_name(Some("OrderCancelled"))
                .with_deserialized(true),
        ]
    }

    /// The codec of the mount names the media type of the models it decodes; a model that reads
    /// its own bytes keeps none.
    #[test]
    fn the_mount_describes_the_models_a_codec_decodes() {
        let mut meta = HandlerMetadata::raw("orders");
        meta.content_type = Some("application/json");
        meta.incoming = set_of_two();
        meta.describe_incoming();

        assert_eq!(meta.incoming[0].content_type, Some("application/json"));
        assert_eq!(meta.incoming[1].content_type, None);
        assert_eq!(
            meta.incoming[0].discriminator,
            Some(HeaderDiscriminator::new("event", "created"))
        );
    }

    /// A copy of a set's delivery is whichever of its models arrived, so the destination lists
    /// every model under its own name, and a model that reads its own bytes stays schema-free.
    #[test]
    fn a_copy_of_a_set_lists_every_model() {
        let mut meta = HandlerMetadata::raw("orders");
        meta.content_type = Some("application/json");
        meta.incoming = set_of_two();
        meta.declare_copy(Cow::Borrowed("orders.dlq"), OutgoingKind::DeadLetter);
        meta.describe_copies(|_| PublishDescription::default());

        let copies: Vec<_> = meta
            .outgoing
            .iter()
            .map(|entry| {
                (
                    entry.channel.as_ref(),
                    entry.message_name,
                    entry.serialized,
                    entry.content_type,
                    entry.kind,
                )
            })
            .collect();
        assert_eq!(
            copies,
            vec![
                (
                    "orders.dlq",
                    Some("OrderCreated"),
                    false,
                    Some("application/json"),
                    OutgoingKind::DeadLetter
                ),
                (
                    "orders.dlq",
                    Some("OrderCancelled"),
                    true,
                    None,
                    OutgoingKind::DeadLetter
                ),
            ]
        );
        assert_eq!(meta.outgoing[0].payload_schema.as_deref(), Some("{}"));
        assert_eq!(
            meta.outgoing[0].message_description,
            Some("An order was placed.")
        );
        // The copy of a header-selected model carries the rule the receive side documents.
        assert_eq!(
            meta.outgoing[0].discriminator,
            Some(HeaderDiscriminator::new("event", "created"))
        );
        assert_eq!(meta.outgoing[1].discriminator, None);
    }

    /// The subscription's own header contract (a `Headers<T>` parameter) reaches every model of
    /// a set, joined with the model's contract where it has one.
    #[cfg(feature = "asyncapi")]
    #[test]
    fn the_mount_joins_the_subscription_headers_into_each_model() {
        let mut meta = HandlerMetadata::raw("orders")
            .with_headers_schema("{\"type\":\"object\",\"properties\":{\"tenant\":{\"type\":\"string\"}},\"required\":[\"tenant\"]}");
        meta.incoming = set_of_two();
        meta.describe_incoming();

        let own: serde_json::Value =
            serde_json::from_str(meta.incoming[0].headers_schema.as_deref().unwrap()).unwrap();
        assert_eq!(own["properties"]["tenant"]["type"], "string");
        let none: serde_json::Value =
            serde_json::from_str(meta.incoming[1].headers_schema.as_deref().unwrap()).unwrap();
        assert_eq!(none["required"], serde_json::json!(["tenant"]));
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
