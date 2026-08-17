//! The `include` family on [`BrokerScope`]: mounting macro-generated definitions.
//!
//! `include` is one entry point for every single-message definition form and `include_batch` for
//! both batch forms; which machinery runs is picked by the definition's form token
//! ([`IncludeDef::Form`]), so `b.include(handle)`, `b.include(respond).publisher(..)` and
//! `b.include(forward).publisher(..)` all read the same. Publisher-producing forms return a
//! registration builder that commits when the statement ends; `.publisher(..)` attaches the
//! publish policy (or a [`Bound`](crate::runtime::Bound) token for a cross-broker target).

use std::fmt;
use std::marker::PhantomData;

use serde::Serialize;

use crate::codec::Codec;
// The typed default-reply commits need a default codec, so that import is gated the same way;
// the raw default-reply commit publishes bare bytes and needs only `DefaultPublish`.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
// The default-reply commits build a `TypedPublisher` over the broker's default policy, so those
// imports are gated with the default codec they require.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::Publisher;
use crate::{
    BatchSubscriber, Broker, BuildContext, Connected, DefaultPublish, PublishPolicy, Subscriber,
    SubscriptionSource,
};

use crate::runtime::batch::BatchDef;
use crate::runtime::batch_inject::BatchInjectCall;
use crate::runtime::batch_publishing::BatchPublishingCall;
use crate::runtime::handler::Handler;
use crate::runtime::inject::{FromStartup, InjectCall, InjectHandler};
use crate::runtime::input::{DecodeWith, InputKind, RawBytes};
use crate::runtime::middleware::Layer;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::TypedPublisher;
use crate::runtime::publish::{PublishPipeline, ReplyPublisher};
use crate::runtime::publishing::{PublishingCall, PublishingHandler, ReplySink};
use crate::runtime::slot::{
    BindSlot, BindSlots, HasSlots, InitSlots, IntoSlotSource, MissingSlot, OutSlot, WithSource,
};
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::typed::Typed;
use crate::runtime::{SliceHandler, SourceMessage, SourceSubscriber};

use super::scope::BrokerScope;

/// Ties a definition type to its form token.
///
/// One `include` entry point then dispatches to the right mounting machinery at compile time.
/// Implemented by the `#[subscriber]` macro; a hand-written definition adds it next to its def
/// trait impl.
pub trait IncludeDef {
    /// The form token: one of the markers in [`forms`].
    type Form;
}

/// Form tokens for [`IncludeDef`]: which mounting machinery a definition uses.
pub mod forms {
    /// A plain subscriber (`#[subscriber("in")]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Subscribing;
    /// A raw-bytes subscriber (`#[subscriber("in", raw)]`): no decode, no codec.
    #[derive(Debug, Clone, Copy)]
    pub struct RawSubscribing;
    /// A byte-reply subscriber (`#[subscriber("in", publish_raw("out"))]`, with or without
    /// `raw` on the input side): the reply bytes go out as-is through a bare publisher.
    #[derive(Debug, Clone, Copy)]
    pub struct RawReply;
    /// A reply-publishing subscriber (`#[subscriber("in", publish("out"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Publishing;
    /// A subscriber whose startup injections need publisher attachments.
    ///
    /// The signature carries `Out(out): Out<impl Publisher[, Marker]>` parameters (optionally
    /// next to a `Seek` one), so the include site chains `.publisher(..)` (single slot) or
    /// `.out(marker, ..)` per named slot.
    #[derive(Debug, Clone, Copy)]
    pub struct Out;
    /// A subscriber whose startup injections need nothing from the include site.
    ///
    /// The signature carries a `Seek(seeker): Seek<K>` parameter (and no `Out`).
    #[derive(Debug, Clone, Copy)]
    pub struct Seek;
    /// A reply-publishing subscriber whose handler also takes `Out` parameters, so the
    /// include site chains `.out(marker, ..)` per slot next to the (optional)
    /// `.publisher(..)`.
    #[derive(Debug, Clone, Copy)]
    pub struct PublishingOut;
    /// A byte-reply subscriber whose handler also takes an `Out` parameter.
    #[derive(Debug, Clone, Copy)]
    pub struct RawReplyOut;
    /// A batch subscriber (`#[subscriber(batch("in"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Batch;
    /// A batch reply-publishing subscriber (`#[subscriber(batch("in"), publish("out"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct BatchPublishing;
    /// A batch subscriber whose startup injections need a publisher attachment (an `Out`
    /// parameter, optionally next to a `Seek` one).
    #[derive(Debug, Clone, Copy)]
    pub struct BatchOut;
    /// A batch subscriber whose startup injections need nothing from the include site (a
    /// `Seek` parameter and no `Out`).
    #[derive(Debug, Clone, Copy)]
    pub struct BatchSeek;
    /// A batch reply-publishing subscriber whose handler also takes `Out` parameters, so the
    /// include site chains `.out(marker, ..)` per slot next to the (optional)
    /// `.publisher(..)`.
    #[derive(Debug, Clone, Copy)]
    pub struct BatchPublishingOut;
}

/// Form-token dispatch for [`BrokerScope::include`]: implemented by the tokens in [`forms`],
/// generic over the definition and the scope. Machinery; you never implement or name it.
#[doc(hidden)]
pub trait IncludeMount<'s, B: Broker, Layers, C, State, Pipeline, Def> {
    /// What `include` hands back: `()` for eager forms, a registration builder for the
    /// publisher-producing ones.
    type Out;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out;
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a single-message `#[subscriber]` definition: a plain handler mounts eagerly, a
    /// `publish("dest")` or `Out`-taking handler returns a registration builder that commits
    /// at the end of the statement; chain [`publisher`](IncludePublishing::publisher) on it to
    /// attach the publish policy.
    ///
    /// Decoding uses the scope codec when one was set
    /// ([`with_broker_codec`](crate::runtime::RustStream::with_broker_codec)), else the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    pub fn include<'s, Def>(
        &'s mut self,
        def: Def,
    ) -> <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::Out
    where
        Def: IncludeDef,
        Def::Form: IncludeMount<'s, B, Layers, C, State, Pipeline, Def>,
    {
        <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::begin(def, self)
    }

    /// Mounts a batch `#[subscriber(batch(..))]` definition; the `publish("dest")` form returns
    /// a registration builder, exactly like [`include`](Self::include).
    pub fn include_batch<'s, Def>(
        &'s mut self,
        def: Def,
    ) -> <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::Out
    where
        Def: IncludeDef,
        Def::Form: IncludeMount<'s, B, Layers, C, State, Pipeline, Def>,
    {
        <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::begin(def, self)
    }
}

/// The codec a scope decodes with: the scope's own codec when one was set, else the default.
/// Machinery behind `include`; the two impls mirror the two `with_broker` forms.
#[doc(hidden)]
pub trait ScopeCodec {
    type Codec: Codec + Clone + Send + Sync + 'static;
    fn scope_codec(&self) -> Self::Codec;
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl ScopeCodec for () {
    type Codec = DefaultCodec;
    fn scope_codec(&self) -> Self::Codec {
        DefaultCodec::default()
    }
}

impl<C: Codec + Clone + Send + Sync + 'static> ScopeCodec for C {
    type Codec = C;
    fn scope_codec(&self) -> Self::Codec {
        self.clone()
    }
}

// ---------------------------------------------------------------------------------------------
// Plain subscribing: eager, no builder.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Subscribing
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: SubscriberDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message: 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Handler: 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
        Typed<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Input,
            C::Codec,
            Def::Handler,
        >,
    >,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let codec = scope.codec.scope_codec();
        scope.mount_subscriber(source, def, codec);
    }
}

// ---------------------------------------------------------------------------------------------
// Raw subscribing: eager, no builder, and no codec anywhere - the byte input kind decodes with
// `()`, so the scope codec parameter `C` is left unconstrained and the mount works without any
// codec feature.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawSubscribing
where
    B: Broker + 'static,
    Def: SubscriberDef<Input = RawBytes>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message: 'static,
    Def::Handler: 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
        Typed<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            RawBytes,
            (),
            Def::Handler,
        >,
    >,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_subscriber(source, def, ());
    }
}

// ---------------------------------------------------------------------------------------------
// Attachment-free injections: eager, no builder - a definition whose startup injections need
// nothing from the include site (a Seek parameter without an Out one) resolves against the
// subscription itself.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Seek
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: InjectCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<InjectHandler<Def, <C as ScopeCodec>::Codec>> + Clone + Send + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_inject(source, def, ((),));
    }
}

// ---------------------------------------------------------------------------------------------
// Plain batch: eager, no builder.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Batch
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: BatchDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Handler: SliceHandler<<Def::Input as InputKind>::Owned, State> + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let codec = scope.codec.scope_codec();
        scope.mount_batch(source, def, codec);
    }
}

// ---------------------------------------------------------------------------------------------
// Builder-producing forms: reply publishing, out injection, and their batch counterparts.
//
// The builder commits on Drop, so `b.include(def)` alone still registers (with the broker's
// default publish policy where one exists), while `b.include(def).publisher(src)` replaces the
// commit with the attached source. User sources are wrapped in `WithSource` so the default
// marker and the source-driven commit live on different type constructors (disjoint impls, no
// negative reasoning needed).
//
// Every form family shares one commit trait, keyed by a mount token: strategies of different
// families are impls on the same attachment types with different concrete tokens, so they
// never overlap without negative reasoning. Two generic builders then serve every family -
// [`IncludeWith`] (one attachment, replaced by `.publisher(..)`) and [`IncludeWithOut`] (a
// reply attachment plus the `Out` parameter's own `.out(..)`) - and the per-form names are
// aliases picking the token and the initial attachment.

/// The default reply commit: the broker's default publish policy under the default codec.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultReply;

/// The default commit of the byte-reply form: the broker's plain publish policy taken bare.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultBareReply;

/// The mount tokens keying [`CommitVia`]: which mount a committed attachment drives.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PublishMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct InjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchInjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PublishInjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishInjectMount;

/// One commit strategy of a registration builder, keyed by its `Mount` token. Machinery;
/// never named directly.
#[doc(hidden)]
pub trait CommitVia<Mount, B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def> CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>
    for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>:
        CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The typed default reply: the broker's plain publish policy under the default codec,
        // committed as if the user had chained `.publisher(TypedPublisher::new(<policy>))`.
        CommitVia::commit(
            WithSource::new(TypedPublisher::new(
                <B::Connected as DefaultPublish>::Policy::default(),
            )),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def> CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>
    for DefaultBareReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<<B::Connected as DefaultPublish>::Policy>:
        CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        CommitVia::commit(
            WithSource::new(<B::Connected as DefaultPublish>::Policy::default()),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source>
    CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: PublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    Def::Reply: Send + Sync + 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Source: PublishPolicy<Connected<B>> + Send + 'static,
    Source::Live: ReplySink<Def::Reply, Def::Context, Pipeline> + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<PublishingHandler<Def, <C as ScopeCodec>::Codec, Source::Live, Pipeline>>
        + Clone
        + Send
        + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_publishing_source(source, def, self.into_source(), ((),));
    }
}

/// A registration builder over one attachment, generic over its mount token.
///
/// Commits when dropped (the end of the `b.include(..)` / `b.include_batch(..)` statement);
/// [`publisher`](Self::publisher) replaces the reply source (defaulted when the call is
/// omitted). The per-form names are aliases: [`IncludePublishing`],
/// [`IncludeBatchPublishing`].
pub struct IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    // Options only so `publisher` can move the pieces into the replacement builder out of a
    // Drop type; both stay `Some` until the commit or that replacement.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Attachment)>,
    _mount: PhantomData<Mount>,
}

/// The builder [`BrokerScope::include`] returns for a `publish("dest")` definition: the
/// attachment is the reply source, defaulting to the broker's default publish policy under
/// the default codec.
pub type IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, Source> =
    IncludeWith<'s, PublishMount, B, Layers, C, State, Pipeline, Def, Source>;

/// The builder [`BrokerScope::include`] returns for a handler with
/// [`Out`](crate::runtime::Out) parameters: the attachment is the slot tuple, with no
/// defaults.
pub type IncludeOut<'s, B, Layers, C, State, Pipeline, Def, Slots> =
    IncludeSlots<'s, InjectMount, B, Layers, C, State, Pipeline, Def, Slots>;

/// The builder [`BrokerScope::include_batch`] returns for a `batch(.., publish("dest"))`
/// definition.
///
/// The attachment is the batch reply source: a typed stack, or its
/// [`transactional`](TypedPublisher::transactional) form for one transaction per batch.
pub type IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, Def, Source> =
    IncludeWith<'s, BatchPublishMount, B, Layers, C, State, Pipeline, Def, Source>;

/// The builder [`BrokerScope::include_batch`] returns for a batch handler with
/// [`Out`](crate::runtime::Out) parameters.
pub type IncludeBatchOut<'s, B, Layers, C, State, Pipeline, Def, Slots> =
    IncludeSlots<'s, BatchInjectMount, B, Layers, C, State, Pipeline, Def, Slots>;

impl<'s, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
    IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    /// Attaches the reply source: a [`TypedPublisher`] stack naming the reply codec and
    /// transforms, a bare policy on the byte-reply form, or a [`Bound`](crate::runtime::Bound)
    /// token wrapping one for a cross-broker target. The runtime pairs it after the brokers
    /// connect.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<NewSource>(
        mut self,
        source: NewSource,
    ) -> IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, WithSource<NewSource>>
    where
        WithSource<NewSource>: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, _default) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, WithSource::new(source))),
            _mount: PhantomData,
        }
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Attachment> fmt::Debug
    for IncludeWith<'_, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncludeWith").finish_non_exhaustive()
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Attachment> Drop
    for IncludeWith<'_, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
        }
    }
}

/// The commit of a fully-bound slot registration, keyed by its `Mount` token. Machinery behind
/// [`IncludeSlots::mount`]; implemented only for attachment tuples with every position bound,
/// which is what turns a forgotten `.out(marker, policy)` into a compile error naming the slot
/// (the unbound position shows as `MissingSlot<TheMarker>` in `{Self}`).
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "not every Out slot of this handler is bound",
    label = "the attachment still contains a `MissingSlot<..>` naming the unbound slot",
    note = "bind each remaining slot with .out(marker, policy) before .mount()"
)]
pub trait SlotCommit<Mount, B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

/// A registration builder for a handler with [`Out`](crate::runtime::Out) slots.
///
/// Unlike the reply builders, it does not commit on drop: each [`out`](Self::out) call binds
/// one named slot (in any order), and the terminal [`mount`](Self::mount) commits - it exists
/// only once every slot is bound, so a forgotten binding is a compile error naming the slot. A
/// handler with a single slot skips the ceremony: [`publisher`](Self::publisher) binds it and
/// commits in one call. The per-form names are aliases: [`IncludeOut`], [`IncludeBatchOut`].
#[must_use = "an Out handler registers nothing until .publisher(policy) or .out(..)+.mount() commits it"]
pub struct IncludeSlots<'s, Mount, B, Layers, C, State, Pipeline, Def, Slots>
where
    B: Broker + 'static,
{
    // Options only so the binding methods can move the pieces into the next state out of a
    // Drop type; both stay `Some` until the commit consumes them.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Slots)>,
    _mount: PhantomData<Mount>,
}

impl<'s, Mount, B, Layers, C, State, Pipeline, Def, Slots>
    IncludeSlots<'s, Mount, B, Layers, C, State, Pipeline, Def, Slots>
where
    B: Broker + 'static,
{
    fn new(
        def: Def,
        slots: Slots,
        scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) -> Self {
        Self {
            scope: Some(scope),
            parts: Some((def, slots)),
            _mount: PhantomData,
        }
    }

    fn take(
        mut self,
    ) -> (
        Def,
        Slots,
        &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) {
        let (def, slots) = self
            .parts
            .take()
            .expect("builder parts are present until the commit consumes them");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until the commit consumes them");
        (def, slots, scope)
    }

    /// Binds one named [`Out`](crate::runtime::Out) slot: `marker` picks the slot (the second
    /// type argument of the handler's `Out<impl Publisher, Marker>` parameter) and `source` is
    /// its publish policy (or a [`Bound`](crate::runtime::Bound) token for a cross-broker
    /// target). Calls bind by marker, so their order does not matter; binding the same slot
    /// twice, or a marker the handler does not declare, fails to compile. Finish with
    /// [`mount`](Self::mount).
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit consumes them.
    // The marker travels by value on purpose (a unit inference driver keeps the call site
    // `.out(Encoded, ..)`), and the return type is the builder itself with the bound slot - an
    // alias would only hide which position changed.
    #[allow(clippy::needless_pass_by_value, clippy::type_complexity)]
    pub fn out<M, NewSource, Index>(
        self,
        marker: M,
        source: NewSource,
    ) -> IncludeSlots<
        's,
        Mount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        <Slots as BindSlot<M, NewSource, Index>>::Out,
    >
    where
        M: OutSlot,
        Slots: BindSlot<M, NewSource, Index>,
    {
        // The marker is inference input only; its value carries no data.
        let _ = marker;
        let (def, slots, scope) = self.take();
        IncludeSlots::new(def, slots.bind(source), scope)
    }

    /// Commits the registration. Exists only once every slot is bound: a chain that still has
    /// a `MissingSlot<..>` in its attachment fails to compile here, naming the slot.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until this
    /// commit consumes them.
    pub fn mount(self)
    where
        Slots: SlotCommit<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, slots, scope) = self.take();
        slots.commit(def, scope);
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, M>
    IncludeSlots<'_, Mount, B, Layers, C, State, Pipeline, Def, (MissingSlot<M>,)>
where
    B: Broker + 'static,
{
    /// Binds the handler's single [`Out`](crate::runtime::Out) slot and commits, no
    /// [`mount`](Self::mount) needed: the one-slot shorthand
    /// (`b.include(forward).publisher(MemoryPublish)`).
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until this
    /// commit consumes them.
    pub fn publisher<NewSource>(self, source: NewSource)
    where
        (WithSource<NewSource>,): SlotCommit<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, _missing, scope) = self.take();
        (WithSource::new(source),).commit(def, scope);
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Slots> fmt::Debug
    for IncludeSlots<'_, Mount, B, Layers, C, State, Pipeline, Def, Slots>
where
    B: Broker + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncludeSlots").finish_non_exhaustive()
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Slots> Drop
    for IncludeSlots<'_, Mount, B, Layers, C, State, Pipeline, Def, Slots>
where
    B: Broker + 'static,
{
    fn drop(&mut self) {
        // A build-time assert, like the on_startup ordering check: the compiler already warns
        // through must_use, but a deliberately discarded incomplete registration must not
        // silently vanish - the handler would never consume.
        assert!(
            self.parts.is_none(),
            "an Out handler was included but never mounted: finish the chain with .mount() \
             (or .publisher(policy) for a single slot)",
        );
    }
}

/// A registration builder for a publishing handler that also takes
/// [`Out`](crate::runtime::Out) slots: the reply attachment next to the slot tuple.
///
/// The reply side defaults like [`IncludeWith`] (override with
/// [`publisher`](Self::publisher)); each slot binds with [`out`](Self::out), and the terminal
/// [`mount`](Self::mount) commits - it exists only once every slot is bound, so a forgotten
/// binding is a compile error naming the slot. The per-form names are aliases:
/// [`IncludePublishingOut`], [`IncludeBatchPublishingOut`].
#[must_use = "a publishing handler with Out slots registers nothing until .out(..)+.mount() commits it"]
pub struct IncludeSlotsWithReply<'s, Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Reply, Slots)>,
    _mount: PhantomData<Mount>,
}

/// The builder [`BrokerScope::include`] returns for a `publish("dest")` /
/// `publish_raw("dest")` definition whose handler also takes
/// [`Out`](crate::runtime::Out) parameters.
pub type IncludePublishingOut<'s, B, Layers, C, State, Pipeline, Def, Reply, Slots> =
    IncludeSlotsWithReply<'s, PublishInjectMount, B, Layers, C, State, Pipeline, Def, Reply, Slots>;

/// The builder [`BrokerScope::include_batch`] returns for a `batch(.., publish("dest"))`
/// definition whose handler also takes [`Out`](crate::runtime::Out) parameters.
pub type IncludeBatchPublishingOut<'s, B, Layers, C, State, Pipeline, Def, Reply, Slots> =
    IncludeSlotsWithReply<
        's,
        BatchPublishInjectMount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        Reply,
        Slots,
    >;

impl<'s, Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots>
    IncludeSlotsWithReply<'s, Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    fn new(
        def: Def,
        reply: Reply,
        slots: Slots,
        scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) -> Self {
        Self {
            scope: Some(scope),
            parts: Some((def, reply, slots)),
            _mount: PhantomData,
        }
    }

    #[allow(clippy::type_complexity)] // the builder's own pieces; an alias would hide them
    fn take(
        mut self,
    ) -> (
        Def,
        Reply,
        Slots,
        &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) {
        let (def, reply, slots) = self
            .parts
            .take()
            .expect("builder parts are present until the commit consumes them");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until the commit consumes them");
        (def, reply, slots, scope)
    }

    /// Attaches the reply source, like [`IncludeWith::publisher`].
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit consumes them.
    pub fn publisher<NewSource>(
        self,
        source: NewSource,
    ) -> IncludeSlotsWithReply<
        's,
        Mount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        WithSource<NewSource>,
        Slots,
    > {
        let (def, _default, slots, scope) = self.take();
        IncludeSlotsWithReply::new(def, WithSource::new(source), slots, scope)
    }

    /// Binds one named [`Out`](crate::runtime::Out) slot, like [`IncludeSlots::out`]: by
    /// marker, in any order, next to the (optional) reply-side
    /// [`publisher`](Self::publisher). Finish with [`mount`](Self::mount).
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit consumes them.
    // See `IncludeSlots::out` for why the marker is by value and the return type stays spelled.
    #[allow(clippy::needless_pass_by_value, clippy::type_complexity)]
    pub fn out<M, NewSource, Index>(
        self,
        marker: M,
        source: NewSource,
    ) -> IncludeSlotsWithReply<
        's,
        Mount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        Reply,
        <Slots as BindSlot<M, NewSource, Index>>::Out,
    >
    where
        M: OutSlot,
        Slots: BindSlot<M, NewSource, Index>,
    {
        // The marker is inference input only; its value carries no data.
        let _ = marker;
        let (def, reply, slots, scope) = self.take();
        IncludeSlotsWithReply::new(def, reply, slots.bind(source), scope)
    }

    /// Commits the registration (reply attachment plus every bound slot). Exists only once
    /// every slot is bound: a chain that still has a `MissingSlot<..>` in its attachment fails
    /// to compile here, naming the slot.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until this
    /// commit consumes them.
    pub fn mount(self)
    where
        (Reply, Slots): SlotCommit<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, reply, slots, scope) = self.take();
        (reply, slots).commit(def, scope);
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots> fmt::Debug
    for IncludeSlotsWithReply<'_, Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncludeSlotsWithReply")
            .finish_non_exhaustive()
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots> Drop
    for IncludeSlotsWithReply<'_, Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    fn drop(&mut self) {
        // See `IncludeSlots`'s drop: a build-time assert against a discarded registration.
        assert!(
            self.parts.is_none(),
            "a publishing handler with Out slots was included but never mounted: finish the \
             chain with .mount()",
        );
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Publishing
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultReply: CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, DefaultReply)),
            _mount: PhantomData,
        }
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawReply
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultBareReply: CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultBareReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, DefaultBareReply)),
            _mount: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Reply publishing with Out slots: two attachment axes on one builder. The reply side keeps its
// default commits (typed or bare policy); the slot side starts all-unbound, each
// `.out(marker, ..)` binds one position, and the SlotCommit impls exist only for fully-bound
// tuples - so `.mount()` on an incomplete chain is a compile error naming the slot.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def, Slots>
    SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def> for (DefaultReply, Slots)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (
        WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>,
        Slots,
    ): SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The typed default reply, as if the user had chained `.publisher(..)` themselves.
        SlotCommit::commit(
            (
                WithSource::new(TypedPublisher::new(
                    <B::Connected as DefaultPublish>::Policy::default(),
                )),
                self.1,
            ),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def, Slots>
    SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def> for (DefaultBareReply, Slots)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (WithSource<<B::Connected as DefaultPublish>::Policy>, Slots):
        SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        SlotCommit::commit(
            (
                WithSource::new(<B::Connected as DefaultPublish>::Policy::default()),
                self.1,
            ),
            def,
            scope,
        );
    }
}

/// Implements the slot-tuple commit of the publishing-with-Out forms for each slot arity, for
/// fully-bound tuples only: the bound sources instantiate the definition ([`BindSlots`],
/// named `Bound` / `Extra` here), the reply source pairs at startup, and the injections
/// resolve against the slot extras.
macro_rules! impl_publishing_out_commit {
    ($(($($attach:ident),+))+) => {$(
        impl<B, Layers, C, State, Pipeline, Def, Source, Bound, Extra, $($attach),+>
            SlotCommit<PublishInjectMount, B, Layers, C, State, Pipeline, Def>
            for (WithSource<Source>, ($(WithSource<$attach>,)+))
        where
            B: Broker + 'static,
            C: ScopeCodec,
            Def: BindSlots<Connected<B>, ($(($attach, C::Codec),)+), Bound = Bound, Extra = Extra>,
            Bound: PublishingCall<State> + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: Sync + Send + 'static,
            SourceMessage<B, Bound::Source>: Send + Sync + 'static,
            Bound::Input: DecodeWith<C::Codec>,
            Bound::Injections: FromStartup<B, SourceSubscriber<B, Bound::Source>, Extra>
                + Send
                + Sync
                + 'static,
            Bound::Reply: Send + Sync + 'static,
            Bound::Context: BuildContext<SourceMessage<B, Bound::Source>> + Send + Sync + 'static,
            Source: PublishPolicy<Connected<B>> + Send + 'static,
            Source::Live: ReplySink<Bound::Reply, Bound::Context, Pipeline> + 'static,
            Extra: Send + Sync + 'static,
            Pipeline: PublishPipeline + Clone + Send + 'static,
            State: Send + Sync + 'static,
            Layers: Layer<PublishingHandler<Bound, C::Codec, Source::Live, Pipeline>>
                + Clone
                + Send
                + 'static,
            Layers::Handler:
                Handler<SourceMessage<B, Bound::Source>, Bound::Context, State> + 'static,
        {
            fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                let codec = scope.codec.scope_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                scope.mount_publishing_source(source, def, reply.into_source(), extra);
            }
        }
    )+};
}

impl_publishing_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::PublishingOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: HasSlots,
    Def::Markers: InitSlots,
{
    type Out = IncludePublishingOut<
        's,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        DefaultReply,
        <Def::Markers as InitSlots>::Init,
    >;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeSlotsWithReply::new(
            def,
            DefaultReply,
            <Def::Markers as InitSlots>::init(),
            scope,
        )
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawReplyOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: HasSlots,
    Def::Markers: InitSlots,
{
    type Out = IncludePublishingOut<
        's,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        DefaultBareReply,
        <Def::Markers as InitSlots>::Init,
    >;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeSlotsWithReply::new(
            def,
            DefaultBareReply,
            <Def::Markers as InitSlots>::init(),
            scope,
        )
    }
}

// ---------------------------------------------------------------------------------------------
// Out injection: the attachment is a positional slot tuple, one element per marker, starting
// all-unbound. `.publisher(..)` is the single-slot shorthand (it binds and commits in one
// call); `.out(marker, ..)` binds one named position and `.mount()` commits - it compiles only
// once every position is bound.

/// Implements the slot-tuple commit of the plain Out form for each slot arity, for fully-bound
/// tuples only. `Bound` / `Extra` name the definition's [`BindSlots`] outputs so the bounds
/// read flat instead of through `<Def::Bound as ..>` projections.
macro_rules! impl_inject_out_commit {
    ($(($($attach:ident),+))+) => {$(
        impl<B, Layers, C, State, Pipeline, Def, Bound, Extra, $($attach),+>
            SlotCommit<InjectMount, B, Layers, C, State, Pipeline, Def>
            for ($(WithSource<$attach>,)+)
        where
            B: Broker + 'static,
            C: ScopeCodec,
            Def: BindSlots<Connected<B>, ($(($attach, C::Codec),)+), Bound = Bound, Extra = Extra>,
            Bound: InjectCall<State> + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: Sync + Send + 'static,
            SourceMessage<B, Bound::Source>: Send + Sync + 'static,
            Bound::Input: DecodeWith<C::Codec>,
            Bound::Context: BuildContext<SourceMessage<B, Bound::Source>> + Send + Sync + 'static,
            Bound::Injections: FromStartup<B, SourceSubscriber<B, Bound::Source>, Extra>
                + Send
                + Sync
                + 'static,
            Extra: Send + Sync + 'static,
            State: Send + Sync + 'static,
            Layers: Layer<InjectHandler<Bound, C::Codec>> + Clone + Send + 'static,
            Layers::Handler:
                Handler<SourceMessage<B, Bound::Source>, Bound::Context, State> + 'static,
        {
            fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
                #[allow(non_snake_case)]
                let ($($attach,)+) = self;
                let codec = scope.codec.scope_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                scope.mount_inject(source, def, extra);
            }
        }
    )+};
}

impl_inject_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Out
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: HasSlots,
    Def::Markers: InitSlots,
{
    type Out =
        IncludeOut<'s, B, Layers, C, State, Pipeline, Def, <Def::Markers as InitSlots>::Init>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeSlots::new(def, <Def::Markers as InitSlots>::init(), scope)
    }
}

// ---------------------------------------------------------------------------------------------
// Batch injections: the batch counterparts of the Seek (eager) and Out (builder) forms.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchSeek
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: BatchInjectCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_batch_inject(source, def, ((),));
    }
}

/// Implements the slot-tuple commit of the batch Out form for each slot arity, for fully-bound
/// tuples only. `Bound` / `Extra` name the definition's [`BindSlots`] outputs.
macro_rules! impl_batch_inject_out_commit {
    ($(($($attach:ident),+))+) => {$(
        impl<B, Layers, C, State, Pipeline, Def, Bound, Extra, $($attach),+>
            SlotCommit<BatchInjectMount, B, Layers, C, State, Pipeline, Def>
            for ($(WithSource<$attach>,)+)
        where
            B: Broker + 'static,
            C: ScopeCodec,
            Def: BindSlots<Connected<B>, ($(($attach, C::Codec),)+), Bound = Bound, Extra = Extra>,
            Bound: BatchInjectCall<State> + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + Sync + Send + 'static,
            SourceMessage<B, Bound::Source>: Send + 'static,
            Bound::Input: DecodeWith<C::Codec>,
            Bound::Injections: FromStartup<B, SourceSubscriber<B, Bound::Source>, Extra>
                + Send
                + Sync
                + 'static,
            Extra: Send + Sync + 'static,
            State: Send + Sync + 'static,
        {
            fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
                #[allow(non_snake_case)]
                let ($($attach,)+) = self;
                let codec = scope.codec.scope_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                scope.mount_batch_inject(source, def, extra);
            }
        }
    )+};
}

impl_batch_inject_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: HasSlots,
    Def::Markers: InitSlots,
{
    type Out =
        IncludeBatchOut<'s, B, Layers, C, State, Pipeline, Def, <Def::Markers as InitSlots>::Init>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeSlots::new(def, <Def::Markers as InitSlots>::init(), scope)
    }
}

// ---------------------------------------------------------------------------------------------
// Batch publishing: the same builder shape; the reply source pairs into a ReplyPublisher.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def>
    CommitVia<BatchPublishMount, B, Layers, C, State, Pipeline, Def> for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    C: ScopeCodec,
    Def: BatchPublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live:
        Publisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let reply = TypedPublisher::new(<B::Connected as DefaultPublish>::Policy::default());
        scope.mount_batch_publishing_source(source, def, reply, ((),));
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source, BatchReply>
    CommitVia<BatchPublishMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: BatchPublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ((),)>
        + Send
        + Sync
        + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    Source: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
    BatchReply: ReplyPublisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_batch_publishing_source(source, def, self.into_source(), ((),));
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchPublishing
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultReply: CommitVia<BatchPublishMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, DefaultReply)),
            _mount: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Batch publishing with Out slots: the two-attachment builder at the batch shape.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def, Slots>
    SlotCommit<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>
    for (DefaultReply, Slots)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (
        WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>,
        Slots,
    ): SlotCommit<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The typed default reply, as if the user had chained `.publisher(..)` themselves.
        SlotCommit::commit(
            (
                WithSource::new(TypedPublisher::new(
                    <B::Connected as DefaultPublish>::Policy::default(),
                )),
                self.1,
            ),
            def,
            scope,
        );
    }
}

/// Implements the slot-tuple commit of the batch-publishing-with-Out form for each slot arity,
/// for fully-bound tuples only. `Bound` / `Extra` name the definition's [`BindSlots`] outputs.
macro_rules! impl_batch_publishing_out_commit {
    ($(($($attach:ident),+))+) => {$(
        impl<B, Layers, C, State, Pipeline, Def, Source, BatchReply, Bound, Extra, $($attach),+>
            SlotCommit<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>
            for (WithSource<Source>, ($(WithSource<$attach>,)+))
        where
            B: Broker + 'static,
            C: ScopeCodec,
            Def: BindSlots<Connected<B>, ($(($attach, C::Codec),)+), Bound = Bound, Extra = Extra>,
            Bound: BatchPublishingCall<State> + 'static,
            Bound::Source: SubscriptionSource<Connected<B>> + Send + 'static,
            SourceSubscriber<B, Bound::Source>: BatchSubscriber + Sync + Send + 'static,
            SourceMessage<B, Bound::Source>: Send + 'static,
            Bound::Input: DecodeWith<C::Codec>,
            Bound::Injections: FromStartup<B, SourceSubscriber<B, Bound::Source>, Extra>
                + Send
                + Sync
                + 'static,
            Bound::Reply: Serialize + Send + Sync + 'static,
            Source: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
            BatchReply: ReplyPublisher + 'static,
            Extra: Send + Sync + 'static,
            Pipeline: PublishPipeline + Clone + Send + 'static,
            State: Send + Sync + 'static,
        {
            fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
                let (reply, slots) = self;
                #[allow(non_snake_case)]
                let ($($attach,)+) = slots;
                let codec = scope.codec.scope_codec();
                let (def, extra) = def.bind(($(($attach.into_source(), codec.clone()),)+));
                let source = def.source();
                scope.mount_batch_publishing_source(source, def, reply.into_source(), extra);
            }
        }
    )+};
}

impl_batch_publishing_out_commit! {
    (A0)
    (A0, A1)
    (A0, A1, A2)
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchPublishingOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: HasSlots,
    Def::Markers: InitSlots,
{
    type Out = IncludeBatchPublishingOut<
        's,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        DefaultReply,
        <Def::Markers as InitSlots>::Init,
    >;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeSlotsWithReply::new(
            def,
            DefaultReply,
            <Def::Markers as InitSlots>::init(),
            scope,
        )
    }
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a plain `#[subscriber]` definition on an explicit subscription `source`
    /// (overriding the macro's own source), decoding with the scope codec (or the default).
    pub fn include_on<Source, Def>(&mut self, source: Source, def: Def)
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        <Source::Subscriber as Subscriber>::Message: 'static,
        C: ScopeCodec,
        Def: SubscriberDef,
        Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
        Def::Handler: 'static,
        Def::Context: BuildContext<<Source::Subscriber as Subscriber>::Message> + Send + 'static,
        State: Send + Sync + 'static,
        Layers: Layer<
            Typed<<Source::Subscriber as Subscriber>::Message, Def::Input, C::Codec, Def::Handler>,
        >,
        Layers::Handler:
            Handler<<Source::Subscriber as Subscriber>::Message, Def::Context, State> + 'static,
    {
        let codec = self.codec.scope_codec();
        self.mount_subscriber(source, def, codec);
    }

    /// Mounts a `#[subscriber(batch(..))]` definition on an explicit subscription `source`,
    /// decoding each element with the scope codec (or the default).
    pub fn include_batch_on<Source, Def>(&mut self, source: Source, def: Def)
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        C: ScopeCodec,
        Def: BatchDef,
        Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
        Def::Handler: SliceHandler<<Def::Input as InputKind>::Owned, State> + 'static,
        State: Send + Sync + 'static,
    {
        let codec = self.codec.scope_codec();
        self.mount_batch(source, def, codec);
    }
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a `publish("dest")` definition on an explicit subscription `source`, replying
    /// through `publisher` (a typed policy stack, or a [`Bound`](crate::runtime::Bound) token
    /// wrapping one). The positional counterpart of `include(def).publisher(..)` for the
    /// source-override case.
    pub fn include_publishing_on<Source, Def, ReplySource>(
        &mut self,
        source: Source,
        def: Def,
        publisher: ReplySource,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Sync + Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + Sync + 'static,
        C: ScopeCodec,
        Def: PublishingCall<State> + 'static,
        Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
        Def::Injections: FromStartup<B, Source::Subscriber, ((),)> + Send + Sync + 'static,
        Def::Reply: Send + Sync + 'static,
        Def::Context:
            BuildContext<<Source::Subscriber as Subscriber>::Message> + Send + Sync + 'static,
        ReplySource: PublishPolicy<Connected<B>> + Send + 'static,
        ReplySource::Live: ReplySink<Def::Reply, Def::Context, Pipeline> + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
        Layers: Layer<PublishingHandler<Def, C::Codec, ReplySource::Live, Pipeline>>
            + Clone
            + Send
            + 'static,
        Layers::Handler:
            Handler<<Source::Subscriber as Subscriber>::Message, Def::Context, State> + 'static,
    {
        self.mount_publishing_source(source, def, publisher, ((),));
    }

    /// Mounts a `batch(.., publish("dest"))` definition on an explicit subscription `source`,
    /// replying through `publisher` (a typed policy stack, its transactional form, or a
    /// [`Bound`](crate::runtime::Bound) token wrapping either).
    pub fn include_batch_publishing_on<Source, Def, ReplySource, BatchReply>(
        &mut self,
        source: Source,
        def: Def,
        publisher: ReplySource,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Sync + Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + 'static,
        C: ScopeCodec,
        Def: BatchPublishingCall<State> + 'static,
        Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
        Def::Injections: FromStartup<B, Source::Subscriber, ((),)> + Send + Sync + 'static,
        Def::Reply: Serialize + Send + Sync + 'static,
        ReplySource: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
        BatchReply: ReplyPublisher + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
    {
        self.mount_batch_publishing_source(source, def, publisher, ((),));
    }
}
