//! Marker-identified `Out` slots: the type-level machinery pairing each
//! [`Out`](super::Out) parameter with the publish policy attached at the include site.
//!
//! A handler declares `Out(out): Out<impl Publisher, MySlot>`; the marker (`MySlot`, or the
//! implicit [`DefaultSlot`]) identifies the slot, and the concrete publisher type is inferred
//! from the attachment's [`PublishPolicy::Live`](crate::PublishPolicy::Live) - fully monomorphized, no erasure. The pieces:
//!
//! - [`OutSlot`] / [`DefaultSlot`]: the marker vocabulary (`#[derive(OutSlot)]` for named ones).
//! - [`PublishedThrough`]: membership in a marker's `#[publishes(..)]` dictionary, so what the
//!   document reports as leaving a slot is exactly what the publish builder admits.
//! - [`SlotPublisher`]: the transparent wrapper the handler actually receives; it delegates the
//!   publisher capabilities and, under the `testing` feature, attributes publishes to the slot.
//! - [`HasSlots`] / [`BindSlots`]: the macro-implemented contract on a definition (the marker
//!   list, and the instantiation of the publisher-generic definition from the bound sources).
//! - [`BindSlot`] / [`InitSlots`] / [`MissingSlot`]: the include-site builder machinery that
//!   places each `.out(marker, policy)` attachment into its marker's position, in any order;
//!   a registration commits only when no `MissingSlot` remains, so a forgotten binding is a
//!   compile error naming the slot.

use std::marker::PhantomData;
use std::time::Duration;

use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publish::{
    AddBatchReplyTransform, AddReplyTransform, CallCodec, CodecSlotOpen, LowerOutTransforms,
    MapReplyPolicy, NameReplyCodec, OutTransformIdentity, OutTransformStack, PublishingDirectly,
    TransactionalReply, UnnamedCodec,
};
use crate::runtime::router::{DefaultReply, ReplyAttachment};
#[cfg(feature = "testing")]
use crate::testing::coordinator::record_slot_publish;
use crate::{
    ConnectedBroker, HeaderMap, OutgoingMessage, OwnedTransactions, Publisher, RequestReply,
    TransactionalPublisher,
};

/// A slot marker: the identity of one [`Out`](super::Out) injection.
///
/// Markers decouple a handler signature from the concrete publisher type: the handler names the
/// slot, the include site names the policy, and the runtime pairs them. Derive it on a unit
/// struct with `#[derive(OutSlot)]` (`macros` feature); [`NAME`](Self::NAME) labels the slot in
/// startup errors and test assertions.
///
/// # Examples
///
/// ```
/// use ruststream::prelude::*;
///
/// #[derive(Clone, Copy, Debug)]
/// struct Encoded;
///
/// impl OutSlot for Encoded {
///     const NAME: &'static str = "Encoded";
/// }
///
/// assert_eq!(<Encoded as OutSlot>::NAME, "Encoded");
/// ```
pub trait OutSlot: 'static {
    /// The human-readable slot name, used by diagnostics and test assertions.
    const NAME: &'static str;

    /// The slot's publish dictionary as `AsyncAPI` metadata, one entry per
    /// `#[publishes(..)]` type. The derive fills this in; the default (a marker without a
    /// dictionary) publishes nothing declared. Called once at registration; never on the
    /// publish path.
    #[must_use]
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        Vec::new()
    }
}

/// The implicit marker of a single unnamed `Out<impl Publisher>` parameter.
///
/// A handler with one `Out` parameter needs no marker: the parameter binds to this slot, and
/// the include site attaches its policy with the plain
/// [`publisher`](super::IncludeWith::publisher) call.
///
/// # Examples
///
/// ```
/// use ruststream::prelude::*;
///
/// assert_eq!(<DefaultSlot as OutSlot>::NAME, "default");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct DefaultSlot;

impl OutSlot for DefaultSlot {
    const NAME: &'static str = "default";
}

/// The marker of a handler's reply position: `.out(Reply, policy)` names the publish policy the
/// value a `publish("dest")` handler returns leaves through.
///
/// A mount site attaches every publish policy with one verb, and this is the marker of the one
/// position that is not an [`Out`](super::Out) slot. It carries no dictionary: what a reply may be
/// is the reply type's own [`ReplyShape`](crate::runtime::ReplyShape), while what leaves a slot is
/// the slot marker's `#[publishes(..)]` list.
///
/// Without the call the reply takes the broker's own
/// [`DefaultPublish`](crate::DefaultPublish) policy, so naming it is the exception, not the rule.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// # use ruststream::subscriber;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Order { id: u64 }
/// # #[derive(serde::Serialize, schemars::JsonSchema)]
/// # struct Confirmation { id: u64 }
///
/// #[subscriber("orders", publish("confirmations"))]
/// async fn confirm(order: &Order) -> Confirmation {
///     Confirmation { id: order.id }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(confirm).out(Reply, Publish);
///     })
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Reply;

/// The reply attachment of a registration whose handler declares no reply: there is no
/// [`Reply`] position to bind, so `.out(Reply, ..)` on it does not compile.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NoReply;

/// Membership of a message type in a slot marker's `#[publishes(..)]` dictionary: the type may
/// leave through a slot identified by `Slot`.
///
/// `#[derive(OutSlot)]` emits one impl per listed type, and the publish builder's typed entry
/// point ([`Slot::message`](crate::runtime::Slot::message)) requires it. That is what keeps
/// the generated document honest: an unrestricted `Out<impl Publisher, Marker>` reports the
/// marker's dictionary as what the handler sends, so a message outside it would be a publish the
/// document never declared.
///
/// The membership is declared on the message type, with the slot as the parameter, so the
/// compile error names the message that is not in the dictionary.
///
/// # Examples
///
/// ```
/// use ruststream::prelude::*;
///
/// struct Progress;
///
/// struct Events;
///
/// impl OutSlot for Events {
///     const NAME: &'static str = "Events";
/// }
///
/// // What `#[derive(OutSlot)]` + `#[publishes(Progress)]` generates:
/// impl PublishedThrough<Events> for Progress {}
///
/// fn admits<T: PublishedThrough<Slot>, Slot>() {}
/// admits::<Progress, Events>();
/// ```
#[diagnostic::on_unimplemented(
    message = "the `{Slot}` slot does not publish `{Self}`",
    note = "the marker lists what leaves through the slot, and the generated document reports \
            that list: add the type to it (`#[derive(OutSlot)] #[publishes({Self}, ..)]`), or \
            publish through a slot that lists it"
)]
pub trait PublishedThrough<Slot> {}

// The implicit slot has no declaration site to list types on, so it admits every message.
impl<T> PublishedThrough<DefaultSlot> for T {}

/// The live publisher an [`Out`](super::Out) slot injects: the attachment's paired publisher,
/// wrapped with the slot identity.
///
/// The wrapper is a zero-cost newtype (static dispatch, no boxing): every capability of the
/// underlying publisher ([`Publisher`], [`TransactionalPublisher`], [`OwnedTransactions`],
/// [`RequestReply`]) is delegated, so an `Out<impl OwnedTransactions, M>` bound holds exactly
/// when the attached policy's live form supports it. Under the `testing` feature, publishes made
/// through the wrapper inside a `TestApp`-driven handler are also recorded against the slot,
/// which is what backs the harness's per-slot assertions.
///
/// You never name this type: the handler sees it as `impl Publisher` (or a capability
/// refinement), and the include machinery constructs it.
#[derive(Debug)]
pub struct SlotPublisher<P, M> {
    inner: P,
    _slot: PhantomData<fn() -> M>,
}

impl<P, M> SlotPublisher<P, M> {
    pub(crate) fn new(inner: P) -> Self {
        Self {
            inner,
            _slot: PhantomData,
        }
    }

    /// The paired value this wrapper delegates to.
    ///
    /// This is the wrapper's own delegation seam, not the extension point for broker-defined
    /// capabilities: a handler body holds the arena entry
    /// ([`Slot`](crate::runtime::Slot)), never this wrapper, so a trait grafted here is
    /// unreachable from one. A broker whose paired value offers more than the core capability
    /// vocabulary ([`Publisher`], [`TransactionalPublisher`], [`OwnedTransactions`],
    /// [`RequestReply`]) - or is not a publisher at all (a per-partition producer cache, a shard
    /// router) - implements its capability trait for the live value and grafts it onto the entry
    /// instead, as [`Slot`](crate::runtime::Slot) documents.
    ///
    /// Publishes made through the value obtained here bypass the slot's test-capture
    /// attribution, like a settled owned transaction's buffer: assert on the broker's publish
    /// log for those.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::{OutSlot, SlotPublisher};
    ///
    /// /// A broker-defined capability the core knows nothing about.
    /// trait PartitionLanes {
    ///     fn lane_id(&self, partition: i32) -> String;
    /// }
    ///
    /// // The wrapper's own delegation: a slot entry reaches the live value through it. The
    /// // impl a handler body needs is the one on `Slot`, not this one.
    /// impl<P: PartitionLanes, M: OutSlot> PartitionLanes for SlotPublisher<P, M> {
    ///     fn lane_id(&self, partition: i32) -> String {
    ///         self.inner().lane_id(partition)
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn inner(&self) -> &P {
        &self.inner
    }
}

impl<P: Publisher, M: OutSlot> Publisher for SlotPublisher<P, M> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        #[cfg(feature = "testing")]
        record_slot_publish(M::NAME, &msg);
        self.inner.publish(msg).await
    }

    // The slot is attribution, not policy: whatever the broker's publisher contributes to every
    // message it sends has to reach the builder through the wrapper unchanged.
    fn base_headers(&self) -> Option<&HeaderMap> {
        self.inner.base_headers()
    }
}

impl<P: TransactionalPublisher, M: OutSlot> TransactionalPublisher for SlotPublisher<P, M> {
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.inner.begin_transaction().await
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        self.inner.commit().await
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        self.inner.abort().await
    }
}

// Transaction values own their buffer and leave the slot's scope, so publishes made through
// them are visible in the broker's publish log but are not attributed to the slot.
impl<P: OwnedTransactions, M: OutSlot> OwnedTransactions for SlotPublisher<P, M> {
    type Transaction = P::Transaction;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        self.inner.transaction().await
    }
}

impl<P: RequestReply, M: OutSlot> RequestReply for SlotPublisher<P, M> {
    type Reply = P::Reply;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        #[cfg(feature = "testing")]
        record_slot_publish(M::NAME, &msg);
        self.inner.request(msg, timeout).await
    }
}

/// Membership of a message type in an `Out` parameter's declared message list.
///
/// The declaration is a tuple listing types, a set-defining type (a `#[derive(MessageInfo)]` type
/// declares itself, a `#[derive(OutMessages)]` enum declares its variants' models), or the
/// unrestricted `()` (any dictionary type). The `Index` parameter is inferred per call, like
/// the slot-binding machinery's positions; a duplicate type in a declaration is rejected where
/// it is declared, so the index is always unambiguous.
#[diagnostic::on_unimplemented(
    message = "`{T}` is not in this Out parameter's declared message set `{Self}`",
    note = "the third Out argument declares what the handler publishes: list the type there \
            (`Out<impl Publisher, Marker, (A, B)>`), leave the slot unrestricted with `()`, or \
            publish through another slot"
)]
pub trait ContainsMessage<T, Index> {}

/// The [`ContainsMessage`] index of the unrestricted `()` declaration.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Unrestricted;

// The unrestricted declaration: any message the marker's dictionary admits.
impl<T> ContainsMessage<T, Unrestricted> for () {}

/// Implements [`ContainsMessage`] for each (arity, position) pair of the message-list tuple.
macro_rules! impl_contains_message {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        impl<T $(, $before)* $(, $after)*> ContainsMessage<T, SlotPos<$pos>>
            for ($($before,)* T, $($after,)*)
        {
        }
    )+};
}

impl_contains_message! {
    (@ 0)
    (@ 0, A1)
    (A0, @ 1)
    (@ 0, A1, A2)
    (A0, @ 1, A2)
    (A0, A1, @ 2)
    (@ 0, A1, A2, A3)
    (A0, @ 1, A2, A3)
    (A0, A1, @ 2, A3)
    (A0, A1, A2, @ 3)
}

/// A declared message set's `AsyncAPI` contribution: one
/// [`OutgoingMessageMetadata`] entry per member, each read off the member's own declaration.
///
/// Implemented by `#[derive(Outgoing)]` (the type declares itself and where it goes) and by
/// `#[derive(OutMessages)]` on a set enum (each variant's model); `()` falls back to the whole
/// dictionary. A tuple declaration needs no impl - the `#[subscriber]` macro enumerates its
/// elements itself.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::prelude::*;
/// use serde::Serialize;
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "chunks.progress")]
/// struct Progress {
///     percent: u8,
/// }
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "chunks.done")]
/// struct ChunkDone {
///     output_key: String,
/// }
///
/// // A reusable named set: the variants' models are what a handler naming `ConvertSends` as
/// // its third Out argument publishes. The enum is never constructed.
/// #[derive(OutMessages)]
/// enum ConvertSends {
///     Progress(Progress),
///     Done(ChunkDone),
/// }
/// # }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not define a message set for the `{M}` slot",
    note = "the third Out argument is a tuple of types, `()` (unrestricted), a \
            #[derive(Outgoing)] type (declares itself and where it goes), or a \
            #[derive(OutMessages)] enum (declares its variants' models)"
)]
pub trait OutMessages<M: OutSlot> {
    /// The set's declared outgoing messages, for the generated document. Called once at
    /// registration; never on the publish path.
    #[must_use]
    fn outgoing() -> Vec<OutgoingMessageMetadata>;
}

// The unrestricted declaration documents the whole dictionary.
impl<M: OutSlot> OutMessages<M> for () {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        M::outgoing()
    }
}

/// The "not bound yet" placeholder of the `Out` slot marked `M` at the include site.
///
/// The marker rides in the type so the compile error of an incomplete registration (a
/// `.build()` whose attachment tuple still contains a `MissingSlot<..>`) names the slot that
/// was forgotten. A value of this type never reaches the runtime: committing requires every
/// position bound.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct MissingSlot<M>(PhantomData<fn() -> M>);

impl<M> MissingSlot<M> {
    pub(crate) fn new() -> Self {
        Self(PhantomData)
    }
}

/// What one `.out(marker, policy)` call attaches: the slot's publish policy and the
/// [`OutTransform`] stack the `.transform(..)` steps after it compose.
///
/// The stack is pure declaration, like the policy: it lowers onto the app's publish pipeline at
/// the mount ([`LowerOutTransforms`]), and the composed pipeline is what the slot entry publishes
/// through. Machinery; the chain builds it and the mount consumes it, so it is never named in
/// service code.
#[doc(hidden)]
#[derive(Debug)]
pub struct OutAttachment<Policy, Layers = OutTransformIdentity, Enc = UnnamedCodec> {
    policy: Policy,
    layers: Layers,
    enc: Enc,
}

impl<Policy> OutAttachment<Policy> {
    /// The attachment a bare `.out(marker, policy)` produces: the policy, no transforms, the
    /// surface's own codec.
    pub(crate) fn new(policy: Policy) -> Self {
        Self {
            policy,
            layers: OutTransformIdentity,
            enc: UnnamedCodec::new(),
        }
    }
}

impl<Policy, Layers, Enc> OutAttachment<Policy, Layers, Enc> {
    /// Composes one more transform on top of the stack: the `.transform(..)` step.
    pub(crate) fn add_transform<N>(
        self,
        transform: N,
    ) -> OutAttachment<Policy, OutTransformStack<Layers, N>, Enc> {
        OutAttachment {
            policy: self.policy,
            layers: OutTransformStack {
                inner: self.layers,
                outer: transform,
            },
            enc: self.enc,
        }
    }

    /// Fills the slot's codec position: the `.codec(..)` step.
    pub(crate) fn name_codec<C>(self, codec: C) -> OutAttachment<Policy, Layers, CallCodec<C>> {
        OutAttachment {
            policy: self.policy,
            layers: self.layers,
            enc: CallCodec(codec),
        }
    }

    /// Replaces the policy, keeping everything the chain already named: the `.map_publisher(..)`
    /// step a broker's own settings trait layers on.
    pub(crate) fn map_policy(self, f: impl FnOnce(Policy) -> Policy) -> Self {
        Self {
            policy: f(self.policy),
            layers: self.layers,
            enc: self.enc,
        }
    }

    /// Splits the attachment into what one slot resolves from at startup: the policy the runtime
    /// pairs, the encode codec (this slot's own when it named one, the surface's otherwise), and
    /// the pipeline the entry publishes through (this slot's transforms lowered onto the app's).
    pub(crate) fn wire<Surface, Pipeline>(
        self,
        surface: Surface,
        pipeline: Pipeline,
    ) -> (Policy, Enc::Codec, Layers::Out)
    where
        Enc: SlotCodec<Surface>,
        Layers: LowerOutTransforms<Pipeline>,
    {
        (
            self.policy,
            self.enc.resolve(surface),
            self.layers.lower(pipeline),
        )
    }
}

/// Resolves one slot's encode codec: the codec the chain named for that slot, or the registration
/// surface's own when it named none. The slot counterpart of a reply wiring's
/// [`NameReplyCodec`] position.
#[doc(hidden)]
pub trait SlotCodec<Surface> {
    /// The resolved codec the slot entry encodes with.
    type Codec;

    /// Resolves it against the surface's codec.
    fn resolve(self, surface: Surface) -> Self::Codec;
}

impl<Surface> SlotCodec<Surface> for UnnamedCodec {
    type Codec = Surface;

    fn resolve(self, surface: Surface) -> Surface {
        surface
    }
}

impl<Surface, C> SlotCodec<Surface> for CallCodec<C> {
    type Codec = C;

    fn resolve(self, _surface: Surface) -> C {
        self.0
    }
}

/// The step a mount site's chain has named so far, before it has named any.
///
/// It is not a [`NamedStep`], so a `.transform(..)` here fails on that bound rather than on a
/// missing method, and the call site reads the guidance.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOutBound;

/// A step a `.transform(..)` can ride: what the chain named right before it.
///
/// The step traits below report it as an associated type ([`TransformAt::Step`],
/// [`TransformLast::Step`]) rather than refusing to apply, so the transform's own bound is what
/// fails and this note is what the call site reads.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this `.transform(..)` has no step to apply to",
    label = "no `.out(marker, policy)` or `.publisher(policy)` call precedes it in this chain",
    note = "a transform rides the step named right before it: \
            `.out(Marker, Policy).transform(..)` for a slot, `.publisher(Policy).transform(..)` \
            for a reply"
)]
pub trait NamedStep {}

impl<const POS: usize> NamedStep for SlotPos<POS> {}

/// The position a step applies to when the chain last named the reply rather than a slot.
///
/// A mount chain's steps read as "on the position before them": after `.out(Reply, ..)` they grow
/// the reply's wiring, after `.out(marker, ..)` that slot's own.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct ReplyLast;

impl NamedStep for ReplyLast {}

/// Binds one `.out(marker, policy)` call into a mount chain's attachment: the reply position for
/// [`Reply`], one [`Out`](super::Out) slot for a slot marker.
///
/// `Index` is inferred per call - [`ReplyLast`] for the reply, [`SlotPos`] for a slot - which is
/// what makes the calls order-independent and what the steps after the call ride. Machinery;
/// never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this handler has no unbound publish position marked `{M}`",
    label = "`.out({M}, ..)` has no position to bind here",
    note = "`.out(marker, policy)` binds one position: `Reply` for the value a \
            `publish(\"dest\")` handler returns, an `Out` slot's own marker for a slot. Check the \
            marker, that the handler declares it, and that it was not bound twice"
)]
pub trait BindAt<Mount, M, Policy, Index> {
    /// The attachment with that position bound.
    type Out;

    /// Binds it.
    fn bind_at(self, policy: Policy) -> Self::Out;
}

// The reply position, open exactly while it still carries the broker's default: a second
// `.out(Reply, ..)` finds no impl and reads the trait's own note.
impl<Mount, Policy, Slots> BindAt<Mount, Reply, Policy, ReplyLast> for (DefaultReply, Slots)
where
    Mount: ReplyAttachment<Policy>,
{
    type Out = (WithSource<Mount::Wiring>, Slots);

    fn bind_at(self, policy: Policy) -> Self::Out {
        (WithSource::new(Mount::wire(policy)), self.1)
    }
}

// One slot position, placed by marker through the positional machinery.
impl<Mount, M, Policy, const POS: usize, Rep, Slots> BindAt<Mount, M, Policy, SlotPos<POS>>
    for (Rep, Slots)
where
    M: OutSlot,
    Slots: BindSlot<M, OutAttachment<Policy>, SlotPos<POS>>,
{
    type Out = (Rep, Slots::Out);

    fn bind_at(self, policy: Policy) -> Self::Out {
        (self.0, self.1.bind(OutAttachment::new(policy)))
    }
}

/// Grows the transform stack of the slot bound at `Index`: the `.transform(..)` step of a mount
/// chain, applied to a slot. Machinery; never named directly.
///
/// The index is the position the preceding `.out(marker, policy)` bound, carried by the builder,
/// so the step reads as "on the slot just named" rather than repeating the marker.
#[doc(hidden)]
pub trait TransformAt<N, Index> {
    /// The attachment tuple with that slot's stack grown.
    type Out;

    /// Composes it.
    fn transform_at(self, transform: N) -> Self::Out;
}

/// Fills the codec position of the slot bound at `Index`: the `.codec(..)` step applied to a
/// slot. Machinery; never named directly.
#[doc(hidden)]
pub trait CodecAt<Cd, Index> {
    /// The attachment tuple with that slot's codec named.
    type Out;

    /// Names it.
    fn codec_at(self, codec: Cd) -> Self::Out;
}

/// Replaces the publish policy of the slot bound at `Index`: the `.map_publisher(..)` step
/// applied to a slot. Machinery; never named directly.
#[doc(hidden)]
pub trait MapPolicyAt<Index>: Sized {
    /// The policy the slot carries.
    type Policy;

    /// Replaces it.
    fn map_policy_at(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self;
}

/// Composes a transform onto whatever a mount chain named last: the reply's wiring
/// ([`ReplyLast`]) or one bound slot ([`SlotPos`]). Machinery; never named directly.
#[doc(hidden)]
pub trait TransformLast<N, Last> {
    /// The position the transform rides, or [`NoOutBound`] when the chain has named none.
    type Step;

    /// The attachment after the step.
    type Out;

    /// Composes it.
    fn transform_last(self, transform: N) -> Self::Out;
}

impl<N, W, Slots> TransformLast<N, ReplyLast> for (WithSource<W>, Slots)
where
    W: AddReplyTransform<N>,
{
    type Step = ReplyLast;
    type Out = (WithSource<W::Out>, Slots);

    fn transform_last(self, transform: N) -> Self::Out {
        let (reply, slots) = self;
        (reply.map(|wiring| wiring.add_transform(transform)), slots)
    }
}

impl<N, const POS: usize, Rep, Slots> TransformLast<N, SlotPos<POS>> for (Rep, Slots)
where
    Slots: TransformAt<N, SlotPos<POS>>,
{
    type Step = SlotPos<POS>;
    type Out = (Rep, Slots::Out);

    fn transform_last(self, transform: N) -> Self::Out {
        let (reply, slots) = self;
        (reply, slots.transform_at(transform))
    }
}

// The "nothing named yet" arm: it exists so the step resolves as a method and fails on
// `Step: NamedStep`, which is where the guidance lives. It never runs.
impl<N, Rep, Slots> TransformLast<N, NoOutBound> for (Rep, Slots) {
    type Step = NoOutBound;
    type Out = Self;

    fn transform_last(self, _transform: N) -> Self {
        self
    }
}

/// Names the codec of whatever a mount chain named last: the reply's encode codec after
/// `.out(Reply, ..)`, one slot's after `.out(marker, ..)`. Machinery; never named directly.
#[doc(hidden)]
pub trait CodecLast<Cd, Last> {
    /// See [`TransformLast::Step`].
    type Step;

    /// The attachment after the step.
    type Out;

    /// Names it.
    fn codec_last(self, codec: Cd) -> Self::Out;
}

impl<Cd, W, Slots> CodecLast<Cd, ReplyLast> for (WithSource<W>, Slots)
where
    W: NameReplyCodec<Cd, Slot: CodecSlotOpen>,
{
    type Step = ReplyLast;
    type Out = (WithSource<W::Out>, Slots);

    fn codec_last(self, codec: Cd) -> Self::Out {
        let (reply, slots) = self;
        (reply.map(|wiring| wiring.name_codec(codec)), slots)
    }
}

impl<Cd, const POS: usize, Rep, Slots> CodecLast<Cd, SlotPos<POS>> for (Rep, Slots)
where
    Slots: CodecAt<Cd, SlotPos<POS>>,
{
    type Step = SlotPos<POS>;
    type Out = (Rep, Slots::Out);

    fn codec_last(self, codec: Cd) -> Self::Out {
        let (reply, slots) = self;
        (reply, slots.codec_at(codec))
    }
}

// See `TransformLast`'s own arm.
impl<Cd, Rep, Slots> CodecLast<Cd, NoOutBound> for (Rep, Slots) {
    type Step = NoOutBound;
    type Out = Self;

    fn codec_last(self, _codec: Cd) -> Self {
        self
    }
}

/// Composes a batch transform onto the reply a mount chain named last. Reply-only: a slot
/// publish is one message, so there is no page for a batch transform to run over. Machinery;
/// never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`.batch_transform(..)` does not apply to an Out slot",
    label = "the position named before it is a slot, not the reply",
    note = "a slot publish is one message with no page: `.batch_transform(..)` rides the reply \
            of a page handler (`.out(Reply, policy).batch_transform(..)`), and a slot takes \
            `.transform(..)` instead"
)]
pub trait BatchTransformLast<N, Last> {
    /// See [`TransformLast::Step`].
    type Step;

    /// The attachment after the step.
    type Out;

    /// Composes it.
    fn batch_transform_last(self, transform: N) -> Self::Out;
}

impl<N, W, Slots> BatchTransformLast<N, ReplyLast> for (WithSource<W>, Slots)
where
    W: AddBatchReplyTransform<N>,
{
    type Step = ReplyLast;
    type Out = (WithSource<W::Out>, Slots);

    fn batch_transform_last(self, transform: N) -> Self::Out {
        let (reply, slots) = self;
        (
            reply.map(|wiring| wiring.add_batch_transform(transform)),
            slots,
        )
    }
}

// See `TransformLast`'s own arm.
impl<N, Rep, Slots> BatchTransformLast<N, NoOutBound> for (Rep, Slots) {
    type Step = NoOutBound;
    type Out = Self;

    fn batch_transform_last(self, _transform: N) -> Self {
        self
    }
}

/// Marks the reply a mount chain named last as publishing inside one broker transaction.
/// Reply-only, for the same reason [`BatchTransformLast`] is. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`.transactional()` does not apply to an Out slot",
    label = "the position named before it is a slot, not the reply",
    note = "`.transactional()` makes a page's replies one broker transaction, and a slot publish \
            is one message with no page: a body opens its own slot transaction with \
            `entry.begin()` or `entry.transaction()`"
)]
pub trait TransactionalLast<Last> {
    /// See [`TransformLast::Step`].
    type Step;

    /// The attachment after the step.
    type Out;

    /// Marks it.
    fn transactional_last(self) -> Self::Out;
}

impl<W, Slots> TransactionalLast<ReplyLast> for (WithSource<W>, Slots)
where
    W: TransactionalReply<State: PublishingDirectly>,
{
    type Step = ReplyLast;
    type Out = (WithSource<W::Out>, Slots);

    fn transactional_last(self) -> Self::Out {
        let (reply, slots) = self;
        (reply.map(TransactionalReply::into_transactional), slots)
    }
}

// See `TransformLast`'s own arm.
impl<Rep, Slots> TransactionalLast<NoOutBound> for (Rep, Slots) {
    type Step = NoOutBound;
    type Out = Self;

    fn transactional_last(self) -> Self {
        self
    }
}

/// Replaces the publish policy of whatever a mount chain named last: the hook a broker crate
/// layers its own publisher settings on. Machinery; never named directly.
#[doc(hidden)]
pub trait MapPolicyLast<Last>: Sized {
    /// See [`TransformLast::Step`].
    type Step;

    /// The policy that position carries.
    type Policy;

    /// Replaces it.
    fn map_policy_last(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self;
}

impl<W: MapReplyPolicy, Slots> MapPolicyLast<ReplyLast> for (WithSource<W>, Slots) {
    type Step = ReplyLast;
    type Policy = W::Policy;

    fn map_policy_last(self, f: impl FnOnce(W::Policy) -> W::Policy) -> Self {
        let (reply, slots) = self;
        (reply.map(|wiring| wiring.map_policy(f)), slots)
    }
}

impl<const POS: usize, Rep, Slots: MapPolicyAt<SlotPos<POS>>> MapPolicyLast<SlotPos<POS>>
    for (Rep, Slots)
{
    type Step = SlotPos<POS>;
    type Policy = Slots::Policy;

    fn map_policy_last(self, f: impl FnOnce(Slots::Policy) -> Slots::Policy) -> Self {
        let (reply, slots) = self;
        (reply, slots.map_policy_at(f))
    }
}

// See `TransformLast`'s own arm.
impl<Rep, Slots> MapPolicyLast<NoOutBound> for (Rep, Slots) {
    type Step = NoOutBound;
    type Policy = ();

    fn map_policy_last(self, _f: impl FnOnce(())) -> Self {
        self
    }
}

/// A user-attached publisher source, wrapped so the bound and unbound attachment states live on
/// different type constructors (disjoint impls, no negative reasoning needed).
#[doc(hidden)]
#[derive(Debug)]
pub struct WithSource<Source>(Source);

impl<Source> WithSource<Source> {
    pub(crate) fn new(source: Source) -> Self {
        Self(source)
    }

    /// Grows the wrapped source in place: how a mount site's reply chain adds one step to the
    /// wiring it already attached.
    pub(crate) fn map<NewSource>(
        self,
        f: impl FnOnce(Source) -> NewSource,
    ) -> WithSource<NewSource> {
        WithSource(f(self.0))
    }
}

impl<Source> IntoSlotSource for WithSource<Source> {
    type Source = Source;

    fn into_source(self) -> Self::Source {
        self.0
    }
}

/// A definition whose handler carries marker-identified `Out` slots.
///
/// Implemented by `#[subscriber]` on the value you pass to `include`; [`Markers`](Self::Markers)
/// lists the slot markers in signature order, which is the order the positional machinery
/// ([`InitSlots`], [`BindSlot`], [`BindSlots`]) works in. You never implement this by hand
/// unless you hand-write a definition.
pub trait HasSlots {
    /// The slot markers, as a tuple in the handler signature's parameter order.
    type Markers;
}

/// Builds the initial all-unbound attachment tuple for a definition's markers. Machinery behind
/// `include`; never named directly.
#[doc(hidden)]
pub trait InitSlots {
    /// One [`MissingSlot`] per marker, carrying it.
    type Init;

    fn init() -> Self::Init;
}

/// Unwraps one include-site attachment into the policy the runtime pairs: a bound
/// [`WithSource`] yields its policy. Machinery; never named directly.
#[doc(hidden)]
pub trait IntoSlotSource {
    /// The publish policy this attachment resolves to.
    type Source;

    fn into_source(self) -> Self::Source;
}

/// Positional placement of one `.out(marker, policy)` attachment: finds the (still unbound)
/// element carrying the marker and replaces it. The `Index` parameter is inferred per call,
/// which is what makes the calls order-independent; binding the same slot twice finds no
/// still-unbound element and fails to compile. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this handler has no unbound Out slot marked `{M}`",
    note = "each .out(marker, policy) call binds one still-unbound slot the handler declares; \
            check the marker, and that the slot was not bound twice"
)]
pub trait BindSlot<M, Src, Index> {
    /// The attachment tuple with the marker's position bound.
    type Out;

    fn bind(self, src: Src) -> Self::Out;
}

/// The position tokens inferred by [`BindSlot`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct SlotPos<const N: usize>;

/// Instantiates a slot-carrying definition from the bound sources.
///
/// Implemented by `#[subscriber]` next to [`HasSlots`]: given the source tuple (in marker
/// order), it names the fully-applied definition type - each slot's publisher is
/// [`SlotPublisher`] over the source's [`PublishPolicy::Live`](crate::PublishPolicy::Live) - and pads the sources into the
/// definition's injection-extra tuple. The connected
/// broker type `C` is the pairing target the publisher types are computed against.
pub trait BindSlots<C: ConnectedBroker, Sources>: Sized {
    /// The publisher-applied definition type.
    type Bound;

    /// The extra tuple the startup resolution (`FromStartup`) resolves the injections against:
    /// the sources, padded to the injection tuple's arity.
    type Extra;

    /// Instantiates the definition and arranges the sources for resolution.
    fn bind(self, sources: Sources) -> (Self::Bound, Self::Extra);
}

/// Implements [`InitSlots`] for each marker-tuple arity: every position starts as its marker's
/// [`MissingSlot`].
macro_rules! impl_init_slots {
    ($(($($marker:ident),+))+) => {$(
        impl<$($marker),+> InitSlots for ($($marker,)+) {
            type Init = ($(MissingSlot<$marker>,)+);

            fn init() -> Self::Init {
                ($(MissingSlot::<$marker>::new(),)+)
            }
        }
    )+};
}

impl_init_slots! {
    (M0)
    (M0, M1)
    (M0, M1, M2)
}

/// Implements [`BindSlot`] for each (arity, position) pair: the element carrying the marker
/// (`@ <position>`) becomes [`WithSource`], the surrounding elements pass through unchanged.
macro_rules! impl_bind_slot {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        impl<M, Src $(, $before)* $(, $after)*> BindSlot<M, Src, SlotPos<$pos>>
            for ($($before,)* MissingSlot<M>, $($after,)*)
        {
            type Out = ($($before,)* WithSource<Src>, $($after,)*);

            fn bind(self, src: Src) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($before,)* _missing, $($after,)*) = self;
                ($($before,)* WithSource::new(src), $($after,)*)
            }
        }
    )+};
}

impl_bind_slot! {
    (@ 0)
    (@ 0, A1)
    (A0, @ 1)
    (@ 0, A1, A2)
    (A0, @ 1, A2)
    (A0, A1, @ 2)
}

/// Implements [`TransformAt`] for each (arity, position) pair: the attachment at the position the
/// last `.out(..)` bound (`@ <position>`) grows its stack, the surrounding elements pass through.
macro_rules! impl_step_at {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        impl<N, Policy, Layers, Enc $(, $before)* $(, $after)*> TransformAt<N, SlotPos<$pos>>
            for ($($before,)* WithSource<OutAttachment<Policy, Layers, Enc>>, $($after,)*)
        {
            type Out = (
                $($before,)*
                WithSource<OutAttachment<Policy, OutTransformStack<Layers, N>, Enc>>,
                $($after,)*
            );

            fn transform_at(self, transform: N) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($before,)* bound, $($after,)*) = self;
                ($($before,)* bound.map(|slot| slot.add_transform(transform)), $($after,)*)
            }
        }

        impl<Cd, Policy, Layers $(, $before)* $(, $after)*> CodecAt<Cd, SlotPos<$pos>>
            for ($($before,)* WithSource<OutAttachment<Policy, Layers, UnnamedCodec>>, $($after,)*)
        {
            type Out = (
                $($before,)*
                WithSource<OutAttachment<Policy, Layers, CallCodec<Cd>>>,
                $($after,)*
            );

            fn codec_at(self, codec: Cd) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($before,)* bound, $($after,)*) = self;
                ($($before,)* bound.map(|slot| slot.name_codec(codec)), $($after,)*)
            }
        }

        impl<Policy, Layers, Enc $(, $before)* $(, $after)*> MapPolicyAt<SlotPos<$pos>>
            for ($($before,)* WithSource<OutAttachment<Policy, Layers, Enc>>, $($after,)*)
        {
            type Policy = Policy;

            fn map_policy_at(self, f: impl FnOnce(Policy) -> Policy) -> Self {
                #[allow(non_snake_case)]
                let ($($before,)* bound, $($after,)*) = self;
                ($($before,)* bound.map(|slot| slot.map_policy(f)), $($after,)*)
            }
        }
    )+};
}

impl_step_at! {
    (@ 0)
    (@ 0, A1)
    (A0, @ 1)
    (@ 0, A1, A2)
    (A0, @ 1, A2)
    (A0, A1, @ 2)
}

#[cfg(test)]
mod tests;
