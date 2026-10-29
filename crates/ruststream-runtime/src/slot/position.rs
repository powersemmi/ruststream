//! The include-site attachment of one `Out` slot and the steps that ride a chain's last-named
//! position: `.transform(..)`, `.codec(..)`, policy edits and the transactional reply.

use std::borrow::Cow;
use std::marker::PhantomData;

use super::{BindSlot, DestinationOffer, OutSlot, Reply, SlotPos, WithSource};
use crate::codec::Codec;
use crate::handle::{DeclaresReply, ReplyShape};
use crate::metadata::PublishDescription;
use crate::publish::{
    AddBatchReplyTransform, AddReplyTransform, CallCodec, CodecSlotOpen, FitsOffer, ForReply,
    ForSlot, LowerOutTransforms, MapReplyPolicy, NameReplyCodec, NarrowToUse, PublishTransform,
    PublishTransformIdentity, PublishTransformStack, PublishingDirectly, RawReplyWiring, Reads,
    ReplyWiring, SlotStackUse, TransactionalReply, UnnamedCodec,
};
use crate::router::{
    BatchPublishInjectMount, BatchPublishMount, DefaultReply, PublishInjectMount, PublishMount,
    RawReplyInjectMount, RawReplyMount, ReplyAttachment,
};
use crate::{Broker, Connected, ConnectedBroker, OutgoingDestination, PublishPolicy, Publisher};

/// What one `.out(marker, policy)` call attaches: the slot's publish policy and the
/// [`PublishTransform`](crate::PublishTransform) stack the `.transform(..)` steps after it
/// compose.
///
/// The stack is pure declaration, like the policy: it lowers onto the app's publish pipeline at
/// the mount ([`LowerOutTransforms`]), and the composed pipeline is what the slot entry publishes
/// through. Machinery; the chain builds it and the mount consumes it, so it is never named in
/// service code.
#[doc(hidden)]
#[derive(Debug)]
pub struct OutAttachment<M, Policy, Layers = PublishTransformIdentity, Enc = UnnamedCodec> {
    policy: Policy,
    layers: Layers,
    enc: Enc,
    // The marker the `.out(marker, policy)` call bound, carried so the steps after it can ask what
    // the slot's own declaration offers: the `.transform(..)` step asks it whether the slot lets a
    // transform name the destination.
    _marker: PhantomData<fn() -> M>,
}

impl<M, Policy> OutAttachment<M, Policy> {
    /// The attachment a bare `.out(marker, policy)` produces: the policy, no transforms, the
    /// surface's own codec.
    pub(crate) fn new(policy: Policy) -> Self {
        Self {
            policy,
            layers: PublishTransformIdentity,
            enc: UnnamedCodec::new(),
            _marker: PhantomData,
        }
    }
}

impl<M, Policy, Layers, Enc> OutAttachment<M, Policy, Layers, Enc> {
    /// Composes one more transform on top of the stack: the `.transform(..)` step.
    pub(crate) fn add_transform<N>(
        self,
        transform: N,
    ) -> OutAttachment<M, Policy, PublishTransformStack<Layers, N>, Enc> {
        OutAttachment {
            policy: self.policy,
            layers: PublishTransformStack {
                inner: self.layers,
                outer: transform,
            },
            enc: self.enc,
            _marker: PhantomData,
        }
    }

    /// Fills the slot's codec position: the `.codec(..)` step.
    pub(crate) fn name_codec<C>(self, codec: C) -> OutAttachment<M, Policy, Layers, CallCodec<C>> {
        OutAttachment {
            policy: self.policy,
            layers: self.layers,
            enc: CallCodec(codec),
            _marker: PhantomData,
        }
    }

    /// Replaces the policy, keeping everything the chain already named: the `.map_publisher(..)`
    /// step a broker's own settings trait layers on.
    pub(crate) fn map_policy(self, f: impl FnOnce(Policy) -> Policy) -> Self {
        Self {
            policy: f(self.policy),
            layers: self.layers,
            enc: self.enc,
            _marker: PhantomData,
        }
    }

    /// What this slot contributes to the generated document: the destinations its marker
    /// declares, and what its policy and codec say about each of them.
    ///
    /// The policy answers once per destination, because a binding may carry the channel's own
    /// name and a dictionary names several.
    ///
    /// Read before [`wire`](Self::wire) consumes the attachment, because the slot's policy is
    /// what answers and wiring moves it. Nothing here runs per message.
    pub(crate) fn describe<C, Surface>(&self) -> Vec<(Cow<'static, str>, PublishDescription)>
    where
        M: OutSlot,
        C: ConnectedBroker,
        Policy: PublishPolicy<C>,
        Enc: SlotCodec<Surface>,
        <Enc as SlotCodec<Surface>>::Codec: Codec,
    {
        M::outgoing()
            .into_iter()
            .map(|entry| {
                // A slot publishes where its dictionary says, so no transform on it names a reply
                // address: that question belongs to the reply position.
                let description = PublishDescription::of::<C, Policy>(
                    &self.policy,
                    Some(<<Enc as SlotCodec<Surface>>::Codec as Codec>::CONTENT_TYPE),
                    false,
                    entry.channel.as_ref(),
                );
                (entry.channel, description)
            })
            .collect()
    }

    /// Hands back the pieces without folding them: what the deferred-retry position resolves
    /// from, which keeps the mount site's transform stack out of the publish pipeline so it can
    /// be run against the delivery being retried.
    pub(crate) fn into_parts(self) -> (Policy, Layers, Enc) {
        (self.policy, self.layers, self.enc)
    }

    /// Splits the attachment into what one slot resolves from at startup: the policy the runtime
    /// pairs (narrowed to plain sending where this slot's transforms name the destination), the
    /// encode codec (this slot's own when it named one, the surface's otherwise), and the pipeline
    /// the entry publishes through.
    pub(crate) fn wire<Surface, Pipeline, Live>(
        self,
        surface: Surface,
        pipeline: Pipeline,
    ) -> (SlotPolicy<Layers, Policy, Live>, Enc::Codec, Layers::Out)
    where
        M: OutSlot,
        Enc: SlotCodec<Surface>,
        Layers: LowerOutTransforms<Pipeline> + SlotStackUse<Live>,
        <Layers as SlotStackUse<Live>>::Destination: NarrowToUse<Policy>,
    {
        let codec = self.enc.resolve(surface);
        let policy = <<Layers as SlotStackUse<Live>>::Destination as NarrowToUse<Policy>>::narrow(
            self.policy,
        );
        (policy, codec, self.layers.lower(M::NAME, pipeline))
    }
}

/// The policy one slot pairs: its own, narrowed where the stack names the destination. Machinery;
/// the commit spells it so the bound reads.
#[doc(hidden)]
pub type SlotPolicy<Layers, Policy, Live> =
    <<Layers as SlotStackUse<Live>>::Destination as NarrowToUse<Policy>>::Out;

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

/// A position a chain's steps can ride: what the chain named right before them.
///
/// The step traits below report it as an associated type ([`TransformLast::Step`] and its
/// siblings) rather than refusing to apply, so the step's own bound is what fails and this note
/// is what the call site reads.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this step has no publish position to apply to",
    label = "no `.out(marker, policy)` call precedes it in this chain",
    note = "a step rides the position named right before it: `.out(Reply, Policy).transform(..)` \
            for a reply, `.out(Marker, Policy).transform(..)` for a slot"
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

/// One `.out(marker, policy)` call, keyed by the marker: what the position it names becomes.
///
/// The dispatch is on the marker rather than on the attachment so that the call site's own
/// argument settles which position is meant, and the error names that marker when the position is
/// not there to bind. Every marker grows the attachment through [`BindAt`]: [`Reply`], the
/// handler's own slot markers, and [`Retry`](crate::Retry), which is the slot the
/// runtime publishes a deferred `retry_after` copy through.
///
/// Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this registration has no unbound publish position marked `{Self}`",
    label = "`.out({Self}, ..)` has no position to bind here",
    note = "`.out(marker, policy)` binds one position: `Reply` for the value a \
            `reply(\"dest\")` handler returns, an `Out` slot's own marker for a slot, `Retry` \
            for the deferred `retry_after` copy. Check the marker, that the handler declares it, \
            and that it was not bound twice"
)]
pub trait OutPosition<Mount, B, Attach, Policy, Index> {
    /// The grown attachment the chain carries after the call.
    type Out;

    /// Binds the position.
    fn bind(attach: Attach, policy: Policy) -> Self::Out;
}

// The reply and the slots are positions of the attachment itself, so both delegate. The broker
// takes no part: a policy for one of these positions pairs where the registration mounts.
impl<Mount, B, Attach, Policy> OutPosition<Mount, B, Attach, Policy, ReplyLast> for Reply
where
    Attach: BindAt<Mount, Self, Policy, ReplyLast>,
{
    type Out = <Attach as BindAt<Mount, Self, Policy, ReplyLast>>::Out;

    fn bind(attach: Attach, policy: Policy) -> Self::Out {
        attach.bind_at(policy)
    }
}

impl<Mount, B, M, Attach, Policy, Index> OutPosition<Mount, B, Attach, Policy, Index> for M
where
    M: OutSlot,
    Attach: BindAt<Mount, M, Policy, Index>,
{
    type Out = <Attach as BindAt<Mount, M, Policy, Index>>::Out;

    fn bind(attach: Attach, policy: Policy) -> Self::Out {
        attach.bind_at(policy)
    }
}

/// Binds one `.out(marker, policy)` call into a mount chain's attachment: the reply position for
/// [`Reply`], one [`Out`](super::Out) slot for a slot marker, the deferred-retry slot for
/// [`Retry`](crate::Retry).
///
/// `Index` is inferred per call - [`ReplyLast`] for the reply, [`SlotPos`] for a slot,
/// [`RetryPos`](crate::RetryPos) for the deferred retry - which is what makes the calls
/// order-independent and what the steps after the call ride. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this registration has no unbound publish position marked `{M}`",
    label = "`.out({M}, ..)` has no position to bind here",
    note = "`.out(marker, policy)` binds one position: `Reply` for the value a \
            `reply(\"dest\")` handler returns, an `Out` slot's own marker for a slot, `Retry` \
            for the deferred `retry_after` copy. Check the marker, that the handler declares it, \
            and that it was not bound twice"
)]
pub trait BindAt<Mount, M, Policy, Index> {
    /// The attachment with that position bound.
    type Out;

    /// Binds it.
    fn bind_at(self, policy: Policy) -> Self::Out;
}

/// A reply position still open: what `.out(Reply, policy)` binds.
///
/// The bind step states this about the position rather than about the whole attachment, so a
/// second `.out(Reply, ..)` - and an `.out(Reply, ..)` on a handler that declares no reply -
/// fails on this bound and the call site reads the note instead of a bare "no method".
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this registration has no open reply position",
    label = "`.out(Reply, ..)` has nothing to bind here",
    note = "`.out(Reply, policy)` names the publish policy of a `reply(\"dest\")` handler's \
            returned value, once: a handler that declares no reply has no `Reply` position at \
            all, and one whose policy is already named has none left open"
)]
pub trait ReplyOpen {}

impl ReplyOpen for DefaultReply {}

// The reply position, open exactly while it still carries the broker's default: the impl matches
// on any reply attachment so that a second `.out(Reply, ..)` fails on `ReplyOpen` and reads its
// note, rather than finding no impl at all.
impl<Mount, Policy, Rep, Slots> BindAt<Mount, Reply, Policy, ReplyLast> for (Rep, Slots)
where
    Rep: ReplyOpen,
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
    Slots: BindSlot<M, OutAttachment<M, Policy>, SlotPos<POS>>,
{
    type Out = (Rep, Slots::Out);

    fn bind_at(self, policy: Policy) -> Self::Out {
        (self.0, self.1.bind(OutAttachment::<M, Policy>::new(policy)))
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
    #[must_use]
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

/// Whether the position a chain named last admits the transform the `.transform(..)` step is
/// about to compose, given what its stack has already declared.
///
/// This is where the six facts about naming a destination live. The position states what it
/// offers - a reply type that leaves its destination open offers [`Names`], one that fixes it
/// offers [`Reads`], a batch's replies offer [`Reads`] because they answer many deliveries, a slot
/// offers what its `#[publishes(..)]` dictionary adds up to - and [`Admits`] settles it against
/// what the transform declares and what the stack has taken. Machinery; never named directly.
#[doc(hidden)]
pub trait AdmitsAt<N, Last, Mount, Def, B> {}

/// The broker's per-message options one position's live publisher takes, which is what a
/// transform mounted there writes. Machinery behind the `.transform(..)` step's own bound.
#[doc(hidden)]
pub type PositionOptions<Policy, B> =
    <<Policy as PublishPolicy<Connected<B>>>::Live as Publisher>::Options;

// The reply position: the mount token turns the definition's reply type into an offer, and the
// wiring's own stack says whether the right has been taken already.
impl<N, Policy, Enc, PL, BL, Tx, Slots, Mount, Def, B> AdmitsAt<N, ReplyLast, Mount, Def, B>
    for (WithSource<ReplyWiring<Policy, Enc, PL, BL, Tx>>, Slots)
where
    B: Broker,
    Policy: PublishPolicy<Connected<B>, Live: Publisher>,
    Mount: MountOffer<Def>,
    Def: DeclaresReply,
    PL: PublishTransform<ForReply<Def::Context>, PositionOptions<Policy, B>>,
    N: PublishTransform<ForReply<Def::Context>, PositionOptions<Policy, B>>,
    <N as PublishTransform<ForReply<Def::Context>, PositionOptions<Policy, B>>>::Destination:
        FitsOffer<
            Mount::Offer,
            <PL as PublishTransform<ForReply<Def::Context>, PositionOptions<Policy, B>>>::Destination,
            Def::Reply,
        >,
{
}

// The byte-for-byte reply carries no transform stack at all, so `AddReplyTransform` is what has to
// fail there; this arm keeps this check from failing first and hiding it.
impl<N, Policy, Slots, Mount, Def, B> AdmitsAt<N, ReplyLast, Mount, Def, B>
    for (WithSource<RawReplyWiring<Policy>>, Slots)
{
}

// One slot position, asked of the slot tuple through the positional machinery.
impl<N, const POS: usize, Rep, Slots, Mount, Def, B> AdmitsAt<N, SlotPos<POS>, Mount, Def, B>
    for (Rep, Slots)
where
    Slots: AdmitsSlotAt<N, SlotPos<POS>, B>,
{
}

// See `TransformLast`'s own arm: `Step: NamedStep` is the bound that should fail with nothing
// named, so this one must not fail first.
impl<N, Rep, Slots, Mount, Def, B> AdmitsAt<N, NoOutBound, Mount, Def, B> for (Rep, Slots) {}

/// Whether the slot bound at `Index` admits the transform the `.transform(..)` step is about to
/// compose. The positional half of [`AdmitsAt`]; machinery, never named directly.
#[doc(hidden)]
pub trait AdmitsSlotAt<N, Index, B> {}

/// What one mount token offers a reply transform, read off the definition's reply type.
///
/// A one-by-one reply offers what its type's destination form offers; a batch's replies offer
/// [`Reads`] whatever their type says, because a batch answers many deliveries and carries none of
/// their headers, so there is nothing a transform could name a destination from. Machinery.
#[doc(hidden)]
pub trait MountOffer<Def> {
    /// The most a transform on this mount's reply may do to the destination.
    type Offer;
}

/// What one reply type offers, folded out of its payload's destination declaration. Machinery
/// behind [`MountOffer`].
#[doc(hidden)]
pub trait ReplyOffer {
    /// The most a transform on this reply may do to the destination.
    type Offer;
}

// The nested projection is the machinery of the resolution, not the user's mistake.
#[diagnostic::do_not_recommend]
impl<R> ReplyOffer for R
where
    R: ReplyShape<Body: OutgoingDestination<Form: DestinationOffer>>,
{
    type Offer = <<R::Body as OutgoingDestination>::Form as DestinationOffer>::Offer;
}

/// Implements [`MountOffer`] for the mounts whose reply answers one delivery: the offer is the
/// reply type's own.
macro_rules! impl_solo_mount_offer {
    ($($mount:ident),+ $(,)?) => {$(
        impl<Def: DeclaresReply<Reply: ReplyOffer>> MountOffer<Def> for $mount {
            type Offer = <Def::Reply as ReplyOffer>::Offer;
        }
    )+};
}

impl_solo_mount_offer!(PublishMount, PublishInjectMount);

/// Implements [`MountOffer`] for the mounts that offer nothing: a batch answers many deliveries,
/// and a byte-for-byte reply has no transform stack to offer anything to.
macro_rules! impl_reads_only_mount_offer {
    ($($mount:ident),+ $(,)?) => {$(
        impl<Def> MountOffer<Def> for $mount {
            type Offer = Reads;
        }
    )+};
}

impl_reads_only_mount_offer!(
    BatchPublishMount,
    BatchPublishInjectMount,
    RawReplyMount,
    RawReplyInjectMount,
);

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

/// A position the batch-only steps can ride: the reply, and nothing else.
///
/// [`BatchTransformLast`] and [`TransactionalLast`] report the position as an associated type and
/// state this about it, so the step fails on this bound and the call site reads the note rather
/// than a bare "no method".
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`.batch_transform(..)` and `.transactional()` ride the reply position",
    label = "the position this chain named before it is not `Reply`",
    note = "name the reply's publish policy first (`.out(Reply, policy)`); a slot publish is one \
            message with no batch, so a slot takes `.transform(..)` instead and a body opens its \
            own slot transaction with `entry.begin()`"
)]
pub trait ReplyStep {}

impl ReplyStep for ReplyLast {}

/// Composes a batch transform onto the reply a mount chain named last. Reply-only: a slot
/// publish is one message, so there is no batch for a batch transform to run over. Machinery;
/// never named directly.
#[doc(hidden)]
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

// The slot and the "nothing named yet" arms: present so the step resolves as a method and fails
// on `Step: ReplyStep`, which is where the guidance lives. Neither ever runs.
impl<N, const POS: usize, Rep, Slots> BatchTransformLast<N, SlotPos<POS>> for (Rep, Slots) {
    type Step = SlotPos<POS>;
    type Out = Self;

    fn batch_transform_last(self, _transform: N) -> Self {
        self
    }
}

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

// See `BatchTransformLast`'s own arms: present so the step fails on `Step: ReplyStep`.
impl<const POS: usize, Rep, Slots> TransactionalLast<SlotPos<POS>> for (Rep, Slots) {
    type Step = SlotPos<POS>;
    type Out = Self;

    fn transactional_last(self) -> Self {
        self
    }
}

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
    #[must_use]
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

/// Implements [`TransformAt`] for each (arity, position) pair: the attachment at the position the
/// last `.out(..)` bound (`@ <position>`) grows its stack, the surrounding elements pass through.
macro_rules! impl_step_at {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        impl<N, M, Policy, Layers, Enc $(, $before)* $(, $after)*> TransformAt<N, SlotPos<$pos>>
            for ($($before,)* WithSource<OutAttachment<M, Policy, Layers, Enc>>, $($after,)*)
        {
            type Out = (
                $($before,)*
                WithSource<OutAttachment<M, Policy, PublishTransformStack<Layers, N>, Enc>>,
                $($after,)*
            );

            fn transform_at(self, transform: N) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($before,)* bound, $($after,)*) = self;
                ($($before,)* bound.map(|slot| slot.add_transform(transform)), $($after,)*)
            }
        }

        impl<N, M, Policy, Layers, Enc, B $(, $before)* $(, $after)*>
            AdmitsSlotAt<N, SlotPos<$pos>, B>
            for ($($before,)* WithSource<OutAttachment<M, Policy, Layers, Enc>>, $($after,)*)
        where
            B: Broker,
            M: OutSlot,
            Policy: PublishPolicy<Connected<B>, Live: Publisher>,
            Layers: PublishTransform<ForSlot, PositionOptions<Policy, B>>,
            N: PublishTransform<ForSlot, PositionOptions<Policy, B>>,
            <N as PublishTransform<ForSlot, PositionOptions<Policy, B>>>::Destination: FitsOffer<
                    M::Destination,
                    <Layers as PublishTransform<ForSlot, PositionOptions<Policy, B>>>::Destination,
                    M,
                >,
        {
        }

        impl<Cd, M, Policy, Layers $(, $before)* $(, $after)*> CodecAt<Cd, SlotPos<$pos>>
            for (
                $($before,)*
                WithSource<OutAttachment<M, Policy, Layers, UnnamedCodec>>,
                $($after,)*
            )
        {
            type Out = (
                $($before,)*
                WithSource<OutAttachment<M, Policy, Layers, CallCodec<Cd>>>,
                $($after,)*
            );

            fn codec_at(self, codec: Cd) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($before,)* bound, $($after,)*) = self;
                ($($before,)* bound.map(|slot| slot.name_codec(codec)), $($after,)*)
            }
        }

        impl<M, Policy, Layers, Enc $(, $before)* $(, $after)*> MapPolicyAt<SlotPos<$pos>>
            for ($($before,)* WithSource<OutAttachment<M, Policy, Layers, Enc>>, $($after,)*)
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
