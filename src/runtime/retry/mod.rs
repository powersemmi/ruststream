//! The deferred-retry position of a mount chain: `.out(Retry, policy)`.
//!
//! A handler that asks for a delay ([`HandlerOutcome::retry_after`](super::HandlerOutcome::retry_after))
//! on a broker without native delayed redelivery gets it from a copy the runtime publishes after
//! the delay. [`Retry`] is the `Out` slot that copy leaves through; the position is filled from the
//! broker's default publish policy and a mount site replaces it once per registration.
//!
//! The position is a slot in the full sense: the call attaches an [`OutAttachment`] like any
//! `.out(marker, policy)`, the steps after it are the slot steps (`.codec(..)`, `.transform(..)`,
//! a broker's own `map_publisher` settings), and the mount wires the attachment into the same
//! triple a handler's slot resolves from - the policy, the codec and the composed publish
//! pipeline. What differs is who publishes: the runtime does, from the delivery it is deferring,
//! so the copy travels the slot's pipeline as bytes it already has and no codec encodes it.
//!
//! The chain carries the bound position on [`Retried`], a wrapper around whatever attachment the
//! registration already had. That is what keeps every form's commit untouched - a registration
//! that binds nothing carries nothing - and what makes a second `.out(Retry, ..)` a compile
//! error: nothing binds the position on a wrapper that already holds one.

mod declare;
mod destination;

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

pub use declare::{
    Absent, CapOpen, DeadLetterOpen, DeclareCap, DeclareDeadLetter, DeclareMount, Declaring,
    Present, RouteDeclaring, StepOpen, StepTaken,
};
pub use destination::{
    DeclaredDestination, DestinationLast, DestinationOpen, FixedDestination, OpenDestination,
    RetryOffer, RetryStackUse,
};

use crate::Broker;
use crate::runtime::publish::{CallCodec, PublishTransformStack, Reads, UnnamedCodec};
use crate::runtime::router::{AttachRetry, Router, RouterCommit, RouterWith};
use crate::runtime::slot::{
    AdmitsAt, BatchTransformLast, BindAt, CodecLast, MapPolicyLast, NamedStep, NoReply,
    OutAttachment, OutSlot, Reply, ReplyLast, SlotPos, TransactionalLast, TransformLast,
};

/// The marker of a registration's deferred-retry slot: `.out(Retry, policy)` names the publish
/// policy a `retry_after` copy leaves through.
///
/// The slot is an ordinary [`Out`](super::Out) position, so the chain after the call takes the
/// steps every slot takes: `.codec(..)` names the position's codec, `.transform(..)` composes a
/// [`PublishTransform`](crate::runtime::PublishTransform) the copy travels through, and a
/// broker's own publisher settings reach the
/// policy through [`map_publisher`](super::MapPublisher::map_publisher). The deferred copy is
/// already serialized - it is the delivery's own bytes with the retry count incremented - so it
/// passes the codec by, the way a [`Serialized`](super::Serialized) value does on any slot.
///
/// The position is filled before the call: every registration whose subscription declares
/// [`AddressedCopies`](crate::AddressedCopies) or
/// [`NamedCopies`](crate::NamedCopies) gets a publisher from the broker's own
/// [`DefaultPublish`](crate::DefaultPublish) policy, and this call replaces it. On a descriptor
/// that declares [`BrokerMoves`](crate::BrokerMoves) the call does not compile: the broker moves
/// the delivery itself and there is nothing of this process's to publish. It comes after the
/// registration's declaration, [`max_attempts`](super::RouterWith::max_attempts) and
/// [`dead_letter`](super::RouterWith::dead_letter).
///
/// Where a copy goes is the subscription's own answer, read at startup from
/// [`RedeliveryAddressed`](crate::RedeliveryAddressed): a
/// subscription name and a publish destination are one string on a subject or a topic, and
/// separate resources on Google Pub/Sub. A registration over a subscription which publishes its
/// copies here and reports no address refuses to start, naming the subscription and its
/// descriptor, instead of publishing copies into nothing once a handler asks for a delay. The slot
/// therefore never lets a transform name the destination, and the generated `AsyncAPI` document
/// ignores the position: a copy goes to the subscription's own address, or to the declared
/// dead-letter destination, and neither is this position's to name.
///
/// Brokers with native delayed redelivery do not need the position: the runtime uses their
/// [`nack_after`](crate::IncomingMessage::nack_after) instead, and no copy is published there.
///
/// # Cancel safety
///
/// The deferred copy is at-most-once over the delay window: see
/// [`HandlerOutcome::retry_after`](super::HandlerOutcome::retry_after).
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::memory::prelude::*;
/// # use ruststream::subscriber;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Order { id: u64 }
///
/// #[subscriber("orders")]
/// async fn reconcile(order: &Order) -> HandlerOutcome {
///     let _ = order.id;
///     HandlerOutcome::retry_after(Duration::from_secs(30))
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(reconcile).out(Retry, Publish);
///     })
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Retry;

// The copy is published to the subscription's own redelivery address, which the mount site does
// not choose, so no transform on this slot may name a destination. The slot declares nothing it
// publishes: the document reports channels, and this one is the subscription's own.
impl OutSlot for Retry {
    const NAME: &'static str = "Retry";
    type Destination = Reads;
}

/// The position `.out(Retry, ..)` names, and the index the steps after it ride. Machinery; never
/// named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct RetryPos;

impl NamedStep for RetryPos {}

/// The commit token of a deferred-retry position bound on a registration that is already a route:
/// the chain has no definition left to mount, only the slot to wire. Machinery; never named
/// directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct RetryMount;

/// A mount chain's attachment with the deferred-retry slot bound: whatever the registration
/// already attached, and the slot's own attachment beside it.
///
/// Wrapping rather than growing the attachment tuple is what keeps the forms' commits untouched,
/// and what makes a second `.out(Retry, ..)` a compile error: nothing binds the position on a
/// wrapper that already carries one.
#[doc(hidden)]
pub struct Retried<Under, Attachment, Dest = OpenDestination> {
    under: Under,
    slot: Attachment,
    destination: Option<Cow<'static, str>>,
    _dest: PhantomData<fn() -> Dest>,
}

// The chain's own state, like `RouterWith`'s: what it carries is machinery, not data to print.
impl<Under, Attachment, Dest> fmt::Debug for Retried<Under, Attachment, Dest> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Retried")
            .field("destination", &self.destination)
            .finish_non_exhaustive()
    }
}

impl<Under, Attachment, Dest> Retried<Under, Attachment, Dest> {
    pub(crate) fn new(under: Under, slot: Attachment) -> Self {
        Self {
            under,
            slot,
            destination: None,
            _dest: PhantomData,
        }
    }

    /// Grows the wrapped attachment in place: how every step on another position reaches it.
    fn map<NewUnder>(
        self,
        f: impl FnOnce(Under) -> NewUnder,
    ) -> Retried<NewUnder, Attachment, Dest> {
        Retried {
            under: f(self.under),
            slot: self.slot,
            destination: self.destination,
            _dest: PhantomData,
        }
    }

    /// Grows the retry slot's own attachment: how every step on this position reaches it.
    fn map_slot<NewAttachment>(
        self,
        f: impl FnOnce(Attachment) -> NewAttachment,
    ) -> Retried<Under, NewAttachment, Dest> {
        Retried {
            under: self.under,
            slot: f(self.slot),
            destination: self.destination,
            _dest: PhantomData,
        }
    }

    /// Fixes where the copies go: the `.to(name)` step.
    fn name_destination(
        self,
        destination: Cow<'static, str>,
    ) -> Retried<Under, Attachment, FixedDestination> {
        Retried {
            under: self.under,
            slot: self.slot,
            destination: Some(destination),
            _dest: PhantomData,
        }
    }
}

/// A registration whose deferred-retry position is still open: what `.out(Retry, policy)` binds.
///
/// Held by every mount chain's attachment and by every route a mount site produces, and never by
/// a registration that already bound the position, so a second call fails here and the call site
/// reads the note. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this registration has no open deferred-retry position",
    label = "`.out(Retry, ..)` has nothing to bind here",
    note = "`.out(Retry, policy)` - or its sugar `.out_retry(policy)` - names the publish policy \
            a deferred `retry_after` copy leaves through, once per registration"
)]
pub trait RetryOpen {}

// Every mount chain starts with the position open; binding it wraps the attachment, and the
// wrapper has no impl.
impl<Rep, Slots> RetryOpen for (Rep, Slots) {}

// The position itself. It is keyed by the marker like every slot, so `.out(Retry, policy)`
// resolves through the same `OutPosition` blanket that carries a handler's own slots.
impl<Mount, Policy, Attach: RetryOpen + DeclaredDestination> BindAt<Mount, Retry, Policy, RetryPos>
    for Attach
{
    type Out = Retried<Self, OutAttachment<Retry, Policy>, Attach::Destination>;

    fn bind_at(self, policy: Policy) -> Self::Out {
        Retried::new(self, OutAttachment::new(policy))
    }
}

/// One `.out(marker, policy)` call on a registration that is already a route: the surface a form
/// with nothing of the handler's own to attach hands back.
///
/// Keyed by the marker, like [`OutPosition`](super::OutPosition), so the call site's own argument
/// settles which position is meant. Only the deferred retry can be bound here: every other
/// position belongs to a publish the handler makes, and a form that makes none has none. The call
/// opens a mount chain over the finished route, so the steps after it are the ones every position
/// offers. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this registration has no unbound publish position marked `{Self}`",
    label = "`.out({Self}, ..)` has no position to bind here",
    note = "a handler that publishes nothing of its own binds one position, `Retry`, the deferred \
            `retry_after` copy's publisher - and binds it once"
)]
pub trait RoutePosition<B, Chain, Policy> {
    /// The chain that position's steps continue on.
    type Out;

    /// Binds it.
    fn bind(chain: Chain, policy: Policy) -> Self::Out;
}

/// The chain a route-level `.out(Retry, policy)` opens: the finished router, with the slot's
/// attachment waiting for the steps and for the commit that wires it.
#[doc(hidden)]
pub type RetryChain<Chain, Policy> = RouterWith<
    RetryMount,
    Chain,
    (),
    Retried<(NoReply, ()), OutAttachment<Retry, Policy>, OpenDestination>,
    RetryPos,
>;

impl<B, Head, Tail, C, Layers, Pipe, Policy>
    RoutePosition<B, Router<B, (Head, Tail), C, Layers, Pipe>, Policy> for Retry
where
    B: Broker + 'static,
    Head: RetryOpen,
{
    type Out = RetryChain<Router<B, (Head, Tail), C, Layers, Pipe>, Policy>;

    fn bind(chain: Router<B, (Head, Tail), C, Layers, Pipe>, policy: Policy) -> Self::Out {
        RouterWith::new(
            (),
            Retried::new((NoReply, ()), OutAttachment::new(policy)),
            chain,
        )
    }
}

// The registration a route-level binding wraps is already mounted, so its "commit" is the route
// itself; the wrapper's own commit then wires the slot onto it.
impl<R> RouterCommit<RetryMount, R, ()> for (NoReply, ()) {
    type Out = R;

    fn commit(self, (): (), router: R) -> R {
        router
    }
}

// The reply and the handler's own slots keep binding after the retry, in any order.
impl<Mount, Policy, Under, Attachment, Dest> BindAt<Mount, Reply, Policy, ReplyLast>
    for Retried<Under, Attachment, Dest>
where
    Under: BindAt<Mount, Reply, Policy, ReplyLast>,
{
    type Out = Retried<Under::Out, Attachment, Dest>;

    fn bind_at(self, policy: Policy) -> Self::Out {
        self.map(|under| under.bind_at(policy))
    }
}

impl<Mount, M, Policy, Under, Attachment, Dest, const POS: usize>
    BindAt<Mount, M, Policy, SlotPos<POS>> for Retried<Under, Attachment, Dest>
where
    Under: BindAt<Mount, M, Policy, SlotPos<POS>>,
{
    type Out = Retried<Under::Out, Attachment, Dest>;

    fn bind_at(self, policy: Policy) -> Self::Out {
        self.map(|under| under.bind_at(policy))
    }
}

// The steps on the retry position itself: each one is the slot attachment's own operation, the
// same call the handler's slots make through the positional machinery.

impl<Cd, Under, Policy, Layers, Dest> CodecLast<Cd, RetryPos>
    for Retried<Under, OutAttachment<Retry, Policy, Layers, UnnamedCodec>, Dest>
{
    type Step = RetryPos;
    type Out = Retried<Under, OutAttachment<Retry, Policy, Layers, CallCodec<Cd>>, Dest>;

    fn codec_last(self, codec: Cd) -> Self::Out {
        self.map_slot(|slot| slot.name_codec(codec))
    }
}

impl<N, Under, Policy, Layers, Enc, Dest> TransformLast<N, RetryPos>
    for Retried<Under, OutAttachment<Retry, Policy, Layers, Enc>, Dest>
{
    type Step = RetryPos;
    type Out =
        Retried<Under, OutAttachment<Retry, Policy, PublishTransformStack<Layers, N>, Enc>, Dest>;

    fn transform_last(self, transform: N) -> Self::Out {
        self.map_slot(|slot| slot.add_transform(transform))
    }
}

// What this position offers a transform depends on the subscription's descriptor, which the chain
// does not know here: a route-level chain carries no definition at all, and no def trait exposes
// the descriptor uniformly. The whole check therefore runs at the commit (see `AttachRetry`),
// where the route is in hand, and this arm admits every transform so the commit is what speaks.
impl<N, Under, Policy, Layers, Enc, Mount, Def, B, Dest> AdmitsAt<N, RetryPos, Mount, Def, B>
    for Retried<Under, OutAttachment<Retry, Policy, Layers, Enc>, Dest>
{
}

impl<Under, Policy, Layers, Enc, Dest> MapPolicyLast<RetryPos>
    for Retried<Under, OutAttachment<Retry, Policy, Layers, Enc>, Dest>
{
    type Step = RetryPos;
    type Policy = Policy;

    fn map_policy_last(self, f: impl FnOnce(Policy) -> Policy) -> Self {
        self.map_slot(|slot| slot.map_policy(f))
    }
}

// The two reply-only steps, as a slot carries them: the arm exists so the call resolves as a
// method and fails on `Step: ReplyStep`, which is where the guidance lives. Neither ever runs.
impl<N, Under, Attachment, Dest> BatchTransformLast<N, RetryPos>
    for Retried<Under, Attachment, Dest>
{
    type Step = RetryPos;
    type Out = Self;

    fn batch_transform_last(self, _transform: N) -> Self {
        self
    }
}

impl<Under, Attachment, Dest> TransactionalLast<RetryPos> for Retried<Under, Attachment, Dest> {
    type Step = RetryPos;
    type Out = Self;

    fn transactional_last(self) -> Self {
        self
    }
}

// The steps on another position ride the attachment underneath, unchanged. They are written per
// position rather than over any `Last`, because the retry's own arms above are the ones that have
// to win at `RetryPos`.

impl<Cd, Under, Attachment, Dest> CodecLast<Cd, ReplyLast> for Retried<Under, Attachment, Dest>
where
    Under: CodecLast<Cd, ReplyLast>,
{
    type Step = Under::Step;
    type Out = Retried<Under::Out, Attachment, Dest>;

    fn codec_last(self, codec: Cd) -> Self::Out {
        self.map(|under| under.codec_last(codec))
    }
}

impl<Cd, Under, Attachment, Dest, const POS: usize> CodecLast<Cd, SlotPos<POS>>
    for Retried<Under, Attachment, Dest>
where
    Under: CodecLast<Cd, SlotPos<POS>>,
{
    type Step = Under::Step;
    type Out = Retried<Under::Out, Attachment, Dest>;

    fn codec_last(self, codec: Cd) -> Self::Out {
        self.map(|under| under.codec_last(codec))
    }
}

impl<N, Under, Attachment, Dest> TransformLast<N, ReplyLast> for Retried<Under, Attachment, Dest>
where
    Under: TransformLast<N, ReplyLast>,
{
    type Step = Under::Step;
    type Out = Retried<Under::Out, Attachment, Dest>;

    fn transform_last(self, transform: N) -> Self::Out {
        self.map(|under| under.transform_last(transform))
    }
}

impl<N, Under, Attachment, Dest, const POS: usize> TransformLast<N, SlotPos<POS>>
    for Retried<Under, Attachment, Dest>
where
    Under: TransformLast<N, SlotPos<POS>>,
{
    type Step = Under::Step;
    type Out = Retried<Under::Out, Attachment, Dest>;

    fn transform_last(self, transform: N) -> Self::Out {
        self.map(|under| under.transform_last(transform))
    }
}

impl<N, Under, Attachment, Dest, Mount, Def, B> AdmitsAt<N, ReplyLast, Mount, Def, B>
    for Retried<Under, Attachment, Dest>
where
    Under: AdmitsAt<N, ReplyLast, Mount, Def, B>,
{
}

impl<N, Under, Attachment, Dest, Mount, Def, B, const POS: usize>
    AdmitsAt<N, SlotPos<POS>, Mount, Def, B> for Retried<Under, Attachment, Dest>
where
    Under: AdmitsAt<N, SlotPos<POS>, Mount, Def, B>,
{
}

impl<N, Under, Attachment, Dest> BatchTransformLast<N, ReplyLast>
    for Retried<Under, Attachment, Dest>
where
    Under: BatchTransformLast<N, ReplyLast>,
{
    type Step = Under::Step;
    type Out = Retried<Under::Out, Attachment, Dest>;

    fn batch_transform_last(self, transform: N) -> Self::Out {
        self.map(|under| under.batch_transform_last(transform))
    }
}

impl<N, Under, Attachment, Dest, const POS: usize> BatchTransformLast<N, SlotPos<POS>>
    for Retried<Under, Attachment, Dest>
where
    Under: BatchTransformLast<N, SlotPos<POS>>,
{
    type Step = Under::Step;
    type Out = Retried<Under::Out, Attachment, Dest>;

    fn batch_transform_last(self, transform: N) -> Self::Out {
        self.map(|under| under.batch_transform_last(transform))
    }
}

impl<Under, Attachment, Dest> TransactionalLast<ReplyLast> for Retried<Under, Attachment, Dest>
where
    Under: TransactionalLast<ReplyLast>,
{
    type Step = Under::Step;
    type Out = Retried<Under::Out, Attachment, Dest>;

    fn transactional_last(self) -> Self::Out {
        self.map(TransactionalLast::transactional_last)
    }
}

impl<Under, Attachment, Dest, const POS: usize> TransactionalLast<SlotPos<POS>>
    for Retried<Under, Attachment, Dest>
where
    Under: TransactionalLast<SlotPos<POS>>,
{
    type Step = Under::Step;
    type Out = Retried<Under::Out, Attachment, Dest>;

    fn transactional_last(self) -> Self::Out {
        self.map(TransactionalLast::transactional_last)
    }
}

impl<Under: MapPolicyLast<ReplyLast>, Attachment, Dest> MapPolicyLast<ReplyLast>
    for Retried<Under, Attachment, Dest>
{
    type Step = Under::Step;
    type Policy = Under::Policy;

    fn map_policy_last(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self {
        self.map(|under| under.map_policy_last(f))
    }
}

impl<Under, Attachment, Dest, const POS: usize> MapPolicyLast<SlotPos<POS>>
    for Retried<Under, Attachment, Dest>
where
    Under: MapPolicyLast<SlotPos<POS>>,
{
    type Step = Under::Step;
    type Policy = Under::Policy;

    fn map_policy_last(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self {
        self.map(|under| under.map_policy_last(f))
    }
}

// The `.to(name)` step: open on a registration that named the retry publisher and no destination,
// closed everywhere else. The closed arms exist so the call resolves as a method and fails on
// `Step: DestinationOpen`, which is where the guidance lives; neither ever runs.
impl<Under, Attachment> DestinationLast<RetryPos> for Retried<Under, Attachment, OpenDestination> {
    type Step = StepOpen;
    type Out = Retried<Under, Attachment, FixedDestination>;

    fn destination_last(self, destination: Cow<'static, str>) -> Self::Out {
        self.name_destination(destination)
    }
}

impl<Under, Attachment> DestinationLast<RetryPos> for Retried<Under, Attachment, FixedDestination> {
    type Step = StepTaken;
    type Out = Self;

    fn destination_last(self, _destination: Cow<'static, str>) -> Self {
        self
    }
}

/// Implements the closed arm of [`DestinationLast`] for the positions that take no destination of
/// their own: a reply names one through its type or the mount site, a slot through the type it
/// publishes.
macro_rules! no_destination_step {
    ($([$($extra:tt)*] $index:ty => $attach:ty),+ $(,)?) => {$(
        impl<$($extra)*> DestinationLast<$index> for $attach {
            type Step = StepTaken;
            type Out = Self;

            fn destination_last(self, _destination: Cow<'static, str>) -> Self {
                self
            }
        }
    )+};
}

no_destination_step!(
    [Rep, Slots] ReplyLast => (Rep, Slots),
    [Rep, Slots, const POS: usize] SlotPos<POS> => (Rep, Slots),
    [Under, Cap, Dead, Dest] ReplyLast => Declaring<Under, Cap, Dead, Dest>,
    [Under, Cap, Dead, Dest, const POS: usize] SlotPos<POS> => Declaring<Under, Cap, Dead, Dest>,
    [Under, Attachment, Dest] ReplyLast => Retried<Under, Attachment, Dest>,
    [Under, Attachment, Dest, const POS: usize] SlotPos<POS> => Retried<Under, Attachment, Dest>,
);

// The commit is the attachment's own, with the slot wired onto the registration it grew into.
impl<Mount, R, Def, Under, Policy, Layers, Enc, Dest> RouterCommit<Mount, R, Def>
    for Retried<Under, OutAttachment<Retry, Policy, Layers, Enc>, Dest>
where
    Under: RouterCommit<Mount, R, Def>,
    Under::Out: AttachRetry<OutAttachment<Retry, Policy, Layers, Enc>, Dest>,
{
    type Out = <Under::Out as AttachRetry<OutAttachment<Retry, Policy, Layers, Enc>, Dest>>::Out;

    fn commit(self, def: Def, router: R) -> Self::Out {
        self.under
            .commit(def, router)
            .attach_retry(self.slot, self.destination)
    }
}
