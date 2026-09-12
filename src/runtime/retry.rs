//! The deferred-retry position of a mount chain: `.out(Retry, policy)`.
//!
//! A handler that asks for a delay ([`HandlerOutcome::retry_after`](super::HandlerOutcome::retry_after))
//! on a broker without native delayed redelivery gets it from a copy the runtime publishes after
//! the delay. This is where the mount site names the policy that copy leaves through, once per
//! registration. The position rides beside the reply-and-slots attachment rather than inside it:
//! the policy is erased against the chain's broker as it binds ([`RetryPairing`]), so a
//! registration that binds nothing carries nothing and the forms' commits are untouched.
//!
//! The position takes a policy and no steps: the deferred copy leaves as the bytes it arrived as,
//! with the retry-count header incremented, so there is no codec and no transform to name.

use std::fmt;

use crate::runtime::redelivery::RetryPairing;
use crate::runtime::router::{AttachRetry, RouterCommit};
use crate::runtime::slot::{
    AdmitsAt, BatchTransformLast, BindAt, CodecLast, MapPolicyLast, OutPosition, Reply, ReplyLast,
    SlotPos, TransactionalLast, TransformLast,
};
use crate::{Broker, Connected, PublishPolicy, Publisher};

/// The marker of a registration's deferred-retry position: `.out(Retry, policy)` names the publish
/// policy a `retry_after` copy leaves through.
///
/// Where that copy goes is the subscription's own answer, read at startup from
/// [`SubscriptionSource::redelivery_address`](crate::SubscriptionSource::redelivery_address): a
/// subscription name and a publish destination are one string on a subject or a topic, and
/// separate resources on Google Pub/Sub. A registration that binds this position over a
/// subscription which reports no address refuses to start, naming the subscription and its source,
/// instead of publishing copies into nothing once a handler asks for a delay.
///
/// Brokers with native delayed redelivery do not need the position: the runtime uses their
/// [`nack_after`](crate::IncomingMessage::nack_after) instead. Without it, a `retry_after` on a
/// non-native broker degrades to an immediate requeue (with a warning).
///
/// The position takes a policy and nothing else - no codec, no transform - and the generated
/// `AsyncAPI` document ignores it: the copy goes to the subscription's own address, not to a
/// declared channel.
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

/// The position `.out(Retry, ..)` names, and the one a chain's steps cannot ride: the deferred
/// copy carries no codec and no transform stack, so nothing follows the call.
///
/// It is not a [`NamedStep`](crate::runtime::NamedStep), so a `.codec(..)` or `.transform(..)`
/// after the call fails on that bound and the call site reads the guidance.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct RetryPos;

/// A mount chain's attachment with the deferred-retry position bound: the reply-and-slots
/// attachment, and beside it the pairing the bound policy owes.
///
/// Wrapping rather than growing the attachment tuple is what keeps the forms' commits untouched,
/// and what makes a second `.out(Retry, ..)` a compile error: nothing binds the position on a
/// wrapper that already carries one.
#[doc(hidden)]
pub struct Retried<Attach, B: Broker> {
    attach: Attach,
    retry: RetryPairing<B>,
}

// The chain's own state, like `RouterWith`'s: what it carries is machinery, not data to print.
impl<Attach, B: Broker> fmt::Debug for Retried<Attach, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Retried").finish_non_exhaustive()
    }
}

impl<Attach, B: Broker> Retried<Attach, B> {
    /// Grows the wrapped attachment in place: how every step after the call reaches it.
    fn map<NewAttach>(self, f: impl FnOnce(Attach) -> NewAttach) -> Retried<NewAttach, B> {
        Retried {
            attach: f(self.attach),
            retry: self.retry,
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

// The position itself: the policy is erased against the chain's broker here, at the call, which is
// where the broker is known and the policy is still typed.
impl<Mount, B, Attach, Policy> OutPosition<Mount, B, Attach, Policy, RetryPos> for Retry
where
    B: Broker + 'static,
    Attach: RetryOpen,
    Policy: PublishPolicy<Connected<B>> + Send + 'static,
    Policy::Live: Publisher + 'static,
{
    type Out = Retried<Attach, B>;

    fn bind(attach: Attach, policy: Policy) -> Self::Out {
        Retried {
            attach,
            retry: RetryPairing::new(policy),
        }
    }
}

/// One `.out(marker, policy)` call on a registration that is already a route: the surface a form
/// with nothing of the handler's own to attach hands back.
///
/// Keyed by the marker, like [`OutPosition`], so the call site's own argument settles which
/// position is meant. Only the deferred retry can be bound here: every other position belongs to
/// a publish the handler makes, and a form that makes none has none. Machinery; never named
/// directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this registration has no unbound publish position marked `{Self}`",
    label = "`.out({Self}, ..)` has no position to bind here",
    note = "a handler that publishes nothing of its own binds one position, `Retry`, the deferred \
            `retry_after` copy's publisher - and binds it once"
)]
pub trait RoutePosition<B, Chain, Policy> {
    /// The registration with that position bound.
    type Out;

    /// Binds it.
    fn bind(chain: Chain, policy: Policy) -> Self::Out;
}

impl<B, Chain, Policy> RoutePosition<B, Chain, Policy> for Retry
where
    B: Broker + 'static,
    Chain: AttachRetry<B>,
    Policy: PublishPolicy<Connected<B>> + Send + 'static,
    Policy::Live: Publisher + 'static,
{
    type Out = <Chain as AttachRetry<B>>::Out;

    fn bind(chain: Chain, policy: Policy) -> Self::Out {
        chain.attach_retry(RetryPairing::new(policy))
    }
}

// The reply and the slots keep binding after the retry, in any order.
impl<Mount, Policy, Attach, B> BindAt<Mount, Reply, Policy, ReplyLast> for Retried<Attach, B>
where
    B: Broker,
    Attach: BindAt<Mount, Reply, Policy, ReplyLast>,
{
    type Out = Retried<Attach::Out, B>;

    fn bind_at(self, policy: Policy) -> Self::Out {
        self.map(|attach| attach.bind_at(policy))
    }
}

impl<Mount, M, Policy, Attach, B, const POS: usize> BindAt<Mount, M, Policy, SlotPos<POS>>
    for Retried<Attach, B>
where
    B: Broker,
    Attach: BindAt<Mount, M, Policy, SlotPos<POS>>,
{
    type Out = Retried<Attach::Out, B>;

    fn bind_at(self, policy: Policy) -> Self::Out {
        self.map(|attach| attach.bind_at(policy))
    }
}

// The steps a chain can name ride the position named before them, and after `.out(Retry, ..)` that
// position takes none: these arms exist so each step resolves as a method and fails on its own
// bound (`Step: NamedStep`, `Step: ReplyStep`), which is where the guidance lives. None of them
// ever runs.
impl<Cd, Rep, Slots> CodecLast<Cd, RetryPos> for (Rep, Slots) {
    type Step = RetryPos;
    type Out = Self;

    fn codec_last(self, _codec: Cd) -> Self {
        self
    }
}

impl<N, Rep, Slots> TransformLast<N, RetryPos> for (Rep, Slots) {
    type Step = RetryPos;
    type Out = Self;

    fn transform_last(self, _transform: N) -> Self {
        self
    }
}

impl<N, Rep, Slots, Mount, Def> AdmitsAt<N, RetryPos, Mount, Def> for (Rep, Slots) {}

impl<N, Rep, Slots> BatchTransformLast<N, RetryPos> for (Rep, Slots) {
    type Step = RetryPos;
    type Out = Self;

    fn batch_transform_last(self, _transform: N) -> Self {
        self
    }
}

impl<Rep, Slots> TransactionalLast<RetryPos> for (Rep, Slots) {
    type Step = RetryPos;
    type Out = Self;

    fn transactional_last(self) -> Self {
        self
    }
}

impl<Rep, Slots> MapPolicyLast<RetryPos> for (Rep, Slots) {
    type Step = RetryPos;
    type Policy = ();

    fn map_policy_last(self, _f: impl FnOnce(())) -> Self {
        self
    }
}

// Every step a chain names after the retry position rides the attachment underneath, unchanged:
// the retry carries no wiring of its own for a step to reach.
impl<Cd, Last, Attach, B> CodecLast<Cd, Last> for Retried<Attach, B>
where
    B: Broker,
    Attach: CodecLast<Cd, Last>,
{
    type Step = Attach::Step;
    type Out = Retried<Attach::Out, B>;

    fn codec_last(self, codec: Cd) -> Self::Out {
        self.map(|attach| attach.codec_last(codec))
    }
}

impl<N, Last, Attach, B> TransformLast<N, Last> for Retried<Attach, B>
where
    B: Broker,
    Attach: TransformLast<N, Last>,
{
    type Step = Attach::Step;
    type Out = Retried<Attach::Out, B>;

    fn transform_last(self, transform: N) -> Self::Out {
        self.map(|attach| attach.transform_last(transform))
    }
}

impl<N, Last, Attach, B, Mount, Def> AdmitsAt<N, Last, Mount, Def> for Retried<Attach, B>
where
    B: Broker,
    Attach: AdmitsAt<N, Last, Mount, Def>,
{
}

impl<N, Last, Attach, B> BatchTransformLast<N, Last> for Retried<Attach, B>
where
    B: Broker,
    Attach: BatchTransformLast<N, Last>,
{
    type Step = Attach::Step;
    type Out = Retried<Attach::Out, B>;

    fn batch_transform_last(self, transform: N) -> Self::Out {
        self.map(|attach| attach.batch_transform_last(transform))
    }
}

impl<Last, Attach, B> TransactionalLast<Last> for Retried<Attach, B>
where
    B: Broker,
    Attach: TransactionalLast<Last>,
{
    type Step = Attach::Step;
    type Out = Retried<Attach::Out, B>;

    fn transactional_last(self) -> Self::Out {
        self.map(TransactionalLast::transactional_last)
    }
}

impl<Last, Attach, B> MapPolicyLast<Last> for Retried<Attach, B>
where
    B: Broker,
    Attach: MapPolicyLast<Last>,
{
    type Step = Attach::Step;
    type Policy = Attach::Policy;

    fn map_policy_last(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self {
        self.map(|attach| attach.map_policy_last(f))
    }
}

// The commit is the attachment's own, with the pairing handed to the registration it grew into.
impl<Mount, R, Def, Attach, B> RouterCommit<Mount, R, Def> for Retried<Attach, B>
where
    B: Broker + 'static,
    Attach: RouterCommit<Mount, R, Def>,
    Attach::Out: AttachRetry<B>,
{
    type Out = <Attach::Out as AttachRetry<B>>::Out;

    fn commit(self, def: Def, router: R) -> Self::Out {
        self.attach.commit(def, router).attach_retry(self.retry)
    }
}
