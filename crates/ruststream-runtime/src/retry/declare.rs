//! The two declaration steps of a mount chain: `max_attempts(n)` and `dead_letter(name)`.
//!
//! They are the registration's own statement about its retries - how many deliveries one message
//! gets, and where it goes when they run out - and they are the same two words on every broker.
//! The declaration reaches the subscription descriptor at startup
//! ([`SubscriptionSource::declare_retry`](crate::SubscriptionSource::declare_retry)), so a broker
//! that applies a delivery limit and a dead-letter destination itself configures its queue with
//! them; everywhere else the runtime applies them on the retry path.
//!
//! The chain carries the declaration on [`Declaring`], a wrapper around whatever attachment the
//! registration already had, with one type parameter per step. That is what makes each step
//! bindable once - the parameter is [`Present`] afterwards, and no step binds on a present one -
//! and what keeps the declaration ahead of `out_retry(policy)`: a chain that bound the publisher
//! carries [`Retried`](super::Retried), which takes no declaration.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU32;

use crate::RetryDeclaration;
use crate::runtime::router::{AttachDeclaration, RouterCommit};
use crate::runtime::slot::{
    AdmitsAt, BatchTransformLast, BindAt, CodecLast, MapPolicyLast, NoReply, Reply, ReplyLast,
    SlotPos, TransactionalLast, TransformLast,
};

use super::{Retried, RetryOpen};

/// A declaration step this registration has not taken yet. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Absent;

/// A declaration step this registration has taken. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Present;

/// A mount chain's attachment with the registration's retry declaration beside it.
#[doc(hidden)]
pub struct Declaring<Under, Cap = Absent, Dead = Absent> {
    under: Under,
    declaration: RetryDeclaration,
    _steps: PhantomData<fn() -> (Cap, Dead)>,
}

// The chain's own state, like `RouterWith`'s: what it carries is machinery, not data to print.
impl<Under, Cap, Dead> fmt::Debug for Declaring<Under, Cap, Dead> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Declaring")
            .field("declaration", &self.declaration)
            .finish_non_exhaustive()
    }
}

impl<Under, Cap, Dead> Declaring<Under, Cap, Dead> {
    fn new(under: Under, declaration: RetryDeclaration) -> Self {
        Self {
            under,
            declaration,
            _steps: PhantomData,
        }
    }

    /// Grows the wrapped attachment in place: how every step on a publish position reaches it.
    fn map<NewUnder>(self, f: impl FnOnce(Under) -> NewUnder) -> Declaring<NewUnder, Cap, Dead> {
        Declaring {
            under: f(self.under),
            declaration: self.declaration,
            _steps: PhantomData,
        }
    }

    /// Takes the other declaration step, moving the typestate for it.
    fn declare<NewCap, NewDead>(
        self,
        f: impl FnOnce(RetryDeclaration) -> RetryDeclaration,
    ) -> Declaring<Under, NewCap, NewDead> {
        Declaring {
            under: self.under,
            declaration: f(self.declaration),
            _steps: PhantomData,
        }
    }
}

/// A declaration step this registration can still take. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct StepOpen;

/// A declaration step this registration is past: it took it already, or the chain has moved on to
/// the publisher. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct StepTaken;

/// Whether the chain can still be capped. Machinery; the guidance a mount site reads lives here.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`max_attempts(..)` has nothing to declare here",
    label = "`{Self}`: the cap was declared already, or the chain has moved past the \
             declaration",
    note = "a registration declares `max_attempts(n)` once, right after `include(..)` and before \
            `out_retry(policy)`: \
            `b.include(reconcile).max_attempts(nonzero!(5)).dead_letter(\"orders.dead\")`"
)]
pub trait CapOpen {}

impl CapOpen for StepOpen {}

/// Declares how many deliveries one message of this registration gets. Machinery; never named
/// directly.
#[doc(hidden)]
pub trait DeclareCap {
    /// Whether this chain can still be capped; where the guidance lives.
    type Step;

    /// The attachment carrying the cap.
    type Out;

    /// Declares it.
    fn declare_cap(self, attempts: NonZeroU32) -> Self::Out;
}

// The first step of a registration that has declared nothing: wrap what `include` produced.
impl<Rep, Slots> DeclareCap for (Rep, Slots) {
    type Step = StepOpen;
    type Out = Declaring<Self, Present, Absent>;

    fn declare_cap(self, attempts: NonZeroU32) -> Self::Out {
        Declaring::new(self, RetryDeclaration::new().with_max_attempts(attempts))
    }
}

// The second step, over a registration that has already named a destination.
impl<Under, Dead> DeclareCap for Declaring<Under, Absent, Dead> {
    type Step = StepOpen;
    type Out = Declaring<Under, Present, Dead>;

    fn declare_cap(self, attempts: NonZeroU32) -> Self::Out {
        self.declare(|declaration| declaration.with_max_attempts(attempts))
    }
}

// The two closed arms, as the slot steps carry theirs: the impl exists so the call resolves as a
// method and fails on `Step: CapOpen`, which is where the guidance lives. Neither ever runs.
impl<Under, Dead> DeclareCap for Declaring<Under, Present, Dead> {
    type Step = StepTaken;
    type Out = Self;

    fn declare_cap(self, _attempts: NonZeroU32) -> Self {
        self
    }
}

impl<Under, Attachment, Dest> DeclareCap for Retried<Under, Attachment, Dest> {
    type Step = StepTaken;
    type Out = Self;

    fn declare_cap(self, _attempts: NonZeroU32) -> Self {
        self
    }
}

/// Whether the chain can still name a dead-letter destination. Machinery; the guidance a mount
/// site reads lives here.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`dead_letter(..)` has nothing to declare here",
    label = "`{Self}`: the destination was declared already, or the chain has moved past the \
             declaration",
    note = "a registration declares `dead_letter(name)` once, right after `include(..)` and \
            before `out_retry(policy)`: \
            `b.include(reconcile).max_attempts(nonzero!(5)).dead_letter(\"orders.dead\")`"
)]
pub trait DeadLetterOpen {}

impl DeadLetterOpen for StepOpen {}

/// Declares where a delivery of this registration goes once its attempts run out. Machinery;
/// never named directly.
#[doc(hidden)]
pub trait DeclareDeadLetter {
    /// Whether this chain can still name one; where the guidance lives.
    type Step;

    /// The attachment carrying the destination.
    type Out;

    /// Declares it.
    fn declare_dead_letter(self, destination: Cow<'static, str>) -> Self::Out;
}

impl<Rep, Slots> DeclareDeadLetter for (Rep, Slots) {
    type Step = StepOpen;
    type Out = Declaring<Self, Absent, Present>;

    fn declare_dead_letter(self, destination: Cow<'static, str>) -> Self::Out {
        Declaring::new(self, RetryDeclaration::new().with_dead_letter(destination))
    }
}

impl<Under, Cap> DeclareDeadLetter for Declaring<Under, Cap, Absent> {
    type Step = StepOpen;
    type Out = Declaring<Under, Cap, Present>;

    fn declare_dead_letter(self, destination: Cow<'static, str>) -> Self::Out {
        self.declare(|declaration| declaration.with_dead_letter(destination))
    }
}

// See the capped arms above: these exist so the call resolves and fails on `Step: DeadLetterOpen`.
impl<Under, Cap> DeclareDeadLetter for Declaring<Under, Cap, Present> {
    type Step = StepTaken;
    type Out = Self;

    fn declare_dead_letter(self, _destination: Cow<'static, str>) -> Self {
        self
    }
}

impl<Under, Attachment, Dest> DeclareDeadLetter for Retried<Under, Attachment, Dest> {
    type Step = StepTaken;
    type Out = Self;

    fn declare_dead_letter(self, _destination: Cow<'static, str>) -> Self {
        self
    }
}

/// The attachment a route-level declaration starts from: the registration is already mounted, so
/// there is no publish position of the handler's own left to carry.
#[doc(hidden)]
pub type RouteDeclaring<Cap, Dead> = Declaring<(NoReply, ()), Cap, Dead>;

/// The commit token of a declaration made on a registration that is already a route: the chain
/// has no definition left to mount, only the declaration to wire. Machinery; never named
/// directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DeclareMount;

// The registration a route-level declaration wraps is already mounted, so its "commit" is the
// route itself; the wrapper's own commit then wires the declaration onto it.
impl<R> RouterCommit<DeclareMount, R, ()> for (NoReply, ()) {
    type Out = R;

    fn commit(self, (): (), router: R) -> R {
        router
    }
}

// A declaration is not a publish position, so the retry publisher still binds after it - which is
// the order the chain reads in.
impl<Under: RetryOpen, Cap, Dead> RetryOpen for Declaring<Under, Cap, Dead> {}

// The publish positions keep binding after the declaration, in any order.
impl<Mount, Policy, Under, Cap, Dead> BindAt<Mount, Reply, Policy, ReplyLast>
    for Declaring<Under, Cap, Dead>
where
    Under: BindAt<Mount, Reply, Policy, ReplyLast>,
{
    type Out = Declaring<Under::Out, Cap, Dead>;

    fn bind_at(self, policy: Policy) -> Self::Out {
        self.map(|under| under.bind_at(policy))
    }
}

impl<Mount, M, Policy, Under, Cap, Dead, const POS: usize> BindAt<Mount, M, Policy, SlotPos<POS>>
    for Declaring<Under, Cap, Dead>
where
    Under: BindAt<Mount, M, Policy, SlotPos<POS>>,
{
    type Out = Declaring<Under::Out, Cap, Dead>;

    fn bind_at(self, policy: Policy) -> Self::Out {
        self.map(|under| under.bind_at(policy))
    }
}

/// Forwards every chain step through the declaration wrapper, for one position: the declaration
/// adds no position of its own, so each step rides the attachment underneath unchanged.
macro_rules! forward_steps {
    ($([$($extra:tt)*] $index:ty),+ $(,)?) => {$(
        impl<Cd, Under, Cap, Dead, $($extra)*> CodecLast<Cd, $index> for Declaring<Under, Cap, Dead>
        where
            Under: CodecLast<Cd, $index>,
        {
            type Step = Under::Step;
            type Out = Declaring<Under::Out, Cap, Dead>;

            fn codec_last(self, codec: Cd) -> Self::Out {
                self.map(|under| under.codec_last(codec))
            }
        }

        impl<N, Under, Cap, Dead, $($extra)*> TransformLast<N, $index>
            for Declaring<Under, Cap, Dead>
        where
            Under: TransformLast<N, $index>,
        {
            type Step = Under::Step;
            type Out = Declaring<Under::Out, Cap, Dead>;

            fn transform_last(self, transform: N) -> Self::Out {
                self.map(|under| under.transform_last(transform))
            }
        }

        impl<N, Under, Cap, Dead, Mount, Def, B, $($extra)*> AdmitsAt<N, $index, Mount, Def, B>
            for Declaring<Under, Cap, Dead>
        where
            Under: AdmitsAt<N, $index, Mount, Def, B>,
        {
        }

        impl<N, Under, Cap, Dead, $($extra)*> BatchTransformLast<N, $index>
            for Declaring<Under, Cap, Dead>
        where
            Under: BatchTransformLast<N, $index>,
        {
            type Step = Under::Step;
            type Out = Declaring<Under::Out, Cap, Dead>;

            fn batch_transform_last(self, transform: N) -> Self::Out {
                self.map(|under| under.batch_transform_last(transform))
            }
        }

        impl<Under, Cap, Dead, $($extra)*> TransactionalLast<$index>
            for Declaring<Under, Cap, Dead>
        where
            Under: TransactionalLast<$index>,
        {
            type Step = Under::Step;
            type Out = Declaring<Under::Out, Cap, Dead>;

            fn transactional_last(self) -> Self::Out {
                self.map(TransactionalLast::transactional_last)
            }
        }

        impl<Under, Cap, Dead, $($extra)*> MapPolicyLast<$index> for Declaring<Under, Cap, Dead>
        where
            Under: MapPolicyLast<$index>,
        {
            type Step = Under::Step;
            type Policy = Under::Policy;

            fn map_policy_last(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self {
                self.map(|under| under.map_policy_last(f))
            }
        }
    )+};
}

forward_steps!([] ReplyLast, [const POS: usize] SlotPos<POS>);

// The commit is the attachment's own, with the declaration wired onto the registration it grew
// into.
impl<Mount, R, Def, Under, Cap, Dead> RouterCommit<Mount, R, Def> for Declaring<Under, Cap, Dead>
where
    Under: RouterCommit<Mount, R, Def>,
    Under::Out: AttachDeclaration,
{
    type Out = <Under::Out as AttachDeclaration>::Out;

    fn commit(self, def: Def, router: R) -> Self::Out {
        self.under
            .commit(def, router)
            .attach_declaration(self.declaration)
    }
}
