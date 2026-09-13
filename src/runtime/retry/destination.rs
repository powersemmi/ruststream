//! Where a registration's retry copies go, and who says so.
//!
//! Three things can settle it. A descriptor that addresses its own subscription
//! ([`AddressedCopies`]) answers at startup. A mount site answers statically with
//! [`to`](crate::runtime::RouterWith::to) after `out_retry(policy)`. A publish transform on the
//! position answers per delivery, reading the delivery being retried. A registration on a
//! descriptor that addresses nothing ([`NamedCopies`]) and says neither does not compile.
//!
//! The position offers a transform the right to name the destination exactly where nothing else
//! declared it, which is the same rule a reply follows: a declared destination and a naming
//! transform are mutually exclusive.

use std::borrow::Cow;

use crate::Publisher;
use crate::runtime::publish::{
    DestinationUse, Either, FitsTaken, ForReply, Names, PublishTransform, PublishTransformIdentity,
    PublishTransformStack, Reads,
};

use super::StepOpen;
use crate::{AddressedCopies, BrokerMoves, NamedCopies};

/// A registration whose retry copies have no destination of their own yet. Machinery; never named
/// directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenDestination;

/// A registration whose retry copies go where `.to(name)` said. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct FixedDestination;

/// What the deferred-retry position offers a transform, read off the descriptor's copy path and
/// whatever the mount site named.
///
/// [`Names`] only where the descriptor addresses nothing and no `.to(name)` has spoken; [`Reads`]
/// everywhere else, so a transform that would name the destination is refused there with the same
/// diagnostic a declared reply gives. Machinery; never named directly.
#[doc(hidden)]
pub trait RetryOffer<Dest> {
    /// The most a transform on this position may do to the destination.
    type Offer: DestinationUse;
}

/// Implements [`RetryOffer`] for the copy paths that carry a destination of their own: nothing is
/// offered, because the descriptor already declared one (or publishes nothing at all).
macro_rules! impl_declared_offer {
    ($($path:ident),+ $(,)?) => {$(
        impl<Dest> RetryOffer<Dest> for $path {
            type Offer = Reads;
        }
    )+};
}

impl_declared_offer!(AddressedCopies, BrokerMoves);

impl RetryOffer<OpenDestination> for NamedCopies {
    type Offer = Names;
}

impl RetryOffer<FixedDestination> for NamedCopies {
    type Offer = Reads;
}

/// What the mount site's transform stack on the deferred-retry position declares about the
/// destination, read against the live publisher the copies leave through.
///
/// The slot counterpart projects at [`ForSlot`](crate::runtime::ForSlot); this position hands its
/// transforms the delivery being retried, so it projects at [`ForReply`]. An empty stack answers
/// [`Reads`] whatever it runs over. Machinery; never named directly.
#[doc(hidden)]
pub trait RetryStackUse<Live, Cx> {
    /// The most the stack does to the destination.
    type Destination: DestinationUse;
}

impl<Live, Cx> RetryStackUse<Live, Cx> for PublishTransformIdentity {
    type Destination = Reads;
}

// One destination, one transform that names it: the stack folds element by element, and each
// element has to fit what the elements under it left.
impl<Live: Publisher, Cx, Inner, Outer> RetryStackUse<Live, Cx>
    for PublishTransformStack<Inner, Outer>
where
    Inner: RetryStackUse<Live, Cx>,
    Outer: PublishTransform<ForReply<Cx>, Live::Options>,
    Outer::Destination: FitsTaken<Inner::Destination> + Either<Inner::Destination>,
    <Outer::Destination as Either<Inner::Destination>>::Out: DestinationUse,
{
    type Destination = <Outer::Destination as Either<Inner::Destination>>::Out;
}

/// What a chain has already said about where its retry copies go, read off the attachment so the
/// position inherits it when `out_retry(policy)` binds. Machinery; never named directly.
#[doc(hidden)]
pub trait DeclaredDestination {
    /// [`FixedDestination`] once a `.to(name)` ahead of the publisher has spoken.
    type Destination;
}

impl<Rep, Slots> DeclaredDestination for (Rep, Slots) {
    type Destination = OpenDestination;
}

/// Whether the deferred-retry position can still be given a destination. Machinery; the guidance a
/// mount site reads lives here.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`.to(..)` has no destination to name here",
    label = "`{Self}`: the destination was named already, or this position takes none",
    note = "`.to(name)` names where a registration's retry copies go, once, on either side of \
            `out_retry(policy)`; a reply's destination comes from its own type or from the mount \
            site's `publish(\"dest\")`, and an `Out` slot's from the type it publishes"
)]
pub trait DestinationOpen {}

impl DestinationOpen for StepOpen {}

/// Names where the copies of the position the chain named last go: the `.to(name)` step.
/// Machinery; never named directly.
#[doc(hidden)]
pub trait DestinationLast<Index> {
    /// Whether this position can still be given one; where the guidance lives.
    type Step;

    /// The attachment carrying the destination.
    type Out;

    /// Names it.
    fn destination_last(self, destination: Cow<'static, str>) -> Self::Out;
}
