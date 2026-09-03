//! The reply wiring a mount site's chain builds up.
//!
//! A mount site names the broker's publish policy and then the knobs that policy alone does not
//! carry: the codec the reply encodes with, the static transform stacks that run on it, and
//! whether a page's replies ride one broker transaction. Each step fills its own slot in the
//! wiring's type, so naming one twice is a compile error rather than a silent overwrite, and the
//! whole value stays pure declaration - it pairs with the connected broker at startup, like any
//! other [`PublishPolicy`].

use std::fmt;
use std::marker::PhantomData;

use super::{
    BatchPublishTransformStack, BatchTransformIdentity, CallCodec, PublishCodec,
    PublishTransformIdentity, PublishTransformStack, Transactional, TypedPublisher, UnnamedCodec,
};
use crate::{ConnectedBroker, PairError, PublishPolicy, TransactionalPublisher};

/// The transaction state a wiring starts in: each reply publishes on its own.
#[derive(Debug, Clone, Copy, Default)]
pub struct Direct;

/// The transaction state [`transactional`](TransactionalReply::into_transactional) moves a wiring
/// to: a page's replies publish inside one broker transaction.
#[derive(Debug, Clone, Copy, Default)]
pub struct InTransaction;

/// A reply publish policy with the mount site's chain steps folded into its type.
///
/// Built by `.publisher(<policy>)` on a mount site's chain and grown by `.codec(..)`,
/// `.transform(..)`, `.batch_transform(..)` and `.transactional()`; the runtime pairs it with the
/// connected broker at startup, which is where the policy becomes a live publisher and the
/// wiring becomes the reply sink the dispatch publishes through. Machinery: the chain builds it
/// and the mount consumes it, so it is never named in user code.
pub struct ReplyWiring<
    Policy,
    Enc = UnnamedCodec,
    PL = PublishTransformIdentity,
    BL = BatchTransformIdentity,
    Tx = Direct,
> {
    policy: Policy,
    enc: Enc,
    layers: PL,
    batch_layers: BL,
    _tx: PhantomData<fn() -> Tx>,
}

impl<Policy> ReplyWiring<Policy> {
    /// The wiring a bare `.publisher(policy)` produces: no codec named (the default applies), no
    /// transforms, one broker call per reply.
    pub(crate) fn new(policy: Policy) -> Self {
        Self {
            policy,
            enc: UnnamedCodec::new(),
            layers: PublishTransformIdentity,
            batch_layers: BatchTransformIdentity,
            _tx: PhantomData,
        }
    }
}

impl<Policy, Enc, PL, BL, Tx> fmt::Debug for ReplyWiring<Policy, Enc, PL, BL, Tx> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyWiring").finish_non_exhaustive()
    }
}

/// Naming the reply codec: the `.codec(..)` step of a mount site's chain.
///
/// Implemented for a wiring whose codec slot is still open, so a second `.codec(..)` - and the
/// call on a byte-for-byte reply, which has no codec slot at all - fails here.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not take a reply codec",
    label = "this reply's codec cannot be named here",
    note = "`.codec(..)` names an encoded reply's codec once, right after `.publisher(..)`; a \
            `Serialized` reply carries its own bytes and takes no codec at all"
)]
pub trait NameReplyCodec<C> {
    /// The wiring with the codec named.
    type Out;

    /// Names it.
    fn name_codec(self, codec: C) -> Self::Out;
}

impl<Policy, PL, BL, Tx, C> NameReplyCodec<C> for ReplyWiring<Policy, UnnamedCodec, PL, BL, Tx> {
    type Out = ReplyWiring<Policy, CallCodec<C>, PL, BL, Tx>;

    fn name_codec(self, codec: C) -> Self::Out {
        ReplyWiring {
            policy: self.policy,
            enc: CallCodec(codec),
            layers: self.layers,
            batch_layers: self.batch_layers,
            _tx: PhantomData,
        }
    }
}

/// Composing a static [`PublishTransform`](super::PublishTransform) onto the reply: the
/// `.transform(..)` step. The stack grows, so the step repeats; the first one added runs first
/// (closest to the encoded value).
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not take a publish transform",
    label = "this reply has no transform stack",
    note = "`.transform(..)` composes a `PublishTransform` onto an encoded reply, right after \
            `.publisher(..)`; a `Serialized` reply's bytes leave as they are"
)]
pub trait AddReplyTransform<N> {
    /// The wiring with the transform on top of its stack.
    type Out;

    /// Composes it.
    fn add_transform(self, transform: N) -> Self::Out;
}

impl<Policy, Enc, PL, BL, Tx, N> AddReplyTransform<N> for ReplyWiring<Policy, Enc, PL, BL, Tx> {
    type Out = ReplyWiring<Policy, Enc, PublishTransformStack<PL, N>, BL, Tx>;

    fn add_transform(self, transform: N) -> Self::Out {
        ReplyWiring {
            policy: self.policy,
            enc: self.enc,
            layers: PublishTransformStack {
                inner: self.layers,
                outer: transform,
            },
            batch_layers: self.batch_layers,
            _tx: PhantomData,
        }
    }
}

/// Composing a [`BatchPublishTransform`](super::BatchPublishTransform) onto a page's replies: the
/// `.batch_transform(..)` step. It runs on the page path only; a per-message transform wanted on
/// both is added to each, reused here through [`for_batch`](super::for_batch).
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not take a batch publish transform",
    label = "this reply has no batch transform stack",
    note = "`.batch_transform(..)` composes a `BatchPublishTransform` onto an encoded page reply \
            (`&[T]` plus `publish(..)`), right after `.publisher(..)`"
)]
pub trait AddBatchReplyTransform<N> {
    /// The wiring with the transform on top of its batch stack.
    type Out;

    /// Composes it.
    fn add_batch_transform(self, transform: N) -> Self::Out;
}

impl<Policy, Enc, PL, BL, Tx, N> AddBatchReplyTransform<N>
    for ReplyWiring<Policy, Enc, PL, BL, Tx>
{
    type Out = ReplyWiring<Policy, Enc, PL, BatchPublishTransformStack<BL, N>, Tx>;

    fn add_batch_transform(self, transform: N) -> Self::Out {
        ReplyWiring {
            policy: self.policy,
            enc: self.enc,
            layers: self.layers,
            batch_layers: BatchPublishTransformStack {
                inner: self.batch_layers,
                outer: transform,
            },
            _tx: PhantomData,
        }
    }
}

/// Wrapping a page's replies in one broker transaction: the `.transactional()` step.
///
/// Implemented for a wiring still publishing directly, so a second `.transactional()` fails here;
/// that the policy's live publisher actually is a
/// [`TransactionalPublisher`](crate::TransactionalPublisher) is checked where the wiring pairs,
/// against the broker the mount site names.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot publish its replies inside a transaction",
    label = "this reply has no transaction to open",
    note = "`.transactional()` marks an encoded page reply's wiring once, right after \
            `.publisher(..)`; the policy it marks must pair into a `TransactionalPublisher`"
)]
pub trait TransactionalReply {
    /// The wiring publishing inside a transaction.
    type Out;

    /// Marks it.
    fn into_transactional(self) -> Self::Out;
}

impl<Policy, Enc, PL, BL> TransactionalReply for ReplyWiring<Policy, Enc, PL, BL, Direct> {
    type Out = ReplyWiring<Policy, Enc, PL, BL, InTransaction>;

    fn into_transactional(self) -> Self::Out {
        ReplyWiring {
            policy: self.policy,
            enc: self.enc,
            layers: self.layers,
            batch_layers: self.batch_layers,
            _tx: PhantomData,
        }
    }
}

// A wiring is a policy over a policy: pairing swaps the leaf for its live form and resolves the
// codec position, while the transform stacks travel unchanged.
impl<CB, Policy, Enc, PL, BL> PublishPolicy<CB> for ReplyWiring<Policy, Enc, PL, BL, Direct>
where
    CB: ConnectedBroker,
    Policy: PublishPolicy<CB> + Send,
    Enc: PublishCodec<Codec: Clone> + Send,
    PL: Send,
    BL: Send,
{
    type Live = TypedPublisher<Policy::Live, Enc::Codec, PL, BL>;

    async fn pair(self, connected: &CB) -> Result<Self::Live, PairError> {
        let codec = self.enc.codec().clone();
        Ok(TypedPublisher::live(
            self.policy.pair(connected).await?,
            codec,
            self.layers,
            self.batch_layers,
        ))
    }
}

// The transactional wiring pairs into the transactional reply sink, which is where the leaf's
// live form has to carry broker transactions - so a broker without them fails at the mount that
// named `.transactional()`, not at the step.
impl<CB, Policy, Enc, PL, BL> PublishPolicy<CB> for ReplyWiring<Policy, Enc, PL, BL, InTransaction>
where
    CB: ConnectedBroker,
    Policy: PublishPolicy<CB> + Send,
    Policy::Live: TransactionalPublisher,
    Enc: PublishCodec<Codec: Clone> + Send,
    PL: Send,
    BL: Send,
{
    type Live = Transactional<Policy::Live, Enc::Codec, PL, BL>;

    async fn pair(self, connected: &CB) -> Result<Self::Live, PairError> {
        let codec = self.enc.codec().clone();
        Ok(Transactional::live(TypedPublisher::live(
            self.policy.pair(connected).await?,
            codec,
            self.layers,
            self.batch_layers,
        )))
    }
}
