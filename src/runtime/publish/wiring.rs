//! The reply wiring a mount site's chain builds up.
//!
//! A mount site names the broker's publish policy and then the knobs that policy alone does not
//! carry: the codec the reply encodes with, the static transform stacks that run on it, and
//! whether a batch's replies ride one broker transaction. Each step fills its own slot in the
//! wiring's type, so naming one twice is a compile error rather than a silent overwrite, and the
//! whole value stays pure declaration - it pairs with the connected broker at startup, like any
//! other [`PublishPolicy`].

use std::fmt;
use std::marker::PhantomData;

use super::{
    BatchPublishTransformStack, BatchTransformIdentity, CallCodec, PublishCodec,
    PublishTransformIdentity, PublishTransformStack, Transactional, TypedPublisher, UnnamedCodec,
};
#[cfg(feature = "asyncapi")]
use crate::asyncapi::Bindings;
use crate::{ConnectedBroker, PairError, PublishPolicy, TransactionalPublisher};

/// The publish policy of a byte-for-byte reply, as the mount chain carries it.
///
/// A [`Serialized`](super::Serialized) reply leaves with no codec and no transform stack, so its
/// wiring is the policy and nothing else; the newtype exists so the two wires are different type
/// constructors, which is what lets `.codec(..)`, `.transform(..)`, `.batch_transform(..)` and
/// `.transactional()` report "not on this wire" instead of "no such method". Machinery; the chain
/// builds it and the mount pairs it.
pub struct RawReplyWiring<Policy>(Policy);

impl<Policy> RawReplyWiring<Policy> {
    /// The wiring `.out(Reply, policy)` produces on the serialized wire.
    pub(crate) const fn new(policy: Policy) -> Self {
        Self(policy)
    }
}

impl<Policy> fmt::Debug for RawReplyWiring<Policy> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawReplyWiring").finish_non_exhaustive()
    }
}

// The wiring is transparent to the pairing: the bytes leave through the policy's own live form.
impl<CB: ConnectedBroker, Policy: PublishPolicy<CB> + Send> PublishPolicy<CB>
    for RawReplyWiring<Policy>
{
    type Live = Policy::Live;

    async fn pair(self, connected: &CB) -> Result<Self::Live, PairError> {
        self.0.pair(connected).await
    }

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        self.0.channel_bindings(channel)
    }

    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self, channel: &str) -> Bindings {
        self.0.operation_bindings(channel)
    }

    #[cfg(feature = "asyncapi")]
    fn message_bindings(&self, channel: &str) -> Bindings {
        self.0.message_bindings(channel)
    }

    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        self.0.reply_address_location()
    }
}

/// Replaces the publish policy a wiring carries, keeping every step the chain already named:
/// the hook [`map_publisher`](crate::runtime::MapPublisher::map_publisher) - and so a broker
/// crate's own publisher settings - reaches the policy through.
///
/// Implemented for both reply wires and for one [`Out`](crate::runtime::Out) slot's attachment,
/// so a broker's settings trait is written once and applies wherever a policy is named.
#[doc(hidden)]
pub trait MapReplyPolicy: Sized {
    /// The policy the wiring carries.
    type Policy;

    /// Replaces it with one the broker's own settings produced.
    #[must_use]
    fn map_policy(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self;
}

impl<Policy, Enc, PL, BL, Tx> MapReplyPolicy for ReplyWiring<Policy, Enc, PL, BL, Tx> {
    type Policy = Policy;

    fn map_policy(self, f: impl FnOnce(Policy) -> Policy) -> Self {
        Self {
            policy: f(self.policy),
            enc: self.enc,
            layers: self.layers,
            batch_layers: self.batch_layers,
            _tx: PhantomData,
        }
    }
}

impl<Policy> MapReplyPolicy for RawReplyWiring<Policy> {
    type Policy = Policy;

    fn map_policy(self, f: impl FnOnce(Policy) -> Policy) -> Self {
        Self(f(self.0))
    }
}

/// The transaction state a wiring starts in: each reply publishes on its own.
#[derive(Debug, Clone, Copy, Default)]
pub struct Direct;

/// The transaction state [`transactional`](TransactionalReply::into_transactional) moves a wiring
/// to: a batch's replies publish inside one broker transaction.
#[derive(Debug, Clone, Copy, Default)]
pub struct InTransaction;

/// A reply publish policy with the mount site's chain steps folded into its type.
///
/// Built by `.out(Reply, <policy>)` on a mount site's chain and grown by `.codec(..)`,
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
    /// The wiring a bare `.out(Reply, policy)` produces: no codec named (the default applies), no
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
/// Implemented for every reply wiring and for nothing else, so the call on a byte-for-byte reply
/// - which carries the publish policy alone and has no codec slot at all - fails here. Whether
/// the slot is still open is the separate question [`CodecSlotOpen`] answers, so that a second
/// `.codec(..)` reports the slot rather than the whole wiring.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not take a reply codec",
    label = "this reply's codec cannot be named here",
    note = "`.codec(..)` names an encoded reply's codec, right after `.out(Reply, ..)`; a \
            `Serialized` reply carries its own bytes and takes no codec at all"
)]
pub trait NameReplyCodec<C> {
    /// The codec slot the wiring holds right now: [`UnnamedCodec`] until a `.codec(..)` fills it.
    type Slot;

    /// The wiring with the codec named.
    type Out;

    /// Names it.
    fn name_codec(self, codec: C) -> Self::Out;
}

impl<Policy, Enc, PL, BL, Tx, C> NameReplyCodec<C> for ReplyWiring<Policy, Enc, PL, BL, Tx> {
    type Slot = Enc;
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

/// A reply codec slot still open: what a `.codec(..)` step fills.
///
/// The step states this about the slot it is about to fill
/// ([`NameReplyCodec::Slot`]) rather than about the wiring, so a second `.codec(..)` fails on the
/// slot the first one already took - and this message, not a bare "no method", is what the call
/// site reads.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this reply's codec is already named",
    label = "`.codec(..)` fills the reply's codec slot, and it is filled",
    note = "a reply encodes with exactly one codec: drop one of the `.codec(..)` calls"
)]
pub trait CodecSlotOpen {}

impl CodecSlotOpen for UnnamedCodec {}

/// Composing a static [`PublishTransform`](super::PublishTransform) onto the reply: the
/// `.transform(..)` step. The stack grows, so the step repeats; the first one added runs first
/// (closest to the encoded value).
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not take a publish transform",
    label = "this reply has no transform stack",
    note = "`.transform(..)` composes a `PublishTransform` onto an encoded reply, right after \
            `.out(Reply, ..)`; a `Serialized` reply's bytes leave as they are"
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

/// Composing a [`BatchPublishTransform`](super::BatchPublishTransform) onto a batch's replies: the
/// `.batch_transform(..)` step. It runs on the batch path only; a per-message transform wanted on
/// both is added to each, reused here through [`for_batch`](super::for_batch).
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not take a batch publish transform",
    label = "this reply has no batch transform stack",
    note = "`.batch_transform(..)` composes a `BatchPublishTransform` onto an encoded batch reply \
            (`&[T]` plus `publish(..)`), right after `.out(Reply, ..)`"
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

/// Wrapping a batch's replies in one broker transaction: the `.transactional()` step.
///
/// Implemented for every reply wiring and for nothing else, so the call on a byte-for-byte reply
/// fails here. Whether the wiring still publishes directly is the separate question
/// [`PublishingDirectly`] answers, and that the policy's live publisher actually is a
/// [`TransactionalPublisher`](crate::TransactionalPublisher) is checked where the wiring pairs,
/// against the broker the mount site names.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot publish its replies inside a transaction",
    label = "this reply has no transaction to open",
    note = "`.transactional()` marks an encoded batch reply's wiring, right after \
            `.out(Reply, ..)`; the policy it marks must pair into a `TransactionalPublisher`"
)]
pub trait TransactionalReply {
    /// How the wiring publishes right now: [`Direct`] until a `.transactional()` marks it.
    type State;

    /// The wiring publishing inside a transaction.
    type Out;

    /// Marks it.
    fn into_transactional(self) -> Self::Out;
}

impl<Policy, Enc, PL, BL, Tx> TransactionalReply for ReplyWiring<Policy, Enc, PL, BL, Tx> {
    type State = Tx;
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

/// A wiring still publishing each reply on its own: what a `.transactional()` step marks.
///
/// Stated about the publish state ([`TransactionalReply::State`]) rather than about the wiring,
/// so a second `.transactional()` reports the mark the first one already made.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this reply already publishes inside a transaction",
    label = "`.transactional()` marks the reply's publish state, and it is marked",
    note = "a batch's replies ride one transaction: drop one of the `.transactional()` calls"
)]
pub trait PublishingDirectly {}

impl PublishingDirectly for Direct {}

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

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        self.policy.channel_bindings(channel)
    }

    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self, channel: &str) -> Bindings {
        self.policy.operation_bindings(channel)
    }

    #[cfg(feature = "asyncapi")]
    fn message_bindings(&self, channel: &str) -> Bindings {
        self.policy.message_bindings(channel)
    }

    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        self.policy.reply_address_location()
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

    #[cfg(feature = "asyncapi")]
    fn channel_bindings(&self, channel: &str) -> Bindings {
        self.policy.channel_bindings(channel)
    }

    #[cfg(feature = "asyncapi")]
    fn operation_bindings(&self, channel: &str) -> Bindings {
        self.policy.operation_bindings(channel)
    }

    #[cfg(feature = "asyncapi")]
    fn message_bindings(&self, channel: &str) -> Bindings {
        self.policy.message_bindings(channel)
    }

    #[cfg(feature = "asyncapi")]
    fn reply_address_location(&self) -> Option<&'static str> {
        self.policy.reply_address_location()
    }
}

#[cfg(all(test, feature = "memory", feature = "json", feature = "asyncapi"))]
mod tests {
    use std::collections::BTreeMap;
    use std::future::Future;

    use super::{RawReplyWiring, ReplyWiring, TransactionalReply};
    use crate::asyncapi::{Binding, Bindings};
    use crate::memory::{ConnectedMemoryBroker, MemoryPublish, MemoryPublisher};
    use crate::runtime::publish::{Names, NarrowToUse};
    use crate::{PairError, PublishPolicy};

    /// One binding under `protocol` whose body is the channel the hook was asked about, so a
    /// wrapper that drops a level or passes a name of its own is visible.
    fn one(protocol: &'static str, channel: &str) -> Bindings {
        let body = BTreeMap::from([("channel".to_owned(), channel.to_owned())]);
        Bindings::new().with(Binding::new(protocol, "0.1.0", &body).expect("a listed protocol"))
    }

    /// A policy that answers every document hook with a different protocol, so a wrapper that
    /// crosses two levels or drops one is visible.
    #[derive(Clone, Copy)]
    struct Described;

    impl PublishPolicy<ConnectedMemoryBroker> for Described {
        type Live = MemoryPublisher;

        fn pair(
            self,
            connected: &ConnectedMemoryBroker,
        ) -> impl Future<Output = Result<Self::Live, PairError>> {
            <MemoryPublish as PublishPolicy<ConnectedMemoryBroker>>::pair(MemoryPublish, connected)
        }

        fn channel_bindings(&self, channel: &str) -> Bindings {
            one("nats", channel)
        }

        fn operation_bindings(&self, channel: &str) -> Bindings {
            one("kafka", channel)
        }

        fn message_bindings(&self, channel: &str) -> Bindings {
            one("mqtt", channel)
        }

        fn reply_address_location(&self) -> Option<&'static str> {
            Some("$message.header#/reply-to")
        }
    }

    fn assert_forwards<P: PublishPolicy<ConnectedMemoryBroker>>(wrapper: &P) {
        assert_eq!(wrapper.channel_bindings("orders"), one("nats", "orders"));
        assert_eq!(wrapper.operation_bindings("orders"), one("kafka", "orders"));
        assert_eq!(wrapper.message_bindings("orders"), one("mqtt", "orders"));
        assert_eq!(
            wrapper.reply_address_location(),
            Some("$message.header#/reply-to"),
        );
    }

    /// Every step a mount chain adds wraps the policy, and none of them may swallow what the
    /// policy says about itself: the document is built from the wrapper the chain ended on. The
    /// resolved destination travels the same way, unchanged by any step.
    #[test]
    fn every_wrapper_forwards_what_the_leaf_policy_says() {
        assert_forwards(&RawReplyWiring::new(Described));
        assert_forwards(&ReplyWiring::new(Described));
        assert_forwards(&ReplyWiring::new(Described).into_transactional());
        // What a naming transform narrows the policy to, on a slot and on the retry position.
        assert_forwards(&<Names as NarrowToUse<Described>>::narrow(Described));
    }
}
