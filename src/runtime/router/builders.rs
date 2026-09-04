//! The one mount chain: the attachments a form needs before it becomes a route.
//!
//! Every publish policy a registration names is attached with one verb,
//! [`out`](RouterWith::out): [`Reply`] names the policy the value a `publish("dest")` handler
//! returns leaves through, and a slot marker names one [`Out`](crate::runtime::Out) slot's. The
//! steps after a call ride the position it named - [`codec`](RouterWith::codec),
//! [`transform`](RouterWith::transform),
//! [`map_publisher`](crate::runtime::MapPublisher::map_publisher) on either kind,
//! [`batch_transform`](RouterWith::batch_transform) and
//! [`transactional`](RouterWith::transactional) on the reply alone - so the chain reads the same
//! whichever position it is filling.
//!
//! A [`Router`](super::Router) is a consuming builder, so the chain commits through an explicit
//! terminal, [`build`](RouterWith::build): `Drop` cannot return the router the registration grew
//! into. A chain left unfinished never becomes a router, so a forgotten `.build()` is a compile
//! error at the next use. A [`BrokerScope`](crate::runtime::BrokerScope) drives the same chain
//! through a guard that commits when the statement ends.

use std::fmt;
use std::marker::PhantomData;

#[cfg(doc)]
use crate::runtime::slot::Reply;
use crate::runtime::slot::{
    BatchTransformLast, BindAt, CodecLast, MapPolicyLast, NamedStep, NoOutBound, TransactionalLast,
    TransformLast,
};

/// One commit strategy of a mount chain's attachment, keyed by its `Mount` token and the chain
/// it grew on. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this registration is not ready to mount",
    label = "the attachment `{Self}` has an unfilled position",
    note = "bind every Out slot the handler declares with .out(marker, policy) before .build(); \
            an unbound one shows as `MissingSlot<..>` naming its marker"
)]
pub trait RouterCommit<Mount, R, Def>: Sized {
    /// The router the committed registration grows into.
    type Out;

    fn commit(self, def: Def, router: R) -> Self::Out;
}

/// A mount chain over one registration's attachment.
///
/// The attachment is a pair: the reply position (the broker's default until `.out(Reply, ..)`
/// names a policy, or [`NoReply`](crate::runtime::NoReply) on a handler that declares no reply)
/// and the slot tuple (one position per [`Out`](crate::runtime::Out) marker the handler declares,
/// each unbound until its `.out(marker, ..)`). `Last` is the position the chain named most
/// recently, which is what the steps after it ride; it starts as
/// [`NoOutBound`](crate::runtime::NoOutBound), where those steps do not exist.
#[must_use = "a router registration is only added once .build() commits it"]
pub struct RouterWith<Mount, R, Def, Attach, Last = NoOutBound> {
    def: Def,
    attach: Attach,
    router: R,
    _mount: PhantomData<fn() -> Mount>,
    _last: PhantomData<fn() -> Last>,
}

impl<Mount, R, Def, Attach, Last> RouterWith<Mount, R, Def, Attach, Last> {
    pub(crate) fn new(def: Def, attach: Attach, router: R) -> Self {
        Self {
            def,
            attach,
            router,
            _mount: PhantomData,
            _last: PhantomData,
        }
    }

    /// Names the publish policy of one position: [`Reply`] for the value a `publish("dest")`
    /// handler returns, an [`Out`](crate::runtime::Out) slot's own marker for a slot.
    ///
    /// `policy` is one of the broker prelude's (`Publish`, `TransactionalPublish`, ...), or a
    /// [`Bound`](crate::runtime::Bound) token wrapping one for a cross-broker target; the runtime
    /// pairs it after the brokers connect. Calls bind by marker, so their order does not matter,
    /// and each position takes exactly one: binding one twice does not compile. Omitting
    /// `.out(Reply, ..)` leaves the reply on the broker's own
    /// [`DefaultPublish`](crate::DefaultPublish) policy; omitting a slot's call does not compile.
    ///
    /// The steps after the call fill the rest of that position's wiring.
    // The unit marker drives inference, so it travels by value to keep the call site
    // `.out(Reply, ..)`; the return type names the chain with the bound position.
    #[allow(clippy::needless_pass_by_value, clippy::type_complexity)]
    pub fn out<M, Policy, Index>(
        self,
        marker: M,
        policy: Policy,
    ) -> RouterWith<Mount, R, Def, <Attach as BindAt<Mount, M, Policy, Index>>::Out, Index>
    where
        Attach: BindAt<Mount, M, Policy, Index>,
    {
        // The marker is inference input only; its value carries no data.
        let _ = marker;
        RouterWith::new(self.def, self.attach.bind_at(policy), self.router)
    }

    /// Encodes what leaves the position named last with `codec` instead of the registration
    /// surface's own.
    ///
    /// Named once per position: the codec slot the call fills is filled, so a second one does not
    /// compile. A byte-for-byte ([`Serialized`](crate::runtime::Serialized)) reply carries its own
    /// bytes and has no codec position at all.
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn codec<Cd>(
        self,
        codec: Cd,
    ) -> RouterWith<Mount, R, Def, <Attach as CodecLast<Cd, Last>>::Out, Last>
    where
        Attach: CodecLast<Cd, Last, Step: NamedStep>,
    {
        RouterWith::new(self.def, self.attach.codec_last(codec), self.router)
    }

    /// Composes a static transform onto everything that leaves the position named last: a
    /// [`PublishTransform`](crate::runtime::PublishTransform) on the reply, an
    /// [`OutTransform`](crate::runtime::OutTransform) on a slot.
    ///
    /// The step repeats and the first one added runs first (closest to the encoded value), so a
    /// chain can name one per position:
    /// `.out(Reply, Publish).transform(StampSource).out(Audit, Publish).transform(Envelope)`.
    /// Before any `.out(..)` the step has no position to ride, and the call fails naming the fix.
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn transform<N>(
        self,
        transform: N,
    ) -> RouterWith<Mount, R, Def, <Attach as TransformLast<N, Last>>::Out, Last>
    where
        Attach: TransformLast<N, Last, Step: NamedStep>,
    {
        RouterWith::new(self.def, self.attach.transform_last(transform), self.router)
    }

    /// Composes a [`BatchPublishTransform`](crate::runtime::BatchPublishTransform) onto every
    /// reply of a page (`&[T]` plus `publish(..)`), after the per-message stack. Wrap a
    /// per-message transform with [`for_batch`](crate::runtime::for_batch) to reuse it here.
    ///
    /// Reply-only: a slot publish is one message with no page to run a batch transform over.
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn batch_transform<N>(
        self,
        transform: N,
    ) -> RouterWith<Mount, R, Def, <Attach as BatchTransformLast<N, Last>>::Out, Last>
    where
        Attach: BatchTransformLast<N, Last, Step: NamedStep>,
    {
        RouterWith::new(
            self.def,
            self.attach.batch_transform_last(transform),
            self.router,
        )
    }

    /// Publishes a page's replies inside one broker transaction: they all become visible
    /// atomically on commit, or none of them do.
    ///
    /// The policy's live publisher has to be a
    /// [`TransactionalPublisher`](crate::TransactionalPublisher), which the pairing checks against
    /// the chain's own broker; a one-message reply has no page to make atomic, so the wiring only
    /// mounts on the page forms. Reply-only, for the same reason
    /// [`batch_transform`](Self::batch_transform) is.
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn transactional(
        self,
    ) -> RouterWith<Mount, R, Def, <Attach as TransactionalLast<Last>>::Out, Last>
    where
        Attach: TransactionalLast<Last, Step: NamedStep>,
    {
        RouterWith::new(self.def, self.attach.transactional_last(), self.router)
    }

    /// Adds the registration to the router, with whatever the chain attached - the broker's own
    /// [`DefaultPublish`](crate::DefaultPublish) policy for a reply no `.out(Reply, ..)` named.
    #[allow(clippy::type_complexity)] // the commit's own output; an alias would hide the router
    pub fn build(self) -> <Attach as RouterCommit<Mount, R, Def>>::Out
    where
        Attach: RouterCommit<Mount, R, Def>,
    {
        self.attach.commit(self.def, self.router)
    }
}

impl<Mount, R, Def, Attach, Last> fmt::Debug for RouterWith<Mount, R, Def, Attach, Last> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouterWith").finish_non_exhaustive()
    }
}

/// Replaces the publish policy of the position a mount chain named last, keeping every step the
/// chain already filled: the hook a broker crate layers its own publisher settings on.
///
/// The publish-side mirror of
/// [`map_source`](crate::runtime::SubscriberSettings::map_source). Core cannot know that a publish
/// has an exchange, a partition key or a confirm mode, so the broker declares a trait over the
/// mount chain bound to its own policy type and implements each method as one `map_publisher`
/// call. The bound means those methods simply do not exist on a chain that named another broker's
/// policy - or none at all.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::MemoryPublish;
/// use ruststream::runtime::MapPublisher;
///
/// /// What a broker crate ships next to its policy: the publisher's own settings, reachable
/// /// wherever a mount chain named that policy.
/// pub trait MemoryPublishSettings: Sized {
///     /// Publishes every message through `prefix` instead of the declared destination.
///     fn prefixed(self, prefix: &'static str) -> Self;
/// }
///
/// impl<T: MapPublisher<Policy = MemoryPublish>> MemoryPublishSettings for T {
///     fn prefixed(self, prefix: &'static str) -> Self {
///         let _ = prefix;
///         self.map_publisher(|policy| policy)
///     }
/// }
/// # }
/// ```
pub trait MapPublisher: Sized {
    /// The policy the position a mount chain named last carries.
    type Policy;

    /// Replaces it with what the broker's own settings made of it.
    ///
    /// The replacement is the same policy type: a broker's publisher settings are that policy's
    /// own fields (an exchange, a partition key, a confirm mode), while a different policy type
    /// is a different publish mode and belongs in the `.out(marker, policy)` call itself.
    fn map_publisher(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self;
}

impl<Mount, R, Def, Attach, Last> MapPublisher for RouterWith<Mount, R, Def, Attach, Last>
where
    Attach: MapPolicyLast<Last, Step: NamedStep>,
{
    type Policy = Attach::Policy;

    fn map_publisher(self, f: impl FnOnce(Self::Policy) -> Self::Policy) -> Self {
        RouterWith::new(self.def, self.attach.map_policy_last(f), self.router)
    }
}

use super::mount::DefaultReply;
use crate::runtime::slot::NoReply;

/// The attachment a reply-only form starts with: the broker's default policy, no slots.
pub(crate) type ReplyOnly = (DefaultReply, ());

/// The attachment a slot-only form starts with: no reply position, one unbound slot per marker.
pub(crate) type SlotsOnly<Slots> = (NoReply, Slots);

/// The attachment a reply-and-slots form starts with.
pub(crate) type ReplyAndSlots<Slots> = (DefaultReply, Slots);

/// The chain a reply-only form hands back.
pub type RouterPublishing<Mount, R, Def> = RouterWith<Mount, R, Def, ReplyOnly>;

/// The chain a slot-carrying form hands back.
pub type RouterOut<Mount, R, Def, Slots> = RouterWith<Mount, R, Def, SlotsOnly<Slots>>;

/// The chain a reply-and-slots form hands back.
pub type RouterPublishingOut<Mount, R, Def, Slots> =
    RouterWith<Mount, R, Def, ReplyAndSlots<Slots>>;
