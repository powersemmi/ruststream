//! The router's registration builders: the attachment a form needs before it becomes a route.
//!
//! A [`Router`] is a consuming builder, so these commit through an explicit terminal rather than
//! on `Drop`: `Drop` cannot return the router the registration grew into. The one terminal is
//! [`build`](RouterWith::build); before it, [`publisher`](RouterWith::publisher) names the reply's
//! publish policy (or a [`Bound`](crate::runtime::Bound) token for a cross-broker target) and the
//! steps after it fill the rest of the reply wiring. Without a `publisher` call the registration
//! takes the broker's own [`DefaultPublish`](crate::DefaultPublish) policy. A chain left
//! unfinished never becomes a router, so a forgotten `.build()` is a compile error at the next
//! use.

use std::fmt;
use std::marker::PhantomData;

use crate::Broker;

use crate::runtime::publish::{
    AddBatchReplyTransform, AddReplyTransform, CodecSlotOpen, NameReplyCodec, PublishingDirectly,
    TransactionalReply,
};
use crate::runtime::slot::{BindSlot, MissingSlot, OutSlot, WithSource};

use super::builder::Router;
use super::mount::{
    BatchInjectMount, BatchPublishInjectMount, BatchPublishMount, InjectMount, PublishInjectMount,
    PublishMount, RawReplyInjectMount, RawReplyMount, ReplyAttachment,
};

/// One commit strategy of a router registration builder, keyed by its `Mount` token. Machinery;
/// never named directly.
#[doc(hidden)]
pub trait RouterCommit<Mount, B: Broker, Routes, RouteCodec, RouteLayers, Def>: Sized {
    /// The router the committed registration grows into.
    type Out;

    fn commit(self, def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out;
}

/// The commit of a fully-bound slot registration, keyed by its `Mount` token. Implemented only
/// for attachment tuples with every position bound, which is what turns a forgotten
/// `.out(marker, policy)` into a compile error naming the slot (the unbound position shows as
/// `MissingSlot<TheMarker>` in `{Self}`).
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "not every Out slot of this handler is bound",
    label = "the attachment still contains a `MissingSlot<..>` naming the unbound slot",
    note = "bind each remaining slot with .out(marker, policy) before .build()"
)]
pub trait RouterSlotCommit<Mount, B: Broker, Routes, RouteCodec, RouteLayers, Def>: Sized {
    /// See [`RouterCommit::Out`].
    type Out;

    fn commit(self, def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self::Out;
}

/// A registration builder over one attachment, generic over its mount token.
///
/// [`publisher`](Self::publisher) names the reply's publish policy and the steps after it
/// ([`codec`](Self::codec), [`transform`](Self::transform),
/// [`batch_transform`](Self::batch_transform), [`transactional`](Self::transactional)) fill the
/// rest of the wiring; [`build`](Self::build) consumes the builder and returns the grown router,
/// taking the broker's default policy when no `publisher` was named. The per-form names are
/// aliases: [`RouterPublishing`], [`RouterBatchPublishing`].
#[must_use = "a router registration is only added once .build() commits it"]
pub struct RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, Attach>
where
    B: Broker + 'static,
{
    def: Def,
    attach: Attach,
    router: Router<B, Routes, RouteCodec, RouteLayers>,
    _mount: PhantomData<fn() -> Mount>,
}

/// The builder [`Router::include`](super::Router::include) returns for a `publish("dest")`
/// definition.
pub type RouterPublishing<B, Routes, RouteCodec, RouteLayers, Def, Attach> =
    RouterWith<PublishMount, B, Routes, RouteCodec, RouteLayers, Def, Attach>;

/// The builder [`Router::include`](super::Router::include) returns for a `publish("dest")`
/// definition whose reply type is [`Serialized`](crate::runtime::Serialized).
///
/// The reply bytes go out as-is through a bare publisher.
pub type RouterRawReply<B, Routes, RouteCodec, RouteLayers, Def, Attach> =
    RouterWith<RawReplyMount, B, Routes, RouteCodec, RouteLayers, Def, Attach>;

/// The builder [`Router::include`](super::Router::include) returns for a
/// `batch(.., publish("dest"))` definition.
///
/// The attachment is the page's reply wiring, which
/// [`transactional`](RouterWith::transactional) switches to one transaction per page.
pub type RouterBatchPublishing<B, Routes, RouteCodec, RouteLayers, Def, Attach> =
    RouterWith<BatchPublishMount, B, Routes, RouteCodec, RouteLayers, Def, Attach>;

/// The builder [`Router::include`](super::Router::include) returns for a handler with
/// [`Out`](crate::runtime::Out) parameters.
pub type RouterOut<B, Routes, RouteCodec, RouteLayers, Def, Slots> =
    RouterSlots<InjectMount, B, Routes, RouteCodec, RouteLayers, Def, Slots>;

/// The builder [`Router::include`](super::Router::include) returns for a batch
/// handler with [`Out`](crate::runtime::Out) parameters.
pub type RouterBatchOut<B, Routes, RouteCodec, RouteLayers, Def, Slots> =
    RouterSlots<BatchInjectMount, B, Routes, RouteCodec, RouteLayers, Def, Slots>;

/// The builder [`Router::include`](super::Router::include) returns for a `publish("dest")`
/// definition whose handler also takes [`Out`](crate::runtime::Out) parameters.
pub type RouterPublishingOut<B, Routes, RouteCodec, RouteLayers, Def, Reply, Slots> =
    RouterSlotsWithReply<PublishInjectMount, B, Routes, RouteCodec, RouteLayers, Def, Reply, Slots>;

/// The builder [`Router::include`](super::Router::include) returns for a `publish("dest")`
/// definition whose reply type is [`Serialized`](crate::runtime::Serialized).
///
/// The handler also takes [`Out`](crate::runtime::Out) parameters, bound one by one at the
/// include site.
pub type RouterRawReplyOut<B, Routes, RouteCodec, RouteLayers, Def, Reply, Slots> =
    RouterSlotsWithReply<
        RawReplyInjectMount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
        Reply,
        Slots,
    >;

/// The builder [`Router::include`](super::Router::include) returns for a
/// `batch(.., publish("dest"))` definition whose handler also takes
/// [`Out`](crate::runtime::Out) parameters.
pub type RouterBatchPublishingOut<B, Routes, RouteCodec, RouteLayers, Def, Reply, Slots> =
    RouterSlotsWithReply<
        BatchPublishInjectMount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
        Reply,
        Slots,
    >;

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, Attach>
    RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, Attach>
where
    B: Broker + 'static,
{
    pub(super) fn new(
        def: Def,
        attach: Attach,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self {
        Self {
            def,
            attach,
            router,
            _mount: PhantomData,
        }
    }

    /// Names the reply's publish policy: one of the broker prelude's (`Publish`,
    /// `TransactionalPublish`, ...), or a [`Bound`](crate::runtime::Bound) token wrapping one for
    /// a cross-broker target. The runtime pairs it after the brokers connect.
    ///
    /// On an encoded reply the call opens the reply wiring, and [`codec`](Self::codec),
    /// [`transform`](Self::transform), [`batch_transform`](Self::batch_transform) and
    /// [`transactional`](Self::transactional) chain onto it; a byte-for-byte
    /// ([`Serialized`](crate::runtime::Serialized)) reply takes the policy and nothing else.
    /// Finish the registration with [`build`](Self::build).
    pub fn publisher<Policy>(
        self,
        policy: Policy,
    ) -> RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, WithSource<Mount::Wiring>>
    where
        Mount: ReplyAttachment<Policy>,
    {
        RouterWith::new(self.def, WithSource::new(Mount::wire(policy)), self.router)
    }

    /// Adds the registration to the router, with whatever the chain attached - the broker's own
    /// [`DefaultPublish`](crate::DefaultPublish) policy when no
    /// [`publisher`](Self::publisher) was named.
    #[allow(clippy::type_complexity)] // the commit's own output; an alias would hide the router
    pub fn build(
        self,
    ) -> <Attach as RouterCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>>::Out
    where
        Attach: RouterCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>,
    {
        self.attach.commit(self.def, self.router)
    }
}

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, W>
    RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, WithSource<W>>
where
    B: Broker + 'static,
{
    /// Rebuilds the builder over a grown reply wiring.
    fn map_wiring<W2>(
        self,
        f: impl FnOnce(W) -> W2,
    ) -> RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, WithSource<W2>> {
        RouterWith::new(self.def, self.attach.map(f), self.router)
    }

    /// See [`IncludeWith::codec`](crate::runtime::IncludeWith::codec).
    pub fn codec<Cd>(
        self,
        codec: Cd,
    ) -> RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, WithSource<W::Out>>
    where
        W: NameReplyCodec<Cd, Slot: CodecSlotOpen>,
    {
        self.map_wiring(|wiring| wiring.name_codec(codec))
    }

    /// See [`IncludeWith::transform`](crate::runtime::IncludeWith::transform).
    pub fn transform<N>(
        self,
        transform: N,
    ) -> RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, WithSource<W::Out>>
    where
        W: AddReplyTransform<N>,
    {
        self.map_wiring(|wiring| wiring.add_transform(transform))
    }

    /// See [`IncludeWith::batch_transform`](crate::runtime::IncludeWith::batch_transform).
    pub fn batch_transform<N>(
        self,
        transform: N,
    ) -> RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, WithSource<W::Out>>
    where
        W: AddBatchReplyTransform<N>,
    {
        self.map_wiring(|wiring| wiring.add_batch_transform(transform))
    }

    /// See [`IncludeWith::transactional`](crate::runtime::IncludeWith::transactional).
    pub fn transactional(
        self,
    ) -> RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, WithSource<W::Out>>
    where
        W: TransactionalReply<State: PublishingDirectly>,
    {
        self.map_wiring(TransactionalReply::into_transactional)
    }
}

/// A registration builder for a handler with [`Out`](crate::runtime::Out) slots.
///
/// Each [`out`](Self::out) call binds one named slot (in any order) and the terminal
/// [`build`](Self::build) commits - it exists only once every slot is bound, so a forgotten
/// binding is a compile error naming the slot. A handler with a single slot skips naming it:
/// [`publisher`](Self::publisher) binds that one slot. The per-form names are aliases:
/// [`RouterOut`], [`RouterBatchOut`].
///
/// The subscription source is not carried here: a slot-taking definition is only instantiated
/// once the sources are bound, so its source comes from the instantiated definition at the
/// commit.
#[must_use = "a router registration is only added once .publisher(policy) or .out(..) + .build() commits it"]
pub struct RouterSlots<Mount, B, Routes, RouteCodec, RouteLayers, Def, Slots>
where
    B: Broker + 'static,
{
    def: Def,
    slots: Slots,
    router: Router<B, Routes, RouteCodec, RouteLayers>,
    _attachment: PhantomData<fn() -> Mount>,
}

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, Slots>
    RouterSlots<Mount, B, Routes, RouteCodec, RouteLayers, Def, Slots>
where
    B: Broker + 'static,
{
    pub(super) fn new(
        def: Def,
        slots: Slots,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self {
        Self {
            def,
            slots,
            router,
            _attachment: PhantomData,
        }
    }

    /// Binds one named [`Out`](crate::runtime::Out) slot: `marker` picks the slot (the second
    /// type argument of the handler's `Out<impl Publisher, Marker>` parameter) and `policy` is
    /// its publish policy (or a [`Bound`](crate::runtime::Bound) token for a cross-broker
    /// target). Calls bind by marker, so their order does not matter; binding the same slot
    /// twice, or a marker the handler does not declare, fails to compile. Finish with
    /// [`build`](Self::build).
    // The unit marker drives inference, so it travels by value to keep the call site
    // `.out(Encoded, ..)`; the return type names the builder with the bound slot.
    #[allow(clippy::needless_pass_by_value, clippy::type_complexity)]
    pub fn out<M, Policy, Index>(
        self,
        marker: M,
        policy: Policy,
    ) -> RouterSlots<
        Mount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
        <Slots as BindSlot<M, Policy, Index>>::Out,
    >
    where
        M: OutSlot,
        Slots: BindSlot<M, Policy, Index>,
    {
        // The marker is inference input only; its value carries no data.
        let _ = marker;
        RouterSlots::new(self.def, self.slots.bind(policy), self.router)
    }

    /// Commits the registration. Exists only once every slot is bound: a chain that still has
    /// a `MissingSlot<..>` in its attachment fails to compile here, naming the slot.
    pub fn build(
        self,
    ) -> <Slots as RouterSlotCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>>::Out
    where
        Slots: RouterSlotCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>,
    {
        self.slots.commit(self.def, self.router)
    }
}

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, M>
    RouterSlots<Mount, B, Routes, RouteCodec, RouteLayers, Def, (MissingSlot<M>,)>
where
    B: Broker + 'static,
{
    /// Binds the handler's single [`Out`](crate::runtime::Out) slot without naming its marker:
    /// the one-slot shorthand (`router.include(forward).publisher(Publish).build()`).
    #[allow(clippy::type_complexity)] // the builder's own pieces; an alias would hide them
    pub fn publisher<Policy>(
        self,
        policy: Policy,
    ) -> RouterSlots<Mount, B, Routes, RouteCodec, RouteLayers, Def, (WithSource<Policy>,)> {
        RouterSlots::new(self.def, (WithSource::new(policy),), self.router)
    }
}

/// A registration builder for a publishing handler that also takes
/// [`Out`](crate::runtime::Out) slots: the reply attachment next to the slot tuple.
///
/// [`publisher`](Self::publisher) replaces the reply side (defaulted when the call is omitted),
/// each slot binds with [`out`](Self::out), and the terminal [`build`](Self::build) commits. The
/// per-form names are aliases: [`RouterPublishingOut`], [`RouterBatchPublishingOut`].
#[must_use = "a router registration is only added once .build() commits it"]
pub struct RouterSlotsWithReply<Mount, B, Routes, RouteCodec, RouteLayers, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    def: Def,
    reply: Reply,
    slots: Slots,
    router: Router<B, Routes, RouteCodec, RouteLayers>,
    _attachment: PhantomData<fn() -> Mount>,
}

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, Reply, Slots>
    RouterSlotsWithReply<Mount, B, Routes, RouteCodec, RouteLayers, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    pub(super) fn new(
        def: Def,
        reply: Reply,
        slots: Slots,
        router: Router<B, Routes, RouteCodec, RouteLayers>,
    ) -> Self {
        Self {
            def,
            reply,
            slots,
            router,
            _attachment: PhantomData,
        }
    }

    /// Names the reply's publish policy, like [`RouterWith::publisher`]; the wiring steps
    /// ([`codec`](Self::codec), [`transform`](Self::transform),
    /// [`batch_transform`](Self::batch_transform), [`transactional`](Self::transactional)) chain
    /// onto it, next to the slot side's [`out`](Self::out).
    #[allow(clippy::type_complexity)] // the builder's own pieces; an alias would hide them
    pub fn publisher<Policy>(
        self,
        policy: Policy,
    ) -> RouterSlotsWithReply<
        Mount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
        WithSource<Mount::Wiring>,
        Slots,
    >
    where
        Mount: ReplyAttachment<Policy>,
    {
        RouterSlotsWithReply::new(
            self.def,
            WithSource::new(Mount::wire(policy)),
            self.slots,
            self.router,
        )
    }

    /// Binds one named [`Out`](crate::runtime::Out) slot, like [`RouterSlots::out`]: by marker,
    /// in any order, next to the (optional) reply-side [`publisher`](Self::publisher).
    // See `RouterSlots::out` for why the marker is by value and the return type stays spelled.
    #[allow(clippy::needless_pass_by_value, clippy::type_complexity)]
    pub fn out<M, Policy, Index>(
        self,
        marker: M,
        policy: Policy,
    ) -> RouterSlotsWithReply<
        Mount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
        Reply,
        <Slots as BindSlot<M, Policy, Index>>::Out,
    >
    where
        M: OutSlot,
        Slots: BindSlot<M, Policy, Index>,
    {
        // The marker is inference input only; its value carries no data.
        let _ = marker;
        RouterSlotsWithReply::new(self.def, self.reply, self.slots.bind(policy), self.router)
    }

    /// Commits the registration (reply attachment plus every bound slot). Exists only once
    /// every slot is bound: a chain that still has a `MissingSlot<..>` in its attachment fails
    /// to compile here, naming the slot.
    #[allow(clippy::type_complexity)] // the commit's own output; an alias would hide the router
    pub fn build(
        self,
    ) -> <(Reply, Slots) as RouterSlotCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>>::Out
    where
        (Reply, Slots): RouterSlotCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>,
    {
        (self.reply, self.slots).commit(self.def, self.router)
    }
}

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, W, Slots>
    RouterSlotsWithReply<Mount, B, Routes, RouteCodec, RouteLayers, Def, WithSource<W>, Slots>
where
    B: Broker + 'static,
{
    /// Rebuilds the builder over a grown reply wiring, keeping the slots.
    fn map_wiring<W2>(
        self,
        f: impl FnOnce(W) -> W2,
    ) -> RouterSlotsWithReply<Mount, B, Routes, RouteCodec, RouteLayers, Def, WithSource<W2>, Slots>
    {
        RouterSlotsWithReply::new(self.def, self.reply.map(f), self.slots, self.router)
    }

    /// See [`IncludeWith::codec`](crate::runtime::IncludeWith::codec).
    #[allow(clippy::type_complexity)] // the builder's own pieces; an alias would hide them
    pub fn codec<Cd>(
        self,
        codec: Cd,
    ) -> RouterSlotsWithReply<
        Mount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
        WithSource<W::Out>,
        Slots,
    >
    where
        W: NameReplyCodec<Cd, Slot: CodecSlotOpen>,
    {
        self.map_wiring(|wiring| wiring.name_codec(codec))
    }

    /// See [`IncludeWith::transform`](crate::runtime::IncludeWith::transform).
    #[allow(clippy::type_complexity)] // the builder's own pieces; an alias would hide them
    pub fn transform<N>(
        self,
        transform: N,
    ) -> RouterSlotsWithReply<
        Mount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
        WithSource<W::Out>,
        Slots,
    >
    where
        W: AddReplyTransform<N>,
    {
        self.map_wiring(|wiring| wiring.add_transform(transform))
    }

    /// See [`IncludeWith::batch_transform`](crate::runtime::IncludeWith::batch_transform).
    #[allow(clippy::type_complexity)] // the builder's own pieces; an alias would hide them
    pub fn batch_transform<N>(
        self,
        transform: N,
    ) -> RouterSlotsWithReply<
        Mount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
        WithSource<W::Out>,
        Slots,
    >
    where
        W: AddBatchReplyTransform<N>,
    {
        self.map_wiring(|wiring| wiring.add_batch_transform(transform))
    }

    /// See [`IncludeWith::transactional`](crate::runtime::IncludeWith::transactional).
    #[allow(clippy::type_complexity)] // the builder's own pieces; an alias would hide them
    pub fn transactional(
        self,
    ) -> RouterSlotsWithReply<
        Mount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
        WithSource<W::Out>,
        Slots,
    >
    where
        W: TransactionalReply<State: PublishingDirectly>,
    {
        self.map_wiring(TransactionalReply::into_transactional)
    }
}

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, Attach> fmt::Debug
    for RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, Attach>
where
    B: Broker + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouterWith").finish_non_exhaustive()
    }
}

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, Slots> fmt::Debug
    for RouterSlots<Mount, B, Routes, RouteCodec, RouteLayers, Def, Slots>
where
    B: Broker + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouterSlots").finish_non_exhaustive()
    }
}

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, Reply, Slots> fmt::Debug
    for RouterSlotsWithReply<Mount, B, Routes, RouteCodec, RouteLayers, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouterSlotsWithReply")
            .finish_non_exhaustive()
    }
}
