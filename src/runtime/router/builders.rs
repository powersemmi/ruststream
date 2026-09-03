//! The router's registration builders: the attachment a form needs before it becomes a route.
//!
//! A [`Router`] is a consuming builder, so these commit through an explicit terminal rather than
//! on `Drop`: `Drop` cannot return the router the registration grew into. The terminals are
//! [`publisher`](RouterWith::publisher) (an explicit policy, or a
//! [`Bound`](crate::runtime::Bound) token for a cross-broker target) and
//! [`mount`](RouterWith::mount) (the broker's [`DefaultPublish`](crate::DefaultPublish) policy).
//! A chain left unfinished never becomes a router, so a forgotten terminal is a compile error at
//! the next use.

use std::fmt;
use std::marker::PhantomData;

use crate::Broker;

use crate::runtime::slot::{BindSlot, MissingSlot, OutSlot, WithSource};

use super::builder::Router;
use super::mount::{
    BatchInjectMount, BatchPublishInjectMount, BatchPublishMount, InjectMount, PublishInjectMount,
    PublishMount, RawReplyInjectMount, RawReplyMount,
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
/// [`publisher`](Self::publisher) commits with an explicit reply policy and
/// [`build`](Self::build) commits with the broker's default one; both consume the builder and
/// return the grown router. The per-form names are aliases: [`RouterPublishing`],
/// [`RouterBatchPublishing`].
#[must_use = "a router registration is only added once .publisher(policy) or .build() commits it"]
pub struct RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, Fallback>
where
    B: Broker + 'static,
{
    def: Def,
    router: Router<B, Routes, RouteCodec, RouteLayers>,
    _attachment: PhantomData<fn() -> (Mount, Fallback)>,
}

/// The builder [`Router::include`](super::Router::include) returns for a `publish("dest")`
/// definition.
pub type RouterPublishing<B, Routes, RouteCodec, RouteLayers, Def, Fallback> =
    RouterWith<PublishMount, B, Routes, RouteCodec, RouteLayers, Def, Fallback>;

/// The builder [`Router::include`](super::Router::include) returns for a `publish_raw("dest")`
/// definition, whose reply bytes go out as-is through a bare publisher.
pub type RouterRawReply<B, Routes, RouteCodec, RouteLayers, Def, Fallback> =
    RouterWith<RawReplyMount, B, Routes, RouteCodec, RouteLayers, Def, Fallback>;

/// The builder [`Router::include`](super::Router::include) returns for a
/// `batch(.., publish("dest"))` definition.
///
/// The attachment is the batch reply source: a typed stack, or its
/// [`transactional`](crate::runtime::TypedPublisher::transactional) form for one transaction per
/// batch.
pub type RouterBatchPublishing<B, Routes, RouteCodec, RouteLayers, Def, Fallback> =
    RouterWith<BatchPublishMount, B, Routes, RouteCodec, RouteLayers, Def, Fallback>;

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

/// The builder [`Router::include`](super::Router::include) returns for a `publish_raw("dest")`
/// definition whose handler also takes [`Out`](crate::runtime::Out) parameters.
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

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, Fallback>
    RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, Fallback>
where
    B: Broker + 'static,
{
    pub(super) fn new(def: Def, router: Router<B, Routes, RouteCodec, RouteLayers>) -> Self {
        Self {
            def,
            router,
            _attachment: PhantomData,
        }
    }

    /// Commits the registration with an explicit reply source: a
    /// [`TypedPublisher`](crate::runtime::TypedPublisher) stack naming the reply codec and
    /// transforms, a bare policy on the byte-reply form, or a [`Bound`](crate::runtime::Bound)
    /// token wrapping one for a cross-broker target. The runtime pairs it after the brokers
    /// connect.
    #[allow(clippy::type_complexity)] // the commit's own output; an alias would hide the router
    pub fn publisher<Policy>(
        self,
        policy: Policy,
    ) -> <WithSource<Policy> as RouterCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>>::Out
    where
        WithSource<Policy>: RouterCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>,
    {
        WithSource::new(policy).commit(self.def, self.router)
    }

    /// Commits the registration with the broker's own
    /// [`DefaultPublish`](crate::DefaultPublish) policy, the terminal for a reply that needs no
    /// wiring of its own.
    #[allow(clippy::type_complexity)] // the commit's own output; an alias would hide the router
    pub fn build(
        self,
    ) -> <Fallback as RouterCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>>::Out
    where
        Fallback: Default + RouterCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>,
    {
        Fallback::default().commit(self.def, self.router)
    }
}

/// A registration builder for a handler with [`Out`](crate::runtime::Out) slots.
///
/// Each [`out`](Self::out) call binds one named slot (in any order) and the terminal
/// [`build`](Self::build) commits - it exists only once every slot is bound, so a forgotten
/// binding is a compile error naming the slot. A handler with a single slot skips the ceremony:
/// [`publisher`](Self::publisher) binds it and commits in one call. The per-form names are
/// aliases: [`RouterOut`], [`RouterBatchOut`].
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
    /// Binds the handler's single [`Out`](crate::runtime::Out) slot and commits, no
    /// [`build`](Self::build) needed: the one-slot shorthand
    /// (`router.include(forward).publisher(MemoryPublish)`).
    #[allow(clippy::type_complexity)] // the commit's own output; an alias would hide the router
    pub fn publisher<Policy>(
        self,
        policy: Policy,
    ) -> <(WithSource<Policy>,) as RouterSlotCommit<
        Mount,
        B,
        Routes,
        RouteCodec,
        RouteLayers,
        Def,
    >>::Out
    where
        (WithSource<Policy>,): RouterSlotCommit<Mount, B, Routes, RouteCodec, RouteLayers, Def>,
    {
        (WithSource::new(policy),).commit(self.def, self.router)
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

    /// Attaches the reply source, like [`RouterWith::publisher`]. Unlike there it is not a
    /// terminal: the slots still have to be bound, so the chain finishes with
    /// [`build`](Self::build).
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
        WithSource<Policy>,
        Slots,
    > {
        RouterSlotsWithReply::new(self.def, WithSource::new(policy), self.slots, self.router)
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

impl<Mount, B, Routes, RouteCodec, RouteLayers, Def, Fallback> fmt::Debug
    for RouterWith<Mount, B, Routes, RouteCodec, RouteLayers, Def, Fallback>
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
