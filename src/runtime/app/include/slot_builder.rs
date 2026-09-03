//! The slot-tuple registration builder: one attachment per Out marker.

use std::fmt;
use std::marker::PhantomData;

use crate::Broker;

use crate::runtime::slot::{
    BindSlot, MissingSlot, NamedStep, NoOutBound, OutAttachment, OutSlot, TransformAt, WithSource,
};

use crate::runtime::app::scope::BrokerScope;

/// The commit of a fully-bound slot registration, keyed by its `Mount` token. Machinery behind
/// [`IncludeSlots::mount`]; implemented only for attachment tuples with every position bound,
/// which is what turns a forgotten `.out(marker, policy)` into a compile error naming the slot
/// (the unbound position shows as `MissingSlot<TheMarker>` in `{Self}`).
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "not every Out slot of this handler is bound",
    label = "the attachment still contains a `MissingSlot<..>` naming the unbound slot",
    note = "bind each remaining slot with .out(marker, policy) before .build()"
)]
pub trait SlotCommit<Mount, B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

/// A registration builder for a handler with [`Out`](crate::runtime::Out) slots.
///
/// Unlike the reply builders, it does not commit on drop: each [`out`](Self::out) call binds
/// one named slot (in any order), and the terminal [`build`](Self::build) commits - it exists
/// only once every slot is bound, so a forgotten binding is a compile error naming the slot.
/// [`transform`](Self::transform) composes an [`OutTransform`](crate::runtime::OutTransform) onto
/// the slot the `.out(..)` before it bound. A
/// handler with a single slot skips the ceremony: [`publisher`](Self::publisher) binds it and
/// commits in one call. The per-form names are aliases: [`IncludeOut`](crate::runtime::IncludeOut), [`IncludeBatchOut`](crate::runtime::IncludeBatchOut).
///
/// `Last` is the slot the chain named most recently, which is what a `.transform(..)` applies to;
/// it starts as [`NoOutBound`], where the step does not exist.
#[must_use = "an Out handler registers nothing until .publisher(policy) or .out(..)+.build() commits it"]
pub struct IncludeSlots<'s, Mount, B, Layers, C, State, Pipeline, Def, Slots, Last = NoOutBound>
where
    B: Broker + 'static,
{
    // Options only so the binding methods can move the pieces into the next state out of a
    // Drop type; both stay `Some` until the commit consumes them.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Slots)>,
    _mount: PhantomData<Mount>,
    _last: PhantomData<fn() -> Last>,
}

impl<'s, Mount, B, Layers, C, State, Pipeline, Def, Slots, Last>
    IncludeSlots<'s, Mount, B, Layers, C, State, Pipeline, Def, Slots, Last>
where
    B: Broker + 'static,
{
    pub(super) fn new(
        def: Def,
        slots: Slots,
        scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) -> Self {
        Self {
            scope: Some(scope),
            parts: Some((def, slots)),
            _mount: PhantomData,
            _last: PhantomData,
        }
    }

    fn take(
        mut self,
    ) -> (
        Def,
        Slots,
        &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) {
        let (def, slots) = self
            .parts
            .take()
            .expect("builder parts are present until the commit consumes them");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until the commit consumes them");
        (def, slots, scope)
    }

    /// Binds one named [`Out`](crate::runtime::Out) slot: `marker` picks the slot (the second
    /// type argument of the handler's `Out<impl Publisher, Marker>` parameter) and `source` is
    /// its publish policy (or a [`Bound`](crate::runtime::Bound) token for a cross-broker
    /// target). Calls bind by marker, so their order does not matter; binding the same slot
    /// twice, or a marker the handler does not declare, fails to compile. Finish with
    /// [`build`](Self::build).
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit consumes them.
    // The unit marker drives inference, so it travels by value to keep the call site
    // `.out(Encoded, ..)`; the return type names the builder with the bound slot.
    #[allow(clippy::needless_pass_by_value, clippy::type_complexity)]
    pub fn out<M, NewSource, Index>(
        self,
        marker: M,
        source: NewSource,
    ) -> IncludeSlots<
        's,
        Mount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        <Slots as BindSlot<M, OutAttachment<NewSource>, Index>>::Out,
        Index,
    >
    where
        M: OutSlot,
        Slots: BindSlot<M, OutAttachment<NewSource>, Index>,
    {
        // The marker is inference input only; its value carries no data.
        let _ = marker;
        let (def, slots, scope) = self.take();
        IncludeSlots::new(def, slots.bind(OutAttachment::new(source)), scope)
    }

    /// Composes an [`OutTransform`](crate::runtime::OutTransform) onto the slot the
    /// [`out`](Self::out) call before it bound: it runs on every message that leaves that slot,
    /// after the include site's codec encoded it and before the app-wide publish pipeline.
    ///
    /// The step repeats, and the first one added runs first (closest to the encoded value), like
    /// a reply's [`transform`](crate::runtime::IncludeWith::transform). It applies to one slot,
    /// so a chain binding several transforms each of them separately:
    /// `.out(Audit, Publish).transform(Envelope).out(Journal, Publish)`. Without a preceding
    /// `.out(..)` the step does not exist, and the call fails naming the fix.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit consumes them.
    #[allow(clippy::type_complexity)] // the builder's own pieces; an alias would hide them
    pub fn transform<N>(
        self,
        transform: N,
    ) -> IncludeSlots<
        's,
        Mount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        <Slots as TransformAt<N, Last>>::Out,
        Last,
    >
    where
        Slots: TransformAt<N, Last, Step: NamedStep>,
    {
        let (def, slots, scope) = self.take();
        IncludeSlots::new(def, slots.transform_at(transform), scope)
    }

    /// Commits the registration. Exists only once every slot is bound: a chain that still has
    /// a `MissingSlot<..>` in its attachment fails to compile here, naming the slot.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until this
    /// commit consumes them.
    pub fn build(self)
    where
        Slots: SlotCommit<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, slots, scope) = self.take();
        slots.commit(def, scope);
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, M, Last>
    IncludeSlots<'_, Mount, B, Layers, C, State, Pipeline, Def, (MissingSlot<M>,), Last>
where
    B: Broker + 'static,
{
    /// Binds the handler's single [`Out`](crate::runtime::Out) slot and commits, no
    /// [`build`](Self::build) needed: the one-slot shorthand
    /// (`b.include(forward).publisher(Publish)`).
    ///
    /// The call is the whole registration, so nothing chains onto it: a single slot that also
    /// names a [`transform`](Self::transform) binds by marker instead
    /// (`.out(DefaultSlot, Publish).transform(..).build()`, with the handler's own marker in
    /// place of [`DefaultSlot`](crate::runtime::DefaultSlot) when it declares one).
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until this
    /// commit consumes them.
    pub fn publisher<NewSource>(self, source: NewSource)
    where
        (WithSource<OutAttachment<NewSource>>,):
            SlotCommit<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, _missing, scope) = self.take();
        (WithSource::new(OutAttachment::new(source)),).commit(def, scope);
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Slots, Last> fmt::Debug
    for IncludeSlots<'_, Mount, B, Layers, C, State, Pipeline, Def, Slots, Last>
where
    B: Broker + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncludeSlots").finish_non_exhaustive()
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Slots, Last> Drop
    for IncludeSlots<'_, Mount, B, Layers, C, State, Pipeline, Def, Slots, Last>
where
    B: Broker + 'static,
{
    fn drop(&mut self) {
        // A build-time assert, like the on_startup ordering check: the compiler already warns
        // through must_use, but a deliberately discarded incomplete registration must not
        // silently vanish - the handler would never consume.
        assert!(
            self.parts.is_none(),
            "an Out handler was included but never mounted: finish the chain with .build() \
             (or .publisher(policy) for a single slot)",
        );
    }
}
