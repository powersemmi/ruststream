//! The slot-tuple registration builder: one attachment per Out marker.

use std::fmt;
use std::marker::PhantomData;

// The typed default-reply commits need a default codec, so that import is gated the same way;
// the raw default-reply commit publishes bare bytes and needs only `DefaultPublish`.
// The default-reply commits build a `TypedPublisher` over the broker's default policy, so those
// imports are gated with the default codec they require.
use crate::Broker;

use crate::runtime::slot::{BindSlot, MissingSlot, OutSlot, WithSource};

use crate::runtime::app::scope::BrokerScope;

/// The commit of a fully-bound slot registration, keyed by its `Mount` token. Machinery behind
/// [`IncludeSlots::mount`]; implemented only for attachment tuples with every position bound,
/// which is what turns a forgotten `.out(marker, policy)` into a compile error naming the slot
/// (the unbound position shows as `MissingSlot<TheMarker>` in `{Self}`).
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "not every Out slot of this handler is bound",
    label = "the attachment still contains a `MissingSlot<..>` naming the unbound slot",
    note = "bind each remaining slot with .out(marker, policy) before .mount()"
)]
pub trait SlotCommit<Mount, B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

/// A registration builder for a handler with [`Out`](crate::runtime::Out) slots.
///
/// Unlike the reply builders, it does not commit on drop: each [`out`](Self::out) call binds
/// one named slot (in any order), and the terminal [`mount`](Self::mount) commits - it exists
/// only once every slot is bound, so a forgotten binding is a compile error naming the slot. A
/// handler with a single slot skips the ceremony: [`publisher`](Self::publisher) binds it and
/// commits in one call. The per-form names are aliases: [`IncludeOut`], [`IncludeBatchOut`].
#[must_use = "an Out handler registers nothing until .publisher(policy) or .out(..)+.mount() commits it"]
pub struct IncludeSlots<'s, Mount, B, Layers, C, State, Pipeline, Def, Slots>
where
    B: Broker + 'static,
{
    // Options only so the binding methods can move the pieces into the next state out of a
    // Drop type; both stay `Some` until the commit consumes them.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Slots)>,
    _mount: PhantomData<Mount>,
}

impl<'s, Mount, B, Layers, C, State, Pipeline, Def, Slots>
    IncludeSlots<'s, Mount, B, Layers, C, State, Pipeline, Def, Slots>
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
    /// [`mount`](Self::mount).
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit consumes them.
    // The marker travels by value on purpose (a unit inference driver keeps the call site
    // `.out(Encoded, ..)`), and the return type is the builder itself with the bound slot - an
    // alias would only hide which position changed.
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
        <Slots as BindSlot<M, NewSource, Index>>::Out,
    >
    where
        M: OutSlot,
        Slots: BindSlot<M, NewSource, Index>,
    {
        // The marker is inference input only; its value carries no data.
        let _ = marker;
        let (def, slots, scope) = self.take();
        IncludeSlots::new(def, slots.bind(source), scope)
    }

    /// Commits the registration. Exists only once every slot is bound: a chain that still has
    /// a `MissingSlot<..>` in its attachment fails to compile here, naming the slot.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until this
    /// commit consumes them.
    pub fn mount(self)
    where
        Slots: SlotCommit<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, slots, scope) = self.take();
        slots.commit(def, scope);
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, M>
    IncludeSlots<'_, Mount, B, Layers, C, State, Pipeline, Def, (MissingSlot<M>,)>
where
    B: Broker + 'static,
{
    /// Binds the handler's single [`Out`](crate::runtime::Out) slot and commits, no
    /// [`mount`](Self::mount) needed: the one-slot shorthand
    /// (`b.include(forward).publisher(MemoryPublish)`).
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until this
    /// commit consumes them.
    pub fn publisher<NewSource>(self, source: NewSource)
    where
        (WithSource<NewSource>,): SlotCommit<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, _missing, scope) = self.take();
        (WithSource::new(source),).commit(def, scope);
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Slots> fmt::Debug
    for IncludeSlots<'_, Mount, B, Layers, C, State, Pipeline, Def, Slots>
where
    B: Broker + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncludeSlots").finish_non_exhaustive()
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Slots> Drop
    for IncludeSlots<'_, Mount, B, Layers, C, State, Pipeline, Def, Slots>
where
    B: Broker + 'static,
{
    fn drop(&mut self) {
        // A build-time assert, like the on_startup ordering check: the compiler already warns
        // through must_use, but a deliberately discarded incomplete registration must not
        // silently vanish - the handler would never consume.
        assert!(
            self.parts.is_none(),
            "an Out handler was included but never mounted: finish the chain with .mount() \
             (or .publisher(policy) for a single slot)",
        );
    }
}
