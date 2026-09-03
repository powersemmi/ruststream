//! The two-attachment builder: a reply source alongside the Out slot tuple.

use std::fmt;
use std::marker::PhantomData;

use crate::Broker;

use crate::runtime::slot::{BindSlot, OutSlot, WithSource};

use super::{BatchPublishInjectMount, PublishInjectMount, SlotCommit};
use crate::runtime::app::scope::BrokerScope;

/// A registration builder for a publishing handler that also takes
/// [`Out`](crate::runtime::Out) slots: the reply attachment next to the slot tuple.
///
/// The reply side defaults like [`IncludeWith`](crate::runtime::IncludeWith) (override with
/// [`publisher`](Self::publisher)); each slot binds with [`out`](Self::out), and the terminal
/// [`build`](Self::build) commits - it exists only once every slot is bound, so a forgotten
/// binding is a compile error naming the slot. The per-form names are aliases:
/// [`IncludePublishingOut`], [`IncludeBatchPublishingOut`].
#[must_use = "a publishing handler with Out slots registers nothing until .out(..)+.build() commits it"]
pub struct IncludeSlotsWithReply<'s, Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Reply, Slots)>,
    _mount: PhantomData<Mount>,
}

/// The builder [`BrokerScope::include`] returns for a `publish("dest")` /
/// `publish_raw("dest")` definition whose handler also takes
/// [`Out`](crate::runtime::Out) parameters.
pub type IncludePublishingOut<'s, B, Layers, C, State, Pipeline, Def, Reply, Slots> =
    IncludeSlotsWithReply<'s, PublishInjectMount, B, Layers, C, State, Pipeline, Def, Reply, Slots>;

/// The builder [`BrokerScope::include`] returns for a `batch(.., publish("dest"))`
/// definition whose handler also takes [`Out`](crate::runtime::Out) parameters.
pub type IncludeBatchPublishingOut<'s, B, Layers, C, State, Pipeline, Def, Reply, Slots> =
    IncludeSlotsWithReply<
        's,
        BatchPublishInjectMount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        Reply,
        Slots,
    >;

impl<'s, Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots>
    IncludeSlotsWithReply<'s, Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    pub(super) fn new(
        def: Def,
        reply: Reply,
        slots: Slots,
        scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) -> Self {
        Self {
            scope: Some(scope),
            parts: Some((def, reply, slots)),
            _mount: PhantomData,
        }
    }

    #[allow(clippy::type_complexity)] // the builder's own pieces; an alias would hide them
    fn take(
        mut self,
    ) -> (
        Def,
        Reply,
        Slots,
        &'s mut BrokerScope<B, Layers, C, State, Pipeline>,
    ) {
        let (def, reply, slots) = self
            .parts
            .take()
            .expect("builder parts are present until the commit consumes them");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until the commit consumes them");
        (def, reply, slots, scope)
    }

    /// Attaches the reply source, like [`IncludeWith::publisher`](crate::runtime::IncludeWith::publisher).
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit consumes them.
    pub fn publisher<NewSource>(
        self,
        source: NewSource,
    ) -> IncludeSlotsWithReply<
        's,
        Mount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        WithSource<NewSource>,
        Slots,
    > {
        let (def, _default, slots, scope) = self.take();
        IncludeSlotsWithReply::new(def, WithSource::new(source), slots, scope)
    }

    /// Binds one named [`Out`](crate::runtime::Out) slot, like [`IncludeSlots::out`](crate::runtime::IncludeSlots::out): by
    /// marker, in any order, next to the (optional) reply-side
    /// [`publisher`](Self::publisher). Finish with [`build`](Self::build).
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit consumes them.
    // See `IncludeSlots::out` for why the marker is by value and the return type stays spelled.
    #[allow(clippy::needless_pass_by_value, clippy::type_complexity)]
    pub fn out<M, NewSource, Index>(
        self,
        marker: M,
        source: NewSource,
    ) -> IncludeSlotsWithReply<
        's,
        Mount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        Reply,
        <Slots as BindSlot<M, NewSource, Index>>::Out,
    >
    where
        M: OutSlot,
        Slots: BindSlot<M, NewSource, Index>,
    {
        // The marker is inference input only; its value carries no data.
        let _ = marker;
        let (def, reply, slots, scope) = self.take();
        IncludeSlotsWithReply::new(def, reply, slots.bind(source), scope)
    }

    /// Commits the registration (reply attachment plus every bound slot). Exists only once
    /// every slot is bound: a chain that still has a `MissingSlot<..>` in its attachment fails
    /// to compile here, naming the slot.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until this
    /// commit consumes them.
    pub fn build(self)
    where
        (Reply, Slots): SlotCommit<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, reply, slots, scope) = self.take();
        (reply, slots).commit(def, scope);
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots> fmt::Debug
    for IncludeSlotsWithReply<'_, Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncludeSlotsWithReply")
            .finish_non_exhaustive()
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots> Drop
    for IncludeSlotsWithReply<'_, Mount, B, Layers, C, State, Pipeline, Def, Reply, Slots>
where
    B: Broker + 'static,
{
    fn drop(&mut self) {
        // See `IncludeSlots`'s drop: a build-time assert against a discarded registration.
        assert!(
            self.parts.is_none(),
            "a publishing handler with Out slots was included but never mounted: finish the \
             chain with .build()",
        );
    }
}
