//! The arena itself: the entries a body declared, picked by marker, and the marker tuple an
//! entry tuple stands for.

use alloc::vec::Vec;
use core::fmt;

use crate::inject::{FromStartup, LocalFromStartup};
use crate::metadata::OutgoingMessageMetadata;
use crate::slot::OutSlot;
use crate::{Broker, Connected, PairError};

use super::entry::{OutEntry, Slot};
use super::local_entry::{LocalOutEntry, LocalSlot};

/// The injections arena a slot body receives: its entries mirror the marker tuple the body
/// declared, and [`get`](Self::get) picks one by marker.
pub struct Outs<E> {
    entries: E,
}

impl<E> fmt::Debug for Outs<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outs").finish_non_exhaustive()
    }
}

impl<E> Outs<E> {
    /// Picks the entry of `marker`: the wired live value behind its transparent [`Slot`]
    /// window. The position is inferred, so the call reads the same wherever the marker sits in
    /// the declaration.
    // The marker travels by value like every marker selector (`.out(marker, ..)`): it is a
    // unit token whose only job is naming the slot at the call site.
    #[allow(clippy::needless_pass_by_value)]
    pub fn get<M, I>(&self, marker: M) -> &<E as SelectSlot<M, I>>::Picked
    where
        E: SelectSlot<M, I>,
    {
        let _ = marker;
        self.entries.pick()
    }
}

/// Positional pick of one marker's entry; the index is inferred per call. Machinery; never
/// named in user code.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this arena has no `{M}` slot",
    note = "the body's `Outs<(..)>` declaration lists the slots it may publish through; check \
            the marker"
)]
pub trait SelectSlot<M, I> {
    /// The picked entry.
    type Picked;

    fn pick(&self) -> &Self::Picked;
}

/// The position tokens of [`SelectSlot`], one per arity slot.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct OutPos<const N: usize>;

// The picked entry is reached through `OutEntry`, not by matching `Slot<..>` in the tuple
// position, so an arena a body left generic (`Outs<(A, J)>`) picks exactly like a spelled-out
// one. The declared set rides the index: it is what tells the position impls apart from the
// mount's side, and a type parameter in the trait reference is what keeps it constrained.
macro_rules! impl_select_slot {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        impl<M, Body, Entry $(, $before)* $(, $after)*> SelectSlot<M, (OutPos<$pos>, Body)>
            for ($($before,)* Entry, $($after,)*)
        where
            Entry: OutEntry<M, Body>,
        {
            type Picked = Slot<M, Entry::Wire, Entry::Enc, Entry::Pipe, Body>;

            fn pick(&self) -> &Self::Picked {
                #[allow(non_snake_case)]
                let ($($before,)* picked, $($after,)*) = self;
                $(let _ = $before;)*
                $(let _ = $after;)*
                picked.entry()
            }
        }
    )+};
}

/// The position tokens of [`SelectSlot`] over the entries of the local family, one per arity
/// slot.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct LocalOutPos<const N: usize>;

// The local family's entries pick through their own position tokens: an entry is either a
// multi-thread one or a local one, so exactly one family of impls applies to a declaration.
macro_rules! impl_local_select_slot {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        #[diagnostic::do_not_recommend]
        impl<M, Body, Entry $(, $before)* $(, $after)*> SelectSlot<M, (LocalOutPos<$pos>, Body)>
            for ($($before,)* Entry, $($after,)*)
        where
            Entry: LocalOutEntry<M, Body>,
        {
            type Picked = LocalSlot<M, Entry::Wire, Entry::Enc, Entry::Pipe, Body>;

            fn pick(&self) -> &Self::Picked {
                #[allow(non_snake_case)]
                let ($($before,)* picked, $($after,)*) = self;
                $(let _ = $before;)*
                $(let _ = $after;)*
                picked.entry()
            }
        }
    )+};
}

impl_local_select_slot! {
    (@ 0)
    (@ 0, A1)
    (A0, @ 1)
    (@ 0, A1, A2)
    (A0, @ 1, A2)
    (A0, A1, @ 2)
}

// Polled on the one thread that hosts the app: `Send` exactly where the types it runs are.
#[allow(clippy::future_not_send)]
impl<B, Sub, Extra, E> LocalFromStartup<B, Sub, Extra> for Outs<E>
where
    B: Broker,
    E: LocalFromStartup<B, Sub, Extra>,
{
    async fn resolve_local(
        extra: Extra,
        connected: &Connected<B>,
        subscriber: &Sub,
    ) -> Result<Self, PairError> {
        Ok(Self {
            entries: E::resolve_local(extra, connected, subscriber).await?,
        })
    }
}

impl_select_slot! {
    (@ 0)
    (@ 0, A1)
    (A0, @ 1)
    (@ 0, A1, A2)
    (A0, @ 1, A2)
    (A0, A1, @ 2)
}

impl<B, Sub, Extra, E> FromStartup<B, Sub, Extra> for Outs<E>
where
    B: Broker,
    Sub: Sync,
    Extra: Send,
    E: FromStartup<B, Sub, Extra>,
{
    async fn resolve(
        extra: Extra,
        connected: &Connected<B>,
        subscriber: &Sub,
    ) -> Result<Self, PairError> {
        Ok(Self {
            entries: E::resolve(extra, connected, subscriber).await?,
        })
    }
}

/// The marker tuple of an arena's entry tuple, in declaration order. Machinery; never named in
/// user code.
#[doc(hidden)]
pub trait EntryMarkers {
    /// The markers, as [`HasSlots::Markers`] reports them.
    type Markers;

    /// The markers' `AsyncAPI` dictionaries, one entry set per slot.
    fn outgoing() -> Vec<OutgoingMessageMetadata>;
}

/// The `AsyncAPI` dictionaries of an arena's entries, one entry set per slot, on either family.
/// Machinery; never named in user code.
#[doc(hidden)]
pub trait SlotDictionary {
    /// The markers' dictionaries, in declaration order.
    fn outgoing() -> Vec<OutgoingMessageMetadata>;
}

macro_rules! impl_entry_markers {
    ($(($($m:ident: $w:ident / $e:ident / $p:ident / $b:ident),+))+) => {$(
        impl<$($m: OutSlot, $w, $e, $p, $b),+> EntryMarkers
            for ($(Slot<$m, $w, $e, $p, $b>,)+)
        {
            type Markers = ($($m,)+);

            fn outgoing() -> Vec<OutgoingMessageMetadata> {
                let mut declared = Vec::new();
                $(declared.extend($m::outgoing());)+
                declared
            }
        }

        impl<$($m: OutSlot, $w, $e, $p, $b),+> SlotDictionary
            for ($(Slot<$m, $w, $e, $p, $b>,)+)
        {
            fn outgoing() -> Vec<OutgoingMessageMetadata> {
                <Self as EntryMarkers>::outgoing()
            }
        }
    )+};
}

impl_entry_markers! {
    (M0: W0 / E0 / P0 / B0)
    (M0: W0 / E0 / P0 / B0, M1: W1 / E1 / P1 / B1)
    (M0: W0 / E0 / P0 / B0, M1: W1 / E1 / P1 / B1, M2: W2 / E2 / P2 / B2)
}
