//! Which connected broker a pairing the runtime makes resolves against, for the test harness to
//! attribute what the paired publisher publishes. A [`Bound`](crate::runtime::Bound) token pairs
//! against its own broker and says so here.

use std::cell::Cell;
use std::ptr;

tokio::task_local! {
    /// Set around one pairing the runtime makes: the connected broker the policy pairs against.
    /// The scope's broker unless a `Bound` token, which pairs against its own broker, says
    /// otherwise.
    static PAIRING: Cell<Origin>;
}

/// The broker a publisher the runtime paired publishes to: the connected broker it was paired
/// against, whatever scope holds it. A handler on one broker holding a `Bound` token for another
/// publishes to the other, and its publishes are that broker's.
///
/// Known by the connected form's address, which the harness maps to the broker's registration.
/// A publisher the runtime did not pair (one the service builds or keeps itself) carries none.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Origin(Option<usize>);

impl Origin {
    /// The connected broker at `connected`.
    pub(crate) fn of<C: ?Sized>(connected: &C) -> Self {
        Self(Some(ptr::from_ref(connected).cast::<()>() as usize))
    }

    /// The connected broker's address, or `None` for a publisher the runtime did not pair.
    pub(crate) const fn address(self) -> Option<usize> {
        self.0
    }
}

/// Runs the pairing `pairing` against `connected`, and reports the broker the paired publisher
/// publishes to.
pub(crate) fn paired<C: ?Sized, F: Future>(
    connected: &C,
    pairing: F,
) -> impl Future<Output = (F::Output, Origin)> {
    PAIRING.scope(Cell::new(Origin::of(connected)), async move {
        let live = pairing.await;
        (live, PAIRING.with(Cell::get))
    })
}

/// Notes, inside a pairing, that the policy pairs against `connected` instead of the broker it
/// was handed: what a `Bound` token does.
pub(crate) fn pairs_against<C: ?Sized>(connected: &C) {
    let _ = PAIRING.try_with(|origin| origin.set(Origin::of(connected)));
}
