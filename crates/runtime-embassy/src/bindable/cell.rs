//! The fixed form of the tokens: the connected form lives in a cell the user declares, so a token
//! outlives the app future without a heap.

use core::cell::{Ref, RefCell};
use core::fmt;

#[cfg(feature = "testing")]
use ruststream_core::pairing::pairs_against;
use ruststream_core::{Broker, Cause, Connected, ConnectedBroker, PairError, PublishPolicy};

#[cfg(feature = "testing")]
use alloc::rc::Rc;

use crate::mounts::{ConnectSlot, Own, Registration};

/// The place a [`Bindable`]'s broker keeps its connected form while the app runs, for the tokens
/// to pair against.
///
/// It is declared once per bound broker, in a `static` of `static_cell`'s kind, which hands out
/// the `&'static` reference the wrapper and its tokens hold:
/// `static BUS: StaticCell<BindCell<MyBroker>> = StaticCell::new();` and `let cell =
/// BUS.init(BindCell::new());`.
pub struct BindCell<B: Broker>(RefCell<Option<Own<B::Connected>>>);

impl<B: Broker> BindCell<B> {
    /// An empty cell.
    #[must_use]
    pub const fn new() -> Self {
        Self(RefCell::new(None))
    }
}

impl<B: Broker> Default for BindCell<B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<B: Broker> fmt::Debug for BindCell<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindCell").finish_non_exhaustive()
    }
}

impl<B: Broker> BindCell<B> {
    fn fill(&self, connected: Own<B::Connected>) -> Ref<'_, B::Connected> {
        *self.0.borrow_mut() = Some(connected);
        // Cannot fail: the cell was filled on the line above and nothing borrows it for writing.
        Ref::map(self.0.borrow(), |filled| {
            &**filled.as_ref().expect("the cell was just filled")
        })
    }
}

// The app holds the connected form through a shared borrow of the cell for its whole run, as the
// tokens do while they pair; the shutdown takes it back once the app has let go.
impl<B: Broker + 'static> ConnectSlot<B> for &'static BindCell<B> {
    type Held = Ref<'static, B::Connected>;

    fn hold(&self, connected: B::Connected) -> Ref<'static, B::Connected> {
        self.fill(Own::Alone(connected))
    }

    fn release(&self, held: Ref<'static, B::Connected>) -> Option<B::Connected> {
        drop(held);
        self.0.borrow_mut().take().and_then(Own::into_inner)
    }

    #[cfg(feature = "testing")]
    fn hold_shared(&self, connected: Rc<B::Connected>) -> Ref<'static, B::Connected> {
        self.fill(Own::Shared(connected))
    }
}

/// A broker wrapped for token minting before registration.
///
/// Pass the wrapper itself to [`EmbeddedApp::with_broker`](crate::EmbeddedApp::with_broker). A
/// token whose broker never registers fails at pairing with a clear error.
pub struct Bindable<B: Broker + 'static> {
    broker: B,
    cell: &'static BindCell<B>,
}

impl<B: Broker + 'static> fmt::Debug for Bindable<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bindable").finish_non_exhaustive()
    }
}

impl<B: Broker + 'static> Bindable<B> {
    /// Wraps `broker`, which keeps its connected form in `cell` while the app runs.
    #[must_use]
    pub const fn new(broker: B, cell: &'static BindCell<B>) -> Self {
        Self { broker, cell }
    }

    /// Mints a token that publishes through `source` against this broker.
    #[must_use]
    pub const fn bind<S>(&self, source: S) -> Bound<B, S> {
        Bound {
            cell: self.cell,
            source,
        }
    }
}

impl<B: Broker + 'static> Registration for Bindable<B> {
    type Broker = B;
    type Slot = &'static BindCell<B>;

    fn into_parts(self) -> (B, &'static BindCell<B>) {
        (self.broker, self.cell)
    }
}

/// A publish policy bound to a concrete broker of the app, minted by [`Bindable::bind`].
///
/// It implements [`PublishPolicy`] against any connected broker by ignoring it and pairing
/// against its own.
pub struct Bound<B: Broker + 'static, S> {
    cell: &'static BindCell<B>,
    source: S,
}

impl<B: Broker + 'static, S: fmt::Debug> fmt::Debug for Bound<B, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bound")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl<B: Broker + 'static, S: Clone> Clone for Bound<B, S> {
    fn clone(&self) -> Self {
        Self {
            cell: self.cell,
            source: self.source.clone(),
        }
    }
}

// The borrow is shared and so is every other one while the app runs; the cell is written only by
// `hold`, before any route opens, and by `release`, after the app has let go of its borrow and
// every pairing is over, so the held reference cannot meet a write.
#[allow(clippy::await_holding_refcell_ref)]
impl<C, B, S> PublishPolicy<C> for Bound<B, S>
where
    C: ConnectedBroker,
    B: Broker + 'static,
    S: PublishPolicy<Connected<B>>,
{
    type Live = S::Live;

    async fn pair(self, _connected: &C) -> Result<Self::Live, PairError> {
        // The shared borrow lasts across the pairing, which only reads the connected form; the
        // app holds its own for the whole run, and the shutdown writes only after both are gone.
        let connected = self
            .cell
            .0
            .try_borrow()
            .ok()
            .and_then(|filled| Ref::filter_map(filled, Option::as_ref).ok())
            .ok_or_else(|| {
                PairError::new(Cause::new(
                    "the token's broker is not connected: pairing happens after startup connects \
                     every registered broker",
                    0,
                ))
            })?;
        // The token's publisher publishes to its own broker, whatever scope pairs it.
        #[cfg(feature = "testing")]
        pairs_against(&*connected);
        self.source.pair(&*connected).await
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;
    use core::convert::Infallible;
    use core::ptr;

    use embassy_futures::block_on;

    use super::{BindCell, Bindable};
    use crate::frames::{FrameBroker, FrameConnected, Frames, Settlements};
    use crate::mounts::{ConnectSlot, Registration};
    use ruststream_core::{Broker, PairError, PublishPolicy};

    /// A policy whose live form is the address of the connected form it paired against.
    struct Address;

    impl PublishPolicy<FrameConnected> for Address {
        type Live = usize;

        async fn pair(self, connected: &FrameConnected) -> Result<usize, PairError> {
            Ok(ptr::from_ref(connected) as usize)
        }
    }

    fn broker() -> FrameBroker {
        let frames: &'static Frames = Box::leak(Box::new(Frames::new()));
        let settled: &'static Settlements = Box::leak(Box::new(Settlements::new()));
        FrameBroker::new(frames, settled)
    }

    fn cell() -> &'static BindCell<FrameBroker> {
        Box::leak(Box::new(BindCell::new()))
    }

    /// Connects a broker the way the app does and returns what it holds, with the token's side.
    fn connect(
        bindable: Bindable<FrameBroker>,
    ) -> (
        <&'static BindCell<FrameBroker> as ConnectSlot<FrameBroker>>::Held,
        &'static BindCell<FrameBroker>,
    ) {
        let (broker, slot) = bindable.into_parts();
        let connected: Result<FrameConnected, Infallible> = block_on(broker.connect());
        let Ok(connected) = connected;
        (slot.hold(connected), slot)
    }

    #[test]
    fn a_token_pairs_against_the_connected_form_the_cell_holds() {
        let bindable = Bindable::new(broker(), cell());
        let token = bindable.bind(Address);
        let (held, _slot) = connect(bindable);
        // The scope that pairs the token is another broker's: the token ignores its form.
        let scope = block_on(broker().connect());
        let Ok(scope) = scope;
        let paired = block_on(PublishPolicy::pair(token, &scope));
        assert_eq!(
            paired.ok(),
            Some(ptr::from_ref::<FrameConnected>(&held) as usize)
        );
    }

    #[test]
    fn a_token_pairs_with_an_error_before_its_broker_connected() {
        let bindable = Bindable::new(broker(), cell());
        let token = bindable.bind(Address);
        let scope = block_on(broker().connect());
        let Ok(scope) = scope;
        assert!(block_on(PublishPolicy::pair(token, &scope)).is_err());
    }

    #[test]
    fn releasing_the_connected_form_empties_the_cell() {
        let bindable = Bindable::new(broker(), cell());
        let token = bindable.bind(Address);
        let (held, slot) = connect(bindable);
        assert!(
            slot.release(held).is_some(),
            "the form comes back to shut down"
        );
        let scope = block_on(broker().connect());
        let Ok(scope) = scope;
        assert!(
            block_on(PublishPolicy::pair(token, &scope)).is_err(),
            "a token pairs with nothing once the broker is shut down"
        );
    }
}
