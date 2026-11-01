//! Cross-broker publisher tokens on the embedded runner: a publish policy bound to another
//! broker of the same app.
//!
//! A bare policy attached at an include site pairs with that scope's own broker. To publish to a
//! different broker, wrap it in [`Bindable`] and mint [`Bound`] tokens from it before the scopes
//! are built. A token is a plain [`PublishPolicy`]: it rides the mount chain like any other, and
//! pairs against its own broker once the app has connected it. The slot it reads is filled once
//! at start, so a token adds nothing per message.

use alloc::boxed::Box;
use alloc::rc::Rc;
use core::cell::RefCell;
use core::fmt;

#[cfg(feature = "testing")]
use ruststream_core::pairing::pairs_against;
use ruststream_core::{Broker, Connected, ConnectedBroker, PairError, PublishPolicy};

use crate::mounts::{ConnectSlot, Registration};

/// Where an app puts a broker's connected form for the tokens bound to it.
#[doc(hidden)]
pub struct BindSlot<B: Broker>(Rc<RefCell<Option<Rc<B::Connected>>>>);

impl<B: Broker> fmt::Debug for BindSlot<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindSlot").finish_non_exhaustive()
    }
}

impl<B: Broker> Clone for BindSlot<B> {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl<B: Broker> ConnectSlot<B> for BindSlot<B> {
    fn fill(&self, connected: &Rc<B::Connected>) {
        *self.0.borrow_mut() = Some(Rc::clone(connected));
    }

    fn empty(&self) {
        self.0.borrow_mut().take();
    }
}

/// A broker wrapped for token minting before registration.
///
/// Pass the wrapper itself to [`EmbeddedApp::with_broker`](crate::EmbeddedApp::with_broker). A
/// token whose broker never registers fails at pairing with a clear error.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::{LocalMemoryBroker, LocalMemoryPublish};
/// use ruststream_runtime_embassy::{Bindable, EmbeddedApp, Mounts};
///
/// /// The relay: the actuators' bus is registered through a wrapper, so a scope of the sensors'
/// /// bus can hold a token that publishes to it.
/// fn app(sensors: LocalMemoryBroker<4>, actuators: LocalMemoryBroker<2>) -> EmbeddedApp<impl Mounts> {
///     let actuators = Bindable::new(actuators);
///     let to_actuators = actuators.bind(LocalMemoryPublish);
///     # let _ = &to_actuators;
///     EmbeddedApp::new()
///         .with_broker(actuators, |b| b)
///         .with_broker(sensors, |b| b)
/// }
/// # let _ = app(LocalMemoryBroker::new(), LocalMemoryBroker::new());
/// ```
pub struct Bindable<B: Broker> {
    broker: B,
    slot: BindSlot<B>,
}

impl<B: Broker> fmt::Debug for Bindable<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bindable").finish_non_exhaustive()
    }
}

impl<B: Broker> Bindable<B> {
    /// Wraps `broker`.
    #[must_use]
    pub fn new(broker: B) -> Self {
        Self {
            broker,
            slot: BindSlot(Rc::new(RefCell::new(None))),
        }
    }

    /// Mints a token that publishes through `source` against this broker.
    ///
    /// # Examples
    ///
    /// See [`Bindable`].
    #[must_use]
    pub fn bind<S>(&self, source: S) -> Bound<B, S> {
        Bound {
            slot: self.slot.clone(),
            source,
        }
    }
}

impl<B: Broker + 'static> Registration for Bindable<B> {
    type Broker = B;
    type Slot = BindSlot<B>;

    fn into_parts(self) -> (B, BindSlot<B>) {
        (self.broker, self.slot)
    }
}

/// A publish policy bound to a concrete broker of the app, minted by [`Bindable::bind`].
///
/// It implements [`PublishPolicy`] against any connected broker by ignoring it and pairing
/// against its own.
pub struct Bound<B: Broker, S> {
    slot: BindSlot<B>,
    source: S,
}

impl<B: Broker, S: fmt::Debug> fmt::Debug for Bound<B, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bound")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl<B: Broker, S: Clone> Clone for Bound<B, S> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
            source: self.source.clone(),
        }
    }
}

impl<C, B, S> PublishPolicy<C> for Bound<B, S>
where
    C: ConnectedBroker,
    B: Broker + 'static,
    S: PublishPolicy<Connected<B>>,
{
    type Live = S::Live;

    async fn pair(self, _connected: &C) -> Result<Self::Live, PairError> {
        let connected = self.slot.0.borrow().clone().ok_or_else(|| {
            PairError::from_boxed(Box::from(
                "the token's broker is not connected: pairing happens after startup connects \
                 every registered broker",
            ))
        })?;
        // The token's publisher publishes to its own broker, whatever scope pairs it.
        #[cfg(feature = "testing")]
        pairs_against(connected.as_ref());
        self.source.pair(connected.as_ref()).await
    }
}
