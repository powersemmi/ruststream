//! Object-safe erasure of the broker lifecycle ladder, so
//! [`RustStream`](super::RustStream) can hold brokers of different concrete types in one
//! collection.
//!
//! Only the ladder transitions (`connect`, `shutdown`) are erased here. They run once per broker
//! at startup and shutdown, never on the message hot path, so the boxing cost is negligible.
//! The transitions consume `self` in the public traits, and consuming survives erasure through
//! `self: Box<Self>` receivers. Subscribers and publishers stay fully typed elsewhere: the typed
//! connected broker travels from the erased `connect` to the typed starters through a shared
//! slot ([`ConnectedSlot`]), populated before any subscription opens.

use std::{
    any::type_name,
    error::Error as StdError,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use crate::{Broker, ConnectedBroker};

pub(crate) type BoxError = Box<dyn StdError + Send + Sync>;
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The channel between the erased `connect` and the typed starters: `connect` stores the typed
/// connected broker here, starters clone it out to open their subscriptions. The teardown takes
/// the slot's reference back so the connected broker can be consumed by value.
pub(crate) type ConnectedSlot<B> = Arc<Mutex<Option<Arc<<B as Broker>::Connected>>>>;

/// An unconnected broker, with its concrete type and error erased. Consuming `connect` yields
/// the erased connected form.
pub(crate) trait BrokerLifecycle: Send + Sync {
    fn connect(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<Box<dyn ConnectedLifecycle>, BoxError>>;
}

/// A connected broker, erased. Consuming `shutdown` drives the typed
/// [`ConnectedBroker::shutdown`] and discards the typed witness (it carries no erasable surface).
pub(crate) trait ConnectedLifecycle: Send + Sync {
    fn shutdown(self: Box<Self>) -> BoxFuture<'static, Result<(), BoxError>>;
    /// The concrete broker type's name, for diagnostics and logging.
    fn name(&self) -> &'static str;

    /// The concrete connected broker as `&dyn Any`, so the test harness can recover its type from
    /// the erased registration (`tb.broker::<MemoryBroker>()` resolves the connected form). The
    /// `where Self: 'static` bound keeps the method object-safe (it stays in the vtable) without
    /// tightening the impls below.
    #[cfg(feature = "testing")]
    fn as_any(&self) -> &dyn core::any::Any
    where
        Self: 'static;
}

/// The typed unconnected broker paired with the slot its connected form will be published into.
pub(crate) struct BrokerCell<B: Broker> {
    pub(crate) broker: B,
    pub(crate) slot: ConnectedSlot<B>,
}

impl<B: Broker + 'static> BrokerLifecycle for BrokerCell<B> {
    fn connect(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<Box<dyn ConnectedLifecycle>, BoxError>> {
        Box::pin(async move {
            let connected = Arc::new(
                self.broker
                    .connect()
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?,
            );
            *self.slot.lock().expect("connected slot mutex poisoned") =
                Some(Arc::clone(&connected));
            Ok(Box::new(ConnectedCell::<B> {
                connected,
                slot: self.slot,
            }) as Box<dyn ConnectedLifecycle>)
        })
    }
}

/// The typed connected broker held for teardown, plus the slot still holding a reference to it.
struct ConnectedCell<B: Broker> {
    connected: Arc<B::Connected>,
    slot: ConnectedSlot<B>,
}

impl<B: Broker + 'static> ConnectedLifecycle for ConnectedCell<B> {
    fn name(&self) -> &'static str {
        type_name::<B>()
    }

    #[cfg(feature = "testing")]
    fn as_any(&self) -> &dyn core::any::Any
    where
        Self: 'static,
    {
        self.connected.as_ref()
    }

    fn shutdown(self: Box<Self>) -> BoxFuture<'static, Result<(), BoxError>> {
        Box::pin(async move {
            // Starters cloned the slot's reference only for the duration of opening their
            // subscriptions, so after startup the slot and this cell hold the last two
            // references; dropping the slot's one lets the connected broker be consumed.
            self.slot
                .lock()
                .expect("connected slot mutex poisoned")
                .take();
            let connected = Arc::try_unwrap(self.connected)
                .map_err(|_| Box::from("connected broker still aliased at shutdown") as BoxError)?;
            connected
                .shutdown()
                .await
                .map(|_closed| ())
                .map_err(|e| Box::new(e) as BoxError)
        })
    }
}
