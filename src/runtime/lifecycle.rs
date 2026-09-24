//! Object-safe erasure of the broker lifecycle ladder, so
//! [`RustStream`](super::RustStream) can hold brokers of different concrete types in one
//! collection.
//!
//! Only the ladder transitions (`connect`, `shutdown`) are erased here; they run once per broker
//! at startup and shutdown, never on the message hot path.
//! The transitions consume `self` in the public traits, and consuming survives erasure through
//! `self: Box<Self>` receivers. Subscribers and publishers stay fully typed elsewhere: the typed
//! connected broker travels from the erased `connect` to the typed starters through a shared
//! slot ([`ConnectedSlot`]), populated before any subscription opens.

#[cfg(feature = "testing")]
use std::any::{Any, TypeId};
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

/// The in-process transition of one registered broker type, erased: it takes that broker boxed as
/// `Any` and yields its connected form boxed the same way. A test registration builds it for the
/// concrete type it names, so the erased broker below can connect in process without knowing
/// which transport it is.
#[cfg(feature = "testing")]
pub(crate) type InProcessConnect =
    fn(Box<dyn Any + Send>) -> BoxFuture<'static, Result<Box<dyn Any + Send>, BoxError>>;

/// The channel between the erased `connect` and the typed starters: `connect` stores the typed
/// connected broker here, starters clone it out to open their subscriptions. The teardown takes
/// the slot's reference back so the connected broker can be consumed by value. Public only
/// because [`BrokerRegistration`](crate::runtime::BrokerRegistration) names it in a hidden
/// method; never constructed by users.
#[doc(hidden)]
pub type ConnectedSlot<B> = Arc<Mutex<Option<Arc<<B as Broker>::Connected>>>>;

/// An unconnected broker, with its concrete type and error erased. Consuming `connect` yields
/// the erased connected form.
pub(crate) trait BrokerLifecycle: Send + Sync {
    fn connect(
        self: Box<Self>,
    ) -> BoxFuture<'static, Result<Box<dyn ConnectedLifecycle>, BoxError>>;

    /// The concrete broker type, which is what a test registration is keyed by.
    #[cfg(feature = "testing")]
    fn broker_type(&self) -> TypeId;

    /// The concrete broker type's name, for the harness to name a broker before it connects.
    #[cfg(feature = "testing")]
    fn broker_name(&self) -> &'static str;

    /// Connects through `transition`, the in-process transition registered for this broker's
    /// type, and publishes the connected form exactly as [`connect`](Self::connect) does, so the
    /// subscriptions and publishers resolve against it unchanged.
    #[cfg(feature = "testing")]
    fn connect_in_process(
        self: Box<Self>,
        transition: InProcessConnect,
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
    fn as_any(&self) -> &(dyn Any + Send + Sync)
    where
        Self: 'static;

    /// The concrete broker type this was connected from, which is how the test harness addresses
    /// it (`tb.broker::<MemoryBroker>()` names the broker, not its connected form).
    #[cfg(feature = "testing")]
    fn broker_type(&self) -> TypeId;
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
            let connected = self
                .broker
                .connect()
                .await
                .map_err(|e| Box::new(e) as BoxError)?;
            Ok(publish_connected::<B>(connected, self.slot))
        })
    }

    #[cfg(feature = "testing")]
    fn broker_type(&self) -> TypeId {
        TypeId::of::<B>()
    }

    #[cfg(feature = "testing")]
    fn broker_name(&self) -> &'static str {
        type_name::<B>()
    }

    #[cfg(feature = "testing")]
    fn connect_in_process(
        self: Box<Self>,
        transition: InProcessConnect,
    ) -> BoxFuture<'static, Result<Box<dyn ConnectedLifecycle>, BoxError>> {
        let Self { broker, slot } = *self;
        let connecting = transition(Box::new(broker));
        Box::pin(async move {
            let connected = connecting.await?.downcast::<B::Connected>().map_err(|_| {
                Box::from(format!(
                    "the in-process transition registered for {} produced another connected form",
                    type_name::<B>(),
                )) as BoxError
            })?;
            Ok(publish_connected::<B>(*connected, slot))
        })
    }
}

/// Publishes a freshly connected broker into its slot, for the starters to open subscriptions
/// against, and keeps it for teardown.
fn publish_connected<B: Broker + 'static>(
    connected: B::Connected,
    slot: ConnectedSlot<B>,
) -> Box<dyn ConnectedLifecycle> {
    let connected = Arc::new(connected);
    *slot.lock().expect("connected slot mutex poisoned") = Some(Arc::clone(&connected));
    Box::new(ConnectedCell::<B> { connected, slot })
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
    fn as_any(&self) -> &(dyn Any + Send + Sync)
    where
        Self: 'static,
    {
        self.connected.as_ref()
    }

    #[cfg(feature = "testing")]
    fn broker_type(&self) -> TypeId {
        TypeId::of::<B>()
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
            let connected = Arc::try_unwrap(self.connected).map_err(|_| {
                Box::from(format!(
                    "connected broker {} still aliased at shutdown",
                    type_name::<B>(),
                )) as BoxError
            })?;
            connected
                .shutdown()
                .await
                .map(|_closed| ())
                .map_err(|e| Box::new(e) as BoxError)
        })
    }
}
