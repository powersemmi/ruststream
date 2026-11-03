//! Object-safe erasure of the broker lifecycle ladder, so
//! [`RustStream`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html) can hold brokers of different concrete types in one
//! collection.
//!
//! Only the ladder transitions (`connect`, `shutdown`) are erased here; they run once per broker
//! at startup and shutdown, never on the message hot path.
//! The transitions consume `self` in the public traits, and consuming survives erasure through
//! `self: Box<Self>` receivers. Subscribers and publishers stay fully typed elsewhere: the typed
//! connected broker travels from the erased `connect` to the typed starters through a shared
//! slot ([`ConnectedSlot`]), populated before any subscription opens.

#[cfg(ruststream_alloc)]
use alloc::boxed::Box;
#[cfg(feature = "std")]
use alloc::format;
#[cfg(feature = "std")]
use alloc::sync::Arc;
#[cfg(all(ruststream_alloc, feature = "testing"))]
use core::any::{Any, TypeId};
#[cfg(not(ruststream_alloc))]
use core::any::{Any, type_name};
use core::error::Error as StdError;
#[cfg(ruststream_alloc)]
use core::pin::Pin;
#[cfg(feature = "std")]
use core::{any::type_name, fmt};
#[cfg(not(ruststream_alloc))]
use core::{fmt, ops::Deref};

#[cfg(feature = "std")]
use crate::bindable::ConnectedSlot;
#[cfg(feature = "std")]
use crate::{Broker, ConnectedBroker, SendBroker};
#[cfg(not(ruststream_alloc))]
use ruststream_core::Cause;

/// A broker's error, erased.
#[cfg(ruststream_alloc)]
pub type BoxError = Box<dyn StdError + Send + Sync>;

/// A broker's error as a device without a heap holds it: the [`Cause`] it maps to, which is
/// `Copy`.
///
/// A [`Cause`] passes through as it is. Any other error is named by its type, because the error
/// value itself has no heap to live on: the type name is the `kind` and the code is zero. Like
/// the boxed error of a build with `alloc`, it does not implement `Error` itself; it dereferences
/// to the cause, which does.
#[cfg(not(ruststream_alloc))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoxError(Cause);

#[cfg(not(ruststream_alloc))]
impl BoxError {
    /// An error that says `kind`, with no client code.
    #[must_use]
    pub const fn msg(kind: &'static str) -> Self {
        Self(Cause::new(kind, 0))
    }

    /// The cause as an error to print or to walk the sources of.
    #[must_use]
    pub const fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        &self.0
    }
}

#[cfg(not(ruststream_alloc))]
impl<Error: StdError + Send + Sync + 'static> From<Error> for BoxError {
    fn from(error: Error) -> Self {
        // `Error: 'static`, so a `Cause` the caller already mapped is recognised and kept whole.
        let any: &dyn Any = &error;
        Self(
            any.downcast_ref::<Cause>()
                .copied()
                .unwrap_or_else(|| Cause::new(type_name::<Error>(), 0)),
        )
    }
}

#[cfg(not(ruststream_alloc))]
impl Deref for BoxError {
    type Target = Cause;

    fn deref(&self) -> &Cause {
        &self.0
    }
}

#[cfg(not(ruststream_alloc))]
impl fmt::Display for BoxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A future, erased, that may move between tasks.
#[cfg(ruststream_alloc)]
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
/// A lifecycle step's future, erased. It is not `Send`: a step runs once, on the task that starts
/// or stops the app, and the contract's lifecycle futures carry no `Send`.
#[cfg(ruststream_alloc)]
pub type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// The in-process transition of one registered broker type, erased.
///
/// It takes that broker boxed as `Any` and yields its connected form boxed the same way. A test registration builds it for the
/// concrete type it names, so the erased broker below can connect in process without knowing
/// which transport it is.
#[cfg(all(ruststream_alloc, feature = "testing"))]
pub type InProcessConnect =
    fn(Box<dyn Any + Send>) -> LocalBoxFuture<'static, Result<Box<dyn Any + Send>, BoxError>>;

/// An unconnected broker, with its concrete type and error erased. Consuming `connect` yields
/// the erased connected form.
#[cfg(ruststream_alloc)]
pub trait BrokerLifecycle: Send + Sync {
    /// Connects the broker, yielding its erased connected form.
    fn connect(
        self: Box<Self>,
    ) -> LocalBoxFuture<'static, Result<Box<dyn ConnectedLifecycle>, BoxError>>;

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
    ) -> LocalBoxFuture<'static, Result<Box<dyn ConnectedLifecycle>, BoxError>>;
}

/// A connected broker, erased. Consuming `shutdown` drives the typed
/// [`ConnectedBroker::shutdown`] and discards the typed witness (it carries no erasable surface).
#[cfg(ruststream_alloc)]
pub trait ConnectedLifecycle: Send + Sync {
    /// Shuts the broker down, discarding the typed witness.
    fn shutdown(self: Box<Self>) -> LocalBoxFuture<'static, Result<(), BoxError>>;
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

#[cfg(feature = "std")]
/// The typed unconnected broker paired with the slot its connected form will be published into.
#[doc(hidden)]
pub struct BrokerCell<B: Broker> {
    /// The broker, before it connects.
    pub broker: B,
    /// Where its connected form is published once it has.
    pub slot: ConnectedSlot<B>,
}

#[cfg(feature = "std")]
impl<B: Broker> fmt::Debug for BrokerCell<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerCell")
            .field("broker", &type_name::<B>())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "std")]
impl<B: SendBroker> BrokerLifecycle for BrokerCell<B> {
    fn connect(
        self: Box<Self>,
    ) -> LocalBoxFuture<'static, Result<Box<dyn ConnectedLifecycle>, BoxError>> {
        Box::pin(async move {
            let connected = self.broker.connect().await.map_err(BoxError::from)?;
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
    ) -> LocalBoxFuture<'static, Result<Box<dyn ConnectedLifecycle>, BoxError>> {
        let Self { broker, slot } = *self;
        let connecting = transition(Box::new(broker));
        Box::pin(async move {
            let connected = connecting.await?.downcast::<B::Connected>().map_err(|_| {
                BoxError::from(format!(
                    "the in-process transition registered for {} produced another connected form",
                    type_name::<B>(),
                ))
            })?;
            Ok(publish_connected::<B>(*connected, slot))
        })
    }
}

#[cfg(feature = "std")]
/// Publishes a freshly connected broker into its slot, for the starters to open subscriptions
/// against, and keeps it for teardown.
fn publish_connected<B: SendBroker>(
    connected: B::Connected,
    slot: ConnectedSlot<B>,
) -> Box<dyn ConnectedLifecycle> {
    let connected = Arc::new(connected);
    *slot.lock().expect("connected slot mutex poisoned") = Some(Arc::clone(&connected));
    Box::new(ConnectedCell::<B> { connected, slot })
}

#[cfg(feature = "std")]
/// The typed connected broker held for teardown, plus the slot still holding a reference to it.
struct ConnectedCell<B: Broker> {
    connected: Arc<B::Connected>,
    slot: ConnectedSlot<B>,
}

#[cfg(feature = "std")]
impl<B: SendBroker> ConnectedLifecycle for ConnectedCell<B> {
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

    fn shutdown(self: Box<Self>) -> LocalBoxFuture<'static, Result<(), BoxError>> {
        Box::pin(async move {
            // Starters cloned the slot's reference only for the duration of opening their
            // subscriptions, so after startup the slot and this cell hold the last two
            // references; dropping the slot's one lets the connected broker be consumed.
            self.slot
                .lock()
                .expect("connected slot mutex poisoned")
                .take();
            let connected = Arc::try_unwrap(self.connected).map_err(|_| {
                BoxError::from(format!(
                    "connected broker {} still aliased at shutdown",
                    type_name::<B>(),
                ))
            })?;
            connected
                .shutdown()
                .await
                .map(|_closed| ())
                .map_err(BoxError::from)
        })
    }
}

#[cfg(test)]
#[cfg(not(ruststream_alloc))]
mod fixed_tests {
    use core::error::Error;
    use core::fmt::Write;

    use heapless::String;
    use ruststream_core::Cause;

    use super::BoxError;

    #[derive(Debug, thiserror::Error)]
    #[error("link down")]
    struct LinkDown;

    fn printed(error: &BoxError) -> String<64> {
        let mut text = String::new();
        write!(text, "{error}").expect("fits");
        text
    }

    #[test]
    fn a_cause_passes_through_whole() {
        let error = BoxError::from(Cause::new("link down", 7));

        assert_eq!((error.kind(), error.code()), ("link down", 7));
    }

    #[test]
    fn any_other_error_is_named_by_its_type() {
        let error = BoxError::from(LinkDown);

        assert!(error.kind().ends_with("LinkDown"));
        assert_eq!(error.code(), 0);
    }

    #[test]
    fn a_message_carries_its_text_as_the_kind() {
        let error = BoxError::msg("a batch reply was refused");

        assert_eq!(
            printed(&error).as_str(),
            "a batch reply was refused (code 0)"
        );
    }

    #[test]
    fn the_error_is_copy_and_prints_as_its_cause() {
        let error = BoxError::from(Cause::new("link down", 7));
        let again = error;

        assert_eq!(printed(&error).as_str(), "link down (code 7)");
        assert!(again.source().is_none());
    }
}
