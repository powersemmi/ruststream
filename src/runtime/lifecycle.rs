//! Object-safe erasure of broker lifecycle, so [`RustStream`](super::RustStream) can hold brokers
//! of different concrete types in one collection.
//!
//! Only `connect` / `shutdown` are erased here. They run once per broker at startup and shutdown,
//! never on the message hot path, so the boxing cost is negligible. Subscribers and publishers
//! stay fully typed elsewhere.

use std::{error::Error as StdError, future::Future, pin::Pin};

use crate::Broker;

pub(crate) type BoxError = Box<dyn StdError + Send + Sync>;
pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Lifecycle of a broker, with its concrete type and error erased.
pub(crate) trait BrokerLifecycle: Send + Sync {
    fn connect(&self) -> BoxFuture<'_, Result<(), BoxError>>;
    fn shutdown(&self) -> BoxFuture<'_, Result<(), BoxError>>;
}

impl<B: Broker> BrokerLifecycle for B {
    fn connect(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            Broker::connect(self)
                .await
                .map_err(|e| Box::new(e) as BoxError)
        })
    }

    fn shutdown(&self) -> BoxFuture<'_, Result<(), BoxError>> {
        Box::pin(async move {
            Broker::shutdown(self)
                .await
                .map_err(|e| Box::new(e) as BoxError)
        })
    }
}
