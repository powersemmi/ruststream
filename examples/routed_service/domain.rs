//! The domain model: the messages on the wire, the service's error type, and a stand-in
//! repository shared across handlers.
//!
//! Every payload derives [`JsonSchema`](ruststream::schemars::JsonSchema) so it contributes a
//! schema to the AsyncAPI document, and [`Message`](ruststream::Message) so the document names the
//! component after the type and uses its doc comment as the description. The [`Repository`] is a
//! fake for any real async resource (a connection pool, an HTTP client); it is opened once in the
//! startup hook and shared with every handler through [`Context`](ruststream::runtime::Context).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use ruststream::Message;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// An order placed by a customer, delivered on the `orders` channel.
#[derive(Debug, Clone, Deserialize, JsonSchema, Message)]
pub(crate) struct Order {
    pub(crate) id: u64,
    pub(crate) customer: String,
    pub(crate) item: String,
    pub(crate) quantity: u32,
}

/// A payment for an order, delivered on the `payments` channel and processed per customer.
#[derive(Debug, Clone, Deserialize, JsonSchema, Message)]
pub(crate) struct Payment {
    pub(crate) order_id: u64,
    pub(crate) customer: String,
    pub(crate) amount_cents: u64,
}

/// A cleared payment ready to settle, delivered in batches on the `clearings` channel.
#[derive(Debug, Clone, Deserialize, JsonSchema, Message)]
pub(crate) struct Clearing {
    pub(crate) order_id: u64,
    pub(crate) amount_cents: u64,
}

/// A request to cancel an order, delivered on the `cancellations` channel.
#[derive(Debug, Clone, Deserialize, JsonSchema, Message)]
pub(crate) struct Cancellation {
    pub(crate) order_id: u64,
}

/// The reply published to `confirmations` for each accepted or rejected order.
#[derive(Debug, Clone, Serialize, JsonSchema, Message)]
pub(crate) struct Confirmation {
    pub(crate) order_id: u64,
    pub(crate) accepted: bool,
}

/// The settlement published to `settlements` when a batch of clearings commits.
#[derive(Debug, Clone, Serialize, JsonSchema, Message)]
pub(crate) struct Settlement {
    pub(crate) order_id: u64,
    pub(crate) amount_cents: u64,
}

/// The service's single error type, with variants by source. `is_transient` is what lets a handler
/// distinguish a retryable blip (ask for redelivery) from a permanent failure (drop the message).
// --8<-- [start:error]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub(crate) enum ServiceError {
    /// The backing store is briefly unavailable; the caller should retry.
    #[error("repository is temporarily unavailable")]
    Unavailable,
    /// The order is not known to the store; retrying will not help.
    #[error("order {0} is unknown")]
    UnknownOrder(u64),
}

impl ServiceError {
    /// Whether retrying the operation could succeed. Drives the handlers' retry-versus-drop choice.
    pub(crate) const fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable)
    }
}
// --8<-- [end:error]

/// A stand-in persistence layer. Cheap to clone (the state is `Arc`-backed), so handlers each hold
/// their own handle to one shared store.
#[derive(Debug, Clone)]
pub(crate) struct Repository {
    inner: Arc<RepoInner>,
}

#[derive(Debug)]
struct RepoInner {
    orders: Mutex<HashSet<u64>>,
    // Flips on every `record_order` to make one call in two fail transiently, so the example
    // exercises the retry path deterministically rather than depending on real flakiness.
    fail_next: AtomicBool,
}

impl Repository {
    /// Opens the store. Stands in for the real connect; the startup hook awaits this before any
    /// broker connects.
    pub(crate) async fn open() -> Result<Self, ServiceError> {
        tokio::task::yield_now().await; // stands in for the network round trip
        Ok(Self {
            inner: Arc::new(RepoInner {
                orders: Mutex::new(HashSet::new()),
                fail_next: AtomicBool::new(false),
            }),
        })
    }

    /// Records an accepted order. Returns [`ServiceError::Unavailable`] on every other call to
    /// demonstrate transient-failure handling.
    pub(crate) async fn record_order(&self, id: u64) -> Result<(), ServiceError> {
        tokio::task::yield_now().await;
        if self.inner.fail_next.fetch_xor(true, Ordering::Relaxed) {
            return Err(ServiceError::Unavailable);
        }
        self.inner.orders.lock().expect("orders lock").insert(id);
        Ok(())
    }

    /// Charges a payment against an order. Always succeeds here; a real store would talk to a
    /// payment gateway.
    pub(crate) async fn charge(
        &self,
        _order_id: u64,
        _amount_cents: u64,
    ) -> Result<(), ServiceError> {
        tokio::task::yield_now().await;
        Ok(())
    }

    /// Cancels a recorded order. Unknown orders are a permanent error, not a transient one.
    pub(crate) async fn cancel(&self, order_id: u64) -> Result<(), ServiceError> {
        tokio::task::yield_now().await;
        if self
            .inner
            .orders
            .lock()
            .expect("orders lock")
            .remove(&order_id)
        {
            Ok(())
        } else {
            Err(ServiceError::UnknownOrder(order_id))
        }
    }

    /// Closes the store. The after-shutdown hook awaits this once brokers are down.
    pub(crate) async fn close(&self) {
        tokio::task::yield_now().await;
    }
}
