//! The order-confirmation handler of the routed-service example, written without the `macros`
//! feature. `#[subscriber(MemorySource::new("orders"), publish("confirmations"))]` mints a reply
//! definition, and `subscriber(..).reply().to(..)` builds the same one from values: the same
//! broker descriptor as the source, the same reply channel, and the body's
//! `Result<Confirmation, HandlerOutcome>` return as the `Handle` method's own signature. `include`
//! mounts it exactly as it mounts a generated one.
//!
//! The application state a body reads is the last axis of its `Handle` impl; this one reads a
//! `Repository`, so the mount checks that state against the app's.
//!
//! ```text
//! cargo run --example manual_routed_service_orders --no-default-features --features memory,json
//! ```

use std::collections::HashSet;
use std::error::Error;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

/// An order placed by a customer, delivered on the `orders` channel.
#[derive(Debug, Clone, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
    customer: String,
    item: String,
    quantity: u32,
}

/// The reply published to `confirmations` for each accepted or rejected order.
#[derive(Debug, Clone, Serialize, schemars::JsonSchema)]
struct Confirmation {
    order_id: u64,
    accepted: bool,
}

/// The service's error type. `is_transient` is what lets the handler distinguish a retryable blip
/// (ask for redelivery) from a permanent failure (drop the message).
#[derive(Debug, thiserror::Error)]
enum ServiceError {
    #[error("repository is temporarily unavailable")]
    Unavailable,
}

impl ServiceError {
    const fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

/// A stand-in persistence layer, opened once by the startup hook and shared with every handler as
/// the application state.
#[derive(Debug, Default)]
struct Repository {
    orders: Mutex<HashSet<u64>>,
    // Flips on every `record_order` so one call in two fails transiently, which exercises the
    // retry path deterministically rather than depending on real flakiness.
    fail_next: AtomicBool,
}

impl Repository {
    async fn open() -> Result<Self, ServiceError> {
        tokio::task::yield_now().await; // stands in for the network round trip
        Ok(Self::default())
    }

    async fn record_order(&self, id: u64) -> Result<(), ServiceError> {
        tokio::task::yield_now().await;
        if self.fail_next.fetch_xor(true, Ordering::Relaxed) {
            return Err(ServiceError::Unavailable);
        }
        self.orders.lock().expect("orders lock").insert(id);
        Ok(())
    }
}

/// Confirms an order and replies on `confirmations`.
///
/// Bound through the broker's own descriptor form, `MemorySource::new("orders")`, rather than a
/// bare name - the slot where a real broker takes its own descriptor (a NATS `SubscribeOptions`,
/// say). Returning `Result<Confirmation, HandlerOutcome>` keeps control of the acknowledgement:
/// `Ok` publishes the reply and acks, while `Err` publishes nothing and hands the dispatcher a
/// [`HandlerOutcome`] - here, retry on a transient store error and drop on a permanent one.
/// `.to(..)` names the reply channel; its publisher is wired on the same chain.
// --8<-- [start:descriptor]
struct Confirm;

// The state is named on the body, not on a definition: this one reads a `Repository`, so it is a
// `Handle` for that state alone and mounts only on an application that carries it.
impl Handle<Order, Confirmation, (), (), Repository> for Confirm {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), Repository>,
    ) -> Result<Confirmation, HandlerOutcome> {
        let repo = ctx.state();
        tracing::debug!(
            order = order.id,
            customer = %order.customer,
            item = %order.item,
            "confirming order"
        );
        match repo.record_order(order.id).await {
            Ok(()) => Ok(Confirmation {
                order_id: order.id,
                accepted: order.quantity > 0,
            }),
            Err(e) if e.is_transient() => {
                tracing::warn!(order = order.id, "store busy, asking for redelivery");
                Err(HandlerOutcome::retry())
            }
            Err(e) => {
                tracing::error!(order = order.id, error = %e, "dropping order");
                Err(HandlerOutcome::drop())
            }
        }
    }
}

/// The mount, and the whole declaration the attribute's clauses carried: the broker's own
/// descriptor as the source, `.to(..)` for the reply channel, and `.describe(..)` for the sentence
/// the attribute lifts off the handler's doc comment. The reply publisher is wiring, so it stays a
/// chain step on both paths: `TypedPublisher::new(Publish)` pairs the policy with the
/// default codec at startup.
fn confirm_route() -> impl RouterDef<MemoryBroker, Repository> {
    Router::<MemoryBroker>::new().include(
        subscriber(MemorySource::new("orders"), Confirm)
            .reply()
            .to("confirmations")
            .publisher(TypedPublisher::new(Publish))
            .describe("Confirms an order and replies on `confirmations`.")
            .build(),
    )
}
// --8<-- [end:descriptor]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders-service", "0.1.0"))
        .on_startup(async move |()| Repository::open().await)
        .with_broker(MemoryBroker::new(), |b| {
            b.include_router(confirm_route());
        })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
