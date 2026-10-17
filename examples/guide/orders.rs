//! The orders side of the shop: what a placed order is, and the two subscribers that take it.

use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::notifications::{Notice, Notices};
use crate::payments::{INTERNAL, PaymentRequest};
use crate::state::Shop;

// --8<-- [start:order]
/// One line of an order: what the customer buys, how many, and at what price.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub(crate) struct Line {
    pub(crate) sku: String,
    pub(crate) quantity: u32,
    pub(crate) price_cents: u64,
}

/// An order placed in the storefront. It travels with the headers of the request that placed it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema, Outgoing)]
#[outgoing(name = "orders.placed", headers = RequestMeta)]
pub(crate) struct Order {
    pub(crate) id: u64,
    pub(crate) customer: String,
    pub(crate) lines: Vec<Line>,
}

impl Order {
    /// What the customer pays for the whole order.
    pub(crate) fn total_cents(&self) -> u64 {
        self.lines
            .iter()
            .map(|line| u64::from(line.quantity) * line.price_cents)
            .sum()
    }
}
// --8<-- [end:order]

// --8<-- [start:meta]
/// The headers of a placed order: each field is one header.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub(crate) struct RequestMeta {
    pub(crate) request_id: String,
}
// --8<-- [end:meta]

// --8<-- [start:audit]
/// Writes every placed order to the audit log, with the request that placed it.
#[subscriber("orders.placed")]
async fn audit(order: &Order, Headers(meta): Headers<RequestMeta>) -> HandlerOutcome {
    tracing::info!(
        request = %meta.request_id,
        order = order.id,
        total_cents = order.total_cents(),
        "order placed",
    );
    HandlerOutcome::ack()
}
// --8<-- [end:audit]

// --8<-- [start:accept]
/// Accepts an order: asks payments to charge it, and tells the customer it arrived.
#[subscriber("orders.placed", reply)]
async fn accept(
    order: &Order,
    Headers(meta): Headers<RequestMeta>,
    Out(notices): Out<impl Publisher, Notices>,
) -> Result<PaymentRequest, HandlerOutcome> {
    if order.lines.is_empty() {
        tracing::warn!(order = order.id, "an order with no lines, dropped");
        return Err(HandlerOutcome::drop());
    }
    let notice = Notice {
        customer: order.customer.clone(),
        text: format!("We have your order {}.", order.id),
    };
    if notices.message(&notice).publish().await.is_err() {
        return Err(HandlerOutcome::retry());
    }
    Ok(PaymentRequest {
        order: order.id,
        customer: order.customer.clone(),
        lines: order.lines.clone(),
        amount_cents: order.total_cents(),
        request_id: meta.request_id,
    })
}
// --8<-- [end:accept]

// --8<-- [start:routes]
/// The orders side's subscribers, with where their messages go.
pub(crate) fn routes() -> impl RouterDef<MemoryBroker, Shop> {
    Router::new()
        .include(audit)
        .include(accept)
        // The payment request travels as CBOR, the codec of the shop's own topics.
        .out_reply(Publish)
        .codec(INTERNAL)
        .out(Notices, Publish)
        .build()
}
// --8<-- [end:routes]
