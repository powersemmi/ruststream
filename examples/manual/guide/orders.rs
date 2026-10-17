//! The orders side of the shop without the `macros` feature: the message types declare their
//! destination and headers through the trait impls the derives would write, and each subscriber
//! is a named type with an `impl Handle`.

use std::future::{Future, ready};

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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub(crate) struct Order {
    pub(crate) id: u64,
    pub(crate) customer: String,
    pub(crate) lines: Vec<Line>,
}

// What `#[derive(Outgoing)]` with `#[outgoing(name = "orders.placed", headers = RequestMeta)]`
// writes: the destination, the headers a publish must carry, and the name in the document.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.placed";
}

impl MessageHeaders for Order {
    type Contract = WithHeaders<RequestMeta>;
}

impl MessageInfo for Order {
    const NAME: &'static str = "Order";
    const DESCRIPTION: Option<&'static str> = Some(
        "An order placed in the storefront. It travels with the headers of the request that \
         placed it.",
    );
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
struct Audit;

// The input is the pair of headers and payload: both decode before the body runs, and the
// document shows the header schema, as a `Headers<RequestMeta>` parameter does with the macros.
impl Handle<Message<RequestMeta, Order>, (), (), (), Shop> for Audit {
    fn handle(
        &self,
        placed: &Message<RequestMeta, Order>,
        _outs: &(),
        _ctx: &mut Context<'_, (), Shop>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let (meta, order) = (&placed.headers, &placed.body);
        tracing::info!(
            request = %meta.request_id,
            order = order.id,
            total_cents = order.total_cents(),
            "order placed",
        );
        ready(Ok(()))
    }
}
// --8<-- [end:audit]

// --8<-- [start:accept]
/// Accepts an order: asks payments to charge it, and tells the customer it arrived.
struct Accept;

impl<Entry> Handle<Message<RequestMeta, Order>, PaymentRequest, Outs<(Entry,)>, (), Shop> for Accept
where
    Entry: OutEntry<Notices, Wire: Publisher>,
{
    async fn handle(
        &self,
        placed: &Message<RequestMeta, Order>,
        outs: &Outs<(Entry,)>,
        _ctx: &mut Context<'_, (), Shop>,
    ) -> Result<PaymentRequest, HandlerOutcome> {
        let (meta, order) = (&placed.headers, &placed.body);
        if order.lines.is_empty() {
            tracing::warn!(order = order.id, "an order with no lines, dropped");
            return Err(HandlerOutcome::drop());
        }
        let notice = Notice {
            customer: order.customer.clone(),
            text: format!("We have your order {}.", order.id),
        };
        if outs.get(Notices).message(&notice).publish().await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(PaymentRequest {
            order: order.id,
            customer: order.customer.clone(),
            lines: order.lines.clone(),
            amount_cents: order.total_cents(),
            request_id: meta.request_id.clone(),
        })
    }
}
// --8<-- [end:accept]

// --8<-- [start:routes]
/// The orders side's subscribers, with where their messages go.
pub(crate) fn routes() -> impl RouterDef<MemoryBroker, Shop> {
    Router::new()
        .include(
            subscriber(Order::DESTINATION, Audit)
                .describe(
                    "Writes every placed order to the audit log, with the request that placed it.",
                )
                .build(),
        )
        .include(
            subscriber(Order::DESTINATION, Accept)
                .describe(
                    "Accepts an order: asks payments to charge it, and tells the customer it \
                     arrived.",
                )
                .reply()
                .build(),
        )
        // The payment request travels as CBOR, the codec of the shop's own topics.
        .out_reply(Publish)
        .codec(INTERNAL)
        .out(Notices, Publish)
        .build()
}
// --8<-- [end:routes]
