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
/// How many lines an order may hold.
pub(crate) const MAX_LINES: usize = 100;
/// How many items one line may order.
pub(crate) const MAX_QUANTITY: u32 = 1_000;
/// The highest price of one item, in cents.
pub(crate) const MAX_PRICE_CENTS: u64 = 1_000_000;

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

/// Whether `lines` stay within the shop's bounds.
pub(crate) fn within_bounds(lines: &[Line]) -> bool {
    lines.len() <= MAX_LINES
        && lines
            .iter()
            .all(|line| line.quantity <= MAX_QUANTITY && line.price_cents <= MAX_PRICE_CENTS)
}

/// What `lines` cost together, or `None` past the shop's bounds.
pub(crate) fn total_cents(lines: &[Line]) -> Option<u64> {
    if !within_bounds(lines) {
        return None;
    }
    lines.iter().try_fold(0_u64, |total, line| {
        total.checked_add(u64::from(line.quantity).checked_mul(line.price_cents)?)
    })
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
            total_cents = total_cents(&order.lines),
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
        let Some(amount_cents) = total_cents(&order.lines) else {
            tracing::warn!(order = order.id, "an order past the shop's bounds, dropped");
            return Err(HandlerOutcome::drop());
        };
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
            amount_cents,
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Line, MAX_QUANTITY, Order, RequestMeta};
    use crate::notifications::Notice;
    use crate::payments::PaymentRequest;
    use crate::service::{shop, test_broker};

    fn order(id: u64, quantity: u32, price_cents: u64) -> Order {
        Order {
            id,
            customer: "ada@example.com".to_owned(),
            lines: vec![Line {
                sku: "mug".to_owned(),
                quantity,
                price_cents,
            }],
        }
    }

    #[tokio::test]
    async fn an_order_past_the_bounds_is_not_charged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(shop(test_broker())).await?;
        // A quantity past the bound, and a total that does not fit in 64 bits.
        for placed in [order(10, MAX_QUANTITY + 1, 0), order(11, 2, 1 << 63)] {
            let meta = RequestMeta {
                request_id: format!("req-{}", placed.id),
            };
            tb.message(&placed).with_headers(&meta).publish().await?;
        }

        let broker = tb.broker::<MemoryBroker>();
        broker
            .published::<PaymentRequest>("payments.requested")
            .assert_not_called();
        broker.published::<Notice>("notices").assert_not_called();
        Ok(())
    }
}
