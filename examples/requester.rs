//! `RequestReply`: a handler sends a request and awaits the correlated answer, on a broker that
//! correlates replies natively. The scenario is a test.
//!
//! ```text
//! cargo test --example requester --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Ask {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Quote {
    price: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.priced")]
struct Priced {
    id: u64,
    price: u64,
}

// --8<-- [start:requester]
#[derive(OutSlot)]
#[publishes(Priced)]
struct Pricing;

#[subscriber("orders")]
async fn price(order: &Order, Out(pricing): Out<impl RequestReply, Pricing>) -> HandlerOutcome {
    let Ok(ask) = serde_json::to_vec(&Ask { id: order.id }) else {
        return HandlerOutcome::drop();
    };
    let request = OutgoingMessage::new("quotes.ask", ask.as_slice());
    let Ok(answer) = pricing.request(request, Duration::from_secs(2)).await else {
        return HandlerOutcome::retry();
    };
    let Ok(quote) = serde_json::from_slice::<Quote>(answer.payload()) else {
        return HandlerOutcome::drop();
    };
    let priced = Priced {
        id: order.id,
        price: quote.price,
    };
    if pricing.message(&priced).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:requester]

/// Answers on the inbox the request named in its `reply-to` header.
struct ToInbox;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ToInbox {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(inbox) = cx.headers().get_shared("reply-to")
            && let Ok(name) = Str::try_from(inbox)
        {
            out.set_name(name);
        }
    }
}

#[subscriber("quotes.ask", reply("quotes.answers"))]
async fn quote_price(ask: &Ask) -> Quote {
    Quote { price: ask.id * 10 }
}

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // The broker's request policy: its live publisher correlates replies.
        b.include(price).out(Pricing, Request).build();
        b.include(quote_price).out_reply(Publish).transform(ToInbox);
    })
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Priced, app};

    #[tokio::test]
    async fn an_order_is_priced_by_the_quote_service() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 4 })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Priced>("orders.priced")
            .assert_called_once()
            .with(&Priced { id: 4, price: 40 });
        broker
            .subscriber("quotes.ask")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
