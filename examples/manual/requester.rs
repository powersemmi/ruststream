//! The service of `examples/requester.rs` written without the `macros` feature.
//!
//! ```text
//! cargo test --example manual_requester --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::ready;
use std::time::Duration;

use ruststream::OutgoingMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Ask {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Quote {
    price: u64,
}

impl OutgoingDestination for Quote {
    type Form = CallerName;
}

impl MessageHeaders for Quote {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Priced {
    id: u64,
    price: u64,
}

impl OutgoingDestination for Priced {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.priced";
}

impl MessageHeaders for Priced {
    type Contract = NoHeaders;
}

// --8<-- [start:requester]
struct Pricing;

impl OutSlot for Pricing {
    const NAME: &'static str = "Pricing";
    type Destination = Reads;
}

impl PublishedThrough<Pricing> for Priced {}

struct Price;

impl<P> Handle<Order, (), Outs<(P,)>> for Price
where
    P: OutEntry<Pricing, Wire: RequestReply>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(P,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let pricing = outs.get(Pricing);
        let ask = serde_json::to_vec(&Ask { id: order.id }).map_err(|_| HandlerOutcome::drop())?;
        let request = OutgoingMessage::new("quotes.ask", ask.as_slice());
        let answer = pricing
            .request(request, Duration::from_secs(2))
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        let quote = serde_json::from_slice::<Quote>(answer.payload())
            .map_err(|_| HandlerOutcome::drop())?;
        let priced = Priced {
            id: order.id,
            price: quote.price,
        };
        pricing
            .message(&priced)
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())
    }
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

struct QuoteAsk;

impl Handle<Ask, Quote> for QuoteAsk {
    fn handle(
        &self,
        ask: &Ask,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Quote, HandlerOutcome>> {
        ready(Ok(Quote { price: ask.id * 10 }))
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // The broker's request policy: its live publisher correlates replies.
        b.include(subscriber("orders", Price).build())
            .out(Pricing, Request)
            .build();
        b.include(
            subscriber("quotes.ask", QuoteAsk)
                .reply()
                .to("quotes.answers")
                .build(),
        )
        .out_reply(Publish)
        .transform(ToInbox);
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
