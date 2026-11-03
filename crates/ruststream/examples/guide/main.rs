//! The shop the "Build a service" guide builds, chapter by chapter: orders, payments, shipping
//! and notifications on the in-memory broker. `http.rs` runs the same shop beside an HTTP server.
//!
//! ```text
//! cargo run --example guide --features testing,macros,memory,json,cbor,asyncapi,logging -- run
//! cargo run --example guide --features testing,macros,memory,json,cbor,asyncapi,logging \
//!     -- asyncapi gen
//! cargo test --example guide --features testing,macros,memory,json,cbor,asyncapi,logging
//! ```

mod notifications;
mod observability;
mod orders;
mod payments;
mod service;
mod shipping;
mod state;

use ruststream::memory::prelude::*;

use crate::state::Shop;

// --8<-- [start:main]
/// `run` starts the shop; `asyncapi gen` prints its AsyncAPI document.
#[ruststream::app(flavor = "current_thread")]
fn app() -> impl App<State = Shop> {
    service::shop(MemoryBroker::new())
}
// --8<-- [end:main]

// The generated `main` runs the builder; these tests run the same builder in process.
#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use ruststream::codec::CborCodec;
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use serde_json::Value;

    use super::app;
    use crate::notifications::Notice;
    use crate::orders::{Line, Order, RequestMeta};
    use crate::payments::PaymentRequest;

    fn order(id: u64, lines: Vec<Line>) -> Order {
        Order {
            id,
            customer: "ada@example.com".to_owned(),
            lines,
        }
    }

    fn mugs(quantity: u32) -> Vec<Line> {
        vec![Line {
            sku: "mug".to_owned(),
            quantity,
            price_cents: 1_200,
        }]
    }

    fn meta(request_id: &str) -> RequestMeta {
        RequestMeta {
            request_id: request_id.to_owned(),
        }
    }

    // --8<-- [start:journey]
    #[tokio::test]
    async fn a_placed_order_is_charged_shipped_and_announced() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&order(7, mugs(12)))
            .with_headers(&meta("req-7"))
            .publish()
            .await?;

        let broker = tb.broker::<MemoryBroker>();
        broker
            .published::<PaymentRequest>("payments.requested")
            .assert_called_once()
            .with_codec(
                &CborCodec,
                &PaymentRequest {
                    order: 7,
                    customer: "ada@example.com".to_owned(),
                    lines: mugs(12),
                    amount_cents: 14_400,
                    request_id: "req-7".to_owned(),
                },
            );
        broker
            .published::<Notice>("notices")
            .assert_called(2)
            .with(&Notice {
                customer: "ada@example.com".to_owned(),
                text: "Order 7 is on its way: TRK-7-2.".to_owned(),
            });
        broker
            .subscriber("notices")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
    // --8<-- [end:journey]

    // --8<-- [start:empty]
    #[tokio::test]
    async fn an_order_with_no_lines_goes_no_further() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.message(&order(8, Vec::new()))
            .with_headers(&meta("req-8"))
            .publish()
            .await?;

        let broker = tb.broker::<MemoryBroker>();
        broker
            .published::<PaymentRequest>("payments.requested")
            .assert_not_called();
        broker.published::<Notice>("notices").assert_not_called();
        Ok(())
    }
    // --8<-- [end:empty]

    #[tokio::test]
    async fn an_order_without_its_request_headers_is_dropped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        // The bare publish skips the header contract the typed builder would demand.
        broker.publish("orders.placed", &order(9, mugs(1))).await?;

        broker
            .subscriber("orders.placed")
            .assert_called(2)
            .settled(HandlerOutcome::drop())
            .assert_last_failed_to_decode();
        broker
            .published::<PaymentRequest>("payments.requested")
            .assert_not_called();
        Ok(())
    }

    // Both forms of the shop document the same channels, operations and schemas.
    #[test]
    fn the_document_is_the_published_one() -> Result<(), Box<dyn Error>> {
        let built: Value = serde_json::from_str(&build_spec(&app()).to_json()?)?;
        let published: Value = serde_json::from_str(include_str!("asyncapi.json"))?;
        assert_eq!(built, published);
        Ok(())
    }
}
