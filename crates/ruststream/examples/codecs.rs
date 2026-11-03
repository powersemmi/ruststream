//! Where a mounting's codec comes from: the scope names one for every handler it mounts, a router
//! names one for its own handlers, and a reply position names the codec its replies leave with.
//! The scenario is a test.
//!
//! ```text
//! cargo test --example codecs --features testing,macros,memory,json,cbor
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Receipt {
    id: u64,
}

mod ladder {
    use std::error::Error;

    use ruststream::codec::{CborCodec, JsonCodec};
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt};

    #[subscriber("orders")]
    async fn bill(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "billed");
        HandlerOutcome::ack()
    }

    #[subscriber("audit")]
    async fn audit(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "audited");
        HandlerOutcome::ack()
    }

    #[subscriber("checkout", reply("receipts"))]
    async fn checkout(order: &Order) -> Receipt {
        Receipt { id: order.id }
    }

    // --8<-- [start:app]
    fn app() -> impl App {
        RustStream::new(AppInfo::new("codecs", "0.1.0")).with_broker_codec(
            MemoryBroker::new(),
            // Every handler of this scope decodes CBOR.
            CborCodec,
            |b| {
                // A router names the codec of the handlers it mounts.
                b.include_router(Router::new().with_codec(JsonCodec).include(audit))
                    .include(bill)
                    // A reply takes the default codec, JSON, unless its position names one.
                    .include(checkout)
                    .out_reply(Publish)
                    .codec(CborCodec)
            },
        )
    }
    // --8<-- [end:app]

    #[tokio::test]
    async fn each_mounting_reads_its_own_codec() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        let order = Order { id: 7 };

        broker
            .message(&order)
            .to("orders")
            .with_codec(CborCodec)
            .publish()
            .await?;
        broker
            .message(&order)
            .to("audit")
            .with_codec(JsonCodec)
            .publish()
            .await?;
        broker
            .message(&order)
            .to("checkout")
            .with_codec(CborCodec)
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with_codec(&CborCodec, &order)
            .settled(HandlerOutcome::ack());
        broker
            .subscriber("audit")
            .assert_called_once()
            .with_codec(&JsonCodec, &order)
            .settled(HandlerOutcome::ack());
        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with_codec(&CborCodec, &Receipt { id: 7 });
        Ok(())
    }

    #[tokio::test]
    async fn a_scope_handler_does_not_read_json() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .message(&Order { id: 7 })
            .to("orders")
            .with_codec(JsonCodec)
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .assert_last_failed_to_decode();
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
