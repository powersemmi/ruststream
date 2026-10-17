//! The service of `examples/codecs.rs` written without the `macros` feature: the scope names the
//! codec for every registration, a registration names its own with `.codec(..)`, and a reply
//! position names the codec its replies leave with.
//!
//! ```text
//! cargo test --example manual_codecs --no-default-features --features testing,memory,json,cbor
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Receipt {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

impl OutgoingDestination for Receipt {
    type Form = CallerName;
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

mod ladder {
    use std::error::Error;
    use std::future::{Future, ready};

    use ruststream::codec::{CborCodec, JsonCodec};
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt};

    struct Bill;

    impl Handle<Order> for Bill {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "billed");
            ready(Ok(()))
        }
    }

    struct Audit;

    impl Handle<Order> for Audit {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            tracing::info!(order.id, "audited");
            ready(Ok(()))
        }
    }

    struct Checkout;

    impl Handle<Order, Receipt> for Checkout {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
            ready(Ok(Receipt { id: order.id }))
        }
    }

    // --8<-- [start:app]
    fn app() -> impl App {
        RustStream::new(AppInfo::new("codecs", "0.1.0")).with_broker_codec(
            MemoryBroker::new(),
            // Every registration of this scope decodes CBOR.
            CborCodec,
            |b| {
                b.include(subscriber("orders", Bill).build());
                // A registration names its own codec.
                b.include(subscriber("audit", Audit).codec(JsonCodec).build());
                // A reply takes the default codec, JSON, unless its position names one.
                b.include(
                    subscriber("checkout", Checkout)
                        .reply()
                        .to("receipts")
                        .build(),
                )
                .out_reply(Publish)
                .codec(CborCodec);
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
