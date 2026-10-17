//! A service on the JSON codec. The scenario is a test; with `json` as the only codec
//! feature, the command below builds it without the other formats.
//!
//! ```text
//! cargo test --example codec_json --no-default-features --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

/// Bytes published as themselves: how the test injects a payload in another format.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

/// `{"id": 7}` in CBOR: a payload in another format.
const FOREIGN: &[u8] = b"\xa1\x62id\x07";

mod service {
    use std::error::Error;

    use ruststream::codec::JsonCodec;
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{FOREIGN, Order, Wire};

    // --8<-- [start:app]
    #[subscriber("orders")]
    async fn bill(order: &Order) -> HandlerOutcome {
        tracing::info!(order.id, "billed");
        HandlerOutcome::ack()
    }

    fn app() -> impl App {
        // No codec named anywhere: the handler decodes with the default codec, JSON.
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(bill);
        })
    }
    // --8<-- [end:app]

    #[tokio::test]
    async fn an_order_decodes() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        let order = Order { id: 7 };

        broker
            .message(&order)
            .to("orders")
            .with_codec(JsonCodec)
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with_codec(&JsonCodec, &order)
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn another_format_does_not_decode() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();

        broker
            .message(&Wire(FOREIGN.to_vec()))
            .to("orders")
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
