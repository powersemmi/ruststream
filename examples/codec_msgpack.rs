//! A service on the `MessagePack` codec. The scenario is a test; with `msgpack` as the only codec
//! feature, the command below builds it without the other formats.
//!
//! ```text
//! cargo test --example codec_msgpack --no-default-features --features testing,macros,memory,msgpack
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

/// `{"id": 7}` in JSON: a payload in another format.
const FOREIGN: &[u8] = br#"{"id":7}"#;

mod service {
    use std::error::Error;

    use ruststream::codec::MsgpackCodec;
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
        // Every handler of this scope decodes MessagePack.
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker_codec(
            MemoryBroker::new(),
            MsgpackCodec,
            |b| {
                b.include(bill);
            },
        )
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
            .with_codec(MsgpackCodec)
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .with_codec(&MsgpackCodec, &order)
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
