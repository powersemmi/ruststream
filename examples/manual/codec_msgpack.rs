//! The service of `examples/codec_msgpack.rs` written without the `macros` feature.
//!
//! ```text
//! cargo test --example manual_codec_msgpack --no-default-features --features testing,memory,msgpack
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::convert::Infallible;

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

/// Bytes published as themselves: how the test injects a payload in another format. What
/// `#[derive(Outgoing, Serialized)]` writes, by hand.
struct Wire(Vec<u8>);

impl Serialized for Wire {
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
    }
}

impl MessageWire for Wire {
    type Wire = SerializedWire;
}

impl OutgoingDestination for Wire {
    type Form = CallerName;
}

impl MessageHeaders for Wire {
    type Contract = NoHeaders;
}

/// `{"id": 7}` in JSON: a payload in another format.
const FOREIGN: &[u8] = br#"{"id":7}"#;

mod service {
    use std::error::Error;
    use std::future::ready;

    use ruststream::codec::MsgpackCodec;
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{FOREIGN, Order, Wire};

    // --8<-- [start:app]
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

    fn app() -> impl App {
        // Every registration of this scope decodes MessagePack.
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker_codec(
            MemoryBroker::new(),
            MsgpackCodec,
            |b| b.include(subscriber("orders", Bill).build()),
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
