//! The service of `examples/subscriber_settings.rs` written without the `macros` feature: the
//! settings chain on `subscriber(..)` before `.build()`.
//!
//! ```text
//! cargo test --example manual_subscriber_settings --no-default-features --features testing,memory,json
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

/// Bytes published as themselves: how the test injects a payload that does not decode. What
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

mod settings {
    use std::error::Error;
    use std::future::ready;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Wire};

    // --8<-- [start:settings]
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
        RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(
                subscriber(Unnamed::<Name>::new(), Bill)
                    .name("orders")
                    .workers(nonzero!(4))
                    // A payload that does not decode is acked and skipped, not dropped.
                    .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip))
                    .build(),
            )
        })
    }
    // --8<-- [end:settings]

    #[tokio::test]
    async fn a_malformed_order_is_skipped() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        // Bytes that are no JSON order.
        broker
            .message(&Wire(b"not json".to_vec()))
            .to("orders")
            .publish()
            .await?;

        broker
            .subscriber("orders")
            .assert_called_once()
            .settled(HandlerOutcome::ack())
            .assert_last_failed_to_decode();
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
