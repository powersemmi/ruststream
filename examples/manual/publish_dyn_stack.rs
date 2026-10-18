//! The service of `examples/publish_dyn_stack.rs` written without the `macros` feature. The
//! middleware is the same code in both forms; only the handler and its mounting differ.
//!
//! ```text
//! cargo test --example manual_publish_dyn_stack --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;
use std::pin::Pin;

use ruststream::Bytes;
use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishDynLayer, PublishDynNext};
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

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Receipt {
    order_id: u64,
}

// `#[outgoing(name = "receipts")]` by hand: the type fixes its destination.
impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

// --8<-- [start:layer]
/// Writes one header on every message the app publishes.
struct Stamp {
    // Both clone by reference count, so a publish copies no bytes.
    name: Str,
    value: Bytes,
}

impl PublishDynLayer for Stamp {
    fn on_publish<'a>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishDynNext<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Send + 'a>> {
        out.headers_mut()
            .insert(self.name.clone(), self.value.clone());
        next.run(out)
    }
}
// --8<-- [end:layer]

mod service {
    use std::error::Error;
    use std::future::ready;
    use std::sync::Arc;

    use ruststream::Bytes;
    use ruststream::memory::prelude::*;
    use ruststream::runtime::{PublishDynLayer, PublishDynStack};
    use ruststream::testing::TestApp;

    use super::{Order, Receipt, Stamp};

    // --8<-- [start:app]
    struct IssueReceipt;

    impl Handle<Order, Receipt> for IssueReceipt {
        fn handle(
            &self,
            order: &Order,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
            ready(Ok(Receipt { order_id: order.id }))
        }
    }

    /// The chain comes from configuration: one `Stamp` per header the operator listed.
    fn app(headers: &[(&str, &str)]) -> impl App {
        let chain = headers
            .iter()
            .map(|(name, value)| -> Arc<dyn PublishDynLayer> {
                Arc::new(Stamp {
                    name: Str::from((*name).to_owned()),
                    value: Bytes::from((*value).to_owned()),
                })
            });
        RustStream::new(AppInfo::new("billing", "0.1.0"))
            .publish_layer(PublishDynStack::new(chain))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(subscriber("orders", IssueReceipt).reply().build());
            })
    }
    // --8<-- [end:app]

    #[tokio::test]
    async fn the_receipt_carries_the_configured_headers() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app(&[("region", "eu"), ("tier", "gold")])).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt { order_id: 7 })
            .with_header("region", "eu")
            .with_header("tier", "gold");
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
