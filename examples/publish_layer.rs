//! A publish layer: app-wide middleware around every publish a handler makes, here a header that
//! names the service. The scenario is a test.
//!
//! ```text
//! cargo test --example publish_layer --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;

use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishLayer, PublishNext, PublishPipeline};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

// --8<-- [start:layer]
/// Names the service on every message it publishes.
#[derive(Clone)]
struct Origin {
    service: &'static str,
}

impl PublishLayer for Origin {
    async fn on_publish<'a, N: PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        out.headers_mut().insert("origin", self.service);
        next.run(out).await
    }
}
// --8<-- [end:layer]

mod service {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Origin, Receipt};

    // --8<-- [start:app]
    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Receipt {
        Receipt { order_id: order.id }
    }

    fn app() -> impl App {
        RustStream::new(AppInfo::new("billing", "0.1.0"))
            // The replies and the `Out` slot publishes of the handlers below pass through it.
            .publish_layer(Origin { service: "billing" })
            .with_broker(MemoryBroker::new(), |b| {
                b.include(issue_receipt);
            })
    }
    // --8<-- [end:app]

    #[tokio::test]
    async fn the_receipt_names_its_service() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
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
            .with_header("origin", "billing");
        Ok(())
    }
}

// The scenarios are the tests above; the command in the header runs them.
fn main() {}
