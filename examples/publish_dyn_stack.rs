//! A publish middleware chain assembled at run time: one `Stamp` per header the configuration
//! lists, frozen into a `PublishDynStack` on the app. The scenario is a test.
//!
//! ```text
//! cargo test --example publish_dyn_stack --features testing,macros,memory,json
//! ```

// A plain build compiles the services; the tests that use them exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;
use std::pin::Pin;

use ruststream::Bytes;
use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishDynLayer, PublishDynNext};
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
    use std::sync::Arc;

    use ruststream::Bytes;
    use ruststream::memory::prelude::*;
    use ruststream::runtime::{PublishDynLayer, PublishDynStack};
    use ruststream::testing::TestApp;

    use super::{Order, Receipt, Stamp};

    // --8<-- [start:app]
    #[subscriber("orders", reply)]
    async fn issue_receipt(order: &Order) -> Receipt {
        Receipt { order_id: order.id }
    }

    /// The chain comes from configuration: one `Stamp` per header the operator listed.
    fn app(headers: &[(&str, &str)]) -> impl App {
        let chain = headers.iter().map(|(name, value)| {
            let stamp = Stamp {
                name: Str::from((*name).to_owned()),
                value: Bytes::from((*value).to_owned()),
            };
            Arc::new(stamp) as Arc<dyn PublishDynLayer>
        });
        RustStream::new(AppInfo::new("billing", "0.1.0"))
            .publish_layer(PublishDynStack::new(chain))
            .with_broker(MemoryBroker::new(), |b| {
                b.include(issue_receipt);
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
