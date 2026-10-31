//! Batch reply transforms: the steps a batch handler's replies pass, each reply on its own, with
//! the batch's context. The scenario is a test.
//!
//! ```text
//! cargo test --example batch_transform --features testing,macros,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::memory::prelude::*;
use ruststream::runtime::{BatchPublishTransform, Outgoing, PublishContext, for_batch};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Receipt {
    id: u64,
}

// --8<-- [start:transform]
/// Marks every reply of a batch. It sees the batch's context, not one delivery's.
struct Batched;

impl<C, Options> BatchPublishTransform<C, Options> for Batched {
    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        _cx: &PublishContext<'_, C>,
    ) {
        out.headers_mut().insert("batched", "true");
    }
}

/// A per-message transform, reused on the batch path through `for_batch`.
struct Service(&'static str);

impl<K: ContextKind, Options> PublishTransform<K, Options> for Service {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
        out.headers_mut().insert("service", self.0);
    }
}
// --8<-- [end:transform]

#[subscriber("orders", reply("receipts"))]
async fn confirm(orders: &[Order]) -> Vec<Receipt> {
    orders
        .iter()
        .map(|order| Receipt { id: order.id })
        .collect()
}

// --8<-- [start:app]
fn app(broker: MemoryBroker) -> impl App {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm.batch(nonzero!(64)))
            .out_reply(Publish)
            .batch_transform(Batched)
            .batch_transform(for_batch(Service("billing")))
    })
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt, app};

    #[tokio::test]
    async fn every_reply_of_a_batch_is_stamped() -> Result<(), Box<dyn Error>> {
        let broker = MemoryBroker::new();
        let publisher = broker.publisher();
        let tb = TestApp::start(app(broker)).await?;
        for id in [1, 2] {
            publisher
                .message(&Order { id })
                .to("orders")
                .publish()
                .await?;
        }
        tb.settle().await?;

        let handle = tb.broker::<MemoryBroker>();
        handle
            .subscriber("orders")
            .assert_called_once()
            .assert_batch_sizes(&[2]);
        let receipts = handle.published::<Receipt>("receipts").assert_called(2);
        assert_eq!(receipts.decoded(), [Receipt { id: 1 }, Receipt { id: 2 }]);
        for receipt in receipts.messages() {
            assert_eq!(receipt.headers().get("batched"), Some(b"true".as_slice()));
            assert_eq!(
                receipt.headers().get("service"),
                Some(b"billing".as_slice())
            );
        }
        Ok(())
    }
}
