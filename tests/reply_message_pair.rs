//! A reply that carries its own typed header contract: the handler answers with a
//! `Message<Headers, Payload>`, so the contract is serialized into the outgoing headers and the
//! body through the reply codec, in one publish.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, schemars::JsonSchema)]
struct Order {
    id: u32,
}

/// The contract the answer travels with: the handler declares it as part of the reply value
/// instead of reaching for a publisher to stamp it on.
#[derive(Serialize, Deserialize, Debug, PartialEq, schemars::JsonSchema)]
struct ReceiptMeta {
    tenant: String,
    order_id: u32,
}

#[derive(Outgoing, Serialize, Deserialize, Debug, PartialEq, schemars::JsonSchema)]
struct Receipt {
    total: u32,
}

#[subscriber("pair.orders", publish("pair.receipts"))]
async fn confirm(order: &Order) -> Message<ReceiptMeta, Receipt> {
    Message::new(
        ReceiptMeta {
            tenant: "acme".to_owned(),
            order_id: order.id,
        },
        Receipt {
            total: order.id * 10,
        },
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pair_reply_carries_its_contract_into_the_outgoing_headers() {
    let app = RustStream::new(AppInfo::new("pair-reply", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm).out(Reply, Publish);
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    // The single-broker convenience: the app registers one broker, so the publish needs no
    // addressing.
    tb.publish("pair.orders", &Order { id: 4 })
        .await
        .expect("publish");

    // The contract went out as headers next to a body encoded by the reply codec: a reply that
    // only encoded its body fails the header assertions, one that only stamped headers the body.
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("pair.receipts")
        .assert_called_once()
        .with(&Receipt { total: 40 })
        .with_header("tenant", b"acme")
        .with_header("order_id", b"4");
}
