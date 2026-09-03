//! A reply that carries its own typed header contract: the handler answers with a
//! `Message<Headers, Payload>`, so the contract is serialized into the outgoing headers and the
//! body through the reply codec, in one publish.
//!
//! The consumer downstream reads the reply back through the pair input, which is what proves both
//! halves survived the trip: a reply that only encoded its body would fail the header assertion,
//! and one that only stamped headers would fail the body assertion.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

use std::sync::Mutex;

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

#[derive(Serialize, Deserialize, Debug, PartialEq, schemars::JsonSchema)]
struct Receipt {
    total: u32,
}

/// What the consumer read back off the reply: the two header fields next to the body field.
static CONFIRMED: Mutex<Vec<(String, u32, u32)>> = Mutex::new(Vec::new());

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

#[subscriber("pair.receipts")]
async fn audit(receipt: &Message<ReceiptMeta, Receipt>) -> HandlerOutcome {
    CONFIRMED
        .lock()
        .expect("the test holds no poisoned lock")
        .push((
            receipt.headers.tenant.clone(),
            receipt.headers.order_id,
            receipt.body.total,
        ));
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pair_reply_carries_its_contract_into_the_outgoing_headers() {
    let app = RustStream::new(AppInfo::new("pair-reply", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(confirm).publisher(Publish);
            b.include(audit);
        },
    );
    let tb = TestApp::start(app).await.expect("start");

    // The single-broker convenience: the app registers one broker, so the publish needs no
    // addressing.
    tb.publish("pair.orders", &Order { id: 4 })
        .await
        .expect("publish");

    tb.broker::<MemoryBroker>()
        .subscriber("pair.receipts")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
    assert_eq!(
        CONFIRMED
            .lock()
            .expect("the test holds no poisoned lock")
            .as_slice(),
        [("acme".to_owned(), 4, 40)],
    );

    tb.shutdown().await.expect("shutdown");
}
