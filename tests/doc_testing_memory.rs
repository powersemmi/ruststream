//! The `MemoryBroker` example from docs/guides/testing.md, kept compiling and passing here.
//! The guide embeds this file via snippet markers; keep the marked sections in sync with the
//! surrounding prose when editing.
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

// --8<-- [start:handler]
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    quantity: u32,
}

#[derive(Debug, Serialize)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

#[subscriber("orders", publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}
// --8<-- [end:handler]

// --8<-- [start:test]
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream, TypedPublisher};
use ruststream::testing::TestClient;
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn confirms_valid_orders() {
    let client = MemoryBroker::start().await.expect("start test broker");

    // The app under test: production wiring, in-memory broker. Cloning the broker
    // shares its state, so the client observes everything the app does.
    let ready = Arc::new(Notify::new());
    let on_ready = Arc::clone(&ready);
    let app = RustStream::new(AppInfo::new("orders-test", "0.0.0"))
        .after_startup(move |_state| async move {
            on_ready.notify_one();
            Ok::<_, Infallible>(())
        })
        .with_broker(client.broker().clone(), |b| {
            let replies = TypedPublisher::new(b.broker().publisher());
            b.include_publishing(confirm, replies);
        });

    let stop = Arc::new(Notify::new());
    let stop_signal = Arc::clone(&stop);
    let service = tokio::spawn(app.run_until(async move { stop_signal.notified().await }));

    // Subscriptions are not buffered: wait for after_startup (it runs once they are open),
    // then publish as an external producer.
    ready.notified().await;
    client
        .publish("orders", br#"{"id":1,"quantity":2}"#)
        .await
        .expect("publish");

    // The handler decoded the order and published a confirmation.
    let replies = client
        .expect_published("confirmations", 1, Duration::from_secs(1))
        .await
        .expect("expect_published");
    assert_eq!(replies.len(), 1, "expected one confirmation");
    let confirmation: serde_json::Value =
        serde_json::from_slice(replies[0].payload()).expect("valid JSON reply");
    assert_eq!(confirmation["id"], 1);
    assert_eq!(confirmation["accepted"], true);

    stop.notify_one();
    service.await.expect("join").expect("clean shutdown");
}
// --8<-- [end:test]
