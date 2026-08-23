//! The prelude has to carry a whole canonical service on its own: this test imports nothing from
//! `ruststream` except the glob and the broker, and exercises a subscriber, a publishing handler,
//! shared state and the application builder.

#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Order {
    id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Confirmation {
    id: u64,
}

/// The shared state a handler injects with `State`, reachable through the derived `FromRef`.
#[derive(Clone, FromRef)]
struct Ledger {
    seen: Arc<AtomicU64>,
}

#[subscriber("orders", publish("confirmations"))]
async fn confirm(order: &Order, State(seen): State<Arc<AtomicU64>>) -> Confirmation {
    seen.fetch_add(order.id, Ordering::SeqCst);
    Confirmation { id: order.id }
}

/// A subscription named where the service is wired up, through the settings surface the prelude
/// has to carry too.
#[subscriber]
async fn audit(order: &Order) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_prelude_carries_a_canonical_service() {
    let seen = Arc::new(AtomicU64::new(0));
    let ledger = Ledger {
        seen: Arc::clone(&seen),
    };

    let app = RustStream::new(AppInfo::new("prelude", "1.0"))
        .on_startup(async move |()| Ok::<_, std::convert::Infallible>(ledger))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(confirm)
                .publisher(TypedPublisher::new(MemoryPublish));
            b.include(audit.name("orders"));
        });

    let test = TestApp::start(app).await.expect("the app should start");
    test.broker::<MemoryBroker>()
        .publish("orders", &Order { id: 7 })
        .await
        .expect("publish should reach the subscriber");
    test.settle().await.expect("dispatch should settle");

    assert_eq!(seen.load(Ordering::SeqCst), 7);
    test.broker::<MemoryBroker>()
        .published::<Confirmation>("confirmations")
        .assert_called_once();
    test.shutdown().await.expect("shutdown should be clean");
}
