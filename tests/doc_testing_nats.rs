//! The in-memory NATS example from docs/guides/testing.md, kept compiling and passing here.
//! The guide embeds this file via snippet markers; keep the marked sections in sync with the
//! surrounding prose when editing.
#![cfg(all(feature = "macros", feature = "json"))]

// --8<-- [start:test]
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use ruststream::runtime::{AppInfo, RustStream, TypedPublisher};
use ruststream::subscriber;
use ruststream::testing::TestClient;
use ruststream_nats::testing::NatsTestClient;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Deserialize)]
struct Event {
    id: u64,
}

#[derive(Debug, Serialize)]
struct AuditRecord {
    id: u64,
}

#[subscriber("orders.*", publish("audit"))]
async fn audit(event: &Event) -> AuditRecord {
    AuditRecord { id: event.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_subscriber_audits_all_order_events() {
    let client = NatsTestClient::start().await.expect("start test broker");

    let ready = Arc::new(Notify::new());
    let on_ready = Arc::clone(&ready);
    let app = RustStream::new(AppInfo::new("audit-test", "0.0.0"))
        .after_startup(move |_state| async move {
            on_ready.notify_one();
            Ok::<_, Infallible>(())
        })
        .with_broker(client.broker().clone(), |b| {
            let records = TypedPublisher::new(b.broker().publisher());
            b.include_publishing(audit, records);
        });

    let stop = Arc::new(Notify::new());
    let stop_signal = Arc::clone(&stop);
    let service = tokio::spawn(app.run_until(async move { stop_signal.notified().await }));

    ready.notified().await;
    client
        .publish("orders.created", br#"{"id":1}"#)
        .await
        .expect("publish");
    client
        .publish("orders.updated", br#"{"id":2}"#)
        .await
        .expect("publish");

    // Both subjects matched "orders.*", so the handler ran twice and audited both.
    let records = client
        .expect_published("audit", 2, Duration::from_secs(1))
        .await
        .expect("expect_published");
    assert_eq!(records.len(), 2, "expected two audit records");

    stop.notify_one();
    service.await.expect("join").expect("clean shutdown");
}
// --8<-- [end:test]
