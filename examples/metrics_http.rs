//! Serve Prometheus metrics over HTTP and drive them from an HTTP client.
//!
//! ```text
//! cargo run --example metrics_http --features macros,memory,metrics
//! ```
//!
//! Publish an order, then read the metrics:
//!
//! ```text
//! curl -X POST http://127.0.0.1:8080/orders -d '{"id":1,"quantity":3}'
//! curl http://127.0.0.1:8080/metrics
//! ```
//!
//! Each published order is consumed (incrementing the consume counter) and replied to on
//! `confirmations` through the metrics publish layer (incrementing the publish counter).

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::routing::{get, post};
use ruststream::memory::{MemoryBroker, MemoryPublisher};
use ruststream::metrics::Metrics;
use ruststream::runtime::{AppInfo, PublishExt, RustStream};
use ruststream::{Outgoing, Serialized, subscriber};
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

/// The HTTP request body on its way to the bus: bytes that arrived from outside, with no model of
/// their own. `Serialized` says they are already the payload, so no codec runs on them, and the
/// axum buffer moves in whole - nothing is copied on the way to the broker.
#[derive(Outgoing, Serialized)]
struct Ingest(Bytes);

// --8<-- [start:handler]
#[subscriber("orders", publish("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation {
        id: order.id,
        accepted: order.quantity > 0,
    }
}
// --8<-- [end:handler]

struct AppState {
    metrics: Metrics,
    ingest: MemoryPublisher,
}

async fn publish_order(State(state): State<Arc<AppState>>, body: Bytes) -> &'static str {
    let _ = state
        .ingest
        .message(&Ingest(body))
        .to("orders")
        .publish()
        .await;
    "published\n"
}

async fn serve_metrics(State(state): State<Arc<AppState>>) -> String {
    state.metrics.export().unwrap_or_default()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:wiring]
    let metrics = Metrics::new()?;
    let broker = MemoryBroker::new();
    let ingest = broker.publisher();
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(metrics.consume_layer())
        .publish_layer(metrics.publish_layer())
        .with_broker(broker, |b| {
            b.include(confirm);
        });
    // --8<-- [end:wiring]

    // The messaging side starts in the background and shares the metric collectors with the
    // HTTP state; a startup failure surfaces here, before HTTP accepts traffic.
    let running = app.start().await?;

    let state = Arc::new(AppState { metrics, ingest });
    let router = Router::new()
        .route("/orders", post(publish_order))
        .route("/metrics", get(serve_metrics))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    println!("metrics on http://127.0.0.1:8080/metrics");
    // The host owns the signals. HTTP stops on Ctrl+C, or when the messaging side tears itself
    // down (fail-fast); either way the messaging side then drains gracefully.
    let stopping = running.stopping();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                () = stopping => {}
            }
        })
        .await?;
    running.shutdown().await?;
    Ok(())
}
