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

use std::future::pending;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::routing::{get, post};
use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemoryPublisher};
use ruststream::metrics::Metrics;
use ruststream::runtime::{AppInfo, RustStream, TypedPublisher};
use ruststream::{OutgoingMessage, Publisher, subscriber};
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

struct AppState {
    metrics: Metrics,
    ingest: MemoryPublisher,
}

async fn publish_order(State(state): State<Arc<AppState>>, body: Bytes) -> &'static str {
    let _ = state
        .ingest
        .publish(OutgoingMessage::new("orders", body.as_ref()))
        .await;
    "published\n"
}

async fn serve_metrics(State(state): State<Arc<AppState>>) -> String {
    state.metrics.export().unwrap_or_default()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Metrics::new()?;
    let broker = MemoryBroker::new();
    let ingest = broker.publisher();

    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(metrics.consume_layer())
        .publish_layer(metrics.publish_layer())
        .with_broker(broker, |b| {
            let replies = TypedPublisher::new(b.broker().publisher(), JsonCodec);
            b.include_publishing(confirm, JsonCodec, replies);
        });

    // Run the service in the background; it shares the metric collectors with the HTTP state.
    tokio::spawn(async move {
        let _ = app.run_until(pending::<()>()).await;
    });

    let state = Arc::new(AppState { metrics, ingest });
    let router = Router::new()
        .route("/orders", post(publish_order))
        .route("/metrics", get(serve_metrics))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    println!("metrics on http://127.0.0.1:8080/metrics");
    axum::serve(listener, router).await?;
    Ok(())
}
