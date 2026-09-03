//! The Prometheus example written without the `macros` feature: the reply body is a named type
//! whose `impl Handle` names the reply type, bound to its subject by the `subscriber` constructor
//! and given its destination and reply publisher on the same chain.
//!
//! ```text
//! cargo run --example manual_metrics_http --no-default-features --features memory,metrics,json
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

use std::future::{Future, ready};
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::routing::{get, post};
use ruststream::memory::{MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::metrics::Metrics;
use ruststream::runtime::{
    AppInfo, Context, Handle, HandlerOutcome, MessageWire, PublishExt, RustStream, Serialized,
    SerializedWire, TypedPublisher, subscriber,
};
use ruststream::{CallerName, MessageHeaders, NoHeaders, OutgoingDestination};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
    quantity: u32,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

/// The HTTP request body on its way to the bus: bytes that arrived from outside, with no model of
/// their own. What `#[derive(Outgoing, Serialized)]` writes is these three impls - the bytes are
/// already the payload so no codec runs on them, the wire spelling routes the publish onto that
/// lane, and no destination is declared, so the call site names one. The axum buffer moves in
/// whole; nothing is copied on the way to the broker.
struct Ingest(Bytes);

impl Serialized for Ingest {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl MessageWire for Ingest {
    type Wire = SerializedWire;
}

impl OutgoingDestination for Ingest {
    type Form = CallerName;
}

impl MessageHeaders for Ingest {
    type Contract = NoHeaders;
}

/// The body `#[subscriber("orders", publish("confirmations"))]` generates. A reply-publishing
/// handler returns the value rather than publishing it, so the runtime encodes and sends it -
/// which is what puts it through the app's publish pipeline.
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    // `Err(result)` skips the publish and settles by the returned outcome; `Ok(reply)` publishes
    // and acks.
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation {
            id: order.id,
            accepted: order.quantity > 0,
        }))
    }
}

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
            // The attribute reads the reply publisher off the broker's default publish policy;
            // naming it on the chain is the same step written out.
            b.include(
                subscriber("orders", Confirm)
                    .reply()
                    .to("confirmations")
                    .publisher(TypedPublisher::new(MemoryPublish))
                    .build(),
            );
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
