//! The Prometheus example written without the `macros` feature: the reply-publishing subscriber
//! is a hand-written definition, mounted with `include` and given its reply publisher at the
//! mount site.
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
use ruststream::Name;
use ruststream::memory::{MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::metrics::Metrics;
use ruststream::runtime::{
    AppInfo, Context, Decoded, HandlerResult, IncludeDef, PublishExt, PublishingCall,
    PublishingDef, RustStream, TypedPublisher, forms,
};
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

/// The definition `#[subscriber("orders", publish("confirmations"))]` generates: what it
/// subscribes to, where the reply goes, and the body. A reply-publishing handler returns the
/// value rather than publishing it, so the runtime encodes and sends it - which is what puts it
/// through the app's publish pipeline.
struct Confirm;

impl PublishingDef for Confirm {
    type Input = Decoded<Order>;
    type Injections = ();
    type Reply = Confirmation;
    type Context = ();
    type Source = Name;

    fn source(&self) -> Name {
        Name::new("orders")
    }

    // The trait ties the borrow to `&self`; a destination fixed at compile time outlives it, and
    // saying so is what keeps the literal from reading as borrowed state.
    fn reply_name(&self) -> &'static str {
        "confirmations"
    }
}

// Generic over the app state: this handler reads none, so it mounts on any app. Pinning a
// concrete state type here is what a handler reaching for `ctx.state()` would do instead.
impl<S: Send + Sync> PublishingCall<S> for Confirm {
    // `Err(result)` skips the publish and settles by the returned outcome; `Ok(reply)` publishes
    // and acks.
    fn call(
        &self,
        order: &Order,
        _injections: &(),
        _ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Result<Confirmation, HandlerResult>> + Send {
        ready(Ok(Confirmation {
            id: order.id,
            accepted: order.quantity > 0,
        }))
    }
}

/// The form token tells `include` which mounting machinery this definition needs; a
/// reply-publishing subscriber names `forms::Publishing`.
impl IncludeDef for Confirm {
    type Form = forms::Publishing;
}

struct AppState {
    metrics: Metrics,
    ingest: MemoryPublisher,
}

async fn publish_order(State(state): State<Arc<AppState>>, body: Bytes) -> &'static str {
    let _ = state.ingest.raw(body.as_ref()).to("orders").publish().await;
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
            // naming it at the mount site is the same step written out.
            b.include(Confirm)
                .publisher(TypedPublisher::new(MemoryPublish));
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
