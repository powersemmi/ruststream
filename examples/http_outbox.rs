//! Accept orders over HTTP with axum and hand them to the same service's subscriber through a
//! transactional outbox.
//!
//! ```text
//! cargo run --example http_outbox --features macros,memory,json
//! ```
//!
//! Place an order and watch the subscriber pick it up:
//!
//! ```text
//! curl -X POST http://127.0.0.1:8080/orders \
//!   -H 'content-type: application/json' -d '{"id":1,"item":"book"}'
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use ruststream::codec::{Codec, JsonCodec};
use ruststream::memory::{MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::runtime::{AppInfo, HandlerResult, HealthProbe, HealthState, RustStream};
use ruststream::{Broker, OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// --8<-- [start:event]
/// The integration event the HTTP side hands to the messaging side.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderPlaced {
    id: u64,
    item: String,
}
// --8<-- [end:event]

// --8<-- [start:handler]
/// The same service consumes what its HTTP endpoints produce; any other service subscribed to
/// the broker would see the event too.
#[subscriber("orders.placed")]
async fn fulfil(order: &OrderPlaced) -> HandlerResult {
    println!("fulfilling order {} ({})", order.id, order.item);
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:store]
/// The business records and the pending outbox rows, committed together. With a real database
/// both inserts run in one SQL transaction; the mutex over the pair plays that role here, so an
/// order is never stored without its event, and no event exists without its order.
#[derive(Default)]
struct Store {
    orders: Vec<OrderPlaced>,
    outbox: VecDeque<OrderPlaced>,
}
// --8<-- [end:store]

// --8<-- [start:endpoint]
/// The request path only writes to the store: recording the order and queueing its event is one
/// atomic step, and no broker I/O can fail or stall the HTTP response.
async fn place_order(
    State(store): State<Arc<Mutex<Store>>>,
    Json(order): Json<OrderPlaced>,
) -> &'static str {
    let mut store = store.lock().await;
    store.orders.push(order.clone());
    store.outbox.push_back(order);
    "accepted\n"
}
// --8<-- [end:endpoint]

// --8<-- [start:relay]
/// Drains the outbox into the broker. A row is removed only after its publish succeeds, so a
/// broker outage delays events instead of losing them; a crash between publish and removal
/// re-publishes the row on restart, which is the usual at-least-once contract of an outbox.
async fn relay_outbox(store: Arc<Mutex<Store>>, egress: MemoryPublisher) {
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        tick.tick().await;
        loop {
            let Some(event) = store.lock().await.outbox.front().cloned() else {
                break;
            };
            let payload = JsonCodec.encode(&event).expect("serializable");
            let out = OutgoingMessage::new("orders.placed", payload.as_ref());
            if egress.publish(out).await.is_err() {
                // Keep the row and retry on the next tick.
                break;
            }
            store.lock().await.outbox.pop_front();
        }
    }
}
// --8<-- [end:relay]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:healthz]
    /// The liveness endpoint: the probe outlives `shutdown`, so this route reports the terminal
    /// state for as long as requests still reach it. Here that window is the graceful drain: a
    /// fail-fast also resolves the `stopping()` future below and stops the HTTP server. A host
    /// that should keep serving after the messaging side dies would omit that select arm.
    async fn healthz(State(health): State<HealthProbe>) -> (StatusCode, String) {
        match health.state() {
            HealthState::Running => (StatusCode::OK, "running".to_owned()),
            state => (StatusCode::SERVICE_UNAVAILABLE, format!("{state:?}")),
        }
    }
    // --8<-- [end:healthz]

    // --8<-- [start:wiring]
    let broker = MemoryBroker::new().bindable();
    // A token, not a publisher: minted before registration, paired after start.
    let egress = broker.bind(MemoryPublish);
    let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(fulfil);
    });

    // The messaging side starts in the background; a startup failure (a broker refusing to
    // connect, a subscription failing to open) surfaces here, before HTTP accepts traffic.
    let running = app.start().await?;
    // The handle existing witnesses that startup connected the broker, so the relay task gets a
    // live publisher, never a "not connected" state.
    let egress = running.publisher(egress).await?;

    let store = Arc::new(Mutex::new(Store::default()));
    tokio::spawn(relay_outbox(store.clone(), egress));

    let router = Router::new()
        .route("/orders", post(place_order))
        .with_state(store)
        // The health probe is `Clone` and outlives the app handle; /healthz flips to 503 the
        // moment the messaging side fail-fasts, answering through the graceful drain below.
        .route("/healthz", get(healthz).with_state(running.health()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
    println!("orders API on http://127.0.0.1:8080/orders");
    // The host owns the signals. HTTP stops on Ctrl+C, or when the messaging side tears itself
    // down (fail-fast) so the process does not keep serving with a dead consumer; either way the
    // messaging side then drains gracefully.
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
    // --8<-- [end:wiring]
    Ok(())
}
