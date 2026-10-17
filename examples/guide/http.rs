//! The shop of `main.rs` beside an HTTP server: the storefront places orders over HTTP, and a
//! transactional outbox hands them to the broker.
//!
//! ```text
//! cargo run --example guide_http \
//!     --features testing,macros,memory,json,cbor,asyncapi,logging
//! cargo test --example guide_http \
//!     --features testing,macros,memory,json,cbor,asyncapi,logging
//! ```
//!
//! Place an order and watch the shop take it:
//!
//! ```text
//! curl -X POST http://127.0.0.1:8080/orders -H 'content-type: application/json' \
//!   -H 'x-request-id: req-1' \
//!   -d '{"id":1,"customer":"ada@example.com","lines":[{"sku":"mug","quantity":2,"price_cents":1200}]}'
//! ```

mod notifications;
mod observability;
mod orders;
mod payments;
mod service;
mod shipping;
mod state;

use std::collections::VecDeque;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Json, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use ruststream::codec::JsonCodec;
use ruststream::memory::prelude::*;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::orders::{Order, RequestMeta};
use crate::service::shop;

// --8<-- [start:store]
/// The orders the storefront took, and the events the shop still owes the broker. With a real
/// database both writes run in one transaction; the mutex over the pair plays that role here.
#[derive(Default)]
struct Store {
    orders: Vec<Order>,
    outbox: VecDeque<(RequestMeta, Order)>,
}
// --8<-- [end:store]

// --8<-- [start:endpoint]
/// Takes an order over HTTP. The request only writes to the store, so no broker I/O can fail or
/// stall the response.
async fn place_order(
    State(store): State<Arc<Mutex<Store>>>,
    headers: HeaderMap,
    Json(order): Json<Order>,
) -> (StatusCode, &'static str) {
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown")
        .to_owned();
    {
        let mut store = store.lock().await;
        store.orders.push(order.clone());
        store.outbox.push_back((RequestMeta { request_id }, order));
    }
    (StatusCode::ACCEPTED, "accepted\n")
}
// --8<-- [end:endpoint]

// --8<-- [start:relay]
/// Hands the outbox to the broker, oldest event first. A row leaves the outbox only after its
/// publish succeeded, so a broker outage delays events and loses none.
async fn relay_pending(store: &Mutex<Store>, egress: &impl Publisher) {
    loop {
        let Some((meta, order)) = store.lock().await.outbox.front().cloned() else {
            return;
        };
        let sent = egress
            .message(&order)
            .with_headers(&meta)
            .with_codec(JsonCodec)
            .publish()
            .await;
        if sent.is_err() {
            // The row stays for the next round.
            return;
        }
        store.lock().await.outbox.pop_front();
    }
}

/// Runs the relay every 200 milliseconds, for as long as the process runs.
async fn relay_outbox(store: Arc<Mutex<Store>>, egress: impl Publisher) {
    let mut rounds = tokio::time::interval(Duration::from_millis(200));
    loop {
        rounds.tick().await;
        relay_pending(&store, &egress).await;
    }
}
// --8<-- [end:relay]

// --8<-- [start:main]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    ruststream::logging::init()?;
    let broker = MemoryBroker::new().bindable();
    // A token now, a live publisher once the shop has connected the broker.
    let egress = broker.bind(Publish);
    // The shop starts first: a broker that refuses to connect stops the process before HTTP
    // takes a request.
    let running = shop(broker).start().await?;
    let egress = running.publisher(egress).await?;

    let store = Arc::new(Mutex::new(Store::default()));
    tokio::spawn(relay_outbox(Arc::clone(&store), egress));
    let router = axum::Router::new()
        .route("/orders", post(place_order))
        .with_state(store);

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    tracing::info!("storefront on http://127.0.0.1:8080/orders");
    // HTTP stops on Ctrl+C, or when the shop stops on its own; the shop then drains.
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
// --8<-- [end:main]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use axum::extract::{Json, State};
    use axum::http::{HeaderMap, StatusCode};
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use tokio::sync::Mutex;

    use super::{Store, place_order, relay_pending};
    use crate::orders::{Line, Order};
    use crate::service::{shop, test_broker};

    fn order(id: u64) -> Order {
        Order {
            id,
            customer: "ada@example.com".to_owned(),
            lines: vec![Line {
                sku: "mug".to_owned(),
                quantity: 2,
                price_cents: 1_200,
            }],
        }
    }

    async fn take(store: &Arc<Mutex<Store>>, id: u64) -> StatusCode {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", format!("req-{id}").parse().expect("ascii"));
        let (status, _) = place_order(State(Arc::clone(store)), headers, Json(order(id))).await;
        status
    }

    #[tokio::test]
    async fn an_order_taken_over_http_reaches_the_shop() -> Result<(), Box<dyn Error>> {
        let broker = test_broker().bindable();
        let egress = broker.bind(Publish);
        let tb = TestApp::start(shop(broker)).await?;
        let egress = egress.live().await?;
        let store = Arc::new(Mutex::new(Store::default()));

        assert_eq!(take(&store, 1).await, StatusCode::ACCEPTED);
        relay_pending(&store, &egress).await;
        tb.settle().await?;

        assert!(store.lock().await.outbox.is_empty());
        tb.broker::<MemoryBroker>()
            .published::<Order>("orders.placed")
            .assert_called_once()
            .with(&order(1))
            .with_header("request_id", "req-1");
        tb.broker::<MemoryBroker>()
            .subscriber("payments.requested")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    #[tokio::test]
    async fn an_event_the_broker_refused_stays_in_the_outbox() -> Result<(), Box<dyn Error>> {
        let broker = test_broker().bindable();
        let egress = broker.bind(Publish);
        let tb = TestApp::start(shop(broker)).await?;
        let egress = egress.live().await?;
        tb.shutdown().await?;
        let store = Arc::new(Mutex::new(Store::default()));

        take(&store, 2).await;
        relay_pending(&store, &egress).await;

        let store = store.lock().await;
        assert_eq!(store.orders, [order(2)]);
        assert_eq!(store.outbox.len(), 1);
        Ok(())
    }
}
