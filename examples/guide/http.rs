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
use tokio_util::sync::CancellationToken;

use crate::orders::{Order, RequestMeta, total_cents};
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
    if total_cents(&order.lines).is_none() {
        return (
            StatusCode::BAD_REQUEST,
            "the order is past the shop's bounds\n",
        );
    }
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

/// Runs the relay every 200 milliseconds until `stop` completes, then runs it once more.
async fn relay_outbox(
    store: Arc<Mutex<Store>>,
    egress: impl Publisher,
    stop: impl Future<Output = ()>,
) {
    let mut rounds = tokio::time::interval(Duration::from_millis(200));
    tokio::pin!(stop);
    loop {
        tokio::select! {
            biased;
            () = &mut stop => break,
            _ = rounds.tick() => relay_pending(&store, &egress).await,
        }
    }
    // The last round runs once no request writes a row any more, so it empties the outbox.
    relay_pending(&store, &egress).await;
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
    let stop_relay = CancellationToken::new();
    let relay = tokio::spawn(relay_outbox(
        Arc::clone(&store),
        egress,
        stop_relay.clone().cancelled_owned(),
    ));
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
    // HTTP has drained, so no request writes a row any more: the relay's last round hands the
    // pending rows over while the shop still runs, and only then does the shop drain.
    stop_relay.cancel();
    relay.await?;
    running.shutdown().await?;
    Ok(())
}
// --8<-- [end:main]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::future::ready;
    use std::sync::Arc;

    use axum::extract::{Json, State};
    use axum::http::{HeaderMap, StatusCode};
    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use tokio::sync::Mutex;

    use super::{Store, place_order, relay_outbox, relay_pending};
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

    async fn place(store: &Arc<Mutex<Store>>, order: Order) -> StatusCode {
        let mut headers = HeaderMap::new();
        let request_id = format!("req-{}", order.id);
        headers.insert("x-request-id", request_id.parse().expect("ascii"));
        let (status, _) = place_order(State(Arc::clone(store)), headers, Json(order)).await;
        status
    }

    async fn take(store: &Arc<Mutex<Store>>, id: u64) -> StatusCode {
        place(store, order(id)).await
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

    #[tokio::test]
    async fn an_order_past_the_bounds_is_refused() {
        let store = Arc::new(Mutex::new(Store::default()));
        // A total that does not fit in 64 bits, and a quantity that would fill millions of
        // parcels.
        for (quantity, price_cents) in [(2, 1 << 63), (u32::MAX, 0)] {
            let line = Line {
                sku: "mug".to_owned(),
                quantity,
                price_cents,
            };
            let order = Order {
                lines: vec![line],
                ..order(5)
            };
            assert_eq!(place(&store, order).await, StatusCode::BAD_REQUEST);
        }

        assert_eq!(store.lock().await.orders, []);
        assert!(store.lock().await.outbox.is_empty());
    }

    #[tokio::test]
    async fn the_relay_hands_pending_rows_over_when_it_stops() -> Result<(), Box<dyn Error>> {
        let broker = test_broker().bindable();
        let egress = broker.bind(Publish);
        let tb = TestApp::start(shop(broker)).await?;
        let egress = egress.live().await?;
        let store = Arc::new(Mutex::new(Store::default()));
        take(&store, 3).await;

        // Told to stop before its first round, so only its last round can hand the row over.
        relay_outbox(Arc::clone(&store), egress, ready(())).await;
        tb.settle().await?;

        assert!(store.lock().await.outbox.is_empty());
        tb.broker::<MemoryBroker>()
            .published::<Order>("orders.placed")
            .assert_called_once()
            .with(&order(3));
        Ok(())
    }
}
