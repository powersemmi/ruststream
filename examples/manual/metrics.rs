//! The service of `examples/metrics.rs` without the `macros` feature. The layers and the route
//! are the same code; the reply handler is a named type.
//!
//! ```text
//! cargo run --example manual_metrics --no-default-features --features testing,memory,json,metrics
//! curl http://127.0.0.1:8080/metrics
//! cargo test --example manual_metrics --no-default-features --features testing,memory,json,metrics
//! ```

use std::error::Error;
use std::future::{Future, ready};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use ruststream::memory::prelude::*;
use ruststream::metrics::Metrics;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
    quantity: u32,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

// The mount site names the reply's destination, `reply().to("confirmations")`.
impl OutgoingDestination for Confirmation {
    type Form = CallerName;
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

// --8<-- [start:app]
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
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

fn app(metrics: &Metrics) -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // Counts and times every delivery, by subscription and outcome.
        .layer(metrics.consume_layer())
        // Counts every publish, the replies included, by destination and outcome.
        .publish_layer(metrics.publish_layer())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(
                subscriber("orders", Confirm)
                    .reply()
                    .to("confirmations")
                    .build(),
            );
        })
}
// --8<-- [end:app]

// --8<-- [start:scrape]
/// The `/metrics` route: the registry in the Prometheus text format.
async fn scrape(State(metrics): State<Metrics>) -> Result<String, StatusCode> {
    metrics
        .export()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}
// --8<-- [end:scrape]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // One collector per process, on the global registry.
    let metrics = Metrics::new()?;
    let running = app(&metrics).start().await?;

    let router = Router::new()
        .route("/metrics", get(scrape))
        .with_state(metrics);
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    // HTTP stops on Ctrl+C, or when the messaging side stops on its own; the messaging side then
    // drains.
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use axum::extract::State;
    use prometheus::Registry;
    use ruststream::memory::prelude::*;
    use ruststream::metrics::Metrics;
    use ruststream::testing::TestApp;

    use super::{Confirmation, Order, app, scrape};

    #[tokio::test]
    async fn the_scrape_counts_the_delivery_and_the_reply() -> Result<(), Box<dyn Error>> {
        // A registry of the test's own, so nothing else in the process writes into it.
        let metrics = Metrics::with_registry(Registry::new())?;
        let tb = TestApp::start(app(&metrics)).await?;
        let orders = tb.broker::<MemoryBroker>();
        orders
            .message(&Order { id: 1, quantity: 3 })
            .to("orders")
            .publish()
            .await?;
        orders
            .published::<Confirmation>("confirmations")
            .assert_called_once();

        let text = scrape(State(metrics))
            .await
            .map_err(|status| status.to_string())?;
        for line in [
            r#"ruststream_messages_consumed_total{name="orders",status="ack"} 1"#,
            r#"ruststream_consume_duration_seconds_count{name="orders"} 1"#,
            r#"ruststream_messages_published_total{name="confirmations",status="ok"} 1"#,
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "`{line}` missing from:\n{text}"
            );
        }
        Ok(())
    }
}
