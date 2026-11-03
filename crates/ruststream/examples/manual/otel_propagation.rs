//! The service of `examples/otel_propagation.rs` without the `macros` feature. The layer and the
//! transform mount the same way; the reply handler is a named type and `main` is written by hand.
//!
//! ```text
//! cargo run --example manual_otel_propagation --no-default-features \
//!     --features testing,memory,json,otel -- run
//! cargo test --example manual_otel_propagation --no-default-features \
//!     --features testing,memory,json,otel
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::memory::prelude::*;
use ruststream::otel::OpenTelemetry;
use ruststream::runtime::cli::run_main;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
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
        ready(Ok(Confirmation { id: order.id }))
    }
}

fn app() -> impl App {
    let otel = OpenTelemetry::new();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // A span per delivery, a child of the trace the delivery carries.
        .layer(otel.consume_layer())
        .with_broker(MemoryBroker::new(), |b| {
            // The reply carries the consumer span's trace context.
            b.include(
                subscriber("orders", Confirm)
                    .reply()
                    .to("confirmations")
                    .build(),
            )
            .out_reply(Publish)
            .transform(otel.propagation())
        })
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes: the command line on a multi-threaded Tokio runtime.
fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Confirmation, Order, app};

    const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const PARENT_ID: &str = "00f067aa0ba902b7";

    #[tokio::test]
    async fn the_reply_continues_the_incoming_trace() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let orders = tb.broker::<MemoryBroker>();
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", format!("00-{TRACE_ID}-{PARENT_ID}-01"));
        orders
            .message(&Order { id: 7 })
            .with_headers(headers)
            .to("orders")
            .publish()
            .await?;

        orders
            .published::<Confirmation>("confirmations")
            .assert_called_once();
        let replies = orders.published::<Confirmation>("confirmations");
        let traceparent = replies.messages()[0]
            .headers()
            .get_str("traceparent")
            .ok_or("the reply carries no traceparent")?
            .to_owned();
        // Same trace, a new span: the consumer's, which the reply names as its parent.
        let parts: Vec<&str> = traceparent.split('-').collect();
        assert_eq!(parts[1], TRACE_ID, "{traceparent}");
        assert_ne!(parts[2], PARENT_ID, "{traceparent}");
        Ok(())
    }
}
