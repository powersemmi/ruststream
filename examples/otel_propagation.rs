//! W3C trace context across a service: the consume layer continues the trace a delivery carries,
//! and the propagation transform puts it on the reply, so the next service joins the same trace.
//!
//! ```text
//! cargo run --example otel_propagation --features testing,macros,memory,json,otel -- run
//! cargo test --example otel_propagation --features testing,macros,memory,json,otel
//! ```

use ruststream::memory::prelude::*;
use ruststream::otel::OpenTelemetry;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Confirmation {
    id: u64,
}

// --8<-- [start:app]
#[subscriber("orders", reply("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id }
}

#[ruststream::app]
fn app() -> impl App {
    let otel = OpenTelemetry::new();
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // A span per delivery, a child of the trace the delivery carries.
        .layer(otel.consume_layer())
        .with_broker(MemoryBroker::new(), |b| {
            // The reply carries the consumer span's trace context.
            b.include(confirm)
                .out_reply(Publish)
                .transform(otel.propagation());
        })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::HeaderMap;
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
