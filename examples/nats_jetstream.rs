//! A `JetStream` durable consumer: the same handler, mounted on a pull consumer instead of a Core
//! subscription.
//!
//! A `#[subscriber("subject")]` handler carries a by-name source. To bind it to `JetStream` we
//! override that source with [`SubscribeOptions`] via [`include_on`], naming the stream and a
//! durable consumer so progress survives restarts. The handler's [`HandlerResult::Ack`] acks the
//! message back to `JetStream`; returning [`HandlerResult::Nack`] schedules redelivery.
//!
//! `include_on` is the source-override form; the codec resolves the same way as for `include`
//! (the default, or a scope codec set with `with_broker_codec`).
//! `NatsBroker::new` is synchronous, so this still fits `#[ruststream::app]`; the runtime connects
//! the broker at startup and then opens the consumer. Create the stream once, then run:
//!
//! ```text
//! nats stream add ORDERS --subjects 'orders.*' --defaults
//! cargo run --example nats_jetstream --features macros,json -- run
//! ```
//!
//! Publish into the stream from another terminal:
//!
//! ```text
//! nats pub orders.created '{"id":1}'
//! ```
//!
//! [`include_on`]: ruststream::runtime::BrokerScope::include_on

use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream_nats::{NatsBroker, SubscribeOptions};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders.created")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}

// The descriptor can also sit directly in the decorator: the macro follows the builder chain, so
// this handler mounts with a plain `include` and no separate source argument.
// --8<-- [start:decorator]
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("orders-audit"))]
async fn audit(order: &Order) -> HandlerResult {
    println!("audited order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:decorator]

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
        NatsBroker::new("nats://localhost:4222"),
        |b| {
            // --8<-- [start:include_on]
            b.include_on(
                SubscribeOptions::new("orders.*")
                    .jetstream("ORDERS")
                    .durable("orders-worker"),
                handle,
            );
            // --8<-- [end:include_on]
            b.include(audit);
        },
    )
}
