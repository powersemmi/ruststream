//! Router composition from the Routing guide: per-module router builders, merged and mounted on
//! one broker.
//!
//! ```text
//! cargo run --example routing --features macros,memory,json -- run
//! ```

use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct Shipment {
    order_id: u64,
}

#[subscriber("orders")]
async fn accept(order: &Order) -> HandlerOutcome {
    println!("accepted order {}", order.id);
    HandlerOutcome::ack()
}

#[subscriber("shipments")]
async fn dispatch(shipment: &Shipment) -> HandlerOutcome {
    println!("dispatched shipment for order {}", shipment.order_id);
    HandlerOutcome::ack()
}

// --8<-- [start:builders]
fn orders() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    Router::new().include(accept)
}

fn shipping() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    Router::new().include(dispatch)
}
// --8<-- [end:builders]

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("routing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // --8<-- [start:merge]
        // Merge groups into one router, then mount the result.
        let all = orders().merge(shipping());
        b.include_router(all);
        // --8<-- [end:merge]
    })
}
