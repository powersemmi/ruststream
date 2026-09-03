//! Router composition from the Routing guide, written without the `macros` feature: each group
//! builder mounts a named handler type with the `subscriber` constructor, and merging and mounting
//! are the same router calls the macro form makes.
//!
//! ```text
//! cargo run --example manual_routing --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Shipment {
    order_id: u64,
}

/// The definition value: `#[subscriber("orders")]` generates this struct and this impl.
struct Accept;

impl Handle<Order> for Accept {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("accepted order {}", order.id);
        ready(Ok(()))
    }
}

/// The definition value: `#[subscriber("shipments")]` generates this struct and this impl.
struct Dispatch;

impl Handle<Shipment> for Dispatch {
    fn handle(
        &self,
        shipment: &Shipment,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("dispatched shipment for order {}", shipment.order_id);
        ready(Ok(()))
    }
}

// --8<-- [start:builders]
fn orders() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    Router::new().include(subscriber("orders", Accept).build())
}

fn shipping() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    Router::new().include(subscriber("shipments", Dispatch).build())
}
// --8<-- [end:builders]

fn app() -> RustStream {
    RustStream::new(AppInfo::new("routing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // --8<-- [start:merge]
        // Merge groups into one router, then mount the result. Grouping is router API, not
        // declaration API, so it reads the same with the macros off.
        let all = orders().merge(shipping());
        b.include_router(all);
        // --8<-- [end:merge]
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
