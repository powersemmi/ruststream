//! The handler forms from the Subscribers guide: the basic contract, the context parameter, and
//! the manual (macro-free) registration.
//!
//! ```text
//! cargo run --example subscribers --features macros,memory,json -- run
//! ```

use ruststream::Name;
use ruststream::codec::JsonCodec;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, Context, HandlerMetadata, HandlerResult, RustStream, typed};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:contract]
#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerResult {
    println!("got order {}", order.id);
    HandlerResult::Ack
}
// --8<-- [end:contract]

// --8<-- [start:context]
#[subscriber("orders")]
async fn with_context(order: &Order, ctx: &mut Context<'_>) -> HandlerResult {
    if let Some(id) = ctx.headers().correlation_id() {
        println!("order {} correlates to {id}", order.id);
    }
    HandlerResult::Ack
}
// --8<-- [end:context]

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("subscribers", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(handle);
        b.include(with_context);
        // --8<-- [start:manual]
        b.subscribe(
            Name::new("orders"),
            typed(JsonCodec, |_order: &Order, _ctx: &mut Context| async {
                HandlerResult::Ack
            }),
            HandlerMetadata::typed::<Order>("orders"),
        );
        // --8<-- [end:manual]
    })
}
