//! The landing-page example written without the `macros` feature: the handler is a named type
//! with an `impl Handler`, mounted with the `subscriber` constructor, and `main` is
//! hand-written.
//!
//! ```text
//! cargo run --example manual_quickstart --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use serde::Deserialize;

// --8<-- [start:handler]
#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// The handler: `#[subscriber("orders")]` generates this type and this impl. A body with
/// nothing to await returns the future directly; a body that awaits writes `async fn handle`.
struct Handle;

impl Handler<Order> for Handle {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("got order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Handle));
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
// --8<-- [end:app]
