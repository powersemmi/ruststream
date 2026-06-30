//! Extractor parameters: a handler declares a dependency as an argument and the runtime resolves it
//! from the per-delivery context, instead of reaching for it through `ctx.state()`. The state
//! derives `FromRef`, so any field is injectable with `State<FieldType>` and no hand-written
//! extractor. Driven through the real dispatch path with the in-process `TestApp` harness.
//!
//! ```text
//! cargo run --example from_context --features testing,macros,memory,json
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, RustStream, State};
use ruststream::testing::TestApp;
use ruststream::{FromRef, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Order {
    id: u64,
}

// A small use-case object (an "interactor"): it holds its dependencies and runs one operation.
#[derive(Clone)]
struct CreateOrder {
    processed: Arc<AtomicU64>,
}

impl CreateOrder {
    fn execute(&self, order: &Order) {
        self.processed.fetch_add(order.id, Ordering::Relaxed);
    }
}

// The application state wires the interactors once at startup. `#[derive(FromRef)]` makes each
// field injectable with `State<FieldType>`, so no extractor impl is written by hand.
// --8<-- [start:state]
#[derive(FromRef)]
struct AppState {
    create_order: CreateOrder,
}
// --8<-- [end:state]

// The interactor arrives as a handler argument; no `ctx.state().create_order` reach-through.
// --8<-- [start:handler]
#[subscriber("orders")]
async fn handle(order: &Order, State(create_order): State<CreateOrder>) -> HandlerResult {
    create_order.execute(order);
    HandlerResult::Ack
}
// --8<-- [end:handler]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let processed = Arc::new(AtomicU64::new(0));
    let create_order = CreateOrder {
        processed: processed.clone(),
    };
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(move |()| async move {
            Ok::<_, std::convert::Infallible>(AppState { create_order })
        })
        .with_broker(MemoryBroker::new(), |b| b.include(handle));

    let tb = TestApp::start(app).await?;
    tb.broker::<MemoryBroker>()
        .publish("orders", &Order { id: 40 })
        .await?;
    tb.broker::<MemoryBroker>()
        .publish("orders", &Order { id: 2 })
        .await?;

    println!("processed total = {}", processed.load(Ordering::Relaxed));
    Ok(())
}
