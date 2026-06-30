//! Extractor parameters: a handler declares a dependency as an argument and the runtime resolves it
//! from the per-delivery context with [`FromContext`], instead of reaching for it through
//! `ctx.state()`. Driven through the real dispatch path with the in-process `TestApp` harness.
//!
//! ```text
//! cargo run --example from_context --features testing,macros,memory,json
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, Context, FromContext, HandlerResult, RustStream};
use ruststream::subscriber;
use ruststream::testing::TestApp;
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

// The application state wires the interactors once at startup.
struct AppState {
    create_order: CreateOrder,
}

// The extractor: clone the interactor out of the state so the handler can take it by value.
impl<C: Send> FromContext<C, AppState> for CreateOrder {
    type Rejection = HandlerResult;
    async fn from_context(ctx: &mut Context<'_, C, AppState>) -> Result<Self, HandlerResult> {
        Ok(ctx.state().create_order.clone())
    }
}

// The interactor arrives as a handler argument; no `ctx.state().create_order` reach-through.
#[subscriber("orders")]
async fn handle(order: &Order, create_order: CreateOrder) -> HandlerResult {
    create_order.execute(order);
    HandlerResult::Ack
}

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
