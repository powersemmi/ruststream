//! Extractor parameters without the `macros` feature: `State<T>` is a plain public type, so a
//! hand-written definition binds it exactly as the attribute does - one `FromContext` resolution
//! per extractor, before the body runs. What the feature takes away is only the two derives: the
//! definition is a named type with an `impl Handle`, and the state writes the per-field
//! `FromRef` impl `#[derive(FromRef)]` would have generated. Driven through the real dispatch
//! path with the in-process `TestApp` harness.
//!
//! ```text
//! cargo run --example manual_from_context --no-default-features --features testing,memory,json
//! ```

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
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

// The application state wires the interactors once at startup. `#[derive(FromRef)]` would write
// one impl per field; without the derive the state writes them, and each is what makes
// `State<FieldType>` resolve against this state.
// --8<-- [start:state]
struct AppState {
    create_order: CreateOrder,
}

impl FromRef<AppState> for CreateOrder {
    fn from_ref(state: &AppState) -> Self {
        state.create_order.clone()
    }
}
// --8<-- [end:state]

// The interactor still arrives as a bound value rather than a `ctx.state().create_order`
// reach-through: the extractor is public API, so the definition resolves it itself.
// --8<-- [start:handler]
/// The handler body: `#[subscriber("orders")]` generates this struct and this impl.
struct Receive;

impl Handle<Order, (), (), (), AppState> for Receive {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        // One binding per extractor parameter, in declaration order, before the body: this is
        // what the attribute emits for `State(create_order): State<CreateOrder>`. A rejection
        // settles the delivery by its `HandlerOutcome` and the body never runs.
        let State(create_order) =
            match <State<CreateOrder> as FromContext<(), AppState>>::from_context(ctx).await {
                Ok(value) => value,
                Err(rejection) => return Err(HandlerOutcome::from(rejection)),
            };

        create_order.execute(order);
        Ok(())
    }
}

// The state a body extracts from is the last axis of its own `impl Handle`, so
// `subscriber(source, body)` mounts it unchanged: the mount reads `AppState` off the impl and
// checks it against the app's.
// --8<-- [end:handler]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let processed = Arc::new(AtomicU64::new(0));
    let create_order = CreateOrder {
        processed: processed.clone(),
    };
    let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(AppState { create_order }))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Receive).build());
        });

    let tb = TestApp::start(app).await?;
    tb.publish("orders", &Order { id: 40 }).await?;
    tb.publish("orders", &Order { id: 2 }).await?;

    println!("processed total = {}", processed.load(Ordering::Relaxed));
    Ok(())
}
