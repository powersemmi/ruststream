use ruststream::memory::MemoryBroker;
use ruststream::runtime::{BlanketLayer, Handler, HandlerOutcome, Router};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// A layer written for the app-wide and router-wide stacks: it wraps a handler on any message
/// type, and says nothing about wrapping one concrete handler.
struct Observe;

impl BlanketLayer for Observe {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        handler
    }
}

// A `.layer(..)` after an `include` rides that one registration, whose handler type is concrete
// there, so it takes a `Layer<H>`. A blanket-only layer belongs on the router itself, before its
// first registration.
fn main() {
    let _router = Router::<MemoryBroker>::new().include(handle).layer(Observe);
}
