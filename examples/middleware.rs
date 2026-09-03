//! The middleware forms from the Middleware guide: a hand-written static layer and a dynamic
//! middleware chain built at runtime, both composed into the application stack.
//!
//! ```text
//! AUDIT=1 cargo run --example middleware --features macros,memory,json -- run
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{DynMiddleware, DynStack, Handler, Identity, Layer, Next, Stack};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[subscriber("returns")]
async fn returns(order: &Order) -> HandlerOutcome {
    println!("got return for order {}", order.id);
    HandlerOutcome::ack()
}

// --8<-- [start:layer_impl]
#[derive(Clone)]
struct LogLayer;

struct Logged<H>(H);

impl<H> Layer<H> for LogLayer {
    type Handler = Logged<H>;
    fn layer(&self, inner: H) -> Logged<H> {
        Logged(inner)
    }
}

impl<M: Send + Sync, H: Handler<M>> Handler<M> for Logged<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_>) -> HandlerOutcome {
        println!("-> {}", ctx.name());
        let outcome = self.0.handle(msg, ctx).await;
        println!("<- {}", ctx.name());
        outcome
    }
}
// --8<-- [end:layer_impl]

// --8<-- [start:dyn_middleware]
struct Audit {
    service: String,
}

impl<I: Send + Sync> DynMiddleware<I> for Audit {
    fn handle<'a>(
        &'a self,
        input: &'a I,
        ctx: &'a mut Context<'_>,
        next: Next<'a, I>,
    ) -> Pin<Box<dyn Future<Output = HandlerOutcome> + Send + 'a>> {
        Box::pin(async move {
            println!("[{}] handling {}", self.service, ctx.name());
            next.run(input, ctx).await
        })
    }
}
// --8<-- [end:dyn_middleware]

// The application stack's type names every layer, the dynamic chain included.
#[ruststream::app]
fn app() -> RustStream<Stack<DynStack<MemoryMessage>, Stack<LogLayer, Identity>>> {
    let audit_enabled = std::env::var("AUDIT").is_ok();
    let info = AppInfo::new("middleware", "0.1.0");
    // --8<-- [start:dyn_stack]
    // The chain is decided at runtime...
    let mut middleware: Vec<Arc<dyn DynMiddleware<MemoryMessage>>> = Vec::new();
    if audit_enabled {
        middleware.push(Arc::new(Audit {
            service: "orders".to_owned(),
        }));
    }
    let stack = DynStack::new(middleware); // empty list -> a no-op layer

    // ...but the frozen DynStack is an ordinary static Layer: compose it into the
    // application stack like any other (HandlerExt::with works too, per handler).
    RustStream::new(info)
        .layer(LogLayer)
        .layer(stack)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(handle);
            b.include(returns);
        })
    // --8<-- [end:dyn_stack]
}
