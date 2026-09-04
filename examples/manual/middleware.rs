//! The middleware forms from the Middleware guide written without the `macros` feature: a
//! hand-written static layer on the application stack, and a dynamic middleware chain built at
//! runtime and wrapped around one handler.
//!
//! Middleware is macro-free already: a `Layer` mints a wrapper type that implements the dispatch
//! trait `Handler` by delegating to the one it wraps, and the runtime derives that dispatch
//! handler from either path's body. So the layer and the dynamic chain below are identical to the
//! macro version, and only the handlers they wrap - named types with an `impl Handle` - and their
//! registration differ.
//!
//! ```text
//! AUDIT=1 cargo run --example manual_middleware --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Arc;

use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::runtime::{
    BlanketLayer, DynMiddleware, DynStack, Handler, Identity, Layer, Next, Stack,
};
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

/// The definition value: `#[subscriber("orders")]` generates this struct and this impl.
struct Receive;

impl Handle<Order> for Receive {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("got order {}", order.id);
        ready(Ok(()))
    }
}

/// The body the dynamic chain wraps: an ordinary definition value, like every other.
struct Returns;

impl Handle<Order> for Returns {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("got return for order {}", order.id);
        ready(Ok(()))
    }
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

impl<M: Send + Sync, C: Send, S: Send + Sync, H: Handler<M, C, S>> Handler<M, C, S> for Logged<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        println!("-> {}", ctx.name());
        let outcome = self.0.handle(msg, ctx).await;
        println!("<- {}", ctx.name());
        outcome
    }
}

// The application stack wraps every handler through `BlanketLayer`: the mount site hides the
// handler's concrete type, so the wrap happens through this generic method instead.
impl BlanketLayer for LogLayer {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        Logged(handler)
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

// The application stack's type names every layer it carries. The dynamic chain is not one of
// them: it is fixed to a single input type, so it rides one handler instead.
fn app() -> RustStream<Stack<LogLayer, Identity>> {
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

    // ...and the frozen DynStack is an ordinary static Layer - but one bound to a single input
    // type, so it rides one registration instead of the application stack, which takes only
    // blanket layers (they wrap a handler on any input; this one cannot). `.layer(..)` after an
    // `include` wraps that registration, outside its decode step, so a chain built over the
    // broker's message type sees the raw delivery.
    RustStream::new(info)
        .layer(LogLayer)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Receive).build());
            b.include_router(
                Router::<MemoryBroker>::new()
                    .include(subscriber("returns", Returns).build())
                    .layer(stack),
            );
        })
    // --8<-- [end:dyn_stack]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
