//! The middleware forms from the Middleware guide written without the `macros` feature: a
//! hand-written static layer and a dynamic middleware chain built at runtime, both composed into
//! the application stack.
//!
//! Middleware is macro-free already: a `Layer` mints a wrapper type that implements `Handler` by
//! delegating to the one it wraps, which is the same shape a hand-written handler has. So the
//! layer and the dynamic chain below are identical to the macro version, and only the handlers
//! they wrap - named types with an `impl Handler` - and their registration differ.
//!
//! ```text
//! AUDIT=1 cargo run --example manual_middleware --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Arc;

use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemoryMessage};
use ruststream::prelude::*;
use ruststream::runtime::{
    DynMiddleware, DynStack, Handler, HandlerMetadata, Identity, Layer, Next, Settle, Stack, typed,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// The definition value: `#[subscriber("orders")]` generates this struct and this impl.
struct Handle;

impl Handler<Order> for Handle {
    // A body with nothing to await returns the future directly, the same shape the rest of the
    // workspace uses; `async fn` here would be an unused async on a trait impl.
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("got order {}", order.id);
        ready(HandlerResult::ack().into())
    }
}

struct Returns;

impl Handler<Order> for Returns {
    fn handle(&self, order: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        println!("got return for order {}", order.id);
        ready(HandlerResult::ack().into())
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

impl<M: Send + Sync, H: Handler<M>> Handler<M> for Logged<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_>) -> Settle {
        println!("-> {}", ctx.name());
        let settle = self.0.handle(msg, ctx).await;
        println!("<- {}", ctx.name());
        settle
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
    ) -> Pin<Box<dyn Future<Output = Settle> + Send + 'a>> {
        Box::pin(async move {
            println!("[{}] handling {}", self.service, ctx.name());
            next.run(input, ctx).await
        })
    }
}
// --8<-- [end:dyn_middleware]

// The application stack's type names every layer, the dynamic chain included.
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
            b.subscribe(
                Name::new("orders"),
                typed(JsonCodec, Handle),
                HandlerMetadata::typed::<Order>("orders"),
            );
            b.subscribe(
                Name::new("returns"),
                typed(JsonCodec, Returns),
                HandlerMetadata::typed::<Order>("returns"),
            );
        })
    // --8<-- [end:dyn_stack]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
