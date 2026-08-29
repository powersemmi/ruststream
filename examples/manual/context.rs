//! The Context guide's example written without the `macros` feature: the handler is a named type
//! whose `impl Handler` reads all three things the per-delivery `Context` carries (the channel
//! name, the headers working copy, shared state), plus a middleware that enriches the headers
//! before it runs.
//!
//! ```text
//! cargo run --example manual_context --no-default-features --features memory,json
//! ```

use std::convert::Infallible;
use std::error::Error;
use std::future::{Future, ready};
use std::sync::atomic::{AtomicU64, Ordering};

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{Decoded, Identity, IncludeDef, Layer, Stack, SubscriberDef, forms};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:state]
/// Shared configuration: produced once at startup, read by every handler as the typed app state.
/// Unchanged without the attribute - this handler reads the state through `ctx.state()`, which
/// needs nothing of the state but its type.
#[derive(Debug)]
struct AppConfig {
    reject_zero_ids: bool,
}
// --8<-- [end:state]

// --8<-- [start:handler]
/// The definition value: `#[subscriber("orders")]` generates this struct and this impl. The two
/// context parameters the attribute would infer are spelled out - `()` for the broker's
/// per-delivery context, `AppConfig` for the application state.
struct Handle;

impl Handler<Order, (), AppConfig> for Handle {
    // A body with nothing to await returns the future directly, the same shape the rest of the
    // workspace uses; `async fn` here would be an unused async on a trait impl.
    fn handle(
        &self,
        order: &Order,
        ctx: &mut Context<'_, (), AppConfig>,
    ) -> impl Future<Output = Settle> + Send {
        // 1. The channel the message arrived on.
        println!("received on {}", ctx.name());

        // 2. The headers working copy - including what middleware added on the way in.
        if let Some(id) = ctx.headers().get("x-request-id") {
            println!("request {}", String::from_utf8_lossy(id));
        }

        // 3. The typed app-level shared state, borrowed through state().
        let config = ctx.state();
        if config.reject_zero_ids && order.id == 0 {
            return ready(HandlerResult::drop().into());
        }

        // 4. A post-settle hook: fires after the broker has acked this message, off the delivery
        //    path, so slow follow-up work never gates the ack or the next delivery. At-most-once:
        //    a lost hook does not redeliver.
        let id = order.id;
        ctx.after_ack(async move {
            println!("order {id} acked; sending the confirmation");
        });

        // The future resolves to `Settle`, so the outcome converts: the attribute inserts this
        // conversion around the body it moves here.
        ready(HandlerResult::Ack.into())
    }
}

// The `subscriber(source, handler)` constructor binds a handler over the unit state, so a handler
// that reads a typed state through `ctx.state()` names its own definition instead: `SubscriberDef`
// says what to run and on which source, `IncludeDef` names the mounting machinery, and `include`
// reads both exactly as it reads a generated definition.
impl SubscriberDef for Handle {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Name {
        Name::new("orders")
    }

    fn into_handler(self) -> Self {
        self
    }
}

impl IncludeDef for Handle {
    type Form = forms::Subscribing;
}
// --8<-- [end:handler]

// --8<-- [start:enrich]
/// A layer that stamps a request id onto the context headers before the handler runs.
#[derive(Clone)]
struct RequestId;

struct WithRequestId<H>(H);

impl<H> Layer<H> for RequestId {
    type Handler = WithRequestId<H>;
    fn layer(&self, inner: H) -> WithRequestId<H> {
        WithRequestId(inner)
    }
}

static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);

// The layer is state-agnostic: it threads the context `C` and state `S` through unchanged, so it
// wraps a handler whatever typed state the app declares. Nothing here changes without the macros:
// a layer is already the named-type shape a hand-written handler uses, one level up.
impl<M: Send + Sync, C: Send, S: Send + Sync, H: Handler<M, C, S>> Handler<M, C, S>
    for WithRequestId<H>
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> Settle {
        if ctx.headers().get("x-request-id").is_none() {
            let id = format!("req-{}", NEXT_REQUEST.fetch_add(1, Ordering::Relaxed));
            ctx.headers_mut().insert("x-request-id", id.into_bytes());
        }
        self.0.handle(msg, ctx).await
    }
}
// --8<-- [end:enrich]

// --8<-- [start:app]
// `on_startup` fixes the app's state type to `AppConfig`; `.layer` then grows the global stack.
// `include` mounts the definition: the source resolves at startup, and the scope codec (here the
// default) decodes.
fn app() -> RustStream<Stack<RequestId, Identity>, AppConfig> {
    RustStream::new(AppInfo::new("context", "0.1.0"))
        .on_startup(async move |()| {
            Ok::<_, Infallible>(AppConfig {
                reject_zero_ids: true,
            })
        })
        .layer(RequestId)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(Handle);
        })
}

// `#[ruststream::app]` is what would have written this main (and given it a CLI); `run` itself is
// an ordinary method, so the macro-free service calls it.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
// --8<-- [end:app]
