//! The Context guide's example: a handler reading all three things the per-delivery `Context`
//! carries (the channel name, the headers working copy, shared state), and a middleware that
//! enriches the headers before the handler runs.
//!
//! ```text
//! cargo run --example context --features macros,memory,json -- run
//! ```

use std::sync::atomic::{AtomicU64, Ordering};

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{
    AppInfo, Context, Handler, HandlerResult, Identity, Layer, RustStream, Settle, Stack,
};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:state]
/// Shared configuration: produced once at startup, read by every handler as the typed app state.
#[derive(Debug)]
struct AppConfig {
    reject_zero_ids: bool,
}
// --8<-- [end:state]

// --8<-- [start:handler]
#[subscriber("orders")]
async fn handle(order: &Order, ctx: &mut Context<'_, (), AppConfig>) -> HandlerResult {
    // 1. The channel the message arrived on.
    println!("received on {}", ctx.name());

    // 2. The headers working copy - including what middleware added on the way in.
    if let Some(id) = ctx.headers().get("x-request-id") {
        println!("request {}", String::from_utf8_lossy(id));
    }

    // 3. The typed app-level shared state, borrowed through state().
    let config = ctx.state();
    if config.reject_zero_ids && order.id == 0 {
        return HandlerResult::drop();
    }

    // 4. A post-settle hook: fires after the broker has acked this message, off the delivery
    //    path, so slow follow-up work never gates the ack or the next delivery. At-most-once:
    //    a lost hook does not redeliver.
    let id = order.id;
    ctx.after_ack(async move {
        println!("order {id} acked; sending the confirmation");
    });

    HandlerResult::Ack
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
// wraps a handler whatever typed state the app declares.
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
#[ruststream::app]
fn app() -> RustStream<Stack<RequestId, Identity>, AppConfig> {
    RustStream::new(AppInfo::new("context", "0.1.0"))
        .on_startup(|()| async {
            Ok::<_, std::convert::Infallible>(AppConfig {
                reject_zero_ids: true,
            })
        })
        .layer(RequestId)
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
// --8<-- [end:app]
