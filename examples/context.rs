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
    AppInfo, Context, Handler, HandlerResult, Identity, Layer, RustStream, Stack,
};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

// --8<-- [start:state]
/// Shared configuration: inserted once at build time, read by every handler.
#[derive(Debug)]
struct AppConfig {
    reject_zero_ids: bool,
}
// --8<-- [end:state]

// --8<-- [start:handler]
#[subscriber("orders")]
async fn handle(order: &Order, ctx: &mut Context<'_>) -> HandlerResult {
    // 1. The channel the message arrived on.
    println!("received on {}", ctx.name());

    // 2. The headers working copy - including what middleware added on the way in.
    if let Some(id) = ctx.headers().get("x-request-id") {
        println!("request {}", String::from_utf8_lossy(id));
    }

    // 3. App-level shared state.
    let config = ctx
        .get::<AppConfig>()
        .expect("config inserted at build time");
    if config.reject_zero_ids && order.id == 0 {
        return HandlerResult::drop();
    }
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

impl<M: Send + Sync, H: Handler<M>> Handler<M> for WithRequestId<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_>) -> HandlerResult {
        if ctx.headers().get("x-request-id").is_none() {
            let id = format!("req-{}", NEXT_REQUEST.fetch_add(1, Ordering::Relaxed));
            ctx.headers_mut().insert("x-request-id", id.into_bytes());
        }
        self.0.handle(msg, ctx).await
    }
}
// --8<-- [end:enrich]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> RustStream<Stack<RequestId, Identity>> {
    RustStream::new(AppInfo::new("context", "0.1.0"))
        .insert_state(AppConfig {
            reject_zero_ids: true,
        })
        .layer(RequestId)
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
// --8<-- [end:app]
