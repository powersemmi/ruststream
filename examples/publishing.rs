//! The publishing forms from the Publishing guide: a reply handler, a named publisher resolved
//! from the context, and the two-level publish pipeline (static layer + dynamic middleware).
//!
//! ```text
//! cargo run --example publishing --features macros,memory,json -- run
//! ```

use std::future::Future;
use std::pin::Pin;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{
    AppInfo, HandlerResult, Outgoing, PublishLayer, PublishMiddleware, PublishNext, RustStream,
    TypedPublisher,
};
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Request {
    id: u64,
}

#[derive(Debug, Serialize)]
struct Response {
    ok: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct Event {
    id: u64,
}

// --8<-- [start:reply]
#[subscriber("requests", publish("responses"))]
async fn respond(req: &Request) -> Response {
    println!("responding to request {}", req.id);
    Response { ok: true }
}
// --8<-- [end:reply]

// --8<-- [start:reply_result]
// `Ok` publishes the reply and acks; `Err` publishes nothing and the dispatcher acts on the
// returned HandlerResult (here: drop the malformed request instead of replying).
#[subscriber("validated-requests", publish("responses"))]
async fn validate(req: &Request) -> Result<Response, HandlerResult> {
    if req.id == 0 {
        return Err(HandlerResult::drop());
    }
    Ok(Response { ok: true })
}
// --8<-- [end:reply_result]

// --8<-- [start:forward]
#[subscriber("ingress")]
async fn forward(event: &Event, ctx: &mut Context<'_>) -> HandlerResult {
    if let Some(publisher) = ctx.publisher("egress") {
        let payload = JsonCodec.encode(event).expect("serializable");
        let out = Outgoing::new("egress", payload);
        if publisher.publish(out).await.is_err() {
            return HandlerResult::retry();
        }
    }
    HandlerResult::Ack
}
// --8<-- [end:forward]

// --8<-- [start:static_layer]
/// A static, per-publisher transform: stamps an envelope header on every outgoing message.
struct EnvelopeLayer;

impl PublishLayer for EnvelopeLayer {
    fn apply(&self, out: &mut Outgoing<'_>) {
        out.headers_mut().insert("x-envelope", b"1".to_vec());
    }
}
// --8<-- [end:static_layer]

// --8<-- [start:dynamic_middleware]
/// A dynamic, app-wide middleware: observes every publish, then passes it on.
struct AuditPublish;

impl PublishMiddleware for AuditPublish {
    fn on_publish<'a>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>,
    > {
        Box::pin(async move {
            println!("publishing to {}", out.name());
            next.run(out).await
        })
    }
}
// --8<-- [end:dynamic_middleware]

// --8<-- [start:batch_publishing]
/// Confirms a whole page of orders; the replies become visible atomically on commit.
#[subscriber(batch("orders"), publish("confirmations"))]
async fn confirm(orders: &[Event]) -> Result<Vec<Event>, HandlerResult> {
    if orders.is_empty() {
        return Err(HandlerResult::drop()); // nothing published, whole batch settled
    }
    Ok(orders.iter().map(|o| Event { id: o.id }).collect())
}
// --8<-- [end:batch_publishing]

#[ruststream::app]
fn app() -> RustStream {
    let broker = MemoryBroker::new();
    let egress = broker.publisher();
    // --8<-- [start:pipeline]
    RustStream::new(AppInfo::new("publishing", "0.1.0"))
        // dynamic, app-wide: wraps every published message
        .publish_layer(AuditPublish)
        // a named publisher, resolvable from any handler's context
        .publisher("egress", egress)
        .with_broker(broker, |b| {
            // static, per-publisher: composed onto this TypedPublisher at compile time
            let replies = TypedPublisher::new(b.broker().publisher()).layer(EnvelopeLayer);
            b.include_publishing(respond, replies);
            let validated = TypedPublisher::new(b.broker().publisher());
            b.include_publishing(validate, validated);
            b.include(forward);
            // --8<-- [start:batch_publishing_mount]
            // .transactional() exists only because MemoryPublisher implements
            // TransactionalPublisher; without it, each reply publishes independently.
            let confirmations = TypedPublisher::new(b.broker().publisher()).transactional();
            b.include_batch_publishing(confirm, confirmations);
            // --8<-- [end:batch_publishing_mount]
        })
    // --8<-- [end:pipeline]
}
