//! The publishing forms from the Publishing guide: a reply handler, a named publisher resolved
//! from the context, and the two-level publish pipeline (static layer + dynamic middleware).
//!
//! ```text
//! cargo run --example publishing --features macros,memory,json -- run
//! ```

use std::future::Future;
use std::pin::Pin;

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

// --8<-- [start:forward]
#[subscriber("ingress")]
async fn forward(event: &Event, ctx: &mut Context<'_>) -> HandlerResult {
    if let Some(publisher) = ctx.publisher("egress") {
        let out = Outgoing::new("egress", serde_json::to_vec(event).expect("serializable"));
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
    fn apply(&self, out: &mut Outgoing) {
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
        out: &'a mut Outgoing,
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
            b.include(forward);
        })
    // --8<-- [end:pipeline]
}
