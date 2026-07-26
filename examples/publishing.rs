//! The publishing forms from the Publishing guide: a reply handler, a publisher shared through the
//! typed application state, and the two-level publish pipeline (a per-publisher transform and an
//! app-wide publish layer).
//!
//! ```text
//! cargo run --example publishing --features macros,memory,json -- run
//! ```

use std::error::Error;
use std::io;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::memory::{MemoryBroker, MemoryPublisher};
use ruststream::runtime::{
    App, AppInfo, HandlerResult, Outgoing, PublishLayer, PublishNext, PublishTransform, RustStream,
    TypedPublisher,
};
use ruststream::{OutgoingMessage, Publisher, TransactionalPublisher, subscriber};
use serde::{Deserialize, Serialize};

// A publisher shared with handlers as a typed field of the application state.
struct AppState {
    egress: MemoryPublisher,
}

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
// A `publish(..)` handler that does not read the app state omits the `Context` parameter entirely;
// it stays generic over the state and mounts on an app with any state type.
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
// The egress publisher is a typed field of the app state, so the handler reaches it through
// `ctx.state()` and publishes with the publisher's own API - no registry, no erased lookup.
#[subscriber("ingress")]
async fn forward(event: &Event, ctx: &mut Context<'_, (), AppState>) -> HandlerResult {
    let payload = JsonCodec.encode(event).expect("serializable");
    let out = OutgoingMessage::new("egress", payload.as_ref());
    if ctx.state().egress.publish(out).await.is_err() {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:forward]

// --8<-- [start:static_transform]
/// A static, per-publisher transform: stamps an envelope header on every outgoing message.
struct EnvelopeTransform;

impl<C> PublishTransform<C> for EnvelopeTransform {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &ruststream::runtime::PublishContext<'_, C>) {
        out.headers_mut().insert("x-envelope", b"1".to_vec());
    }
}
// --8<-- [end:static_transform]

// --8<-- [start:app_layer]
/// A static, app-wide publish layer: observes every publish, then passes it on.
#[derive(Clone)]
struct AuditPublish;

impl PublishLayer for AuditPublish {
    async fn on_publish<'a, N: ruststream::runtime::PublishPipeline, P: Publisher>(
        &'a self,
        out: &'a mut Outgoing<'a>,
        next: PublishNext<'a, N, P>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        println!("publishing to {}", out.name());
        next.run(out).await
    }
}
// --8<-- [end:app_layer]

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

// --8<-- [start:manual_transaction]
/// Seeds the reference events inside one broker transaction: both records become visible
/// together on commit, or not at all. The scope owns the transaction, so a commit without a
/// begin, a second commit, or a publish after settling do not compile.
async fn seed_events<P>(publisher: P) -> Result<(), Box<dyn Error + Send + Sync>>
where
    P: TransactionalPublisher,
{
    let seeder = TypedPublisher::new(publisher).transactional();
    let mut scope = seeder.begin().await?;
    scope.publish("events", &Event { id: 1 }).await?;
    scope.publish("events", &Event { id: 2 }).await?;
    scope.commit().await?;
    Ok(())
}
// --8<-- [end:manual_transaction]

// `impl App` hides the composed pipeline type: the app-wide `publish_layer` would otherwise surface
// in the return type as `RustStream<_, AppState, PublishStack<AuditPublish, PublishIdentity>>`.
#[ruststream::app]
fn app() -> impl App {
    let broker = MemoryBroker::new();
    let egress = broker.publisher();
    let seed = broker.publisher();
    // --8<-- [start:pipeline]
    RustStream::new(AppInfo::new("publishing", "0.1.0"))
        // app-wide layer: wraps every published reply
        .publish_layer(AuditPublish)
        // a publisher shared with handlers as typed state, reached via `ctx.state().egress`
        .on_startup(async move |()| {
            seed_events(seed).await.map_err(io::Error::other)?;
            Ok::<_, io::Error>(AppState { egress })
        })
        .with_broker(broker, |b| {
            // static, per-publisher: composed onto this TypedPublisher at compile time
            let replies = TypedPublisher::new(b.broker().publisher()).transform(EnvelopeTransform);
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
