//! The publishing forms from the Publishing guide: a reply handler, a publisher shared through the
//! typed application state, and the two-level publish pipeline (a per-publisher transform and an
//! app-wide publish layer).
//!
//! ```text
//! cargo run --example publishing --features macros,memory,json -- run
//! ```

use std::error::Error;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::memory::{MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::runtime::{
    App, AppInfo, HandlerResult, Out, Outgoing, PublishLayer, PublishNext, PublishTransform,
    RustStream, Transactional, TypedPublisher,
};
use ruststream::{OutgoingMessage, Publisher, TransactionalPublisher, subscriber};
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
// The publisher arrives as a parameter (the Out marker): the source is attached at the include
// site, the runtime pairs it with the connected broker at startup, and the handler always holds
// a live publisher - no registry, no erased lookup, no state plumbing.
#[subscriber("ingress")]
async fn forward(event: &Event, Out(out): Out<MemoryPublisher>) -> HandlerResult {
    let payload = JsonCodec.encode(event).expect("serializable");
    let msg = OutgoingMessage::new("egress", payload.as_ref());
    if out.publish(msg).await.is_err() {
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
/// begin, a second commit, or a publish after settling do not compile. The wiring arrives
/// already paired (the scope's `after_startup` hands it over live), so seeding cannot race the
/// broker connect.
async fn seed_events<P>(
    seeder: Transactional<P, JsonCodec>,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    P: TransactionalPublisher,
{
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
    // --8<-- [start:pipeline]
    RustStream::new(AppInfo::new("publishing", "0.1.0"))
        // app-wide layer: wraps every published reply
        .publish_layer(AuditPublish)
        .with_broker(broker, |b| {
            // the first publish: runs once connected and subscribed, with the transactional
            // wiring already paired
            b.after_startup(
                TypedPublisher::with_codec(MemoryPublish, JsonCodec).transactional(),
                async move |seeder| seed_events(seeder).await.map_err(std::io::Error::other),
            );
            // --8<-- [start:reply_mount]
            // static, per-publisher: a policy stack, composed at compile time and paired with
            // the connected broker at startup
            b.include(respond)
                .publisher(TypedPublisher::new(MemoryPublish).transform(EnvelopeTransform));
            // the default reply wiring: the broker's default policy under the default codec
            b.include(validate);
            // --8<-- [end:reply_mount]
            // --8<-- [start:forward_mount]
            b.include(forward).publisher(MemoryPublish);
            // --8<-- [end:forward_mount]
            // --8<-- [start:batch_publishing_mount]
            // .transactional() marks the wiring; the pairing checks that the policy's live
            // publisher implements TransactionalPublisher. Without it, each reply publishes
            // independently.
            b.include_batch(confirm)
                .publisher(TypedPublisher::new(MemoryPublish).transactional());
            // --8<-- [end:batch_publishing_mount]
        })
    // --8<-- [end:pipeline]
}
