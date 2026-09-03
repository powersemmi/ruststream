//! The publishing forms from the Publishing guide: reply handlers, a publisher injected into a
//! handler with `Out`, the two-level publish pipeline (a per-publisher transform and an app-wide
//! publish layer), transactional batch replies, and a first publish from the scope's
//! `after_startup` hook.
//!
//! ```text
//! cargo run --example publishing --features macros,memory,json -- run
//! ```

use std::error::Error;

use ruststream::codec::JsonCodec;
use ruststream::memory::prelude::*;
// The derive and the pipeline's message type share the name in different namespaces: the derive
// is the macro `ruststream::Outgoing`, the value flowing through a publish transform is the type
// `ruststream::runtime::Outgoing`.
use ruststream::runtime::{
    Outgoing, PublishContext, PublishLayer, PublishNext, PublishPipeline, PublishTransform,
    Transactional,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Request {
    id: u64,
}

#[derive(Debug, Serialize)]
struct Response {
    ok: bool,
}

// The derive with no `name`: an event this service sends wherever the call site says.
#[derive(Debug, Deserialize, Serialize, Outgoing)]
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
// returned HandlerOutcome (here: drop the malformed request instead of replying).
#[subscriber("validated-requests", publish("responses"))]
async fn validate(req: &Request) -> Result<Response, HandlerOutcome> {
    if req.id == 0 {
        return Err(HandlerOutcome::drop());
    }
    Ok(Response { ok: true })
}
// --8<-- [end:reply_result]

// --8<-- [start:forward]
// The publisher arrives as a parameter (the Out marker): the source is attached at the include
// site, the runtime pairs it with the connected broker at startup, and the handler always holds
// a live publisher - no registry, no erased lookup, no state plumbing. `Event` declares no
// destination of its own, so the call site names one.
#[subscriber("ingress")]
async fn forward(event: &Event, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    if out.message(event).to("egress").publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:forward]

// --8<-- [start:slots]
// A handler with several injected publishers names a slot marker per parameter; the include
// site binds each marker to its own policy, in any order. No broker publisher type appears in
// the signature, so the same handler mounts on a production broker and on its in-process test
// transport unchanged. Each marker lists what may leave through it, which is both what the
// generated document reports and what the publish builder admits.
#[derive(OutSlot)]
#[publishes(Event)]
struct Primary;

#[derive(OutSlot)]
#[publishes(Event)]
struct Shadow;

#[subscriber("mirror")]
async fn mirror(
    event: &Event,
    Out(primary): Out<impl Publisher, Primary>,
    Out(shadow): Out<impl Publisher, Shadow>,
) -> HandlerOutcome {
    if primary
        .message(event)
        .to("mirror-primary")
        .publish()
        .await
        .is_err()
        || shadow
            .message(event)
            .to("mirror-shadow")
            .publish()
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:slots]

// --8<-- [start:publish_out]
// A reply form and an injected publisher in one handler: the reply answers on the fixed
// destination while an audit copy fans out through the Out parameter.
#[subscriber("gateway-requests", publish("gateway-responses"))]
async fn gateway(req: &Request, Out(out): Out<impl Publisher>) -> Result<Response, HandlerOutcome> {
    if out
        .message(&Event { id: req.id })
        .to("gateway-audit")
        .publish()
        .await
        .is_err()
    {
        return Err(HandlerOutcome::retry());
    }
    Ok(Response { ok: true })
}
// --8<-- [end:publish_out]

// --8<-- [start:declared]
// What a message says about being sent lives on the type. A fixed name resolves the destination
// for every call site; a name template turns each `{placeholder}` into a setter, so a service
// routing per tenant still declares where the type goes; and the derive alone (like `Event`
// above) leaves the name to the call.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]
struct OrderConfirmed {
    id: u64,
}

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "orders.{tenant}.placed")]
struct OrderPlaced {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(OrderConfirmed, OrderPlaced)]
struct Orders;

#[subscriber("orders.incoming")]
async fn route(
    event: &Event,
    Out(orders): Out<impl Publisher, Orders, (OrderConfirmed, OrderPlaced)>,
) -> HandlerOutcome {
    // Bound to one name: the destination is already resolved.
    if orders
        .message(&OrderConfirmed { id: event.id })
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    // Bound to a space of names: one setter per placeholder, and no publish until the last one
    // is bound.
    if orders
        .message(&OrderPlaced { id: event.id })
        .to()
        .tenant("acme")
        .publish()
        .await
        .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
// --8<-- [end:declared]

// --8<-- [start:static_transform]
/// A static, per-publisher transform: stamps an envelope header on every outgoing message.
struct EnvelopeTransform;

impl<C> PublishTransform<C> for EnvelopeTransform {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, C>) {
        out.headers_mut().insert("x-envelope", b"1".to_vec());
    }
}
// --8<-- [end:static_transform]

// --8<-- [start:app_layer]
/// A static, app-wide publish layer: observes every publish, then passes it on.
#[derive(Clone)]
struct AuditPublish;

impl PublishLayer for AuditPublish {
    async fn on_publish<'a, N: PublishPipeline, P: Publisher>(
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
#[subscriber("orders", publish("confirmations"))]
async fn confirm(orders: &[Event]) -> Result<Vec<Event>, HandlerOutcome> {
    if orders.is_empty() {
        return Err(HandlerOutcome::drop()); // nothing published, whole batch settled
    }
    Ok(orders.iter().map(|o| Event { id: o.id }).collect())
}
// --8<-- [end:batch_publishing]

// --8<-- [start:manual_transaction]
/// Seeds the reference events inside one broker transaction: both records become visible
/// together on commit, or not at all. The scope owns the transaction, so a commit without a
/// begin, a second commit, or a publish after settling do not compile. The wiring arrives
/// already paired (the scope's `after_startup` hands it over live), so seeding cannot race the
/// broker connect; the bound names the capability the seeding needs, not the broker's publisher.
async fn seed_events<P>(
    seeder: Transactional<P, JsonCodec>,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    P: TransactionalPublisher,
{
    let scope = seeder.begin().await?;
    scope
        .message(&Event { id: 1 })
        .to("events")
        .publish()
        .await?;
    scope
        .message(&Event { id: 2 })
        .to("events")
        .publish()
        .await?;
    scope.commit().await?;
    Ok(())
}
// --8<-- [end:manual_transaction]

// `impl App` hides the composed pipeline type: the app-wide `publish_layer` would otherwise surface
// in the return type as `RustStream<_, (), PublishStack<AuditPublish, PublishIdentity>>`.
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
                TypedPublisher::with_codec(TransactionalPublish, JsonCodec).transactional(),
                async move |seeder| seed_events(seeder).await.map_err(std::io::Error::other),
            );
            // --8<-- [start:reply_mount]
            // static, per-publisher: a policy stack, composed at compile time and paired with
            // the connected broker at startup
            b.include(respond)
                .publisher(TypedPublisher::new(Publish).transform(EnvelopeTransform));
            // the default reply wiring: the broker's default policy under the default codec
            b.include(validate);
            // --8<-- [end:reply_mount]
            // --8<-- [start:forward_mount]
            b.include(forward).publisher(Publish);
            // --8<-- [end:forward_mount]
            // --8<-- [start:slots_mount]
            // each named slot binds by marker; the call order does not matter
            b.include(mirror)
                .out(Shadow, Publish)
                .out(Primary, Publish)
                .build();
            // --8<-- [end:slots_mount]
            // --8<-- [start:publish_out_mount]
            // the reply keeps .publisher(..) (or its default); the Out parameter attaches
            // with .out(<marker>, ..) - DefaultSlot for a single unnamed slot
            b.include(gateway).out(DefaultSlot, Publish).build();
            // --8<-- [end:publish_out_mount]
            // --8<-- [start:declared_mount]
            // the slot lists what it may publish; where each message goes is its own declaration
            b.include(route).out(Orders, Publish).build();
            // --8<-- [end:declared_mount]
            // --8<-- [start:batch_publishing_mount]
            // .transactional() marks the wiring; the pairing checks that the policy's live
            // publisher is transactional. Without it, each reply publishes independently.
            b.include(confirm)
                .publisher(TypedPublisher::new(TransactionalPublish).transactional());
            // --8<-- [end:batch_publishing_mount]
        })
    // --8<-- [end:pipeline]
}
