//! The publishing forms from the Publishing guide, written without the `macros` feature. The
//! attribute and the derives are sugar over public value constructors and traits, so every
//! declaration they mint is written out here: the reply bodies, the slot markers and their
//! dictionaries, and what a message type says about being sent. Everything a body does is an axis
//! of its own `impl Handle`, and the mount site then reads exactly as it does with the attribute -
//! `include`, `.publisher(..)`, `.out(marker, ..)`, `.build()`.
//!
//! ```text
//! cargo run --example manual_publishing --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::fmt::Display;
use std::future::{Future, ready};

use ruststream::codec::{Codec, JsonCodec};
use ruststream::memory::prelude::*;
use ruststream::runtime::{
    BoundSegment, MissingSegment, OutMessages, OutgoingMessageMetadata, PublishAt, PublishContext,
    PublishError, PublishLayer, PublishNext, PublishPipeline, PublishTransform, PublishedThrough,
    TemplateAddress, Transactional,
};
// The derive and the pipeline's message type share the name in different namespaces: the derive
// is the macro `ruststream::Outgoing`, the value flowing through a publish transform is the type
// `ruststream::runtime::Outgoing`.
use ruststream::runtime::Outgoing;
use ruststream::{
    CallerName, FixedName, MessageHeaders, NameTemplate, NoHeaders, OutgoingDestination,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct Request {
    id: u64,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct Response {
    ok: bool,
}

/// An event this service sends wherever the call site says.
#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
struct Event {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the destination form is the one that leaves the
// name to the call, so the publish builder offers `to(..)` and nothing is declared as leaving.
impl OutgoingDestination for Event {
    type Form = CallerName;
}

impl MessageHeaders for Event {
    type Contract = NoHeaders;
}

impl<M: OutSlot> OutMessages<M> for Event {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        Vec::new()
    }
}

// --8<-- [start:reply]
// A `publish(..)` handler is a body producing a reply: the reply type is the second axis of
// `Handle`, and the chain names the subscription, the destination and the publisher the reply
// leaves through.
struct Respond;

impl Handle<Request, Response> for Respond {
    fn handle(
        &self,
        req: &Request,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Response, HandlerOutcome>> {
        println!("responding to request {}", req.id);
        ready(Ok(Response { ok: true }))
    }
}
// --8<-- [end:reply]

// --8<-- [start:reply_result]
// `Ok` publishes the reply and acks; `Err` publishes nothing and the dispatcher acts on the
// returned HandlerOutcome (here: drop the malformed request instead of replying).
struct Validate;

impl Handle<Request, Response> for Validate {
    fn handle(
        &self,
        req: &Request,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Response, HandlerOutcome>> {
        if req.id == 0 {
            return ready(Err(HandlerOutcome::drop()));
        }
        ready(Ok(Response { ok: true }))
    }
}
// --8<-- [end:reply_result]

// --8<-- [start:forward]
// The publisher arrives as an injection: the policy is attached at the include site, the runtime
// pairs it with the connected broker at startup, and the body always holds a live publisher - no
// registry, no erased lookup, no state plumbing. The publisher type is not named: the body is
// generic over it (and over the scope codec the slot carries), stating just the capability it
// needs, so the same body mounts on a production broker and its in-process test transport
// unchanged. `Event` declares no destination of its own, so the call site names one.
struct Forward;

impl<P, Enc> Handle<Event, (), Outs<(Slot<DefaultSlot, P, Enc>,)>> for Forward
where
    P: Publisher,
    Enc: Codec + Send + Sync,
{
    async fn handle(
        &self,
        event: &Event,
        outs: &Outs<(Slot<DefaultSlot, P, Enc>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(DefaultSlot)
            .message(event)
            .to("egress")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:forward]

// --8<-- [start:slots]
// A body with several injected publishers names a slot marker per parameter; the include site
// binds each marker to its own policy, in any order. Each marker lists what may leave through
// it, which is both what the generated document reports and what the publish builder admits.
struct Primary;

impl OutSlot for Primary {
    const NAME: &'static str = "Primary";

    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        <Event as OutMessages<Self>>::outgoing()
    }
}

impl PublishedThrough<Primary> for Event {}

struct Shadow;

impl OutSlot for Shadow {
    const NAME: &'static str = "Shadow";

    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        <Event as OutMessages<Self>>::outgoing()
    }
}

impl PublishedThrough<Shadow> for Event {}

struct Mirror;

impl<PA, EncA, PB, EncB> Handle<Event, (), Outs<(Slot<Primary, PA, EncA>, Slot<Shadow, PB, EncB>)>>
    for Mirror
where
    PA: Publisher,
    EncA: Codec + Send + Sync,
    PB: Publisher,
    EncB: Codec + Send + Sync,
{
    async fn handle(
        &self,
        event: &Event,
        outs: &Outs<(Slot<Primary, PA, EncA>, Slot<Shadow, PB, EncB>)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(Primary)
            .message(event)
            .to("mirror-primary")
            .publish()
            .await
            .is_err()
            || outs
                .get(Shadow)
                .message(event)
                .to("mirror-shadow")
                .publish()
                .await
                .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:slots]

// --8<-- [start:publish_out]
// A reply and an injected publisher in one body: the reply answers on the chain's destination
// while an audit copy fans out through the slot. Both are axes of the one trait - the reply type
// and the arena - and the mount site fills both.
struct Gateway;

impl<P, Enc> Handle<Request, Response, Outs<(Slot<DefaultSlot, P, Enc>,)>> for Gateway
where
    P: Publisher,
    Enc: Codec + Send + Sync,
{
    async fn handle(
        &self,
        req: &Request,
        outs: &Outs<(Slot<DefaultSlot, P, Enc>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<Response, HandlerOutcome> {
        if outs
            .get(DefaultSlot)
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
}
// --8<-- [end:publish_out]

// --8<-- [start:declared]
// What a message says about being sent lives on the type. A fixed name resolves the destination
// for every call site; a name template turns each `{placeholder}` into a setter, so a service
// routing per tenant still declares where the type goes; and a declaration with no name (like
// `Event` above) leaves the name to the call.
#[derive(Debug, Serialize)]
struct OrderConfirmed {
    id: u64,
}

impl OutgoingDestination for OrderConfirmed {
    type Form = FixedName;
    const ADDRESS: &'static str = "orders.confirmed";
}

impl MessageHeaders for OrderConfirmed {
    type Contract = NoHeaders;
}

impl<M: OutSlot> OutMessages<M> for OrderConfirmed {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        vec![OutgoingMessageMetadata::new(
            Self::ADDRESS,
            std::any::type_name::<Self>(),
        )]
    }
}

#[derive(Debug, Serialize)]
struct OrderPlaced {
    id: u64,
}

impl OutgoingDestination for OrderPlaced {
    type Form = NameTemplate;
    const ADDRESS: &'static str = "orders.{tenant}.placed";
    const PARAMETERS: &'static [&'static str] = &["tenant"];
}

impl MessageHeaders for OrderPlaced {
    type Contract = NoHeaders;
}

impl<M: OutSlot> OutMessages<M> for OrderPlaced {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        vec![
            OutgoingMessageMetadata::new(Self::ADDRESS, std::any::type_name::<Self>())
                .with_parameters(Self::PARAMETERS),
        ]
    }
}

/// The `tenant` segment of the declared name, named in the compile error of a publish that never
/// bound it. The derive hides it (and the builder below) in a module named after the type; by
/// hand the only requirement is that the names do not collide.
#[derive(Debug, Clone, Copy)]
struct Tenant;

/// One publish whose templated address is being bound, segment by segment. The publish terminal
/// is callable once every placeholder is bound; until then the unbound ones ride in this type, so
/// the compile error names them.
#[must_use = "an address builder does nothing until publish() is awaited"]
struct PlacedAt<Cont, S0> {
    cont: Cont,
    segment_0: S0,
}

impl<Cont> PlacedAt<Cont, MissingSegment<Tenant>> {
    fn start(cont: Cont) -> Self {
        Self {
            cont,
            segment_0: MissingSegment::new(),
        }
    }

    /// Binds the `tenant` segment of the declared name.
    fn tenant(self, value: impl Display) -> PlacedAt<Cont, String> {
        PlacedAt {
            cont: self.cont,
            segment_0: value.to_string(),
        }
    }
}

impl<Cont, S0> PlacedAt<Cont, S0> {
    // Every bound, the segment witness included, sits on the method: on the impl block a publish
    // that forgot a segment reads as "method not found" and loses the guidance the bound carries.
    async fn publish(self) -> Result<(), PublishError<<Cont as PublishAt>::Error>>
    where
        Cont: PublishAt,
        S0: BoundSegment,
    {
        // A templated address is rendered per publish; the fixed form is not.
        let address = format!("orders.{}.placed", self.segment_0);
        PublishAt::publish_at(self.cont, &address).await
    }
}

impl<Cont> TemplateAddress<Cont> for OrderPlaced {
    type Builder = PlacedAt<Cont, MissingSegment<Tenant>>;

    fn begin(cont: Cont) -> Self::Builder {
        PlacedAt::start(cont)
    }
}

struct Orders;

impl OutSlot for Orders {
    const NAME: &'static str = "Orders";

    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        let mut declared = <OrderConfirmed as OutMessages<Self>>::outgoing();
        declared.extend(<OrderPlaced as OutMessages<Self>>::outgoing());
        declared
    }
}

impl PublishedThrough<Orders> for OrderConfirmed {}
impl PublishedThrough<Orders> for OrderPlaced {}

struct Route;

impl<P, Enc> Handle<Event, (), Outs<(Slot<Orders, P, Enc>,)>> for Route
where
    P: Publisher,
    Enc: Codec + Send + Sync,
{
    async fn handle(
        &self,
        event: &Event,
        outs: &Outs<(Slot<Orders, P, Enc>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let orders = outs.get(Orders);
        // Bound to one name: the destination is already resolved.
        if orders
            .message(&OrderConfirmed { id: event.id })
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
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
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
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
/// Confirms a whole page of orders; the replies become visible atomically on commit. The page
/// input and the reply type are two axes of the one trait: one `Vec` of replies per batch, each
/// published to the destination the chain names, and an `Err` settles the page element-wise
/// (one outcome per element) without publishing anything.
struct Confirm;

impl Handle<[Event], Vec<Event>> for Confirm {
    fn handle(
        &self,
        orders: &[Event],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Event>, Vec<HandlerOutcome>>> {
        if orders.iter().any(|o| o.id == 0) {
            // nothing published, every element of the page settled on its own
            return ready(Err(orders.iter().map(|_| HandlerOutcome::drop()).collect()));
        }
        ready(Ok(orders.iter().map(|o| Event { id: o.id }).collect()))
    }
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
            b.include(
                subscriber("requests", Respond)
                    .reply()
                    .to("responses")
                    .publisher(TypedPublisher::new(Publish).transform(EnvelopeTransform))
                    .build(),
            );
            // the default reply wiring: the broker's default policy under the default codec
            b.include(
                subscriber("validated-requests", Validate)
                    .reply()
                    .to("responses")
                    .build(),
            );
            // --8<-- [end:reply_mount]
            // --8<-- [start:forward_mount]
            b.include(subscriber("ingress", Forward).build())
                .publisher(Publish);
            // --8<-- [end:forward_mount]
            // --8<-- [start:slots_mount]
            // each named slot binds by marker; the call order does not matter
            b.include(subscriber("mirror", Mirror).build())
                .out(Shadow, Publish)
                .out(Primary, Publish)
                .build();
            // --8<-- [end:slots_mount]
            // --8<-- [start:publish_out_mount]
            // the reply keeps .publisher(..) (or its default); the Out slot attaches
            // with .out(<marker>, ..) - DefaultSlot for a single unnamed slot
            b.include(
                subscriber("gateway-requests", Gateway)
                    .reply()
                    .to("gateway-responses")
                    .build(),
            )
            .out(DefaultSlot, Publish)
            .build();
            // --8<-- [end:publish_out_mount]
            // --8<-- [start:declared_mount]
            // the slot lists what it may publish; where each message goes is its own declaration
            b.include(subscriber("orders.incoming", Route).build())
                .out(Orders, Publish)
                .build();
            // --8<-- [end:declared_mount]
            // --8<-- [start:batch_publishing_mount]
            // .transactional() marks the wiring; the pairing checks that the policy's live
            // publisher is transactional. Without it, each reply publishes independently.
            b.include(
                subscriber("orders", Confirm)
                    .reply()
                    .to("confirmations")
                    .publisher(TypedPublisher::new(TransactionalPublish).transactional())
                    .build(),
            );
            // --8<-- [end:batch_publishing_mount]
        })
    // --8<-- [end:pipeline]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
