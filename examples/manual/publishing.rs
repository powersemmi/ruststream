//! The publishing forms from the Publishing guide, written without the `macros` feature. The
//! attribute and the derives are sugar over public value constructors and traits, so every
//! declaration they mint is written out here: the reply bodies, the slot markers and their
//! dictionaries, and what a message type says about being sent. The mount site then reads
//! exactly as it does with the attribute - `include`, `.publisher(..)`, `.out(marker, ..)`,
//! `.mount()`.
//!
//! ```text
//! cargo run --example manual_publishing --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::fmt::Display;
use std::future::{Future, ready};

use ruststream::codec::JsonCodec;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
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
    TransactionalPublisher,
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

/// An event this service sends wherever the call site says.
#[derive(Debug, Deserialize, Serialize)]
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
// A `publish(..)` handler is a body producing a reply: `impl Reply` carries it, and the mount
// site names the subscription, the destination and the publisher the reply leaves through. The
// impl stays generic over the state, so it mounts on an app with any state type.
struct Respond;

impl Reply<Request> for Respond {
    type Out = Response;

    // The body awaits nothing, so it is a future-returning method rather than an `async fn`: a
    // body that awaits writes `async fn reply` instead.
    fn reply(
        &self,
        req: &Request,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Response, HandlerResult>> + Send {
        println!("responding to request {}", req.id);
        ready(Ok(Response { ok: true }))
    }
}
// --8<-- [end:reply]

// --8<-- [start:reply_result]
// `Ok` publishes the reply and acks; `Err` publishes nothing and the dispatcher acts on the
// returned HandlerResult (here: drop the malformed request instead of replying).
struct Validate;

impl Reply<Request> for Validate {
    type Out = Response;

    fn reply(
        &self,
        req: &Request,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Response, HandlerResult>> + Send {
        ready(if req.id == 0 {
            Err(HandlerResult::drop())
        } else {
            Ok(Response { ok: true })
        })
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

impl<P, E, S> SlotsHandler<Event, (Out<P, DefaultSlot, (), E>,), (), S> for Forward
where
    P: Publisher,
    E: ruststream::codec::Codec + Send + Sync,
    S: Send + Sync,
{
    async fn handle(
        &self,
        event: &Event,
        slots: &(Out<P, DefaultSlot, (), E>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> Settle {
        let Out(out) = &slots.0;
        if out.message(event).to("egress").publish().await.is_err() {
            return HandlerResult::retry().into();
        }
        HandlerResult::Ack.into()
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

impl<P1, P2, E, S> SlotsHandler<Event, (Out<P1, Primary, (), E>, Out<P2, Shadow, (), E>), (), S>
    for Mirror
where
    P1: Publisher,
    P2: Publisher,
    E: ruststream::codec::Codec + Send + Sync,
    S: Send + Sync,
{
    async fn handle(
        &self,
        event: &Event,
        slots: &(Out<P1, Primary, (), E>, Out<P2, Shadow, (), E>),
        _ctx: &mut Context<'_, (), S>,
    ) -> Settle {
        let Out(primary) = &slots.0;
        let Out(shadow) = &slots.1;
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
            return HandlerResult::retry().into();
        }
        HandlerResult::Ack.into()
    }
}
// --8<-- [end:slots]

// --8<-- [start:publish_out]
// A reply and an injected publisher in one body: the reply answers on the mount site's
// destination while an audit copy fans out through the slot. `SlotsReply` is `Reply` with the
// slot tuple in the middle, and the mount site fills both axes.
struct Gateway;

impl<P, E, S> SlotsReply<Request, (Out<P, DefaultSlot, (), E>,), (), S> for Gateway
where
    P: Publisher,
    E: ruststream::codec::Codec + Send + Sync,
    S: Send + Sync,
{
    type Out = Response;

    async fn reply(
        &self,
        req: &Request,
        slots: &(Out<P, DefaultSlot, (), E>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> Result<Response, HandlerResult> {
        let Out(audit) = &slots.0;
        if audit
            .message(&Event { id: req.id })
            .to("gateway-audit")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerResult::retry());
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

impl<P, E, S> SlotsHandler<Event, (Out<P, Orders, (), E>,), (), S> for Route
where
    P: Publisher,
    E: ruststream::codec::Codec + Send + Sync,
    S: Send + Sync,
{
    async fn handle(
        &self,
        event: &Event,
        slots: &(Out<P, Orders, (), E>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> Settle {
        let Out(orders) = &slots.0;
        // Bound to one name: the destination is already resolved.
        if orders
            .message(&OrderConfirmed { id: event.id })
            .publish()
            .await
            .is_err()
        {
            return HandlerResult::retry().into();
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
            return HandlerResult::retry().into();
        }
        HandlerResult::Ack.into()
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
/// Confirms a whole page of orders; the replies become visible atomically on commit. The batch
/// reply form has no value constructor, so it is the one place the definition traits are still
/// written out: `Declared` names the mount form and the settings builder, `BatchPublishingDef`
/// carries the structure, and `BatchPublishingCall` the body.
struct Confirm;

impl ruststream::runtime::Declared for Confirm {
    type Form = ruststream::runtime::forms::BatchPublishing;
    type Settings =
        ruststream::runtime::SubscriberBuilder<Self, Name, ruststream::runtime::AllOpen>;

    fn declare(self) -> Self::Settings {
        ruststream::runtime::SubscriberBuilder::new(self, Name::new("orders"))
    }
}

impl ruststream::runtime::BatchPublishingDef for Confirm {
    type Input = ruststream::runtime::Decoded<Event>;
    type Injections = ();
    type Reply = Event;
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("orders")
    }

    fn reply_name(&self) -> &'static str {
        "confirmations"
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        vec![OutgoingMessageMetadata::new(
            "confirmations",
            std::any::type_name::<Event>(),
        )]
    }
}

impl<State: Send + Sync> ruststream::runtime::BatchPublishingCall<State> for Confirm {
    fn call(
        &self,
        orders: &[Event],
        _injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> impl Future<Output = Result<Vec<Event>, HandlerResult>> + Send {
        ready(if orders.is_empty() {
            // nothing published, whole batch settled
            Err(HandlerResult::drop())
        } else {
            Ok(orders.iter().map(|o| Event { id: o.id }).collect())
        })
    }
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
                TypedPublisher::with_codec(MemoryPublish, JsonCodec).transactional(),
                async move |seeder| seed_events(seeder).await.map_err(std::io::Error::other),
            );
            // --8<-- [start:reply_mount]
            // static, per-publisher: a policy stack, composed at compile time and paired with
            // the connected broker at startup
            b.include(replying("requests", Respond).to("responses"))
                .publisher(TypedPublisher::new(MemoryPublish).transform(EnvelopeTransform));
            // the default reply wiring: the broker's default policy under the default codec
            b.include(replying("validated-requests", Validate).to("responses"));
            // --8<-- [end:reply_mount]
            // --8<-- [start:forward_mount]
            b.include(with_slots::<Event, (DefaultSlot,), _, _>(
                "ingress", Forward,
            ))
            .publisher(MemoryPublish);
            // --8<-- [end:forward_mount]
            // --8<-- [start:slots_mount]
            // each named slot binds by marker; the call order does not matter
            b.include(with_slots::<Event, (Primary, Shadow), _, _>(
                "mirror", Mirror,
            ))
            .out(Shadow, MemoryPublish)
            .out(Primary, MemoryPublish)
            .mount();
            // --8<-- [end:slots_mount]
            // --8<-- [start:publish_out_mount]
            // the reply keeps .publisher(..) (or its default); the Out slot attaches
            // with .out(<marker>, ..) - DefaultSlot for a single unnamed slot
            b.include(
                replying_with_slots::<Request, (DefaultSlot,), _, _>("gateway-requests", Gateway)
                    .to("gateway-responses"),
            )
            .out(DefaultSlot, MemoryPublish)
            .mount();
            // --8<-- [end:publish_out_mount]
            // --8<-- [start:declared_mount]
            // the slot lists what it may publish; where each message goes is its own declaration
            b.include(with_slots::<Event, (Orders,), _, _>(
                "orders.incoming",
                Route,
            ))
            .out(Orders, MemoryPublish)
            .mount();
            // --8<-- [end:declared_mount]
            // --8<-- [start:batch_publishing_mount]
            // .transactional() marks the wiring; the pairing checks that the policy's live
            // publisher implements TransactionalPublisher. Without it, each reply publishes
            // independently.
            b.include(Confirm)
                .publisher(TypedPublisher::new(MemoryPublish).transactional());
            // --8<-- [end:batch_publishing_mount]
        })
    // --8<-- [end:pipeline]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
