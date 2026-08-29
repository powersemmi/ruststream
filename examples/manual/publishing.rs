//! The publishing forms from the Publishing guide, written without the `macros` feature. The
//! attribute and the derives are sugar over public traits, so every declaration they mint is
//! written out here: the reply definitions, the slot markers and their dictionaries, the
//! injected `Out` parameters, and what a message type says about being sent. The mount site
//! then reads exactly as it does with the attribute - `include`, `.publisher(..)`,
//! `.out(marker, ..)`, `.mount()`.
//!
//! ```text
//! cargo run --example manual_publishing --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::fmt::Display;
use std::future::{Future, ready};
use std::marker::PhantomData;

use ruststream::codec::{Codec, JsonCodec};
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::runtime::{
    AllOpen, BatchPublishingCall, BatchPublishingDef, BindSlots, BoundSegment, Declared, Decoded,
    DefaultSlot, HasSlots, InjectCall, InjectDef, MissingSegment, OutMessages,
    OutgoingMessageMetadata, PublishAt, PublishContext, PublishError, PublishLayer, PublishNext,
    PublishPipeline, PublishTransform, PublishedThrough, PublishingCall, PublishingDef, Settle,
    SlotPublisher, SubscriberBuilder, TemplateAddress, Transactional, forms,
};
// The derive and the pipeline's message type share the name in different namespaces: the derive
// is the macro `ruststream::Outgoing`, the value flowing through a publish transform is the type
// `ruststream::runtime::Outgoing`.
use ruststream::runtime::Outgoing;
use ruststream::{
    CallerName, ConnectedBroker, FixedName, MessageHeaders, NameTemplate, NoHeaders,
    OutgoingDestination, TransactionalPublisher,
};
use serde::{Deserialize, Serialize};

// What the definition traits are not asked for here: the description a doc comment would carry,
// the payload and header schemas the `asyncapi` probes lift off the types, and the `Message`
// name. Every one of them defaults to "not declared", so a hand-written definition fills in only
// what it actually declares - below, that is `outgoing()`: the messages that leave.

/// The whole content of a definition generic over its slot publishers: the inferred types and
/// nothing else, so the definition stays a zero-sized value the mount site builds for free.
type SlotTypes<T> = PhantomData<fn() -> T>;

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
// A `publish(..)` handler is a definition, not a function: the subscription source, the reply
// destination and the reply type live on `PublishingDef`, and the body moves to `PublishingCall`.
// The call stays generic over the state, so it mounts on an app with any state type.
struct Respond;

// What the attribute puts between the definition and `include`: the mount form it dispatches on,
// and the settings builder the mount actually drives. Everything the attribute would have named
// (`workers(..)`, `on_failure(..)`) is a call on that builder, so leaving them out here is what
// keeps them open at the mount site. The source is written twice, exactly as the attribute writes
// it: the builder carries the one the mount reads, and the definition keeps its own so it stands
// alone.
impl Declared for Respond {
    type Form = forms::Publishing;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("requests"))
    }
}

impl PublishingDef for Respond {
    type Input = Decoded<Request>;
    type Injections = ();
    type Reply = Response;
    type Context = ();
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("requests")
    }

    fn reply_name(&self) -> &'static str {
        "responses"
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        vec![OutgoingMessageMetadata::new(
            "responses",
            std::any::type_name::<Response>(),
        )]
    }
}

impl<State: Send + Sync> PublishingCall<State> for Respond {
    // The body awaits nothing, so it is a future-returning method rather than an `async fn`: the
    // attribute picks the same shape from the body it wraps.
    fn call(
        &self,
        req: &Request,
        _injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> impl Future<Output = Result<Response, HandlerResult>> + Send {
        println!("responding to request {}", req.id);
        ready(Ok(Response { ok: true }))
    }
}
// --8<-- [end:reply]

// --8<-- [start:reply_result]
// `Ok` publishes the reply and acks; `Err` publishes nothing and the dispatcher acts on the
// returned HandlerResult (here: drop the malformed request instead of replying). The two arms are
// the call's return type, so the attribute's `Result<Response, HandlerResult>` is what the trait
// already asks for.
struct Validate;

impl Declared for Validate {
    type Form = forms::Publishing;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("validated-requests"))
    }
}

impl PublishingDef for Validate {
    type Input = Decoded<Request>;
    type Injections = ();
    type Reply = Response;
    type Context = ();
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("validated-requests")
    }

    fn reply_name(&self) -> &'static str {
        "responses"
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        vec![OutgoingMessageMetadata::new(
            "responses",
            std::any::type_name::<Response>(),
        )]
    }
}

impl<State: Send + Sync> PublishingCall<State> for Validate {
    fn call(
        &self,
        req: &Request,
        _injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
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
// The publisher arrives as an injection: the source is attached at the include site, the runtime
// pairs it with the connected broker at startup, and the handler always holds a live publisher -
// no registry, no erased lookup, no state plumbing. The publisher type is not named, so the value
// the include site attaches decides it; that is why the definition lives on a second, generic
// type, and the unit struct passed to `include` only carries the slot list and the instantiation.
// `Event` declares no destination of its own, so the call site names one.
struct Forward;

impl Declared for Forward {
    type Form = forms::Out;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("ingress"))
    }
}

impl HasSlots for Forward {
    type Markers = (DefaultSlot,);
}

impl<Conn, Enc, Policy> BindSlots<Conn, ((Policy, Enc),)> for Forward
where
    Conn: ConnectedBroker,
    Policy: PublishPolicy<Conn>,
{
    type Bound = ForwardDef<SlotPublisher<Policy::Live, DefaultSlot>, Enc>;
    type Extra = ((Policy, Enc),);

    fn bind(self, sources: ((Policy, Enc),)) -> (Self::Bound, Self::Extra) {
        (ForwardDef(PhantomData), sources)
    }
}

/// The definition the slot publisher and the scope codec are threaded into. It is never
/// constructed with a value in it: the injected publisher reaches the body through the
/// injections tuple, and the generics only pin its type.
struct ForwardDef<Egress, Enc>(SlotTypes<(Egress, Enc)>);

impl<Egress, Enc> InjectDef for ForwardDef<Egress, Enc>
where
    Egress: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    type Input = Decoded<Event>;
    type Context = ();
    type Source = Name;
    type Injections = (Out<Egress, DefaultSlot, (), Enc>,);

    fn source(&self) -> Self::Source {
        Name::new("ingress")
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        // An unrestricted slot declares its marker's whole dictionary; the implicit one has none.
        <DefaultSlot as OutSlot>::outgoing()
    }
}

impl<State, Egress, Enc> InjectCall<State> for ForwardDef<Egress, Enc>
where
    State: Send + Sync,
    Egress: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    async fn call(
        &self,
        event: &Event,
        injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> Settle {
        let Out(out) = &injections.0;
        if out.message(event).to("egress").publish().await.is_err() {
            return HandlerResult::retry().into();
        }
        HandlerResult::Ack.into()
    }
}
// --8<-- [end:forward]

// --8<-- [start:slots]
// A handler with several injected publishers names a slot marker per parameter; the include
// site binds each marker to its own policy, in any order. No broker publisher type appears in
// the definition, so the same handler mounts on a production broker and on its in-process test
// transport unchanged. Each marker lists what may leave through it, which is both what the
// generated document reports and what the publish builder admits.
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

impl Declared for Mirror {
    type Form = forms::Out;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("mirror"))
    }
}

impl HasSlots for Mirror {
    type Markers = (Primary, Shadow);
}

impl<Conn, Enc, PrimaryPolicy, ShadowPolicy>
    BindSlots<Conn, ((PrimaryPolicy, Enc), (ShadowPolicy, Enc))> for Mirror
where
    Conn: ConnectedBroker,
    PrimaryPolicy: PublishPolicy<Conn>,
    ShadowPolicy: PublishPolicy<Conn>,
{
    type Bound = MirrorDef<
        SlotPublisher<PrimaryPolicy::Live, Primary>,
        SlotPublisher<ShadowPolicy::Live, Shadow>,
        Enc,
    >;
    type Extra = ((PrimaryPolicy, Enc), (ShadowPolicy, Enc));

    fn bind(
        self,
        sources: ((PrimaryPolicy, Enc), (ShadowPolicy, Enc)),
    ) -> (Self::Bound, Self::Extra) {
        (MirrorDef(PhantomData), sources)
    }
}

struct MirrorDef<PrimaryPub, ShadowPub, Enc>(SlotTypes<(PrimaryPub, ShadowPub, Enc)>);

impl<PrimaryPub, ShadowPub, Enc> InjectDef for MirrorDef<PrimaryPub, ShadowPub, Enc>
where
    PrimaryPub: Publisher + Send + Sync + 'static,
    ShadowPub: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    type Input = Decoded<Event>;
    type Context = ();
    type Source = Name;
    type Injections = (
        Out<PrimaryPub, Primary, (), Enc>,
        Out<ShadowPub, Shadow, (), Enc>,
    );

    fn source(&self) -> Self::Source {
        Name::new("mirror")
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        let mut declared = <Primary as OutSlot>::outgoing();
        declared.extend(<Shadow as OutSlot>::outgoing());
        declared
    }
}

impl<State, PrimaryPub, ShadowPub, Enc> InjectCall<State> for MirrorDef<PrimaryPub, ShadowPub, Enc>
where
    State: Send + Sync,
    PrimaryPub: Publisher + Send + Sync + 'static,
    ShadowPub: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    async fn call(
        &self,
        event: &Event,
        injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> Settle {
        let Out(primary) = &injections.0;
        let Out(shadow) = &injections.1;
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
// A reply form and an injected publisher in one definition: the reply answers on the fixed
// destination while an audit copy fans out through the slot. Both axes live on the same
// definition, which is why the form token names both and the mount site fills both.
struct Gateway;

impl Declared for Gateway {
    type Form = forms::PublishingOut;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("gateway-requests"))
    }
}

impl HasSlots for Gateway {
    type Markers = (DefaultSlot,);
}

impl<Conn, Enc, Policy> BindSlots<Conn, ((Policy, Enc),)> for Gateway
where
    Conn: ConnectedBroker,
    Policy: PublishPolicy<Conn>,
{
    type Bound = GatewayDef<SlotPublisher<Policy::Live, DefaultSlot>, Enc>;
    type Extra = ((Policy, Enc),);

    fn bind(self, sources: ((Policy, Enc),)) -> (Self::Bound, Self::Extra) {
        (GatewayDef(PhantomData), sources)
    }
}

struct GatewayDef<Audit, Enc>(SlotTypes<(Audit, Enc)>);

impl<Audit, Enc> PublishingDef for GatewayDef<Audit, Enc>
where
    Audit: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    type Input = Decoded<Request>;
    type Injections = (Out<Audit, DefaultSlot, (), Enc>,);
    type Reply = Response;
    type Context = ();
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("gateway-requests")
    }

    fn reply_name(&self) -> &'static str {
        "gateway-responses"
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        let mut declared = vec![OutgoingMessageMetadata::new(
            "gateway-responses",
            std::any::type_name::<Response>(),
        )];
        declared.extend(<DefaultSlot as OutSlot>::outgoing());
        declared
    }
}

impl<State, Audit, Enc> PublishingCall<State> for GatewayDef<Audit, Enc>
where
    State: Send + Sync,
    Audit: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    async fn call(
        &self,
        req: &Request,
        injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> Result<Response, HandlerResult> {
        let Out(out) = &injections.0;
        if out
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

impl Declared for Route {
    type Form = forms::Out;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("orders.incoming"))
    }
}

impl HasSlots for Route {
    type Markers = (Orders,);
}

impl<Conn, Enc, Policy> BindSlots<Conn, ((Policy, Enc),)> for Route
where
    Conn: ConnectedBroker,
    Policy: PublishPolicy<Conn>,
{
    type Bound = RouteDef<SlotPublisher<Policy::Live, Orders>, Enc>;
    type Extra = ((Policy, Enc),);

    fn bind(self, sources: ((Policy, Enc),)) -> (Self::Bound, Self::Extra) {
        (RouteDef(PhantomData), sources)
    }
}

struct RouteDef<OrdersPub, Enc>(SlotTypes<(OrdersPub, Enc)>);

impl<OrdersPub, Enc> InjectDef for RouteDef<OrdersPub, Enc>
where
    OrdersPub: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    type Input = Decoded<Event>;
    type Context = ();
    type Source = Name;
    // The third position of the slot is the declared message set: a list, checked against the
    // marker's dictionary at every publish.
    type Injections = (Out<OrdersPub, Orders, (OrderConfirmed, OrderPlaced), Enc>,);

    fn source(&self) -> Self::Source {
        Name::new("orders.incoming")
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        let mut declared = <OrderConfirmed as OutMessages<Orders>>::outgoing();
        declared.extend(<OrderPlaced as OutMessages<Orders>>::outgoing());
        declared
    }
}

impl<State, OrdersPub, Enc> InjectCall<State> for RouteDef<OrdersPub, Enc>
where
    State: Send + Sync,
    OrdersPub: Publisher + Send + Sync + 'static,
    Enc: Codec + Send + Sync + 'static,
{
    async fn call(
        &self,
        event: &Event,
        injections: &Self::Injections,
        _ctx: &mut Context<'_, (), State>,
    ) -> Settle {
        let Out(orders) = &injections.0;
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
/// definition differs from the single-message one only in what the call consumes and returns: a
/// slice in, the page's replies out.
struct Confirm;

impl Declared for Confirm {
    type Form = forms::BatchPublishing;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("orders"))
    }
}

impl BatchPublishingDef for Confirm {
    type Input = Decoded<Event>;
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

impl<State: Send + Sync> BatchPublishingCall<State> for Confirm {
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
            b.include(Respond)
                .publisher(TypedPublisher::new(MemoryPublish).transform(EnvelopeTransform));
            // the default reply wiring: the broker's default policy under the default codec
            b.include(Validate);
            // --8<-- [end:reply_mount]
            // --8<-- [start:forward_mount]
            b.include(Forward).publisher(MemoryPublish);
            // --8<-- [end:forward_mount]
            // --8<-- [start:slots_mount]
            // each named slot binds by marker; the call order does not matter
            b.include(Mirror)
                .out(Shadow, MemoryPublish)
                .out(Primary, MemoryPublish)
                .mount();
            // --8<-- [end:slots_mount]
            // --8<-- [start:publish_out_mount]
            // the reply keeps .publisher(..) (or its default); the Out slot attaches
            // with .out(<marker>, ..) - DefaultSlot for a single unnamed slot
            b.include(Gateway).out(DefaultSlot, MemoryPublish).mount();
            // --8<-- [end:publish_out_mount]
            // --8<-- [start:declared_mount]
            // the slot lists what it may publish; where each message goes is its own declaration
            b.include(Route).out(Orders, MemoryPublish).mount();
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
