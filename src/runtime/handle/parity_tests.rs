//! The completion artifact of the manual path: every input spelling, reply shape, injection
//! set and chain axis, mounted on both surfaces. A missing spelling fails this module's build.

use std::future::{Future, ready};

use serde::{Deserialize, Serialize};

use crate::Seeker;
use crate::codec::JsonCodec;
use crate::memory::{
    MemoryBroker, MemoryContext, MemoryPublish, MemoryPublisher, MemorySource, Position, SeekHandle,
};
use crate::nonzero;
use crate::runtime::{
    Context, Deserialized, Handle, HandlerOutcome, Input, Message, MessageWire, Outs, Publish,
    ReplyShape, Router, RouterDef, Serialized, SerializedReply, SerializedWire, Slot,
    SoloDeserialized, SubscriberSettings, Verdict, subscriber,
};

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

/// The self-deserializing input lane, spelled by hand (the derive is `macros`-crate sugar over
/// exactly these impls).
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = core::convert::Infallible;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, Self::Error> {
        Ok(Frame(payload))
    }
}

impl Input for Frame<'_> {
    type Axis = SoloDeserialized<Frame<'static>>;
}

/// The self-serialized reply lane, spelled by hand for the same reason.
struct Export(Vec<u8>);

impl Serialized for Export {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl MessageWire for Export {
    type Wire = SerializedWire;
}

impl ReplyShape for Export {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
struct Meta {
    tenant: String,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct Confirmation {
    id: u64,
}

// ------------------------------------------------------------------------------- the bodies

struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Verdict<Order, ()>> {
        let _ = order.id;
        ready(Ok(()))
    }
}

struct Inspect;

impl<'p> Handle<Frame<'p>> for Inspect {
    fn handle(
        &self,
        frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = frame.0.len();
        ready(Ok(()))
    }
}

struct Expedite;

impl Handle<Message<Meta, Order>> for Expedite {
    fn handle(
        &self,
        msg: &Message<Meta, Order>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = (&msg.headers.tenant, msg.body.id);
        ready(Ok(()))
    }
}

struct SettlePage;

impl Handle<[Order]> for SettlePage {
    fn handle(
        &self,
        page: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = page.len();
        ready(Ok(()))
    }
}

struct Frames;

impl<'p> Handle<[Frame<'p>]> for Frames {
    fn handle(
        &self,
        page: &[Frame<'p>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = page.len();
        ready(Ok(()))
    }
}

struct HeaderedPage;

impl Handle<[Message<Meta, Order>]> for HeaderedPage {
    fn handle(
        &self,
        page: &[Message<Meta, Order>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = page.len();
        ready(Ok(()))
    }
}

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation { id: order.id }))
    }
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct Receipt {
    id: u64,
}

impl crate::OutgoingDestination for Receipt {
    type Form = crate::FixedName;

    const ADDRESS: &'static str = "receipts";
}

struct IssueReceipt;

impl Handle<Order, Receipt> for IssueReceipt {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        ready(Ok(Receipt { id: order.id }))
    }
}

struct Echo;

impl Handle<Order, Export> for Echo {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Export, HandlerOutcome>> {
        ready(Ok(Export(order.id.to_be_bytes().to_vec())))
    }
}

struct RawEcho;

impl<'p> Handle<Frame<'p>, Export> for RawEcho {
    fn handle(
        &self,
        frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Export, HandlerOutcome>> {
        ready(Ok(Export(frame.0.to_vec())))
    }
}

struct ConfirmPages;

impl Handle<[Order], Vec<Confirmation>> for ConfirmPages {
    fn handle(
        &self,
        page: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        ready(Ok(page
            .iter()
            .map(|order| Confirmation { id: order.id })
            .collect()))
    }
}

struct ConfirmWithMeta;

impl Handle<Order, Message<Meta, Confirmation>> for ConfirmWithMeta {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Message<Meta, Confirmation>, HandlerOutcome>> {
        ready(Ok(Message::new(
            Meta {
                tenant: "acme".into(),
            },
            Confirmation { id: order.id },
        )))
    }
}

struct Analytics;

impl crate::runtime::OutSlot for Analytics {
    const NAME: &'static str = "Analytics";
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct Event {
    id: u64,
}

impl crate::OutgoingDestination for Event {
    type Form = crate::CallerName;
}

impl crate::MessageHeaders for Event {
    type Contract = crate::NoHeaders;
}

impl crate::runtime::PublishedThrough<Analytics> for Event {}

// A serialized out type is a first-class dictionary member: it declares its destination and
// membership like any model, and its bytes leave through the same typed builder, uncoded.
impl crate::OutgoingDestination for Export {
    type Form = crate::CallerName;
}

impl crate::MessageHeaders for Export {
    type Contract = crate::NoHeaders;
}

impl crate::runtime::PublishedThrough<Analytics> for Export {}

/// Publishes a serialized dictionary member's own bytes through the slot.
struct RawMirror;

impl<W, E> Handle<Order, (), Outs<(Slot<Analytics, W, E>,)>> for RawMirror
where
    Slot<Analytics, W, E>: Publish,
    W: Send + Sync,
    E: Send + Sync,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Slot<Analytics, W, E>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let export = Export(order.id.to_be_bytes().to_vec());
        if outs
            .get(Analytics)
            .message(&export)
            .to("order-exports")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

struct Mirror;

impl<W, E> Handle<Order, (), Outs<(Slot<Analytics, W, E>,)>> for Mirror
where
    Slot<Analytics, W, E>: Publish,
    W: Send + Sync,
    E: Send + Sync,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Slot<Analytics, W, E>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(Analytics)
            .message(&Event { id: order.id })
            .to("order-events")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// Pins the concrete-binding spelling: the body names the wired live type directly.
fn assert_wired_live(_live: &MemoryPublisher) {}

/// The concrete-binding spelling: the body names the wired live type directly and reaches it
/// through the entry's transparent `Deref`, next to the typed publish surface.
struct PinnedMirror;

impl Handle<Order, (), Outs<(Slot<Analytics, MemoryPublisher, JsonCodec>,)>> for PinnedMirror {
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Slot<Analytics, MemoryPublisher, JsonCodec>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        let entry = outs.get(Analytics);
        // The deref target is the broker's live publisher itself.
        assert_wired_live(entry);
        if entry
            .message(&Event { id: order.id })
            .to("order-events")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

struct PageMirror;

impl<W, E> Handle<[Order], (), Outs<(Slot<Analytics, W, E>,)>> for PageMirror
where
    Slot<Analytics, W, E>: Publish,
    W: Send + Sync,
    E: Send + Sync,
{
    fn handle(
        &self,
        page: &[Order],
        outs: &Outs<(Slot<Analytics, W, E>,)>,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = (page.len(), outs);
        ready(Ok(()))
    }
}

// ------------------------------------------------------------------------------- the mounts

/// Every plain input spelling mounts through the one constructor on a router.
fn eager_axes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .include(subscriber("orders", Audit).workers(nonzero!(4)).build())
        .include(subscriber(MemorySource::new("orders"), Audit).build())
        .include(subscriber("frames", Inspect).build())
        .include(subscriber("orders", Expedite).build())
        .include(subscriber("orders", SettlePage).batch(nonzero!(8)).build())
        .include(subscriber("frames", Frames).build())
        // The cap is a page setting, so it applies on every page spelling: the decoded one
        // above, the self-deserializing one and the paired one.
        .include(subscriber("frames", Frames).batch(nonzero!(8)).build())
        .include(subscriber("orders", HeaderedPage).build())
        .include(
            subscriber("orders", HeaderedPage)
                .batch(nonzero!(8))
                .build(),
        )
        .include(
            subscriber("orders", Audit)
                .describe("Inbound orders")
                .undocumented()
                .build(),
        )
}

#[test]
fn every_eager_spelling_mounts() {
    let _ = eager_axes();
}

/// Every reply shape mounts through the chain: named and declared destinations, an attached
/// and a defaulted policy, the serialized wire (from a decoded and a self-deserializing input,
/// with an explicit and the broker's default publisher - selected by the reply type alone),
/// the page form, and typed reply headers.
fn reply_axes() -> impl RouterDef<MemoryBroker> {
    use crate::runtime::TypedPublisher;

    Router::<MemoryBroker>::new()
        .include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .build(),
        )
        .include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .publisher(TypedPublisher::new(MemoryPublish))
                .build(),
        )
        .include(subscriber("orders", IssueReceipt).reply().build())
        .include(
            subscriber("orders", Echo)
                .reply()
                .to("echoes")
                .publisher(MemoryPublish)
                .build(),
        )
        .include(subscriber("frames", RawEcho).reply().to("echoes").build())
        // The cap on a page reply: each chunk is one call answering with its own reply vector,
        // published on its own.
        .include(
            subscriber("orders", ConfirmPages)
                .reply()
                .to("confirmations")
                .batch(nonzero!(8))
                .build(),
        )
        .include(
            subscriber("orders", ConfirmWithMeta)
                .reply()
                .to("confirmations")
                .build(),
        )
}

#[test]
fn every_reply_spelling_mounts() {
    let _ = reply_axes();
}

struct Gateway;

impl<W, E> Handle<Order, Confirmation, Outs<(Slot<Analytics, W, E>,)>> for Gateway
where
    Slot<Analytics, W, E>: Publish,
    W: Send + Sync,
    E: Send + Sync,
{
    fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Slot<Analytics, W, E>,)>,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        let _ = outs;
        ready(Ok(Confirmation { id: order.id }))
    }
}

struct PageGateway;

impl<W, E> Handle<[Order], Vec<Confirmation>, Outs<(Slot<Analytics, W, E>,)>> for PageGateway
where
    Slot<Analytics, W, E>: Publish,
    W: Send + Sync,
    E: Send + Sync,
{
    fn handle(
        &self,
        page: &[Order],
        outs: &Outs<(Slot<Analytics, W, E>,)>,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        let _ = outs;
        ready(Ok(page.iter().map(|o| Confirmation { id: o.id }).collect()))
    }
}

struct RawGateway;

impl<W, E> Handle<Order, Export, Outs<(Slot<Analytics, W, E>,)>> for RawGateway
where
    Slot<Analytics, W, E>: Publish,
    W: Send + Sync,
    E: Send + Sync,
{
    fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Slot<Analytics, W, E>,)>,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Export, HandlerOutcome>> {
        let _ = outs;
        ready(Ok(Export(order.id.to_be_bytes().to_vec())))
    }
}

/// The slot arena mounts on both families, bound at the include site - the generic and the
/// concrete-typed spelling alike.
fn slot_axes() -> impl RouterDef<MemoryBroker> {
    use crate::runtime::TypedPublisher;

    Router::<MemoryBroker>::new()
        .include(subscriber("orders", Mirror).build())
        .out(Analytics, MemoryPublish)
        .build()
        .include(subscriber("orders", RawMirror).build())
        .out(Analytics, MemoryPublish)
        .build()
        .include(subscriber("orders", PinnedMirror).build())
        .out(Analytics, MemoryPublish)
        .build()
        .include(subscriber("orders", PageMirror).build())
        .out(Analytics, MemoryPublish)
        .build()
        // The cap on a slot-carrying page: the arena rides every chunk the body is handed.
        .include(subscriber("orders", PageMirror).batch(nonzero!(8)).build())
        .out(Analytics, MemoryPublish)
        .build()
        .include(
            subscriber("orders", Gateway)
                .reply()
                .to("confirmations")
                .build(),
        )
        .out(Analytics, MemoryPublish)
        .build()
        .include(
            subscriber("orders", Gateway)
                .reply()
                .to("confirmations")
                .publisher(TypedPublisher::new(MemoryPublish))
                .build(),
        )
        .out(Analytics, MemoryPublish)
        .build()
        .include(
            subscriber("orders", PageGateway)
                .reply()
                .to("confirmations")
                .build(),
        )
        .out(Analytics, MemoryPublish)
        .build()
        // A page that both replies and fans out through the arena, capped.
        .include(
            subscriber("orders", PageGateway)
                .reply()
                .to("confirmations")
                .batch(nonzero!(8))
                .build(),
        )
        .out(Analytics, MemoryPublish)
        .build()
        .include(
            subscriber("orders", RawGateway)
                .reply()
                .to("echoes")
                .publisher(MemoryPublish)
                .build(),
        )
        .out(Analytics, MemoryPublish)
        .build()
}

#[test]
fn every_slot_spelling_mounts() {
    let _ = slot_axes();
}

struct Replayer;

impl Handle<Order, (), (), MemoryContext> for Replayer {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, MemoryContext>,
    ) -> Result<(), HandlerOutcome> {
        let here = ctx.context(Position);
        if order.id == u64::MAX && ctx.context(SeekHandle).seek(here).await.is_err() {
            return Err(HandlerOutcome::drop());
        }
        Ok(())
    }
}

/// The seek axis rides the broker context: the position and the reposition handle are plain
/// context fields read by key, and a source whose context carries them is all the mount asks
/// for.
#[tokio::test]
async fn seek_reaches_the_body_through_the_context() {
    use crate::OutgoingMessage;
    use crate::Publisher;
    use crate::runtime::{AppInfo, RustStream};

    let app = RustStream::new(AppInfo::new("handle-seek", "0.0.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("orders", Replayer).build());
            b.after_startup(MemoryPublish, async move |publisher| {
                publisher
                    .publish(OutgoingMessage::new("orders", br#"{"id":7}"#.as_slice()))
                    .await
            });
        },
    );
    let running = app.start().await.expect("the app starts");
    running.shutdown().await.expect("the app stops cleanly");
}

/// The scope surface mounts the same definitions - the new lanes included: the
/// self-deserializing solo and page inputs, the serialized replies (attached and defaulted,
/// with and without slots) and the serialized dictionary out - and one subscriber dispatches
/// end to end.
#[tokio::test]
async fn a_subscriber_dispatches_end_to_end() {
    use crate::OutgoingMessage;
    use crate::Publisher;
    use crate::runtime::{AppInfo, RustStream, TypedPublisher};

    let app = RustStream::new(AppInfo::new("handle-parity", "0.0.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("orders", Audit).build());
            b.include(
                subscriber("orders", SettlePage)
                    .buffered(nonzero!(4), std::time::Duration::from_millis(5))
                    .build(),
            );
            b.include(subscriber("frames", Inspect).build());
            b.include(subscriber("frames", Frames).build());
            b.include(
                subscriber("orders", Echo)
                    .reply()
                    .to("echoes")
                    .publisher(MemoryPublish)
                    .build(),
            );
            b.include(subscriber("frames", RawEcho).reply().to("echoes").build());
            b.include(
                subscriber("orders", Confirm)
                    .reply()
                    .to("confirmations")
                    .publisher(TypedPublisher::new(MemoryPublish))
                    .build(),
            );
            b.include(subscriber("orders", RawMirror).build())
                .publisher(MemoryPublish);
            b.include(
                subscriber("orders", RawGateway)
                    .reply()
                    .to("echoes")
                    .publisher(MemoryPublish)
                    .build(),
            )
            .out(Analytics, MemoryPublish)
            .build();
            b.after_startup(MemoryPublish, async move |publisher| {
                publisher
                    .publish(OutgoingMessage::new("orders", br#"{"id":7}"#.as_slice()))
                    .await
            });
        },
    );
    let running = app.start().await.expect("the app starts");
    running.shutdown().await.expect("the app stops cleanly");
}
