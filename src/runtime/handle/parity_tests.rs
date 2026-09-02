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
    Bare, Context, Handle, HandlerOutcome, Message, Outs, Payload, Publish, Router, RouterDef,
    Slot, SubscriberSettings, Verdict, subscriber,
};

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
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

impl<'p> Handle<Payload<'p>> for Inspect {
    fn handle(
        &self,
        payload: &Payload<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = payload.len();
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

impl<'p> Handle<[Payload<'p>]> for Frames {
    fn handle(
        &self,
        page: &[Payload<'p>],
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

impl Handle<Order, Vec<u8>> for Echo {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<u8>, HandlerOutcome>> {
        ready(Ok(order.id.to_be_bytes().to_vec()))
    }
}

struct RawEcho;

impl<'p> Handle<Payload<'p>, Vec<u8>> for RawEcho {
    fn handle(
        &self,
        payload: &Payload<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<u8>, HandlerOutcome>> {
        ready(Ok(payload.to_vec()))
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
        .include(subscriber("orders", HeaderedPage).build())
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
/// and a defaulted policy, the bare wire (from a decoded and a byte input, with an explicit
/// and the default bare publisher), the page form, and typed reply headers.
fn reply_axes() -> impl RouterDef<MemoryBroker> {
    use crate::runtime::{DefaultBareReply, TypedPublisher};

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
                .publisher(Bare(MemoryPublish))
                .build(),
        )
        .include(
            subscriber("frames", RawEcho)
                .reply()
                .to("echoes")
                .publisher(DefaultBareReply)
                .build(),
        )
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

impl<W, E> Handle<Order, Vec<u8>, Outs<(Slot<Analytics, W, E>,)>> for RawGateway
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
    ) -> impl Future<Output = Result<Vec<u8>, HandlerOutcome>> {
        let _ = outs;
        ready(Ok(order.id.to_be_bytes().to_vec()))
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
        .include(subscriber("orders", PinnedMirror).build())
        .out(Analytics, MemoryPublish)
        .build()
        .include(subscriber("orders", PageMirror).build())
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
        .include(
            subscriber("orders", RawGateway)
                .reply()
                .to("echoes")
                .publisher(Bare(MemoryPublish))
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

/// The scope surface mounts the same definitions, and one subscriber dispatches end to end.
#[tokio::test]
async fn a_subscriber_dispatches_end_to_end() {
    use crate::OutgoingMessage;
    use crate::Publisher;
    use crate::runtime::{AppInfo, RustStream};

    let app = RustStream::new(AppInfo::new("handle-parity", "0.0.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("orders", Audit).build());
            b.include(
                subscriber("orders", SettlePage)
                    .buffered(nonzero!(4), std::time::Duration::from_millis(5))
                    .build(),
            );
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
