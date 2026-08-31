//! The completion artifact of the manual path: every input spelling, reply shape, injection
//! set and chain axis, mounted on both surfaces. A missing spelling fails this module's build.

use serde::{Deserialize, Serialize};

use crate::memory::{MemoryBroker, MemoryPublish, MemorySource};
use crate::nonzero;
use crate::runtime::{
    Context, Handle, HandlerResult, Message, Payload, Router, RouterDef, SubscriberSettings,
    subscriber,
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
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerResult> {
        let _ = order.id;
        Ok(())
    }
}

struct Inspect;

impl<'p> Handle<Payload<'p>> for Inspect {
    async fn handle(
        &self,
        payload: &Payload<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerResult> {
        let _ = payload.len();
        Ok(())
    }
}

struct Expedite;

impl Handle<Message<Meta, Order>> for Expedite {
    async fn handle(
        &self,
        msg: &Message<Meta, Order>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerResult> {
        let _ = (&msg.headers.tenant, msg.body.id);
        Ok(())
    }
}

struct SettlePage;

impl Handle<[Order]> for SettlePage {
    async fn handle(
        &self,
        page: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), Vec<HandlerResult>> {
        let _ = page.len();
        Ok(())
    }
}

struct Frames;

impl<'p> Handle<[Payload<'p>]> for Frames {
    async fn handle(
        &self,
        page: &[Payload<'p>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerResult> {
        let _ = page.len();
        Ok(())
    }
}

struct HeaderedPage;

impl Handle<[Message<Meta, Order>]> for HeaderedPage {
    async fn handle(
        &self,
        page: &[Message<Meta, Order>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), Vec<HandlerResult>> {
        let _ = page.len();
        Ok(())
    }
}

struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<Confirmation, HandlerResult> {
        Ok(Confirmation { id: order.id })
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
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<Receipt, HandlerResult> {
        Ok(Receipt { id: order.id })
    }
}

struct Echo;

impl Handle<Order, Vec<u8>> for Echo {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<Vec<u8>, HandlerResult> {
        Ok(order.id.to_be_bytes().to_vec())
    }
}

struct ConfirmPages;

impl Handle<[Order], Confirmation> for ConfirmPages {
    async fn handle(
        &self,
        page: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<Vec<Confirmation>, Vec<HandlerResult>> {
        Ok(page.iter().map(|order| Confirmation { id: order.id }).collect())
    }
}

struct ConfirmWithMeta;

impl Handle<Order, Message<Meta, Confirmation>> for ConfirmWithMeta {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<Message<Meta, Confirmation>, HandlerResult> {
        Ok(Message::new(
            Meta {
                tenant: "acme".into(),
            },
            Confirmation { id: order.id },
        ))
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

impl<PA> Handle<Order, (), crate::runtime::Outs<(crate::runtime::Slot<Analytics, PA>,)>> for Mirror
where
    PA: crate::runtime::Publish,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &crate::runtime::Outs<(crate::runtime::Slot<Analytics, PA>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerResult> {
        if outs
            .get(Analytics)
            .message(&Event { id: order.id })
            .to("order-events")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerResult::retry());
        }
        Ok(())
    }
}

struct PageMirror;

impl<PA> Handle<[Order], (), crate::runtime::Outs<(crate::runtime::Slot<Analytics, PA>,)>>
    for PageMirror
where
    PA: crate::runtime::Publish,
{
    async fn handle(
        &self,
        page: &[Order],
        outs: &crate::runtime::Outs<(crate::runtime::Slot<Analytics, PA>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), Vec<HandlerResult>> {
        let _ = (page.len(), outs);
        Ok(())
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
        .include(subscriber("orders", Audit).describe("Inbound orders").undocumented().build())
}

#[test]
fn every_eager_spelling_mounts() {
    let _ = eager_axes();
}

/// Every reply shape mounts through the chain: named and declared destinations, an attached
/// and a defaulted policy, the bare route, the page form, and typed reply headers.
fn reply_axes() -> impl RouterDef<MemoryBroker> {
    use crate::runtime::TypedPublisher;

    Router::<MemoryBroker>::new()
        .include(subscriber("orders", Confirm).reply().on("confirmations").build())
        .include(
            subscriber("orders", Confirm)
                .reply()
                .on("confirmations")
                .publisher(TypedPublisher::new(MemoryPublish))
                .build(),
        )
        .include(subscriber("orders", IssueReceipt).reply().build())
        .include(
            subscriber("orders", Echo)
                .reply_raw()
                .on("echoes")
                .publisher(MemoryPublish)
                .build(),
        )
        .include(
            subscriber("orders", ConfirmPages)
                .reply()
                .on("confirmations")
                .batch(nonzero!(8))
                .build(),
        )
        .include(
            subscriber("orders", ConfirmWithMeta)
                .reply()
                .on("confirmations")
                .build(),
        )
}

#[test]
fn every_reply_spelling_mounts() {
    let _ = reply_axes();
}

struct Gateway;

impl<PA> Handle<Order, Confirmation, crate::runtime::Outs<(crate::runtime::Slot<Analytics, PA>,)>>
    for Gateway
where
    PA: crate::runtime::Publish,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &crate::runtime::Outs<(crate::runtime::Slot<Analytics, PA>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<Confirmation, HandlerResult> {
        let _ = outs;
        Ok(Confirmation { id: order.id })
    }
}

struct PageGateway;

impl<PA> Handle<[Order], Confirmation, crate::runtime::Outs<(crate::runtime::Slot<Analytics, PA>,)>>
    for PageGateway
where
    PA: crate::runtime::Publish,
{
    async fn handle(
        &self,
        page: &[Order],
        outs: &crate::runtime::Outs<(crate::runtime::Slot<Analytics, PA>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<Vec<Confirmation>, Vec<HandlerResult>> {
        let _ = outs;
        Ok(page.iter().map(|o| Confirmation { id: o.id }).collect())
    }
}

struct RawGateway;

impl<PA> Handle<Order, Vec<u8>, crate::runtime::Outs<(crate::runtime::Slot<Analytics, PA>,)>>
    for RawGateway
where
    PA: crate::runtime::Publish,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &crate::runtime::Outs<(crate::runtime::Slot<Analytics, PA>,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<Vec<u8>, HandlerResult> {
        let _ = outs;
        Ok(order.id.to_be_bytes().to_vec())
    }
}

/// The slot arena mounts on both families, bound at the include site.
fn slot_axes() -> impl RouterDef<MemoryBroker> {
    use crate::runtime::TypedPublisher;

    Router::<MemoryBroker>::new()
        .include(subscriber("orders", Mirror).build())
        .out(Analytics, MemoryPublish)
        .build()
        .include(subscriber("orders", PageMirror).build())
        .out(Analytics, MemoryPublish)
        .build()
        .include(
            subscriber("orders", Gateway)
                .reply()
                .on("confirmations")
                .build(),
        )
        .out(Analytics, MemoryPublish)
        .build()
        .include(
            subscriber("orders", Gateway)
                .reply()
                .on("confirmations")
                .publisher(TypedPublisher::new(MemoryPublish))
                .build(),
        )
        .out(Analytics, MemoryPublish)
        .build()
        .include(
            subscriber("orders", PageGateway)
                .reply()
                .on("confirmations")
                .build(),
        )
        .out(Analytics, MemoryPublish)
        .build()
        .include(
            subscriber("orders", RawGateway)
                .reply_raw()
                .on("echoes")
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

impl Handle<Order, (), (), crate::runtime::SeekContext<crate::memory::MemorySeeker>> for Replayer {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, crate::runtime::SeekContext<crate::memory::MemorySeeker>>,
    ) -> Result<(), HandlerResult> {
        let here = *ctx.position();
        if order.id == u64::MAX && ctx.seek(here).await.is_err() {
            return Err(HandlerResult::drop());
        }
        Ok(())
    }
}

/// The seek axis rides the broker context: position and reposition reach the body through
/// `Context`, and a seekable source is all the mount asks for.
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
            b.include(subscriber("orders", SettlePage).buffered(nonzero!(4), std::time::Duration::from_millis(5)).build());
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
