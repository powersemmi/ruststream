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
