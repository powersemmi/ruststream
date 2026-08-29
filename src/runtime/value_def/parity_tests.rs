//! The completion artifact for the value path: one registration per mount form token in
//! `forms.rs`, so the coverage is checked by the compiler rather than by review.
//!
//! Each handler body is the minimal shape its form demands; the mounts run on the in-memory
//! broker's surfaces. A form missing a value spelling fails this module's build.

use std::future::{Future, ready};

use serde::{Deserialize, Serialize};

use super::{
    batch, batch_in, batch_replying, batch_replying_with_slots, batch_with_headers,
    batch_with_seek, batch_with_slots, raw, raw_batch, raw_replying, raw_replying_with_slots,
    replying, replying_in, replying_with_slots, subscriber, subscriber_in, with_seek, with_slots,
};
use crate::codec::JsonCodec;
use crate::memory::{MemoryBroker, MemoryPublish, MemorySeeker, MemorySource};
use crate::runtime::{
    BatchReply, BatchResult, Context, Handler, HandlerResult, Out, OutSlot, RawSliceHandler, Reply,
    Router, RouterDef, Seek, Settle, SliceHandler, SliceHandlerWithHeaders, SlotsBatchReply,
    SlotsHandler, SlotsReply, SlotsSliceHandler, SubscriberSettings,
};
use crate::{Publisher, nonzero};

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct Meta {
    trace: String,
}

#[derive(Debug, Serialize)]
struct Confirmation {
    id: u64,
}

/// A state type some bodies are pinned to, for the `_in` constructors.
struct Ledger;

struct Audit;

impl OutSlot for Audit {
    const NAME: &'static str = "Audit";
}

// -------------------------------------------------------------------------------------------
// The bodies, one per form family.

struct Plain;

impl Handler<Order> for Plain {
    fn handle(&self, _o: &Order, _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        ready(HandlerResult::ack().into())
    }
}

struct Pinned;

impl Handler<Order, (), Ledger> for Pinned {
    fn handle(
        &self,
        _o: &Order,
        _ctx: &mut Context<'_, (), Ledger>,
    ) -> impl Future<Output = Settle> + Send {
        ready(HandlerResult::ack().into())
    }
}

struct Bytes;

impl Handler<[u8]> for Bytes {
    fn handle(&self, _p: &[u8], _ctx: &mut Context<'_>) -> impl Future<Output = Settle> + Send {
        ready(HandlerResult::ack().into())
    }
}

struct Page;

impl SliceHandler<Order> for Page {
    fn handle_slice(
        &self,
        _b: &[Order],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> + Send {
        ready(BatchResult::Uniform(HandlerResult::ack()))
    }
}

struct PinnedPage;

impl SliceHandler<Order, Ledger> for PinnedPage {
    fn handle_slice(
        &self,
        _b: &[Order],
        _ctx: &mut Context<'_, (), Ledger>,
    ) -> impl Future<Output = BatchResult> + Send {
        ready(BatchResult::Uniform(HandlerResult::ack()))
    }
}

struct Frames;

impl RawSliceHandler for Frames {
    fn handle_slice(
        &self,
        _b: &[&[u8]],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> + Send {
        ready(BatchResult::Uniform(HandlerResult::ack()))
    }
}

struct HeaderedPage;

impl SliceHandlerWithHeaders<Order, Meta> for HeaderedPage {
    fn handle_slice(
        &self,
        _b: &[Order],
        _headers: Vec<Meta>,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = BatchResult> + Send {
        ready(BatchResult::Uniform(HandlerResult::ack()))
    }
}

struct Confirm;

impl Reply<Order> for Confirm {
    type Out = Confirmation;

    fn reply(
        &self,
        order: &Order,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerResult>> + Send {
        ready(Ok(Confirmation { id: order.id }))
    }
}

struct PinnedConfirm;

impl Reply<Order, (), Ledger> for PinnedConfirm {
    type Out = Confirmation;

    fn reply(
        &self,
        order: &Order,
        _ctx: &mut Context<'_, (), Ledger>,
    ) -> impl Future<Output = Result<Confirmation, HandlerResult>> + Send {
        ready(Ok(Confirmation { id: order.id }))
    }
}

struct Echo;

impl Reply<[u8]> for Echo {
    type Out = Vec<u8>;

    fn reply(
        &self,
        payload: &[u8],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<u8>, HandlerResult>> + Send {
        ready(Ok(payload.to_vec()))
    }
}

struct ConfirmPage;

impl BatchReply<Order> for ConfirmPage {
    type Out = Confirmation;

    fn reply(
        &self,
        batch: &[Order],
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, HandlerResult>> + Send {
        ready(Ok(batch
            .iter()
            .map(|o| Confirmation { id: o.id })
            .collect()))
    }
}

struct Mirror;

impl<P, E, S> SlotsHandler<Order, (Out<P, Audit, (), E>,), (), S> for Mirror
where
    P: Publisher,
    E: Send + Sync,
    S: Send + Sync,
{
    fn handle(
        &self,
        _o: &Order,
        _slots: &(Out<P, Audit, (), E>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Settle> + Send {
        ready(HandlerResult::ack().into())
    }
}

struct Skipper;

impl<S: Send + Sync> SlotsHandler<Order, (Seek<MemorySeeker>,), (), S> for Skipper {
    fn handle(
        &self,
        _o: &Order,
        _slots: &(Seek<MemorySeeker>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Settle> + Send {
        ready(HandlerResult::ack().into())
    }
}

struct PageMirror;

impl<P, E, S> SlotsSliceHandler<Order, (Out<P, Audit, (), E>,), S> for PageMirror
where
    P: Publisher,
    E: Send + Sync,
    S: Send + Sync,
{
    fn handle_slice(
        &self,
        _b: &[Order],
        _slots: &(Out<P, Audit, (), E>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = BatchResult> + Send {
        ready(BatchResult::Uniform(HandlerResult::ack()))
    }
}

struct PageSkipper;

impl<S: Send + Sync> SlotsSliceHandler<Order, (Seek<MemorySeeker>,), S> for PageSkipper {
    fn handle_slice(
        &self,
        _b: &[Order],
        _slots: &(Seek<MemorySeeker>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = BatchResult> + Send {
        ready(BatchResult::Uniform(HandlerResult::ack()))
    }
}

struct Gateway;

impl<P, E, S> SlotsReply<Order, (Out<P, Audit, (), E>,), (), S> for Gateway
where
    P: Publisher,
    E: Send + Sync,
    S: Send + Sync,
{
    type Out = Confirmation;

    fn reply(
        &self,
        order: &Order,
        _slots: &(Out<P, Audit, (), E>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Result<Confirmation, HandlerResult>> + Send {
        ready(Ok(Confirmation { id: order.id }))
    }
}

struct RawGateway;

impl<P, E, S> SlotsReply<Order, (Out<P, Audit, (), E>,), (), S> for RawGateway
where
    P: Publisher,
    E: Send + Sync,
    S: Send + Sync,
{
    type Out = Vec<u8>;

    fn reply(
        &self,
        order: &Order,
        _slots: &(Out<P, Audit, (), E>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Result<Vec<u8>, HandlerResult>> + Send {
        ready(Ok(order.id.to_be_bytes().to_vec()))
    }
}

struct PageGateway;

impl<P, E, S> SlotsBatchReply<Order, (Out<P, Audit, (), E>,), S> for PageGateway
where
    P: Publisher,
    E: Send + Sync,
    S: Send + Sync,
{
    type Out = Confirmation;

    fn reply(
        &self,
        batch: &[Order],
        _slots: &(Out<P, Audit, (), E>,),
        _ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, HandlerResult>> + Send {
        ready(Ok(batch
            .iter()
            .map(|o| Confirmation { id: o.id })
            .collect()))
    }
}

// -------------------------------------------------------------------------------------------
// The parity mounts: one `include` per form token in `forms.rs`.

/// Every form token in `forms.rs`, mounted through its value constructor on one router.
fn every_form() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        // forms::Subscribing
        .include(subscriber("orders", Plain))
        // forms::RawSubscribing
        .include(raw("frames", Bytes))
        // forms::Batch
        .include(batch("orders", Page))
        // forms::RawBatch
        .include(raw_batch("frames", Frames))
        // forms::BatchWithHeaders
        .include(batch_with_headers("orders", HeaderedPage))
        // forms::Publishing
        .include(replying("orders", Confirm).to("confirmations"))
        .publisher(crate::runtime::TypedPublisher::new(MemoryPublish))
        // forms::RawReply
        .include(raw_replying("orders", Echo).to("echoes"))
        .publisher(MemoryPublish)
        // forms::BatchPublishing
        .include(batch_replying("orders", ConfirmPage).to("confirmations"))
        .publisher(crate::runtime::TypedPublisher::new(MemoryPublish))
        // forms::Seek
        .include(with_seek::<Order, MemorySeeker, _, _>(
            MemorySource::new("orders"),
            Skipper,
        ))
        // forms::BatchSeek
        .include(batch_with_seek::<Order, MemorySeeker, _, _>(
            MemorySource::new("orders"),
            PageSkipper,
        ))
        // forms::Out
        .include(with_slots::<Order, (Audit,), _, _>("orders", Mirror))
        .out(Audit, MemoryPublish)
        .mount()
        // forms::BatchOut
        .include(batch_with_slots::<Order, (Audit,), _, _>(
            "orders", PageMirror,
        ))
        .out(Audit, MemoryPublish)
        .mount()
        // forms::PublishingOut
        .include(
            replying_with_slots::<Order, (Audit,), _, _>("orders", Gateway).to("confirmations"),
        )
        .out(Audit, MemoryPublish)
        .mount()
        // forms::RawReplyOut
        .include(
            raw_replying_with_slots::<Order, (Audit,), _, _>("orders", RawGateway).to("echoes"),
        )
        .out(Audit, MemoryPublish)
        .mount()
        // forms::BatchPublishingOut
        .include(
            batch_replying_with_slots::<Order, (Audit,), _, _>("orders", PageGateway)
                .to("confirmations"),
        )
        .out(Audit, MemoryPublish)
        .mount()
}

/// The codec override rides every decoded form family, eager and builder-committed alike.
fn codec_axis() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .include(
            subscriber("orders", Plain)
                .codec(JsonCodec)
                .workers(nonzero!(2)),
        )
        .include(
            replying("orders", Confirm)
                .to("confirmations")
                .codec(JsonCodec),
        )
        .publisher(crate::runtime::TypedPublisher::new(MemoryPublish))
        .include(batch("orders", Page).codec(JsonCodec))
        .include(batch_with_headers("orders", HeaderedPage).codec(JsonCodec))
        .include(
            batch_replying("orders", ConfirmPage)
                .to("confirmations")
                .codec(JsonCodec),
        )
        .publisher(crate::runtime::TypedPublisher::new(MemoryPublish))
        .include(with_slots::<Order, (Audit,), _, _>("orders", Mirror).codec(JsonCodec))
        .out(Audit, MemoryPublish)
        .mount()
}

/// The `_in` constructors mount bodies pinned to one concrete app state.
fn state_axis() -> impl RouterDef<MemoryBroker, Ledger> {
    Router::<MemoryBroker>::new()
        .include(subscriber_in("orders", Pinned))
        .include(batch_in("orders", PinnedPage))
        .include(replying_in("orders", PinnedConfirm).to("confirmations"))
        .publisher(crate::runtime::TypedPublisher::new(MemoryPublish))
}

#[test]
fn every_form_token_mounts_on_the_value_path() {
    // Building the routers is the assertion: a missing value spelling fails to compile.
    let _ = every_form();
    let _ = codec_axis();
    let _ = state_axis();
}

/// The scope surface mounts the same definitions: one representative per mount family.
#[test]
fn the_scope_surface_mounts_the_same_definitions() {
    use crate::runtime::{AppInfo, RustStream};

    let app =
        RustStream::new(AppInfo::new("parity", "0.0.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Plain).codec(JsonCodec));
            b.include(raw("frames", Bytes));
            b.include(batch("orders", Page));
            b.include(batch_with_headers("orders", HeaderedPage));
            b.include(replying("orders", Confirm).to("confirmations"));
            b.include(raw_replying("orders", Echo).to("echoes"));
            b.include(batch_replying("orders", ConfirmPage).to("confirmations"));
            b.include(with_seek::<Order, MemorySeeker, _, _>(
                MemorySource::new("orders"),
                Skipper,
            ));
            b.include(with_slots::<Order, (Audit,), _, _>("orders", Mirror))
                .out(Audit, MemoryPublish)
                .mount();
            b.include(
                replying_with_slots::<Order, (Audit,), _, _>("orders", Gateway).to("confirmations"),
            )
            .out(Audit, MemoryPublish)
            .mount();
        });
    let _ = app;
}
