//! The completion artifact of the manual path: every input spelling, reply shape, injection
//! set and chain axis, mounted on both surfaces. A missing spelling fails this module's build.
//!
//! The two runtime suites here keep `start()` rather than the `TestApp` harness: what they pin is
//! that every spelling MOUNTS, so they must build wherever this module does - and the harness
//! lives behind the `testing` feature this module's gate deliberately leaves out.

use std::convert::Infallible;
use std::future::ready;

use serde::{Deserialize, Serialize};

use crate::BytesMut;
use crate::PublishedThrough;
use crate::nonzero;
use crate::{
    Buffered, CallerName, FixedName, MessageHeaders, NoHeaders, OutgoingDestination, Publisher,
    Seeker,
};
use crate::{
    Context, ContextKind, Deserialized, Handle, HandlerOutcome, Input, Message, MessageWire,
    OutEntry, OutSlot, Outgoing, Outs, PublishTransform, Reads, Reply, ReplyShape, Router,
    RouterDef, Serialized, SerializedReply, SerializedWire, Slot, SoloDeserialized,
    SubscriberSettings, Verdict, WireBytes, for_batch, subscriber,
};
use ruststream::memory::{
    MemoryBatchContext, MemoryBroker, MemoryContext, MemoryPosition, MemoryPublish,
    MemoryPublisher, Position, Retaining, Retention, SeekHandle,
};
use ruststream_runtime::codec::JsonCodec;

mod slots;

use slots::{
    Analytics, BatchGatewayInContext, Gateway, Ledger, Mirror, PairMirror, RawGateway, RawMirror,
    Trace,
};

/// The parity matrix includes the seek axis (a start position, the context keys), so the whole
/// file mounts on a retaining broker; every other spelling reads the same on either mode.
type Bus = MemoryBroker<Retaining>;

/// The broker behind [`Bus`], with a window wider than anything these tests publish.
fn bus() -> Bus {
    MemoryBroker::retaining(Retention::Messages(nonzero!(32)))
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

/// The self-deserializing input lane, spelled by hand (the derive is `macros`-crate sugar over
/// exactly these impls).
struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = Infallible;

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
    type Error = Infallible;

    fn wire_bytes(&self, _buf: &mut BytesMut) -> Result<WireBytes<'_>, Infallible> {
        Ok(WireBytes::Own(&self.0))
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

impl OutgoingDestination for Confirmation {
    type Form = CallerName;
}

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

struct SettleBatch;

impl Handle<[Order]> for SettleBatch {
    fn handle(
        &self,
        batch: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = batch.len();
        ready(Ok(()))
    }
}

struct Frames;

impl<'p> Handle<[Frame<'p>]> for Frames {
    fn handle(
        &self,
        batch: &[Frame<'p>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = batch.len();
        ready(Ok(()))
    }
}

struct HeaderedBatch;

impl Handle<[Message<Meta, Order>]> for HeaderedBatch {
    fn handle(
        &self,
        batch: &[Message<Meta, Order>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = batch.len();
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

impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "receipts";
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

struct ConfirmBatches;

impl Handle<[Order], Vec<Confirmation>> for ConfirmBatches {
    fn handle(
        &self,
        batch: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        ready(Ok(batch
            .iter()
            .map(|order| Confirmation { id: order.id })
            .collect()))
    }
}

/// A batch that answers and reads the broker's subscription-scoped context in one body: the
/// reply axis and the context axis are independent, so naming one must not close the other.
struct ConfirmBatchesInContext;

impl Handle<[Order], Vec<Confirmation>, (), MemoryBatchContext> for ConfirmBatchesInContext {
    async fn handle(
        &self,
        batch: &[Order],
        _outs: &(),
        ctx: &mut Context<'_, MemoryBatchContext>,
    ) -> Result<Vec<Confirmation>, Vec<HandlerOutcome>> {
        if ctx
            .context(SeekHandle)
            .seek(MemoryPosition::end())
            .await
            .is_err()
        {
            // The per-element failure side of the batch reply verdict, on the context axis.
            return Err(batch.iter().map(|_| HandlerOutcome::retry()).collect());
        }
        Ok(batch
            .iter()
            .map(|order| Confirmation { id: order.id })
            .collect())
    }
}

/// The same cell on the pair batch: typed element headers and a broker batch context together.
struct HeaderedBatchInContext;

impl Handle<[Message<Meta, Order>], Vec<Confirmation>, (), MemoryBatchContext>
    for HeaderedBatchInContext
{
    fn handle(
        &self,
        batch: &[Message<Meta, Order>],
        _outs: &(),
        ctx: &mut Context<'_, MemoryBatchContext>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        let _ = ctx.context(SeekHandle);
        ready(Ok(batch
            .iter()
            .map(|element| Confirmation {
                id: element.body.id,
            })
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

/// Every plain input spelling mounts through the one constructor on a router.
fn eager_axes() -> impl RouterDef<Bus> {
    Router::<Bus>::new()
        .include(subscriber("orders", Audit).workers(nonzero!(4)).build())
        .include(subscriber("frames", Inspect).build())
        .include(subscriber("orders", Expedite).build())
        // Every batch spelling names its size, and none of them mounts without it: the decoded
        // one, the self-deserializing one and the paired one.
        .include(subscriber("orders", SettleBatch).batch(nonzero!(8)).build())
        .include(subscriber("frames", Frames).batch(nonzero!(8)).build())
        .include(
            subscriber("orders", HeaderedBatch)
                .batch(nonzero!(8))
                .build(),
        )
        // The size composes with a start position, in either order.
        .include(
            subscriber("orders", SettleBatch)
                .batch(nonzero!(4))
                .start_at(MemoryPosition::start())
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
/// the batch form, and typed reply headers.
fn reply_axes() -> impl RouterDef<Bus> {
    Router::<Bus>::new()
        .include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .build(),
        )
        .build()
        .include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .build(),
        )
        .out(Reply, MemoryPublish)
        .build()
        // the wiring steps after `.out(Reply, ..)`: a named codec and a static transform
        .include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .build(),
        )
        .out(Reply, MemoryPublish)
        .codec(JsonCodec)
        .transform(StampReply)
        .build()
        .include(subscriber("orders", IssueReceipt).reply().build())
        .build()
        .include(subscriber("orders", Echo).reply().to("echoes").build())
        .out(Reply, MemoryPublish)
        .build()
        .include(subscriber("frames", RawEcho).reply().to("echoes").build())
        .build()
        // A batch that replies names its size like any other batch.
        .include(
            subscriber("orders", ConfirmBatches)
                .reply()
                .to("confirmations")
                .batch(nonzero!(8))
                .build(),
        )
        .build()
        // the batch wiring: one broker transaction per batch, with a batch-only transform
        .include(
            subscriber("orders", ConfirmBatches)
                .reply()
                .to("confirmations")
                .batch(nonzero!(8))
                .build(),
        )
        .out(Reply, MemoryPublish)
        .batch_transform(for_batch(StampReply))
        .transactional()
        .build()
        .include(
            subscriber("orders", ConfirmWithMeta)
                .reply()
                .to("confirmations")
                .build(),
        )
        .build()
        .include(
            subscriber("orders", ConfirmBatchesInContext)
                .reply()
                .to("confirmations")
                .batch(nonzero!(8))
                .build(),
        )
        .build()
        .include(
            subscriber("orders", HeaderedBatchInContext)
                .reply()
                .to("confirmations")
                .batch(nonzero!(8))
                .build(),
        )
        .out(Reply, MemoryPublish)
        .build()
        // The client-side buffer turns any subscriber into a batch source, so the reply and the
        // broker batch context must survive that wrapping too.
        .include(
            subscriber("orders", ConfirmBatchesInContext)
                .reply()
                .to("confirmations")
                .map_source(Buffered::new)
                .batch(nonzero!(4))
                .build(),
        )
        .build()
}

/// A publish transform the reply chain composes on: it only has to exist for the mount to
/// type-check, so it stamps one header and leaves the payload alone.
#[derive(Clone, Copy)]
struct StampReply;

impl<K: ContextKind, Options> PublishTransform<K, Options> for StampReply {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
        out.headers_mut().insert("x-stamped", "1");
    }
}

#[test]
fn every_reply_spelling_mounts() {
    let _ = reply_axes();
}

/// The definition chain has no order to remember: `.reply()` and `.to(..)` close no other step,
/// so the value steps and the declarative settings mount the same registration whichever side of
/// them names it. Both spellings of each pair are built here; a step reachable on one side only
/// would fail this module's build.
fn order_free_reply_axes() -> impl RouterDef<Bus> {
    use crate::FailurePolicies;

    Router::<Bus>::new()
        // a batch setting, before and after the reply declaration
        .include(
            subscriber("orders", ConfirmBatches)
                .reply()
                .to("confirmations")
                .batch(nonzero!(8))
                .build(),
        )
        .out(Reply, MemoryPublish)
        .codec(JsonCodec)
        .build()
        .include(
            subscriber("orders", ConfirmBatches)
                .batch(nonzero!(8))
                .reply()
                .to("confirmations")
                .build(),
        )
        .out(Reply, MemoryPublish)
        .codec(JsonCodec)
        .build()
        // the documentation steps, before and after the reply declaration
        .include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .describe("confirms an order")
                .build(),
        )
        .out(Reply, MemoryPublish)
        .transform(StampReply)
        .build()
        .include(
            subscriber("orders", Confirm)
                .describe("confirms an order")
                .reply()
                .to("confirmations")
                .build(),
        )
        .out(Reply, MemoryPublish)
        .transform(StampReply)
        .build()
        .include(
            subscriber("orders", Echo)
                .reply()
                .to("echoes")
                .undocumented()
                .build(),
        )
        .out(Reply, MemoryPublish)
        .build()
        .include(
            subscriber("orders", Echo)
                .undocumented()
                .reply()
                .to("echoes")
                .build(),
        )
        .out(Reply, MemoryPublish)
        .build()
        // the declarative settings, before the reply declaration, after it, and after the seal
        .include(
            subscriber("orders", Confirm)
                .workers(nonzero!(2))
                .reply()
                .to("confirmations")
                .build(),
        )
        .out(Reply, MemoryPublish)
        .build()
        .include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .workers(nonzero!(2))
                .on_failure(FailurePolicies::default())
                .build(),
        )
        .out(Reply, MemoryPublish)
        .build()
        .include(
            subscriber("orders", Confirm)
                .reply()
                .to("confirmations")
                .build()
                .workers(nonzero!(2)),
        )
        .out(Reply, MemoryPublish)
        .build()
}

#[test]
fn the_reply_chain_takes_its_steps_in_any_order() {
    let _ = order_free_reply_axes();
}

/// The unnamed source is constructed by the same `name(..)` step whichever side of the reply
/// declaration names it, and a source transform still applies through it.
#[test]
fn the_source_steps_reach_through_the_reply_declaration() {
    use crate::SubscriberSettings;
    use crate::{Name, Unnamed};

    let _ = Router::<Bus>::new()
        .include(
            subscriber(Unnamed::<Name>::new(), Confirm)
                .reply()
                .to("confirmations")
                .name("orders")
                .map_source(|source| source)
                .build(),
        )
        .out(Reply, MemoryPublish)
        .build()
        .include(
            subscriber(Unnamed::<Name>::new(), Confirm)
                .name("orders")
                .reply()
                .to("confirmations")
                .build(),
        )
        .out(Reply, MemoryPublish)
        .build();
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
    use crate::{AppInfo, RustStream};

    let app = RustStream::new(AppInfo::new("handle-seek", "0.0.0")).with_broker(bus(), |b| {
        b.after_startup(MemoryPublish, async move |publisher| {
            publisher
                .publish(
                    OutgoingMessage::new("orders", br#"{"id":7}"#.as_slice()),
                    None,
                )
                .await
        });
        b.include(subscriber("orders", Replayer).build())
    });
    let running = app.start().await.expect("the app starts");
    running.shutdown().await.expect("the app stops cleanly");
}

/// The scope surface mounts the same definitions - the new lanes included: the
/// self-deserializing solo and batch inputs, the serialized replies (attached and defaulted,
/// with and without slots) and the serialized dictionary out - and one subscriber dispatches
/// end to end.
#[tokio::test]
async fn a_subscriber_dispatches_end_to_end() {
    use crate::OutgoingMessage;
    use crate::Publisher;
    use crate::{AppInfo, RustStream};

    let app = RustStream::new(AppInfo::new("handle-parity", "0.0.0")).with_broker(bus(), |b| {
        b.after_startup(MemoryPublish, async move |publisher| {
            publisher
                .publish(
                    OutgoingMessage::new("orders", br#"{"id":7}"#.as_slice()),
                    None,
                )
                .await
        });
        b.include(subscriber("orders", Audit).build())
            .include(subscriber("orders", SettleBatch).batch(nonzero!(4)).build())
            .include(subscriber("frames", Inspect).build())
            .include(subscriber("frames", Frames).batch(nonzero!(4)).build())
            .include(subscriber("orders", Echo).reply().to("echoes").build())
            .out(Reply, MemoryPublish)
            .include(subscriber("frames", RawEcho).reply().to("echoes").build())
            .include(
                subscriber("orders", ConfirmBatchesInContext)
                    .reply()
                    .to("confirmations")
                    .batch(nonzero!(4))
                    .build(),
            )
            .include(
                subscriber("orders", BatchGatewayInContext)
                    .reply()
                    .to("confirmations")
                    .batch(nonzero!(4))
                    .build(),
            )
            .out(Analytics, MemoryPublish)
            .include(
                subscriber("orders", Confirm)
                    .reply()
                    .to("confirmations")
                    .build(),
            )
            .out(Reply, MemoryPublish)
            .codec(JsonCodec)
            .include(subscriber("orders", RawMirror).build())
            .out(Analytics, MemoryPublish)
            .include(subscriber("orders", Mirror).build())
            .out(Analytics, MemoryPublish)
            .transform(Trace)
            .include(subscriber("orders", PairMirror).build())
            .out(Analytics, MemoryPublish)
            .transform(Trace)
            .out(Ledger, MemoryPublish)
            .include(
                subscriber("orders", RawGateway)
                    .reply()
                    .to("echoes")
                    .build(),
            )
            .out(Reply, MemoryPublish)
            .out(Analytics, MemoryPublish)
            .include(
                subscriber("orders", Gateway)
                    .reply()
                    .to("confirmations")
                    .build(),
            )
            .out(Reply, MemoryPublish)
            .transform(StampReply)
            .out(Analytics, MemoryPublish)
            .transform(Trace)
    });
    let running = app.start().await.expect("the app starts");
    running.shutdown().await.expect("the app stops cleanly");
}
