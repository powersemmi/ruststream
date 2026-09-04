//! The completion artifact of the manual path: every input spelling, reply shape, injection
//! set and chain axis, mounted on both surfaces. A missing spelling fails this module's build.
//!
//! The two runtime suites here keep `start()` rather than the `TestApp` harness: what they pin is
//! that every spelling MOUNTS, so they must build wherever this module does - and the harness
//! lives behind the `testing` feature this module's gate deliberately leaves out.

use std::future::{Future, ready};

use serde::{Deserialize, Serialize};

use crate::codec::JsonCodec;
use crate::memory::{
    MemoryBatchContext, MemoryBroker, MemoryContext, MemoryPosition, MemoryPublish,
    MemoryPublisher, MemorySource, Position, SeekHandle,
};
use crate::nonzero;
use crate::runtime::{
    Context, Deserialized, Handle, HandlerOutcome, Input, Message, MessageWire, OutEntry,
    OutTransform, Outgoing, Outs, PublishContext, PublishTransform, Reply, ReplyShape, Router,
    RouterDef, Serialized, SerializedReply, SerializedWire, Slot, SoloDeserialized,
    SubscriberSettings, Verdict, for_batch, subscriber,
};
use crate::{Buffered, Publisher, Seeker};

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

impl<A> Handle<Order, (), Outs<(A,)>> for RawMirror
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(A,)>,
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

/// The publish path never reaches the signature: this one body is mounted below both bare and
/// under a slot `.transform(..)`, and would be mounted the same under an app-wide
/// `publish_layer`. The pipeline the mount composes is an [`OutEntry`] projection, so the
/// `where` clause that compiles against one compiles against the other.
struct Mirror;

impl<A> Handle<Order, (), Outs<(A,)>> for Mirror
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(A,)>,
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

/// The second slot of the two-slot spelling: what a chain names when only one of the pair takes
/// a transform.
struct Ledger;

impl crate::runtime::OutSlot for Ledger {
    const NAME: &'static str = "Ledger";
}

impl crate::runtime::PublishedThrough<Ledger> for Event {}

/// Two slots, each with its own publish path.
struct PairMirror;

impl<A, L> Handle<Order, (), Outs<(A, L)>> for PairMirror
where
    A: OutEntry<Analytics, Wire: Publisher>,
    L: OutEntry<Ledger, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(A, L)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(Analytics)
            .message(&Event { id: order.id })
            .to("order-events")
            .publish()
            .await
            .is_err()
            || outs
                .get(Ledger)
                .message(&Event { id: order.id })
                .to("order-ledger")
                .publish()
                .await
                .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// The slot transform the mounts below compose: it stamps what leaves the slot it rides.
struct Trace;

impl OutTransform for Trace {
    fn apply(&self, out: &mut Outgoing<'_>) {
        out.headers_mut().insert("x-trace", b"1".to_vec());
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

struct BatchMirror;

impl<A> Handle<[Order], (), Outs<(A,)>> for BatchMirror
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    fn handle(
        &self,
        batch: &[Order],
        outs: &Outs<(A,)>,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = (batch.len(), outs);
        ready(Ok(()))
    }
}
/// Every plain input spelling mounts through the one constructor on a router.
fn eager_axes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .include(subscriber("orders", Audit).workers(nonzero!(4)).build())
        .include(subscriber(MemorySource::new("orders"), Audit).build())
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
fn reply_axes() -> impl RouterDef<MemoryBroker> {
    use crate::codec::JsonCodec;

    Router::<MemoryBroker>::new()
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

impl<Cx> PublishTransform<Cx> for StampReply {
    fn apply(&self, out: &mut Outgoing<'_>, _cx: &PublishContext<'_, Cx>) {
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
fn order_free_reply_axes() -> impl RouterDef<MemoryBroker> {
    use crate::codec::JsonCodec;
    use crate::runtime::{FailurePolicies, SubscriberSettings};

    Router::<MemoryBroker>::new()
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
    use crate::runtime::SubscriberSettings;
    use crate::{Name, Unnamed};

    let _ = Router::<MemoryBroker>::new()
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

struct Gateway;

impl<A> Handle<Order, Confirmation, Outs<(A,)>> for Gateway
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    fn handle(
        &self,
        order: &Order,
        outs: &Outs<(A,)>,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        let _ = outs;
        ready(Ok(Confirmation { id: order.id }))
    }
}

struct BatchGateway;

impl<A> Handle<[Order], Vec<Confirmation>, Outs<(A,)>> for BatchGateway
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    fn handle(
        &self,
        batch: &[Order],
        outs: &Outs<(A,)>,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        let _ = outs;
        ready(Ok(batch
            .iter()
            .map(|o| Confirmation { id: o.id })
            .collect()))
    }
}

/// The three-axis batch cell: an answer, an injections arena and the broker's batch context in
/// one signature.
struct BatchGatewayInContext;

impl<A> Handle<[Order], Vec<Confirmation>, Outs<(A,)>, MemoryBatchContext> for BatchGatewayInContext
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    fn handle(
        &self,
        batch: &[Order],
        outs: &Outs<(A,)>,
        ctx: &mut Context<'_, MemoryBatchContext>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        let _ = (outs, ctx.context(SeekHandle));
        ready(Ok(batch
            .iter()
            .map(|order| Confirmation { id: order.id })
            .collect()))
    }
}

/// The same arena on a settling batch, so the context axis is open with and without a reply.
struct BatchMirrorInContext;

impl<A> Handle<[Order], (), Outs<(A,)>, MemoryBatchContext> for BatchMirrorInContext
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    fn handle(
        &self,
        batch: &[Order],
        outs: &Outs<(A,)>,
        ctx: &mut Context<'_, MemoryBatchContext>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = (batch.len(), outs, ctx.context(SeekHandle));
        ready(Ok(()))
    }
}

struct RawGateway;

impl<A> Handle<Order, Export, Outs<(A,)>> for RawGateway
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    fn handle(
        &self,
        order: &Order,
        outs: &Outs<(A,)>,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Export, HandlerOutcome>> {
        let _ = outs;
        ready(Ok(Export(order.id.to_be_bytes().to_vec())))
    }
}

/// The slot arena mounts on both families, bound at the include site - the generic and the
/// concrete-typed spelling alike.
fn slot_axes() -> impl RouterDef<MemoryBroker> {
    Router::<MemoryBroker>::new()
        .include(subscriber("orders", Mirror).build())
        .out(Analytics, MemoryPublish)
        .build()
        .include(subscriber("orders", Mirror).build())
        .out(Analytics, MemoryPublish)
        .transform(Trace)
        .build()
        // Two slots, the transform on one of them: it rides the `.out(..)` before it.
        .include(subscriber("orders", PairMirror).build())
        .out(Analytics, MemoryPublish)
        .transform(Trace)
        .out(Ledger, MemoryPublish)
        .build()
        .include(subscriber("orders", PairMirror).build())
        .out(Ledger, MemoryPublish)
        .out(Analytics, MemoryPublish)
        .transform(Trace)
        .build()
        .include(subscriber("orders", RawMirror).build())
        .out(Analytics, MemoryPublish)
        .build()
        .include(subscriber("orders", PinnedMirror).build())
        .out(Analytics, MemoryPublish)
        .build()
        // A slot-carrying batch names its size like any other batch.
        .include(subscriber("orders", BatchMirror).batch(nonzero!(8)).build())
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
                .build(),
        )
        .out(Reply, MemoryPublish)
        .codec(JsonCodec)
        .out(Analytics, MemoryPublish)
        .build()
        // A transform on each position of the same registration: the step applies to what the
        // chain named before it, whether that was the reply or a slot.
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
        .build()
        // A batch that both replies and fans out through the arena.
        .include(
            subscriber("orders", BatchGateway)
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
                .build(),
        )
        .out(Reply, MemoryPublish)
        .out(Analytics, MemoryPublish)
        .build()
        .include(
            subscriber("orders", BatchGatewayInContext)
                .reply()
                .to("confirmations")
                .batch(nonzero!(8))
                .build(),
        )
        .out(Analytics, MemoryPublish)
        .build()
        .include(
            subscriber("orders", BatchMirrorInContext)
                .batch(nonzero!(8))
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
/// self-deserializing solo and batch inputs, the serialized replies (attached and defaulted,
/// with and without slots) and the serialized dictionary out - and one subscriber dispatches
/// end to end.
#[tokio::test]
async fn a_subscriber_dispatches_end_to_end() {
    use crate::OutgoingMessage;
    use crate::Publisher;
    use crate::runtime::{AppInfo, RustStream};

    let app = RustStream::new(AppInfo::new("handle-parity", "0.0.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("orders", Audit).build());
            b.include(subscriber("orders", SettleBatch).batch(nonzero!(4)).build());
            b.include(subscriber("frames", Inspect).build());
            b.include(subscriber("frames", Frames).batch(nonzero!(4)).build());
            b.include(subscriber("orders", Echo).reply().to("echoes").build())
                .out(Reply, MemoryPublish);
            b.include(subscriber("frames", RawEcho).reply().to("echoes").build());
            b.include(
                subscriber("orders", ConfirmBatchesInContext)
                    .reply()
                    .to("confirmations")
                    .batch(nonzero!(4))
                    .build(),
            );
            b.include(
                subscriber("orders", BatchGatewayInContext)
                    .reply()
                    .to("confirmations")
                    .batch(nonzero!(4))
                    .build(),
            )
            .out(Analytics, MemoryPublish)
            .build();
            b.include(
                subscriber("orders", Confirm)
                    .reply()
                    .to("confirmations")
                    .build(),
            )
            .out(Reply, MemoryPublish)
            .codec(JsonCodec);
            b.include(subscriber("orders", RawMirror).build())
                .out(Analytics, MemoryPublish)
                .build();
            b.include(subscriber("orders", Mirror).build())
                .out(Analytics, MemoryPublish)
                .transform(Trace)
                .build();
            b.include(subscriber("orders", PairMirror).build())
                .out(Analytics, MemoryPublish)
                .transform(Trace)
                .out(Ledger, MemoryPublish)
                .build();
            b.include(
                subscriber("orders", RawGateway)
                    .reply()
                    .to("echoes")
                    .build(),
            )
            .out(Reply, MemoryPublish)
            .out(Analytics, MemoryPublish)
            .build();
            b.include(
                subscriber("orders", Gateway)
                    .reply()
                    .to("confirmations")
                    .build(),
            )
            .out(Reply, MemoryPublish)
            .transform(StampReply)
            .out(Analytics, MemoryPublish)
            .transform(Trace)
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
