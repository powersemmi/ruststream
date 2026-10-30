//! The out-slot axis of the parity matrix: the slot dictionaries, the bodies that publish through
//! them, and every slot spelling mounted on a router.

use super::*;

pub(super) struct Analytics;

impl OutSlot for Analytics {
    const NAME: &'static str = "Analytics";
    type Destination = Reads;
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct Event {
    id: u64,
}

impl OutgoingDestination for Event {
    type Form = CallerName;
}

impl MessageHeaders for Event {
    type Contract = NoHeaders;
}

impl PublishedThrough<Analytics> for Event {}

// A serialized out type is a first-class dictionary member: it declares its destination and
// membership like any model, and its bytes leave through the same typed builder, uncoded.
impl OutgoingDestination for Export {
    type Form = CallerName;
}

impl MessageHeaders for Export {
    type Contract = NoHeaders;
}

impl PublishedThrough<Analytics> for Export {}

/// Publishes a serialized dictionary member's own bytes through the slot.
pub(super) struct RawMirror;

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
pub(super) struct Mirror;

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
pub(super) struct Ledger;

impl OutSlot for Ledger {
    const NAME: &'static str = "Ledger";
    type Destination = Reads;
}

impl PublishedThrough<Ledger> for Event {}

/// Two slots, each with its own publish path.
pub(super) struct PairMirror;

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

/// The slot transform the mounts below compose: it stamps what leaves the slot it rides. It reads
/// no context, so one impl serves every position.
pub(super) struct Trace;

impl<K: ContextKind, Options> PublishTransform<K, Options> for Trace {
    type Destination = Reads;

    fn apply(&self, out: &mut Outgoing<'_>, _options: &mut Option<Options>, _cx: &K::View<'_>) {
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

pub(super) struct Gateway;

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
pub(super) struct BatchGatewayInContext;

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

pub(super) struct RawGateway;

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
fn slot_axes() -> impl RouterDef<Bus> {
    Router::<Bus>::new()
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
