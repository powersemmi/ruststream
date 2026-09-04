//! The pair cells of the manual matrix, end to end: a `Message<H, P>` input reaching a body that
//! fans out through an injections arena, one that answers with a reply, and one that does both -
//! at the single-message shape and the batch shape.
//!
//! The header contract is decoded in the same stage as the payload, so what proves it arrived is
//! what leaves the handler: every message the bodies below publish carries the tenant read off
//! the delivery's headers.
#![cfg(all(feature = "memory", feature = "json", feature = "testing"))]

use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct Meta {
    tenant: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
struct Confirmation {
    id: u64,
    tenant: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
struct Event {
    id: u64,
    tenant: String,
}

// The `Outgoing` derive by hand: no declared name, so each call site names the destination.
impl OutgoingDestination for Event {
    type Form = CallerName;
}

impl MessageHeaders for Event {
    type Contract = NoHeaders;
}

// `#[derive(OutSlot)]` plus `#[publishes(Event)]` by hand.
struct Analytics;

impl OutSlot for Analytics {
    const NAME: &'static str = "Analytics";
}

impl PublishedThrough<Analytics> for Event {}

/// The event a delivery turns into: the id off the payload, the tenant off the typed headers.
fn event_of(msg: &Message<Meta, Order>) -> Event {
    Event {
        id: msg.body.id,
        tenant: msg.headers.tenant.clone(),
    }
}

/// The reply a delivery turns into, from the same two halves.
fn confirmation_of(msg: &Message<Meta, Order>) -> Confirmation {
    Confirmation {
        id: msg.body.id,
        tenant: msg.headers.tenant.clone(),
    }
}

/// The arena a single-slot body declares: one entry, described by its marker and the capability
/// the bodies need, never by the mount site's wiring.
type AnalyticsArena<A> = Outs<(A,)>;
/// A pair input fanned out through a slot.
struct MirrorPair;

impl<A> Handle<Message<Meta, Order>, (), AnalyticsArena<A>> for MirrorPair
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    async fn handle(
        &self,
        msg: &Message<Meta, Order>,
        outs: &AnalyticsArena<A>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(Analytics)
            .message(&event_of(msg))
            .to("matrix.events")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

/// A pair input answering with a reply.
struct ConfirmPair;

impl Handle<Message<Meta, Order>, Confirmation> for ConfirmPair {
    fn handle(
        &self,
        msg: &Message<Meta, Order>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(confirmation_of(msg)))
    }
}

/// A pair input answering with a reply and fanning out through a slot in the same signature.
struct GatewayPair;

impl<A> Handle<Message<Meta, Order>, Confirmation, AnalyticsArena<A>> for GatewayPair
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    async fn handle(
        &self,
        msg: &Message<Meta, Order>,
        outs: &AnalyticsArena<A>,
        _ctx: &mut Context<'_>,
    ) -> Result<Confirmation, HandlerOutcome> {
        if outs
            .get(Analytics)
            .message(&event_of(msg))
            .to("matrix.gateway-events")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(confirmation_of(msg))
    }
}
/// A batch of pairs fanned out through a slot.
struct MirrorPairBatch;

impl<A> Handle<[Message<Meta, Order>], (), AnalyticsArena<A>> for MirrorPairBatch
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    async fn handle(
        &self,
        batch: &[Message<Meta, Order>],
        outs: &AnalyticsArena<A>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), Vec<HandlerOutcome>> {
        for msg in batch {
            if outs
                .get(Analytics)
                .message(&event_of(msg))
                .to("matrix.batch-events")
                .publish()
                .await
                .is_err()
            {
                return Err(batch.iter().map(|_| HandlerOutcome::retry()).collect());
            }
        }
        Ok(())
    }
}

/// A batch of pairs answering with one reply per element.
struct ConfirmPairBatch;

impl Handle<[Message<Meta, Order>], Vec<Confirmation>> for ConfirmPairBatch {
    fn handle(
        &self,
        batch: &[Message<Meta, Order>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        ready(Ok(batch.iter().map(confirmation_of).collect()))
    }
}

/// A batch of pairs answering with one reply per element and fanning out through a slot.
struct GatewayPairBatch;

impl<A> Handle<[Message<Meta, Order>], Vec<Confirmation>, AnalyticsArena<A>> for GatewayPairBatch
where
    A: OutEntry<Analytics, Wire: Publisher>,
{
    async fn handle(
        &self,
        batch: &[Message<Meta, Order>],
        outs: &AnalyticsArena<A>,
        _ctx: &mut Context<'_>,
    ) -> Result<Vec<Confirmation>, Vec<HandlerOutcome>> {
        for msg in batch {
            if outs
                .get(Analytics)
                .message(&event_of(msg))
                .to("matrix.gateway-batch-events")
                .publish()
                .await
                .is_err()
            {
                return Err(batch.iter().map(|_| HandlerOutcome::retry()).collect());
            }
        }
        Ok(batch.iter().map(confirmation_of).collect())
    }
}
/// The single-message pair cells: the arena, the reply, and both at once. Each publishes what it
/// read off the delivery's headers, so the assertions prove the contract arrived.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pair_input_reaches_every_single_message_cell() {
    let app =
        RustStream::new(AppInfo::new("matrix", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("matrix.mirror", MirrorPair).build())
                .out(Analytics, Publish)
                .build();
            b.include(
                subscriber("matrix.confirm", ConfirmPair)
                    .reply()
                    .to("matrix.confirmations")
                    .build(),
            );
            b.include(
                subscriber("matrix.gateway", GatewayPair)
                    .reply()
                    .to("matrix.gateway-confirmations")
                    .build(),
            )
            .out(Analytics, Publish)
            .build();
        });
    let tb = TestApp::start(app).await.expect("harness start");

    let meta = Meta {
        tenant: "acme".to_owned(),
    };
    for subject in ["matrix.mirror", "matrix.confirm", "matrix.gateway"] {
        tb.broker::<MemoryBroker>()
            .publish_with_headers(subject, &Order { id: 7 }, &meta)
            .await
            .expect("publish");
    }
    tb.settle().await.expect("settle");

    let expected_event = Event {
        id: 7,
        tenant: "acme".to_owned(),
    };
    let expected_reply = Confirmation {
        id: 7,
        tenant: "acme".to_owned(),
    };
    let broker = tb.broker::<MemoryBroker>();
    broker
        .published::<Event>("matrix.events")
        .assert_called_once()
        .with(&expected_event);
    broker
        .published::<Confirmation>("matrix.confirmations")
        .assert_called_once()
        .with(&expected_reply);
    broker
        .published::<Event>("matrix.gateway-events")
        .assert_called_once()
        .with(&expected_event);
    broker
        .published::<Confirmation>("matrix.gateway-confirmations")
        .assert_called_once()
        .with(&expected_reply);
    // The two slot bodies share the marker, so the arena view holds both of their publishes.
    assert_eq!(tb.out::<Analytics>().messages().len(), 2);
}

/// The batch pair cells: one header contract per element, all the way to the replies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pair_input_reaches_every_batch_cell() {
    let app = RustStream::new(AppInfo::new("matrix-batch", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(
                subscriber("matrix.mirror-batch", MirrorPairBatch)
                    .batch(nonzero!(8))
                    .build(),
            )
            .out(Analytics, Publish)
            .build();
            b.include(
                subscriber("matrix.confirm-batch", ConfirmPairBatch)
                    .reply()
                    .to("matrix.batch-confirmations")
                    .batch(nonzero!(8))
                    .build(),
            );
            b.include(
                subscriber("matrix.gateway-batch", GatewayPairBatch)
                    .reply()
                    .to("matrix.gateway-batch-confirmations")
                    .batch(nonzero!(8))
                    .build(),
            )
            .out(Analytics, Publish)
            .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    let meta = Meta {
        tenant: "acme".to_owned(),
    };
    for subject in [
        "matrix.mirror-batch",
        "matrix.confirm-batch",
        "matrix.gateway-batch",
    ] {
        tb.broker::<MemoryBroker>()
            .publish_with_headers(subject, &Order { id: 9 }, &meta)
            .await
            .expect("publish");
    }
    tb.settle().await.expect("settle");

    let expected_event = Event {
        id: 9,
        tenant: "acme".to_owned(),
    };
    let expected_reply = Confirmation {
        id: 9,
        tenant: "acme".to_owned(),
    };
    let broker = tb.broker::<MemoryBroker>();
    broker
        .published::<Event>("matrix.batch-events")
        .assert_called_once()
        .with(&expected_event);
    broker
        .published::<Confirmation>("matrix.batch-confirmations")
        .assert_called_once()
        .with(&expected_reply);
    broker
        .published::<Event>("matrix.gateway-batch-events")
        .assert_called_once()
        .with(&expected_event);
    broker
        .published::<Confirmation>("matrix.gateway-batch-confirmations")
        .assert_called_once()
        .with(&expected_reply);
}
