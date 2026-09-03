//! The pair cells of the manual matrix, end to end: a `Message<H, P>` input reaching a body that
//! fans out through an injections arena, one that answers with a reply, and one that does both -
//! at the single-message shape and the page shape.
//!
//! The header contract is decoded in the same stage as the payload, so what proves it arrived is
//! what leaves the handler: every message the bodies below publish carries the tenant read off
//! the delivery's headers.
#![cfg(all(feature = "memory", feature = "json", feature = "testing"))]

use std::future::{Future, ready};

use ruststream::codec::Codec;
use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::prelude::*;
use ruststream::runtime::PublishedThrough;
use ruststream::testing::TestApp;
use ruststream::{CallerName, MessageHeaders, NoHeaders, OutgoingDestination};
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

/// The arena a single-slot body declares.
type AnalyticsArena<W, E> = Outs<(Slot<Analytics, W, E>,)>;

// ------------------------------------------------------------------- the single-message bodies

/// A pair input fanned out through a slot.
struct MirrorPair;

impl<W, E> Handle<Message<Meta, Order>, (), AnalyticsArena<W, E>> for MirrorPair
where
    W: Publisher,
    E: Codec + Send + Sync,
{
    async fn handle(
        &self,
        msg: &Message<Meta, Order>,
        outs: &AnalyticsArena<W, E>,
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

impl<W, E> Handle<Message<Meta, Order>, Confirmation, AnalyticsArena<W, E>> for GatewayPair
where
    W: Publisher,
    E: Codec + Send + Sync,
{
    async fn handle(
        &self,
        msg: &Message<Meta, Order>,
        outs: &AnalyticsArena<W, E>,
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

// ------------------------------------------------------------------------------- the page bodies

/// A page of pairs fanned out through a slot.
struct MirrorPairPage;

impl<W, E> Handle<[Message<Meta, Order>], (), AnalyticsArena<W, E>> for MirrorPairPage
where
    W: Publisher,
    E: Codec + Send + Sync,
{
    async fn handle(
        &self,
        page: &[Message<Meta, Order>],
        outs: &AnalyticsArena<W, E>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), Vec<HandlerOutcome>> {
        for msg in page {
            if outs
                .get(Analytics)
                .message(&event_of(msg))
                .to("matrix.page-events")
                .publish()
                .await
                .is_err()
            {
                return Err(page.iter().map(|_| HandlerOutcome::retry()).collect());
            }
        }
        Ok(())
    }
}

/// A page of pairs answering with one reply per element.
struct ConfirmPairPage;

impl Handle<[Message<Meta, Order>], Vec<Confirmation>> for ConfirmPairPage {
    fn handle(
        &self,
        page: &[Message<Meta, Order>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        ready(Ok(page.iter().map(confirmation_of).collect()))
    }
}

/// A page of pairs answering with one reply per element and fanning out through a slot.
struct GatewayPairPage;

impl<W, E> Handle<[Message<Meta, Order>], Vec<Confirmation>, AnalyticsArena<W, E>>
    for GatewayPairPage
where
    W: Publisher,
    E: Codec + Send + Sync,
{
    async fn handle(
        &self,
        page: &[Message<Meta, Order>],
        outs: &AnalyticsArena<W, E>,
        _ctx: &mut Context<'_>,
    ) -> Result<Vec<Confirmation>, Vec<HandlerOutcome>> {
        for msg in page {
            if outs
                .get(Analytics)
                .message(&event_of(msg))
                .to("matrix.gateway-page-events")
                .publish()
                .await
                .is_err()
            {
                return Err(page.iter().map(|_| HandlerOutcome::retry()).collect());
            }
        }
        Ok(page.iter().map(confirmation_of).collect())
    }
}

// ------------------------------------------------------------------------------------ the tests

/// The single-message pair cells: the arena, the reply, and both at once. Each publishes what it
/// read off the delivery's headers, so the assertions prove the contract arrived.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pair_input_reaches_every_single_message_cell() {
    let app =
        RustStream::new(AppInfo::new("matrix", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("matrix.mirror", MirrorPair).build())
                .out(Analytics, MemoryPublish)
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
            .out(Analytics, MemoryPublish)
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

/// The page pair cells: one header contract per element, all the way to the replies.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pair_input_reaches_every_page_cell() {
    let app = RustStream::new(AppInfo::new("matrix-page", "0.1.0")).with_broker(
        MemoryBroker::new(),
        |b| {
            b.include(subscriber("matrix.mirror-page", MirrorPairPage).build())
                .out(Analytics, MemoryPublish)
                .build();
            b.include(
                subscriber("matrix.confirm-page", ConfirmPairPage)
                    .reply()
                    .to("matrix.page-confirmations")
                    .build(),
            );
            b.include(
                subscriber("matrix.gateway-page", GatewayPairPage)
                    .reply()
                    .to("matrix.gateway-page-confirmations")
                    .build(),
            )
            .out(Analytics, MemoryPublish)
            .build();
        },
    );
    let tb = TestApp::start(app).await.expect("harness start");

    let meta = Meta {
        tenant: "acme".to_owned(),
    };
    for subject in [
        "matrix.mirror-page",
        "matrix.confirm-page",
        "matrix.gateway-page",
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
        .published::<Event>("matrix.page-events")
        .assert_called_once()
        .with(&expected_event);
    broker
        .published::<Confirmation>("matrix.page-confirmations")
        .assert_called_once()
        .with(&expected_reply);
    broker
        .published::<Event>("matrix.gateway-page-events")
        .assert_called_once()
        .with(&expected_event);
    broker
        .published::<Confirmation>("matrix.gateway-page-confirmations")
        .assert_called_once()
        .with(&expected_reply);
}
