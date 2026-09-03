//! What the mounted cells of the matrix actually do: the placeholder source every sealed
//! definition reports, the chunk contract of a capped page, a payload the input type refuses to
//! construct, and the arena entries the runtime pairs at startup.
//!
//! `parity_tests` proves every spelling mounts; this module drives the same definitions far
//! enough to settle, so the dispatch adapters behind them are exercised rather than only typed.

use std::borrow::Cow;
use std::fmt;
use std::future::{Future, ready};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::codec::JsonCodec;
use crate::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryPublish, MemoryPublisher};
use crate::nonzero;
use crate::runtime::batch::{BatchDef, BatchResult, SliceHandler};
use crate::runtime::batch_inject::BatchInjectDef;
use crate::runtime::batch_publishing::BatchPublishingDef;
use crate::runtime::context::Context;
use crate::runtime::dispatch::Delivery;
use crate::runtime::failure::{ErrorShutdown, FailurePolicy};
use crate::runtime::handler::{Handler, HandlerOutcome};
use crate::runtime::inject::{FromStartup, InjectCall, InjectDef};
use crate::runtime::publishing::PublishingDef;
use crate::runtime::settings::{SubscriberBuilder, SubscriberSettings};
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::{
    Deserialized, Handle, Input, Message, Outs, Router, Slot, SoloDeserialized, subscriber,
};
use crate::{
    Broker, HeaderMap, Name, OutSlot, OutgoingMessage, PairError, PublishPolicy, Publisher, Unnamed,
};

use super::eager::{construct, settle_page};
use super::reply::page_reply_verdict;

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

/// The construction failure of the validating input below.
#[derive(Debug)]
struct BadFrame;

impl fmt::Display for BadFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the frame is not addressed to this subscriber")
    }
}

/// A self-deserializing input that validates: the lane where a construction can fail, which is
/// what routes a delivery onto the subscriber's decode policy without a codec in sight.
struct Strict<'a>(&'a [u8]);

impl Deserialized for Strict<'_> {
    type Output<'a> = Strict<'a>;
    type Error = BadFrame;

    fn from_payload(payload: &[u8]) -> Result<Strict<'_>, BadFrame> {
        if payload.starts_with(b"ok") {
            Ok(Strict(payload))
        } else {
            Err(BadFrame)
        }
    }
}

impl Input for Strict<'_> {
    type Axis = SoloDeserialized<Strict<'static>>;
}

struct Analytics;

impl OutSlot for Analytics {
    const NAME: &'static str = "Analytics";
}

/// The arena entry the memory broker's plain policy pairs into, spelled once.
type AnalyticsEntry = Slot<Analytics, MemoryPublisher, JsonCodec>;

/// The arena a single-slot body receives.
type AnalyticsArena = Outs<(AnalyticsEntry,)>;

// ------------------------------------------------------------------------------- the bodies

/// One confirmation per element of a page.
fn confirmations(page: &[Order]) -> Vec<Confirmation> {
    page.iter()
        .map(|order| Confirmation { id: order.id })
        .collect()
}

struct Audit;

impl Handle<Order> for Audit {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = order.id;
        ready(Ok(()))
    }
}

struct Inspect;

impl<'p> Handle<Strict<'p>> for Inspect {
    fn handle(
        &self,
        frame: &Strict<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = frame.0.len();
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

struct ConfirmPages;

impl Handle<[Order], Vec<Confirmation>> for ConfirmPages {
    fn handle(
        &self,
        page: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        ready(Ok(confirmations(page)))
    }
}

/// The concrete-binding spelling of a slot body: naming the wired live type pins the arena, so
/// the definition is nameable without an include site to infer it from.
struct PinnedMirror;

impl Handle<Order, (), AnalyticsArena> for PinnedMirror {
    fn handle(
        &self,
        order: &Order,
        _outs: &AnalyticsArena,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = order.id;
        ready(Ok(()))
    }
}

struct PinnedFrameMirror;

impl<'p> Handle<Strict<'p>, (), AnalyticsArena> for PinnedFrameMirror {
    fn handle(
        &self,
        frame: &Strict<'p>,
        _outs: &AnalyticsArena,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        let _ = frame.0.len();
        ready(Ok(()))
    }
}

struct PinnedPageMirror;

impl Handle<[Order], (), AnalyticsArena> for PinnedPageMirror {
    fn handle(
        &self,
        page: &[Order],
        _outs: &AnalyticsArena,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = page.len();
        ready(Ok(()))
    }
}

struct PinnedGateway;

impl Handle<Order, Confirmation, AnalyticsArena> for PinnedGateway {
    fn handle(
        &self,
        order: &Order,
        _outs: &AnalyticsArena,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        ready(Ok(Confirmation { id: order.id }))
    }
}

struct PinnedPageGateway;

impl Handle<[Order], Vec<Confirmation>, AnalyticsArena> for PinnedPageGateway {
    fn handle(
        &self,
        page: &[Order],
        _outs: &AnalyticsArena,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        ready(Ok(confirmations(page)))
    }
}

/// Records every chunk it is handed and settles a full chunk uniformly, a short one per
/// element, so both fan-out shapes of a capped page are exercised in one delivery.
struct ChunkLog {
    seen: Arc<Mutex<Vec<usize>>>,
    cap: usize,
}

impl ChunkLog {
    fn verdict(&self, len: usize) -> Result<(), Vec<HandlerOutcome>> {
        self.seen
            .lock()
            .expect("the test holds no poisoned lock")
            .push(len);
        if len == self.cap {
            Ok(())
        } else {
            Err((0..len).map(|_| HandlerOutcome::drop()).collect())
        }
    }
}

impl Handle<[Order]> for ChunkLog {
    fn handle(
        &self,
        page: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(self.verdict(page.len()))
    }
}

struct PairChunkLog {
    inner: ChunkLog,
}

impl Handle<[Message<Meta, Order>]> for PairChunkLog {
    fn handle(
        &self,
        page: &[Message<Meta, Order>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(self.inner.verdict(page.len()))
    }
}

struct FrameChunkLog {
    inner: ChunkLog,
}

impl<'p> Handle<[Strict<'p>]> for FrameChunkLog {
    fn handle(
        &self,
        page: &[Strict<'p>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(self.inner.verdict(page.len()))
    }
}

/// A policy that refuses to pair, standing in for a broker whose publisher needs real work to
/// come alive (a transactional producer initializing).
struct RefusePairing;

impl PublishPolicy<ConnectedMemoryBroker> for RefusePairing {
    type Live = MemoryPublisher;

    fn pair(
        self,
        _connected: &ConnectedMemoryBroker,
    ) -> impl Future<Output = Result<MemoryPublisher, PairError>> {
        ready(Err(PairError::from_boxed(Box::from(
            "the policy refused to pair",
        ))))
    }
}

// ------------------------------------------------------------------------------ the fixtures

/// Unwraps the definition a sealed chain carries, so a test can call the mount machinery's
/// accessors on it directly.
fn definition_of<Def, Src, State, DC>(builder: SubscriberBuilder<Def, Src, State, DC>) -> Def {
    builder.split_def(|def| ((), def)).1
}

/// The per-delivery context the unit-level calls below run against.
fn context<'a>(
    name: &'a str,
    headers: &'a HeaderMap,
    state: &'a (),
    delivery: &'a Delivery,
) -> Context<'a, (), ()> {
    Context::new(name, headers, state, (), delivery)
}

/// Resolves a single-slot arena against a connected memory broker, exactly as startup does.
async fn analytics_arena(connected: &ConnectedMemoryBroker) -> AnalyticsArena {
    <AnalyticsArena as FromStartup<MemoryBroker, (), ((MemoryPublish, JsonCodec),)>>::resolve(
        ((MemoryPublish, JsonCodec),),
        connected,
        &(),
    )
    .await
    .expect("the memory publish policy pairs infallibly")
}

/// A batch of `len` orders, ids counting up from one.
fn orders(len: u64) -> Vec<Order> {
    (1..=len).map(|id| Order { id }).collect()
}

// ---------------------------------------------------------------------- the placeholder source

/// Every sealed definition carries the placeholder source: the settings builder wrapping it
/// holds the real one, so a bare mount cannot compile.
#[test]
fn every_sealed_definition_reports_the_placeholder_source() {
    let solo = definition_of(subscriber("orders", Audit).build());
    assert!(format!("{:?}", SubscriberDef::source(&solo)).contains("Unnamed"));

    let page = definition_of(subscriber("orders", SettlePage).build());
    assert!(format!("{:?}", BatchDef::source(&page)).contains("Unnamed"));

    let solo_slots = definition_of(subscriber("orders", PinnedMirror).build());
    assert!(format!("{:?}", InjectDef::source(&solo_slots)).contains("Unnamed"));

    let page_slots = definition_of(subscriber("orders", PinnedPageMirror).build());
    assert!(format!("{:?}", BatchInjectDef::source(&page_slots)).contains("Unnamed"));

    let solo_reply = definition_of(
        subscriber("orders", Confirm)
            .reply()
            .to("confirmations")
            .build(),
    );
    assert!(format!("{:?}", PublishingDef::source(&solo_reply)).contains("Unnamed"));

    let page_reply = definition_of(
        subscriber("orders", ConfirmPages)
            .reply()
            .to("confirmations")
            .build(),
    );
    assert!(format!("{:?}", BatchPublishingDef::source(&page_reply)).contains("Unnamed"));

    let solo_reply_slots = definition_of(
        subscriber("orders", PinnedGateway)
            .reply()
            .to("confirmations")
            .build(),
    );
    assert!(format!("{:?}", PublishingDef::source(&solo_reply_slots)).contains("Unnamed"));

    let page_reply_slots = definition_of(
        subscriber("orders", PinnedPageGateway)
            .reply()
            .to("confirmations")
            .build(),
    );
    assert!(format!("{:?}", BatchPublishingDef::source(&page_reply_slots)).contains("Unnamed"));
}

/// The definition values and their dispatch adapters name themselves in a diagnostic, which is
/// all a body-carrying type can say about itself.
#[test]
fn the_definition_values_name_themselves() {
    let value = definition_of(subscriber("orders", Audit));
    assert!(format!("{value:?}").contains("HandleValue"));

    let reply = definition_of(subscriber("orders", Confirm).reply());
    assert!(format!("{reply:?}").contains("ReplyValue"));

    let sealed = definition_of(subscriber("orders", Audit).build());
    assert!(format!("{sealed:?}").contains("Sealed"));

    let solo_body = SubscriberDef::into_handler(definition_of(subscriber("orders", Audit).build()));
    assert!(format!("{solo_body:?}").contains("SoloBody"));

    let page_body = BatchDef::into_handler(definition_of(subscriber("orders", SettlePage).build()));
    assert!(format!("{page_body:?}").contains("PageBody"));
}

/// Every source spelling the constructor accepts converts: an owned subject string, a borrowed
/// one, a built `Name`, and the deferred placeholder the mount site names.
#[test]
fn every_source_spelling_converts_at_the_constructor() {
    let _ = Router::<MemoryBroker>::new()
        .include(subscriber(String::from("orders"), Audit).build())
        .include(subscriber(Cow::Borrowed("orders"), Audit).build())
        .include(subscriber(Name::new("orders"), Audit).build())
        .include(
            subscriber(Unnamed::<Name>::new(), Audit)
                .name("orders")
                .build(),
        );
}

/// The reply chain reaches the inner value's documentation steps, so the chain order is free.
#[test]
fn the_reply_chain_carries_the_documentation_steps() {
    let described = definition_of(
        subscriber("orders", Confirm)
            .reply()
            .to("confirmations")
            .describe("Confirms an order")
            .build(),
    );
    assert_eq!(
        PublishingDef::description(&described),
        Some("Confirms an order")
    );

    let opted_out = definition_of(
        subscriber("orders", Confirm)
            .reply()
            .to("confirmations")
            .undocumented()
            .build(),
    );
    assert!(PublishingDef::input_schema(&opted_out).is_none());
}

/// A page reply chains `.transactional()` - the replies of one page become visible together, or
/// none of them do - and the definition still reports the destination it was given.
#[test]
fn a_page_reply_attaches_a_transactional_publisher() {
    let attached = definition_of(
        subscriber("orders", ConfirmPages)
            .reply()
            .to("confirmations")
            .publisher(MemoryPublish)
            .transactional()
            .build(),
    );
    assert_eq!(BatchPublishingDef::reply_name(&attached), "confirmations");

    let _ = Router::<MemoryBroker>::new().include(
        subscriber("orders", ConfirmPages)
            .reply()
            .to("confirmations")
            .publisher(MemoryPublish)
            .transactional()
            .build(),
    );
}

// ------------------------------------------------------------------------- the page contract

/// A page verdict of `Ok` acks the whole chunk; a per-element vector of the wrong length is a
/// bug in the body, and the panic names the subscription so the culprit is findable.
#[test]
fn a_page_verdict_of_ok_acks_the_whole_chunk() {
    assert!(matches!(
        settle_page(Ok(()), 3, "orders"),
        BatchResult::Uniform(_)
    ));
}

#[test]
#[should_panic(expected = "subscriber 'orders' returned 2 per-element outcomes for a page of 3")]
fn a_short_per_element_page_verdict_names_the_subscription() {
    let _ = settle_page(
        Err(vec![HandlerOutcome::drop(), HandlerOutcome::drop()]),
        3,
        "orders",
    );
}

#[test]
fn a_page_reply_verdict_of_err_settles_per_element() {
    let verdict: Result<Vec<Confirmation>, Vec<HandlerOutcome>> =
        Err(vec![HandlerOutcome::drop(), HandlerOutcome::retry()]);
    let settled = page_reply_verdict(verdict, 2, "orders").expect_err("the page reports outcomes");
    match settled {
        BatchResult::PerElement(outcomes) => assert_eq!(outcomes.len(), 2),
        BatchResult::Uniform(_) => panic!("a per-element verdict never settles uniformly"),
    }
}

#[test]
#[should_panic(expected = "subscriber 'orders' returned 1 replies for a page of 2")]
fn a_short_page_reply_names_the_subscription() {
    let verdict: Result<Vec<Confirmation>, Vec<HandlerOutcome>> = Ok(vec![Confirmation { id: 1 }]);
    let _ = page_reply_verdict(verdict, 2, "orders");
}

#[test]
#[should_panic(expected = "subscriber 'orders' returned 1 per-element outcomes for a page of 2")]
fn a_short_page_reply_outcome_vector_names_the_subscription() {
    let verdict: Result<Vec<Confirmation>, Vec<HandlerOutcome>> = Err(vec![HandlerOutcome::drop()]);
    let _ = page_reply_verdict(verdict, 2, "orders");
}

/// A capped page reaches a decoded body in chunks, each settled on its own: the full chunk acks
/// uniformly (fanned out per element) and the short tail settles per element.
#[tokio::test]
async fn a_capped_page_reaches_a_decoded_body_in_chunks() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let body = ChunkLog {
        seen: Arc::clone(&seen),
        cap: 2,
    };
    let handler = BatchDef::into_handler(definition_of(
        subscriber("orders", body).batch(nonzero!(2)).build(),
    ));

    let page = orders(3);
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("orders", &headers, &state, &delivery);
    let settled = handler.handle_slice(&page, &mut ctx).await;

    assert_eq!(
        *seen.lock().expect("the test holds no poisoned lock"),
        [2, 1]
    );
    match settled {
        BatchResult::PerElement(outcomes) => {
            assert_eq!(outcomes.len(), 3);
            assert!(outcomes[0].is_ack() && outcomes[1].is_ack());
            assert!(outcomes[2].is_drop());
        }
        BatchResult::Uniform(_) => panic!("a capped page settles per element"),
    }
}

/// The same chunking on the pair lane: the typed header contract rides every element.
#[tokio::test]
async fn a_capped_page_reaches_a_pair_body_in_chunks() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let body = PairChunkLog {
        inner: ChunkLog {
            seen: Arc::clone(&seen),
            cap: 2,
        },
    };
    let handler = BatchDef::into_handler(definition_of(
        subscriber("orders", body).batch(nonzero!(2)).build(),
    ));

    let page: Vec<Message<Meta, Order>> = orders(3)
        .into_iter()
        .map(|order| {
            Message::new(
                Meta {
                    tenant: "acme".to_owned(),
                },
                order,
            )
        })
        .collect();
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("orders", &headers, &state, &delivery);
    let settled = handler.handle_slice(&page, &mut ctx).await;

    assert_eq!(
        *seen.lock().expect("the test holds no poisoned lock"),
        [2, 1]
    );
    assert!(matches!(settled, BatchResult::PerElement(outcomes) if outcomes.len() == 3));
}

/// And on the self-deserializing lane, whose elements the dispatch adapter constructed out of
/// the deliveries' payloads before the chunking begins.
#[tokio::test]
async fn a_capped_page_reaches_a_self_deserializing_body_in_chunks() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let body = FrameChunkLog {
        inner: ChunkLog {
            seen: Arc::clone(&seen),
            cap: 2,
        },
    };
    let handler = BatchDef::into_handler(definition_of(
        subscriber("frames", body).batch(nonzero!(2)).build(),
    ));

    let page = [Strict(b"ok1"), Strict(b"ok2"), Strict(b"ok3")];
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("frames", &headers, &state, &delivery);
    let settled = handler.handle_slice(&page, &mut ctx).await;

    assert_eq!(
        *seen.lock().expect("the test holds no poisoned lock"),
        [2, 1]
    );
    assert!(matches!(settled, BatchResult::PerElement(outcomes) if outcomes.len() == 3));
}

// -------------------------------------------------------------- the refused payload construction

/// A payload the input type refuses to construct settles by the subscriber's decode policy,
/// exactly as a codec decode failure does.
#[test]
fn a_refused_construction_settles_by_the_decode_policy() {
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("frames", &headers, &state, &delivery);
    let outcome = construct::<Strict<'static>, (), ()>(b"bad", &mut ctx)
        .err()
        .expect("the frame is refused");
    assert!(outcome.is_drop());
}

/// Under `decode = fail_fast` the refused payload is settled out of the way and the service is
/// torn down: the teardown is what makes the failure loud.
#[test]
fn a_refused_construction_under_fail_fast_tears_the_service_down() {
    let token = CancellationToken::new();
    let shutdown = ErrorShutdown::new(token.clone());
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("frames", &headers, &state, &delivery)
        .with_failfast(&shutdown)
        .with_decode_policy(FailurePolicy::FailFast);

    let outcome = construct::<Strict<'static>, (), ()>(b"bad", &mut ctx)
        .err()
        .expect("the frame is refused");
    assert!(outcome.is_drop());
    assert!(token.is_cancelled());
}

/// The diagnostic names the subscription and the input type, so the offending producer is
/// findable from the logs. Asserted on the construction itself: a field value is only evaluated
/// while a subscriber listens.
#[cfg(feature = "logging")]
#[test]
fn a_refused_construction_names_the_subscription_and_the_input_type() {
    use crate::testkit::log_capture;

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("frames", &headers, &state, &delivery);

    let (events, guard) = log_capture::start();
    let outcome = construct::<Strict<'static>, (), ()>(b"bad", &mut ctx)
        .err()
        .expect("the frame is refused");
    drop(guard);

    let failure = log_capture::find(&events, "payload construction failed");
    assert_eq!(
        failure.get("subscription").map(String::as_str),
        Some("frames")
    );
    assert!(
        failure
            .get("message_type")
            .is_some_and(|name| name.contains("Strict")),
        "the diagnostic must name the input type: {failure:?}",
    );
    assert!(outcome.is_drop());
}

/// The refused payload never reaches the body: the plain single-delivery adapter returns the
/// settlement instead of calling it.
#[tokio::test]
async fn a_refused_payload_never_reaches_a_plain_body() {
    let handler = SubscriberDef::into_handler(definition_of(subscriber("frames", Inspect).build()));
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("frames", &headers, &state, &delivery);

    assert!(handler.handle(b"bad".as_slice(), &mut ctx).await.is_drop());
    assert!(handler.handle(b"ok!".as_slice(), &mut ctx).await.is_ack());
}

/// Nor a slot body: the arena is already live, and the construction still gates the call.
#[tokio::test]
async fn a_refused_payload_never_reaches_a_slot_body() {
    let connected = MemoryBroker::new()
        .connect()
        .await
        .expect("the memory broker connects");
    let arena = analytics_arena(&connected).await;
    let def = definition_of(subscriber("frames", PinnedFrameMirror).build());

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("frames", &headers, &state, &delivery);

    let refused = InjectCall::<()>::call(&def, b"bad".as_slice(), &arena, &mut ctx).await;
    assert!(refused.is_drop());
    let accepted = InjectCall::<()>::call(&def, b"ok!".as_slice(), &arena, &mut ctx).await;
    assert!(accepted.is_ack());
}

// ------------------------------------------------------------------------------- the arena

/// The arena resolves its entries at startup and names itself in a diagnostic.
#[tokio::test]
async fn the_arena_resolves_its_entries_at_startup() {
    let connected = MemoryBroker::new()
        .connect()
        .await
        .expect("the memory broker connects");
    let arena = analytics_arena(&connected).await;
    assert!(format!("{arena:?}").contains("Outs"));
}

/// An entry is a publisher in its own right: the core vocabulary is delegated on the entry
/// itself, so it passes into any position demanding the capability.
#[tokio::test]
async fn a_slot_entry_is_a_publisher_in_its_own_right() {
    let connected = MemoryBroker::new()
        .connect()
        .await
        .expect("the memory broker connects");
    let entry = AnalyticsEntry::test_entry(connected.publisher(), JsonCodec);

    assert!(format!("{entry:?}").contains("Slot"));
    assert!(Publisher::base_headers(&entry).is_none());
    Publisher::publish(
        &entry,
        OutgoingMessage::new("slots.direct", b"bytes".as_slice()),
    )
    .await
    .expect("the in-memory publisher accepts the message");
}

/// A policy the broker refuses to pair fails startup with the slot named, so the binding at
/// fault is findable without reading the wiring.
#[tokio::test]
async fn a_refused_pairing_names_the_slot_it_failed_for() {
    let connected = MemoryBroker::new()
        .connect()
        .await
        .expect("the memory broker connects");
    let failure =
        <AnalyticsEntry as FromStartup<MemoryBroker, (), (RefusePairing, JsonCodec)>>::resolve(
            (RefusePairing, JsonCodec),
            &connected,
            &(),
        )
        .await
        .expect_err("the policy refuses to pair");
    assert!(
        failure.to_string().contains("Analytics"),
        "the pairing failure must name the slot: {failure}",
    );
}
