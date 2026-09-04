//! What the mounted cells of the matrix actually do: the placeholder source every sealed
//! definition reports, the batch a body is handed against the size its registration named, a
//! payload the input type refuses to construct, and the arena entries the runtime pairs at
//! startup.
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
use crate::memory::{
    ConnectedMemoryBroker, MemoryBatchContext, MemoryBroker, MemoryPosition, MemoryPublish,
    MemoryPublisher, SeekHandle,
};
use crate::nonzero;
use crate::runtime::batch::{BatchDef, BatchResult, SliceHandler};
use crate::runtime::batch_inject::{BatchInjectCall, BatchInjectDef};
use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingDef};
use crate::runtime::context::Context;
use crate::runtime::dispatch::Delivery;
use crate::runtime::failure::{ErrorShutdown, FailurePolicy};
use crate::runtime::handler::{Handler, HandlerOutcome};
use crate::runtime::inject::{FromStartup, InjectCall, InjectDef};
use crate::runtime::publish::PublishIdentity;
use crate::runtime::publishing::PublishingDef;
use crate::runtime::settings::{BatchSized, SubscriberBuilder, SubscriberSettings};
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::{
    Deserialized, Handle, Input, Message, Outs, Reply, Router, Slot, SoloDeserialized, subscriber,
};
use crate::testkit::batch::{publish_payloads, pull_batch};
use crate::{
    Broker, BuildBatchContext, HeaderMap, Name, OutSlot, OutgoingMessage, PairError, PublishPolicy,
    Publisher, Seeker, Unnamed,
};

use super::eager::{construct, settle_batch};
use super::reply::batch_reply_verdict;

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
/// One confirmation per element of a batch.
fn confirmations(batch: &[Order]) -> Vec<Confirmation> {
    batch
        .iter()
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

/// One confirmation per element, produced while the body reads the broker's batch context: the
/// cell where the reply axis and the context axis are named together.
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
            return Err(batch.iter().map(|_| HandlerOutcome::retry()).collect());
        }
        Ok(confirmations(batch))
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
        ready(Ok(confirmations(batch)))
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

struct PinnedBatchMirror;

impl Handle<[Order], (), AnalyticsArena> for PinnedBatchMirror {
    fn handle(
        &self,
        batch: &[Order],
        _outs: &AnalyticsArena,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        let _ = batch.len();
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

struct PinnedBatchGateway;

impl Handle<[Order], Vec<Confirmation>, AnalyticsArena> for PinnedBatchGateway {
    fn handle(
        &self,
        batch: &[Order],
        _outs: &AnalyticsArena,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Vec<Confirmation>, Vec<HandlerOutcome>>> {
        ready(Ok(confirmations(batch)))
    }
}

/// Records every batch it is handed and settles a batch of the expected length uniformly, any
/// other per element, so a resizing dispatch would change both the log and the settlement.
struct BatchLog {
    seen: Arc<Mutex<Vec<usize>>>,
    expected: usize,
}

impl BatchLog {
    fn verdict(&self, len: usize) -> Result<(), Vec<HandlerOutcome>> {
        self.seen
            .lock()
            .expect("the test holds no poisoned lock")
            .push(len);
        if len == self.expected {
            Ok(())
        } else {
            Err((0..len).map(|_| HandlerOutcome::drop()).collect())
        }
    }
}

impl Handle<[Order]> for BatchLog {
    fn handle(
        &self,
        batch: &[Order],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(self.verdict(batch.len()))
    }
}

struct PairBatchLog {
    inner: BatchLog,
}

impl Handle<[Message<Meta, Order>]> for PairBatchLog {
    fn handle(
        &self,
        batch: &[Message<Meta, Order>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(self.inner.verdict(batch.len()))
    }
}

struct SlotBatchLog {
    inner: BatchLog,
}

impl Handle<[Order], (), AnalyticsArena> for SlotBatchLog {
    fn handle(
        &self,
        batch: &[Order],
        _outs: &AnalyticsArena,
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(self.inner.verdict(batch.len()))
    }
}

struct FrameBatchLog {
    inner: BatchLog,
}

impl<'p> Handle<[Strict<'p>]> for FrameBatchLog {
    fn handle(
        &self,
        batch: &[Strict<'p>],
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), Vec<HandlerOutcome>>> {
        ready(self.inner.verdict(batch.len()))
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
/// Unwraps the definition a sealed chain carries, so a test can call the mount machinery's
/// accessors on it directly.
fn definition_of<Def, Src, State, DC>(builder: SubscriberBuilder<Def, Src, State, DC>) -> Def {
    builder.into_def()
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

/// Resolves a single-slot arena against a connected memory broker, exactly as startup does: the
/// policy, the mount site's codec, and the publish pipeline a mount with no middleware composes.
async fn analytics_arena(connected: &ConnectedMemoryBroker) -> AnalyticsArena {
    <AnalyticsArena as FromStartup<
        MemoryBroker,
        (),
        ((MemoryPublish, JsonCodec, PublishIdentity),),
    >>::resolve(
        ((MemoryPublish, JsonCodec, PublishIdentity),),
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
/// Every sealed definition carries the placeholder source: the settings builder wrapping it
/// holds the real one, so a bare mount cannot compile.
#[test]
fn every_sealed_definition_reports_the_placeholder_source() {
    let solo = definition_of(subscriber("orders", Audit).build());
    assert!(format!("{:?}", SubscriberDef::source(&solo)).contains("Unnamed"));

    let batch = definition_of(subscriber("orders", SettleBatch).build());
    assert!(format!("{:?}", BatchDef::source(&batch)).contains("Unnamed"));

    let solo_slots = definition_of(subscriber("orders", PinnedMirror).build());
    assert!(format!("{:?}", InjectDef::source(&solo_slots)).contains("Unnamed"));

    let batch_slots = definition_of(subscriber("orders", PinnedBatchMirror).build());
    assert!(format!("{:?}", BatchInjectDef::source(&batch_slots)).contains("Unnamed"));

    let solo_reply = definition_of(
        subscriber("orders", Confirm)
            .reply()
            .to("confirmations")
            .build(),
    );
    assert!(format!("{:?}", PublishingDef::source(&solo_reply)).contains("Unnamed"));

    let batch_reply = definition_of(
        subscriber("orders", ConfirmBatches)
            .reply()
            .to("confirmations")
            .build(),
    );
    assert!(format!("{:?}", BatchPublishingDef::source(&batch_reply)).contains("Unnamed"));

    let solo_reply_slots = definition_of(
        subscriber("orders", PinnedGateway)
            .reply()
            .to("confirmations")
            .build(),
    );
    assert!(format!("{:?}", PublishingDef::source(&solo_reply_slots)).contains("Unnamed"));

    let batch_reply_slots = definition_of(
        subscriber("orders", PinnedBatchGateway)
            .reply()
            .to("confirmations")
            .build(),
    );
    assert!(format!("{:?}", BatchPublishingDef::source(&batch_reply_slots)).contains("Unnamed"));
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

    let batch_body =
        BatchDef::into_handler(definition_of(subscriber("orders", SettleBatch).build()));
    assert!(format!("{batch_body:?}").contains("BatchBody"));
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

/// A batch reply's mount chains `.transactional()` - the replies of one batch become visible
/// together, or none of them do - and the definition still reports the destination it was given.
#[test]
fn a_batch_reply_attaches_a_transactional_publisher() {
    let declared = definition_of(
        subscriber("orders", ConfirmBatches)
            .reply()
            .to("confirmations")
            .build(),
    );
    assert_eq!(BatchPublishingDef::reply_name(&declared), "confirmations");

    let _ = Router::<MemoryBroker>::new()
        .include(
            subscriber("orders", ConfirmBatches)
                .reply()
                .to("confirmations")
                .batch(nonzero!(8))
                .build(),
        )
        .out(Reply, MemoryPublish)
        .transactional()
        .build();
}

/// A replying batch body reaches the broker's subscription-scoped context: the batch's own
/// definition is driven with the context the runtime builds off the batch's first delivery, and
/// the reposition handle it carries is live there.
#[tokio::test]
async fn a_replying_batch_reads_the_brokers_batch_context() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("orders");
    publish_payloads(&broker, "orders", &[br#"{"id":1}"#, br#"{"id":2}"#]).await;
    let batch = pull_batch(&mut sub).await;
    let cx = <MemoryBatchContext as BuildBatchContext<_>>::build(&batch[0]);

    let def = definition_of(
        subscriber("orders", ConfirmBatchesInContext)
            .reply()
            .to("confirmations")
            .build(),
    );

    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = Context::new("orders", &headers, &state, cx, &delivery);
    let replies = BatchPublishingCall::<()>::call(&def, &orders(2), &(), &mut ctx)
        .await
        .expect("the batch answers once its reposition is accepted");

    let ids: Vec<u64> = replies.iter().map(|reply| reply.id).collect();
    assert_eq!(ids, [1, 2], "one reply per element, in batch order");
}
/// A batch verdict of `Ok` acks the whole batch; a per-element vector of the wrong length is a
/// bug in the body, and the panic names the subscription so the culprit is findable.
#[test]
fn a_batch_verdict_of_ok_acks_the_whole_batch() {
    assert!(matches!(
        settle_batch(Ok(()), 3, "orders"),
        BatchResult::Uniform(_)
    ));
}

#[test]
#[should_panic(expected = "subscriber 'orders' returned 2 per-element outcomes for a batch of 3")]
fn a_short_per_element_batch_verdict_names_the_subscription() {
    let _ = settle_batch(
        Err(vec![HandlerOutcome::drop(), HandlerOutcome::drop()]),
        3,
        "orders",
    );
}

#[test]
fn a_batch_reply_verdict_of_err_settles_per_element() {
    let verdict: Result<Vec<Confirmation>, Vec<HandlerOutcome>> =
        Err(vec![HandlerOutcome::drop(), HandlerOutcome::retry()]);
    let settled =
        batch_reply_verdict(verdict, 2, "orders").expect_err("the batch reports outcomes");
    match settled {
        BatchResult::PerElement(outcomes) => assert_eq!(outcomes.len(), 2),
        BatchResult::Uniform(_) => panic!("a per-element verdict never settles uniformly"),
    }
}

#[test]
#[should_panic(expected = "subscriber 'orders' returned 1 replies for a batch of 2")]
fn a_short_batch_reply_names_the_subscription() {
    let verdict: Result<Vec<Confirmation>, Vec<HandlerOutcome>> = Ok(vec![Confirmation { id: 1 }]);
    let _ = batch_reply_verdict(verdict, 2, "orders");
}

#[test]
#[should_panic(expected = "subscriber 'orders' returned 1 per-element outcomes for a batch of 2")]
fn a_short_batch_reply_outcome_vector_names_the_subscription() {
    let verdict: Result<Vec<Confirmation>, Vec<HandlerOutcome>> = Err(vec![HandlerOutcome::drop()]);
    let _ = batch_reply_verdict(verdict, 2, "orders");
}

/// A decoded batch reaches its body exactly as the broker built it: the size the registration
/// named opened the subscription, and the dispatch adds no resizing of its own.
#[tokio::test]
async fn a_batch_reaches_a_decoded_body_whole() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let body = BatchLog {
        seen: Arc::clone(&seen),
        expected: 3,
    };
    let handler = BatchDef::into_handler(definition_of(
        subscriber("orders", body).batch(nonzero!(2)).build(),
    ));

    let batch = orders(3);
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("orders", &headers, &state, &delivery);
    let settled = handler.handle_slice(&batch, &mut ctx).await;

    assert_eq!(*seen.lock().expect("the test holds no poisoned lock"), [3]);
    assert!(matches!(settled, BatchResult::Uniform(outcome) if outcome.is_ack()));
}

/// The same on the pair lane: the typed header contract rides every element of the one batch.
#[tokio::test]
async fn a_batch_reaches_a_pair_body_whole() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let body = PairBatchLog {
        inner: BatchLog {
            seen: Arc::clone(&seen),
            expected: 3,
        },
    };
    let handler = BatchDef::into_handler(definition_of(
        subscriber("orders", body).batch(nonzero!(2)).build(),
    ));

    let batch: Vec<Message<Meta, Order>> = orders(3)
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
    let settled = handler.handle_slice(&batch, &mut ctx).await;

    assert_eq!(*seen.lock().expect("the test holds no poisoned lock"), [3]);
    assert!(matches!(settled, BatchResult::Uniform(outcome) if outcome.is_ack()));
}

/// And on the self-deserializing lane, whose elements the dispatch adapter constructed out of
/// the deliveries' payloads before the body sees the batch.
#[tokio::test]
async fn a_batch_reaches_a_self_deserializing_body_whole() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let body = FrameBatchLog {
        inner: BatchLog {
            seen: Arc::clone(&seen),
            expected: 3,
        },
    };
    let handler = BatchDef::into_handler(definition_of(
        subscriber("frames", body).batch(nonzero!(2)).build(),
    ));

    let batch = [Strict(b"ok1"), Strict(b"ok2"), Strict(b"ok3")];
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("frames", &headers, &state, &delivery);
    let settled = handler.handle_slice(&batch, &mut ctx).await;

    assert_eq!(*seen.lock().expect("the test holds no poisoned lock"), [3]);
    assert!(matches!(settled, BatchResult::Uniform(outcome) if outcome.is_ack()));
}

/// A slot-carrying batch reaches the body whole, with the arena riding it: the injections are
/// what the form adds, and they change nothing about how a batch is handed over.
#[tokio::test]
async fn a_slot_batch_reaches_the_body_whole() {
    let connected = MemoryBroker::new()
        .connect()
        .await
        .expect("the memory broker connects");
    let arena = analytics_arena(&connected).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let body = SlotBatchLog {
        inner: BatchLog {
            seen: Arc::clone(&seen),
            expected: 3,
        },
    };
    let def = definition_of(subscriber("orders", body).batch(nonzero!(2)).build());

    let batch = orders(3);
    let state = ();
    let delivery = Delivery::empty();
    let headers = HeaderMap::new();
    let mut ctx = context("orders", &headers, &state, &delivery);
    let settled = BatchInjectCall::<()>::call(&def, &batch, &arena, &mut ctx).await;

    // The batch the broker built is what arrives, whatever size the registration asked for: the
    // size travels to the subscription, not to the dispatch.
    assert_eq!(*seen.lock().expect("the test holds no poisoned lock"), [3]);
    assert!(matches!(settled, BatchResult::Uniform(outcome) if outcome.is_ack()));
}

/// Every batch form carries the size to the mount, whatever else its signature holds.
#[test]
fn every_batch_form_carries_its_size_to_the_mount() {
    let plain = subscriber("orders", SettleBatch).batch(nonzero!(2)).build();
    assert_eq!(BatchSized::batch_size(&plain), nonzero!(2));

    let replying = subscriber("orders", ConfirmBatches)
        .reply()
        .to("confirmations")
        .batch(nonzero!(4))
        .build();
    assert_eq!(BatchSized::batch_size(&replying), nonzero!(4));

    let with_slots = subscriber("orders", PinnedBatchGateway)
        .reply()
        .to("confirmations")
        .batch(nonzero!(8))
        .build();
    assert_eq!(BatchSized::batch_size(&with_slots), nonzero!(8));
}
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
    let entry = AnalyticsEntry::test_entry(connected.publisher(), JsonCodec, PublishIdentity);

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
    let failure = <AnalyticsEntry as FromStartup<
        MemoryBroker,
        (),
        (RefusePairing, JsonCodec, PublishIdentity),
    >>::resolve((RefusePairing, JsonCodec, PublishIdentity), &connected, &())
    .await
    .expect_err("the policy refuses to pair");
    assert!(
        failure.to_string().contains("Analytics"),
        "the pairing failure must name the slot: {failure}",
    );
}
