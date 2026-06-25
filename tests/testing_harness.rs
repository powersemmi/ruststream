//! Integration tests for the in-process [`TestApp`](ruststream::testing::TestApp) harness: recorded
//! input and outcome, failure-policy / panic / shutdown behaviour, multi-broker addressing, and a
//! cross-broker cascade driven to quiescence.

#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use ruststream::memory::MemoryBroker;
// `Context` is named in handler signatures below but the `#[subscriber]` macro rewrites them, so it
// needs no import (matching the `examples/publishing.rs` pattern).
use ruststream::runtime::{AppInfo, HandlerResult, RustStream};
use ruststream::testing::{Outcome, TestApp, TestError};
use ruststream::{OutgoingMessage, Publisher, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Order {
    id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Event {
    id: u64,
}

/// Acks every order; panics on id 0 (a deliberate negative-test trigger) under the default
/// `panic = fail_fast`.
#[subscriber("orders")]
async fn handle_orders(order: &Order) -> HandlerResult {
    assert!(order.id != 0, "boom on id 0");
    HandlerResult::Ack
}

/// Drops every message (nack without requeue).
#[subscriber("dropme")]
async fn drop_all(order: &Order) -> HandlerResult {
    let _ = order;
    HandlerResult::drop()
}

/// Panics on id 0 but `panic = skip` keeps the service running and acks the offending message.
#[subscriber("skipper", on_failure(panic = skip))]
async fn skip_panics(order: &Order) -> HandlerResult {
    assert!(order.id != 0, "boom on id 0");
    HandlerResult::Ack
}

/// Requeues forever, to exercise the quiescence step-budget guard.
#[subscriber("loops")]
async fn loop_forever(order: &Order) -> HandlerResult {
    let _ = order;
    HandlerResult::retry()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_received_value_and_ack() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));
    let tb = TestApp::start(app).await.unwrap();

    tb.broker::<MemoryBroker>()
        .publish("orders", &Order { id: 7 })
        .await
        .unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 7 })
        .settled(HandlerResult::Ack)
        .assert_outcome(Outcome::Ack);

    // The received messages can also be retrieved for custom inspection.
    let received: Vec<Order> = tb.broker::<MemoryBroker>().subscriber("orders").received();
    assert_eq!(received, vec![Order { id: 7 }]);
    let raw = tb
        .broker::<MemoryBroker>()
        .subscriber("orders")
        .received_raw();
    assert_eq!(raw.len(), 1);

    tb.assert_running();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_drop_outcome() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(drop_all));
    let tb = TestApp::start(app).await.unwrap();

    tb.publish("dropme", &Order { id: 1 }).await.unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("dropme")
        .assert_called_once()
        .assert_outcome(Outcome::Drop)
        .settled(HandlerResult::Nack { requeue: false });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_decode_failure() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));
    let tb = TestApp::start(app).await.unwrap();

    // Not valid JSON for `Order`: the typed adapter fails to decode, the handler never runs.
    tb.broker::<MemoryBroker>()
        .publish_raw("orders", b"not json")
        .await
        .unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .assert_outcome(Outcome::DecodeFailed)
        .assert_last_failed_to_decode();
    // A decode failure under the default policy does not tear the service down.
    tb.assert_running();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_fast_panic_shuts_down_and_blocks_further_publishes() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));
    let tb = TestApp::start(app).await.unwrap();

    // The panicking delivery still drives to quiescence (the message is dropped, unsettled).
    tb.broker::<MemoryBroker>()
        .publish("orders", &Order { id: 0 })
        .await
        .unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .panicked();
    tb.assert_shut_down();
    assert!(matches!(
        tb.run_result(),
        Err(ruststream::runtime::RustStreamError::Dispatch(_))
    ));
    // A publish after the fail-fast shutdown is rejected.
    assert!(matches!(
        tb.broker::<MemoryBroker>()
            .publish("orders", &Order { id: 1 })
            .await,
        Err(TestError::ShutDown)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skip_policy_panic_keeps_running() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(skip_panics));
    let tb = TestApp::start(app).await.unwrap();

    tb.publish("skipper", &Order { id: 0 }).await.unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("skipper")
        .assert_called_once()
        .panicked()
        .settled(HandlerResult::Ack);
    tb.assert_running();
    assert!(tb.run_result().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn perpetual_requeue_hits_the_step_budget() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(loop_forever));
    let tb = TestApp::start(app).await.unwrap();

    let result = tb.publish("loops", &Order { id: 1 }).await;
    assert!(matches!(result, Err(TestError::NotQuiescent { .. })));
    tb.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assert_not_called_when_no_input() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));
    let tb = TestApp::start(app).await.unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_not_called();
}

// --- Requeue-then-ack: a stateful handler that nacks once, proving redelivery and quiescence. ---

struct Counter {
    seen: Arc<AtomicU32>,
}

#[subscriber("retryonce")]
async fn retry_once(order: &Order, ctx: &mut Context<'_, (), Counter>) -> HandlerResult {
    let _ = order;
    if ctx.state().seen.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerResult::retry()
    } else {
        HandlerResult::Ack
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requeue_redelivers_and_settles() {
    let seen = Arc::new(AtomicU32::new(0));
    let state_seen = Arc::clone(&seen);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(move |()| {
            let seen = state_seen;
            async move { Ok::<_, std::convert::Infallible>(Counter { seen }) }
        })
        .with_broker(MemoryBroker::new(), |b| b.include(retry_once));
    let tb = TestApp::start(app).await.unwrap();

    tb.publish("retryonce", &Order { id: 1 }).await.unwrap();

    // Called twice: the first delivery requeued, the redelivery acked.
    tb.broker::<MemoryBroker>()
        .subscriber("retryonce")
        .assert_called(2)
        .settled(HandlerResult::Ack);
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}

// --- Delayed redelivery: retry_after is recorded immediately and driven by advancing time. ---

#[subscriber("delayed")]
async fn delayed_retry(order: &Order, ctx: &mut Context<'_, (), Counter>) -> HandlerResult {
    let _ = order;
    if ctx.state().seen.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerResult::retry_after(std::time::Duration::from_secs(30))
    } else {
        HandlerResult::Ack
    }
}

#[tokio::test(start_paused = true)]
async fn retry_after_redelivers_after_advancing_time() {
    let seen = Arc::new(AtomicU32::new(0));
    let state_seen = Arc::clone(&seen);
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(move |()| {
            let seen = state_seen;
            async move { Ok::<_, std::convert::Infallible>(Counter { seen }) }
        })
        .with_broker(MemoryBroker::new(), |b| b.include(delayed_retry));
    let tb = TestApp::start(app).await.unwrap();

    // The publish records the immediate NackAfter settlement and returns; the redelivery is pending.
    tb.publish("delayed", &Order { id: 1 }).await.unwrap();
    tb.broker::<MemoryBroker>()
        .subscriber("delayed")
        .assert_called_once()
        .settled(HandlerResult::NackAfter {
            delay: std::time::Duration::from_secs(30),
        });
    assert_eq!(seen.load(Ordering::SeqCst), 1);

    // Advancing past the delay fires the redelivery and drives it to settle.
    tb.advance(std::time::Duration::from_secs(30))
        .await
        .unwrap();
    tb.broker::<MemoryBroker>()
        .subscriber("delayed")
        .assert_called(2)
        .settled(HandlerResult::Ack);
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}

// --- Multi-broker: label addressing, ambiguity, and a cross-broker cascade. ---

/// Forwards each order to the `events` channel on a second broker held in state.
#[subscriber("ingress")]
async fn forward(order: &Order, ctx: &mut Context<'_, (), Egress>) -> HandlerResult {
    let event = Event { id: order.id };
    let payload = serde_json::to_vec(&event).expect("serialize");
    if ctx
        .state()
        .egress
        .publish(OutgoingMessage::new("events", &payload))
        .await
        .is_err()
    {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}

#[subscriber("events")]
async fn on_event(event: &Event) -> HandlerResult {
    let _ = event;
    HandlerResult::Ack
}

struct Egress {
    egress: ruststream::memory::MemoryPublisher,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_broker_cascade_settles_before_publish_returns() {
    let nats = MemoryBroker::new();
    let redis = MemoryBroker::new();
    let egress = redis.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(move |()| async move { Ok::<_, std::convert::Infallible>(Egress { egress }) })
        .with_broker_labeled("ingress", nats, |b| b.include(forward))
        .with_broker_labeled("egress", redis, |b| b.include(on_event));
    let tb = TestApp::start(app).await.unwrap();

    // Publishing into "ingress" drives the ingress handler, its publish into "egress", and the
    // egress handler - all before publish returns.
    tb.broker_named("ingress")
        .publish("ingress", &Order { id: 5 })
        .await
        .unwrap();

    tb.broker_named("ingress")
        .subscriber("ingress")
        .assert_called_once()
        .settled(HandlerResult::Ack);
    tb.broker_named("egress")
        .subscriber("events")
        .assert_called_once()
        .with(&Event { id: 5 });
    tb.broker_named("egress")
        .published::<Event>("events")
        .assert_called_once()
        .with(&Event { id: 5 });

    // The published messages themselves are retrievable, not just their count.
    let events: Vec<Event> = tb
        .broker_named("egress")
        .published::<Event>("events")
        .decoded();
    assert_eq!(events, vec![Event { id: 5 }]);
    let raw = tb.broker_named("egress").published::<Event>("events");
    assert_eq!(raw.messages().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unscoped_publish_is_ambiguous_with_two_brokers() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker_labeled("a", MemoryBroker::new(), |b| b.include(handle_orders))
        .with_broker_labeled("b", MemoryBroker::new(), |b| b.include(drop_all));
    let tb = TestApp::start(app).await.unwrap();

    assert!(matches!(
        tb.publish("orders", &Order { id: 1 }).await,
        Err(TestError::Ambiguous)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "more than one broker of type")]
async fn broker_by_type_panics_when_ambiguous() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker_labeled("a", MemoryBroker::new(), |b| b.include(handle_orders))
        .with_broker_labeled("b", MemoryBroker::new(), |b| b.include(drop_all));
    let tb = TestApp::start(app).await.unwrap();

    // Two brokers of the same type: addressing by type is ambiguous, use broker_named.
    let _ = tb.broker::<MemoryBroker>();
}

// --- with_state: inject a mirror state whose publisher binds to the same bus. ---

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_state_injects_a_mirror_state() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(|()| async {
            // The real startup would build a publisher; the harness replaces it below.
            Ok::<_, std::convert::Infallible>(Egress {
                egress: MemoryBroker::new().publisher(),
            })
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(forward);
            b.include(on_event);
        });
    let tb = TestApp::with_state(app, |brokers| Egress {
        egress: brokers.broker::<MemoryBroker>().publisher(),
    })
    .await
    .unwrap();

    tb.broker::<MemoryBroker>()
        .publish("ingress", &Order { id: 9 })
        .await
        .unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("events")
        .assert_called_once()
        .with(&Event { id: 9 });
}
