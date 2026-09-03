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
use ruststream::runtime::{AppInfo, HandlerOutcome, PublishError, PublishExt, RustStream};
use ruststream::testing::{Outcome, TestApp, TestError};
use ruststream::{Outgoing, Serialized, subscriber};
use serde::{Deserialize, Serialize};

#[derive(Outgoing, Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Order {
    id: u64,
}

#[derive(Outgoing, Serialize, Deserialize, PartialEq, Debug, Clone)]
#[outgoing(name = "events")]
struct Event {
    id: u64,
}

/// Bytes injected as themselves: what the decode-failure case sends, where the payload is
/// deliberately not a model. It declares no name, so the injection names its subject.
#[derive(Outgoing, Serialized)]
struct Wire(Vec<u8>);

/// Acks every order; panics on id 0 (a deliberate negative-test trigger) under the default
/// `panic = fail_fast`.
#[subscriber("orders")]
async fn handle_orders(order: &Order) -> HandlerOutcome {
    assert!(order.id != 0, "boom on id 0");
    HandlerOutcome::ack()
}

/// Drops every message (nack without requeue).
#[subscriber("dropme")]
async fn drop_all(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::drop()
}

/// Panics on id 0 but `panic = skip` keeps the service running and acks the offending message.
#[subscriber("skipper", on_failure(panic = skip))]
async fn skip_panics(order: &Order) -> HandlerOutcome {
    assert!(order.id != 0, "boom on id 0");
    HandlerOutcome::ack()
}

/// Requeues forever, to exercise the quiescence step-budget guard.
#[subscriber("loops")]
async fn loop_forever(order: &Order) -> HandlerOutcome {
    let _ = order;
    HandlerOutcome::retry()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_received_value_and_ack() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));
    let tb = TestApp::start(app).await.unwrap();

    tb.broker::<MemoryBroker>()
        .message(&Order { id: 7 })
        .to("orders")
        .publish()
        .await
        .unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with(&Order { id: 7 })
        .settled(HandlerOutcome::ack())
        .assert_outcome(Outcome::Ack);

    // The received messages can also be retrieved for custom inspection.
    let received: Vec<Order> = tb.broker::<MemoryBroker>().subscriber("orders").received();
    assert_eq!(received, vec![Order { id: 7 }]);
    let raw = tb
        .broker::<MemoryBroker>()
        .subscriber("orders")
        .received_raw();
    assert_eq!(raw.len(), 1);

    // A single-message handler is handed one message at a time, which is the shape
    // `assert_page_sizes` reports for it.
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_page_sizes(&[1]);

    tb.assert_running();
}

/// A page body with no cap on it: the whole page reaches it in one slice, which is what the
/// page-size assertion reports for an uncapped registration.
#[subscriber("uncapped")]
async fn take_page(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_uncapped_page_reaches_the_body_whole() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(take_page));
    let tb = TestApp::start(app).await.unwrap();

    tb.message(&Order { id: 1 })
        .to("uncapped")
        .publish()
        .await
        .unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("uncapped")
        .assert_called_once()
        .assert_page_sizes(&[1])
        .settled(HandlerOutcome::ack());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_drop_outcome() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(drop_all));
    let tb = TestApp::start(app).await.unwrap();

    tb.message(&Order { id: 1 })
        .to("dropme")
        .publish()
        .await
        .unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("dropme")
        .assert_called_once()
        .assert_outcome(Outcome::Drop)
        .settled(HandlerOutcome::drop());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_decode_failure() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));
    let tb = TestApp::start(app).await.unwrap();

    // Not valid JSON for `Order`: the typed adapter fails to decode, the handler never runs.
    tb.broker::<MemoryBroker>()
        .message(&Wire(b"not json".to_vec()))
        .to("orders")
        .publish()
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

    // --8<-- [start:panic]
    // The panicking delivery still drives to quiescence (the message is dropped, unsettled).
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 0 })
        .to("orders")
        .publish()
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
            .message(&Order { id: 1 })
            .to("orders")
            .publish()
            .await,
        Err(PublishError::Publish(TestError::ShutDown))
    ));
    // --8<-- [end:panic]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skip_policy_panic_keeps_running() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(skip_panics));
    let tb = TestApp::start(app).await.unwrap();

    tb.message(&Order { id: 0 })
        .to("skipper")
        .publish()
        .await
        .unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("skipper")
        .assert_called_once()
        .panicked()
        .settled(HandlerOutcome::ack());
    tb.assert_running();
    assert!(tb.run_result().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn perpetual_requeue_hits_the_step_budget() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(loop_forever));
    let tb = TestApp::start(app).await.unwrap();

    let result = tb.message(&Order { id: 1 }).to("loops").publish().await;
    assert!(matches!(
        result,
        Err(PublishError::Publish(TestError::NotQuiescent { .. }))
    ));
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

// --- Custom codec: a handler mounted with CBOR; assertions decode with the same codec. ---

#[cfg(feature = "cbor")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_codec_assertions_use_the_handlers_codec() {
    use ruststream::codec::CborCodec;

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker_codec(
        MemoryBroker::new(),
        CborCodec,
        |b| b.include(handle_orders),
    );
    let tb = TestApp::start(app).await.unwrap();

    // Inject CBOR-encoded input (the default codec would be JSON the handler can't read).
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 7 })
        .with_codec(CborCodec)
        .to("orders")
        .publish()
        .await
        .unwrap();

    // `with` (DefaultCodec) would not decode CBOR; `with_codec` uses the handler's actual codec.
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .assert_called_once()
        .with_codec(&CborCodec, &Order { id: 7 })
        .settled(HandlerOutcome::ack());
    let received: Vec<Order> = tb
        .broker::<MemoryBroker>()
        .subscriber("orders")
        .received_with(&CborCodec);
    assert_eq!(received, vec![Order { id: 7 }]);
}

// --- Requeue-then-ack: a stateful handler that nacks once, proving redelivery and quiescence. ---

struct Counter {
    seen: Arc<AtomicU32>,
}

#[subscriber("retryonce")]
async fn retry_once(order: &Order, ctx: &mut Context<'_, (), Counter>) -> HandlerOutcome {
    let _ = order;
    if ctx.state().seen.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerOutcome::retry()
    } else {
        HandlerOutcome::ack()
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

    tb.message(&Order { id: 1 })
        .to("retryonce")
        .publish()
        .await
        .unwrap();

    // Called twice: the first delivery requeued, the redelivery acked.
    tb.broker::<MemoryBroker>()
        .subscriber("retryonce")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}

// --- Delayed redelivery: retry_after is recorded immediately and driven by advancing time. ---

// --8<-- [start:retry_after]
#[subscriber("delayed")]
async fn delayed_retry(order: &Order, ctx: &mut Context<'_, (), Counter>) -> HandlerOutcome {
    let _ = order;
    if ctx.state().seen.fetch_add(1, Ordering::SeqCst) == 0 {
        HandlerOutcome::retry_after(std::time::Duration::from_secs(30))
    } else {
        HandlerOutcome::ack()
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
    tb.message(&Order { id: 1 })
        .to("delayed")
        .publish()
        .await
        .unwrap();
    tb.broker::<MemoryBroker>()
        .subscriber("delayed")
        .assert_called_once()
        .settled(HandlerOutcome::retry_after(std::time::Duration::from_secs(
            30,
        )));
    assert_eq!(seen.load(Ordering::SeqCst), 1);

    // Advancing past the delay fires the redelivery and drives it to settle.
    tb.advance(std::time::Duration::from_secs(30))
        .await
        .unwrap();
    tb.broker::<MemoryBroker>()
        .subscriber("delayed")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}
// --8<-- [end:retry_after]

// --- Multi-broker: label addressing, ambiguity, and a cross-broker cascade. ---

/// Forwards each order to the `events` channel on a second broker held in state.
#[subscriber("ingress")]
async fn forward(order: &Order, ctx: &mut Context<'_, (), Egress>) -> HandlerOutcome {
    let event = Event { id: order.id };
    if ctx.state().egress.message(&event).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

#[subscriber("events")]
async fn on_event(event: &Event) -> HandlerOutcome {
    let _ = event;
    HandlerOutcome::ack()
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
        .message(&Order { id: 5 })
        .to("ingress")
        .publish()
        .await
        .unwrap();

    tb.broker_named("ingress")
        .subscriber("ingress")
        .assert_called_once()
        .settled(HandlerOutcome::ack());
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
        tb.message(&Order { id: 1 }).to("orders").publish().await,
        Err(PublishError::Publish(TestError::Ambiguous))
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
        .on_startup(async move |()| {
            // The real startup would build a publisher; the harness replaces it below.
            Ok::<_, std::convert::Infallible>(Egress {
                egress: MemoryBroker::new().publisher(),
            })
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(forward);
            b.include(on_event);
        });
    let tb = TestApp::with_state(app, |brokers| {
        assert!(format!("{brokers:?}").contains("TestBrokers"));
        Egress {
            egress: brokers.broker::<MemoryBroker>().publisher(),
        }
    })
    .await
    .unwrap();

    tb.broker::<MemoryBroker>()
        .message(&Order { id: 9 })
        .to("ingress")
        .publish()
        .await
        .unwrap();

    tb.broker::<MemoryBroker>()
        .subscriber("events")
        .assert_called_once()
        .with(&Event { id: 9 });
}

// --- Raw inspection, empty-channel and Debug surfaces. ---

#[subscriber("echo", publish("out"))]
async fn echo(order: &Order) -> Order {
    Order { id: order.id }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspect_raw_messages_and_debug_surfaces() {
    use ruststream::codec::{Codec, JsonCodec};

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(echo);
    });
    let tb = TestApp::start(app).await.unwrap();

    let raw = JsonCodec.encode(&Order { id: 7 }).unwrap();
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 7 })
        .to("echo")
        .publish()
        .await
        .unwrap();

    // Raw payloads, for the received delivery and the published reply.
    tb.broker::<MemoryBroker>()
        .subscriber("echo")
        .assert_called_once()
        .with_raw(&raw);
    tb.broker::<MemoryBroker>()
        .published::<Order>("out")
        .assert_called_once()
        .with_raw(&raw);
    // A channel nobody published to.
    tb.broker::<MemoryBroker>()
        .published::<Order>("never")
        .assert_not_called();

    // Debug surfaces and the cooperative drain are exercised here too.
    assert!(format!("{tb:?}").contains("TestApp"));
    assert!(format!("{:?}", tb.broker::<MemoryBroker>()).contains("BrokerHandle"));
    tb.drain().await;
    assert!(tb.run_result().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "was not called")]
async fn with_on_uncalled_subscriber_panics() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));
    let tb = TestApp::start(app).await.unwrap();
    // Nothing was published, so the subscriber was not called.
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .with(&Order { id: 1 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "nothing was published")]
async fn published_with_on_empty_channel_panics() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(echo);
    });
    let tb = TestApp::start(app).await.unwrap();
    tb.broker::<MemoryBroker>()
        .published::<Order>("out")
        .with(&Order { id: 1 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "no broker labeled")]
async fn addressing_an_unknown_label_names_the_label() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker_labeled(
        "a",
        MemoryBroker::new(),
        |b| b.include(handle_orders),
    );
    let tb = TestApp::start(app).await.unwrap();

    // A typo in the label is a test-authoring mistake, so the panic has to quote it back.
    let _ = tb.broker_named("typo");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_startup_hook_is_reported_as_a_startup_error() {
    #[derive(Debug, thiserror::Error)]
    #[error("state could not be built")]
    struct StartupFailed;

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async move |()| Err::<(), _>(StartupFailed))
        .with_broker(MemoryBroker::new(), |b| b.include(handle_orders));

    let started = TestApp::start(app).await;
    match started {
        Err(TestError::Startup(source)) => {
            assert!(source.to_string().contains("state could not be built"));
        }
        other => panic!("expected a startup error, got {:?}", other.map(|_| ())),
    }
}
