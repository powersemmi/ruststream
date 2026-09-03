//! The macro-free counterpart of `tests/testing_harness.rs`: the fail-fast panic path and the
//! delayed-redelivery path, driven through hand-written handler bodies.
//!
//! The harness asserts on what the mount registered, so only the declaration side changes; the two
//! test bodies read exactly as they do with the attribute.
#![cfg(all(feature = "testing", feature = "memory", feature = "json"))]

use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::PublishError;
use ruststream::testing::{TestApp, TestError};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// The `Outgoing` derive by hand: no declared name, so the test names the destination itself.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

/// Acks every order; panics on id 0 (a deliberate negative-test trigger) under the default
/// `panic = fail_fast` policy the definition leaves in place.
struct HandleOrders;

impl Handle<Order> for HandleOrders {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        // The adapter builds this future inside the dispatcher's unwind guard, so the panic is
        // caught rather than escaping the call.
        assert!(order.id != 0, "boom on id 0");
        ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_fast_panic_shuts_down_and_blocks_further_publishes() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", HandleOrders).build());
    });
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

/// The state the delayed handler counts its deliveries in.
struct Counter {
    seen: Arc<AtomicU32>,
}

// --- Delayed redelivery: retry_after is recorded immediately and driven by advancing time. ---

// --8<-- [start:retry_after]
/// A handler bound to one state type: naming `Counter` in the state position of the `Handle` impl
/// is what the attribute's `ctx: &mut Context<'_, (), Counter>` parameter declares. The mount reads
/// the state off the impl, so the definition is built the same way as a stateless one.
struct DelayedRetry;

impl Handle<Order, (), (), (), Counter> for DelayedRetry {
    fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), Counter>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if ctx.state().seen.fetch_add(1, Ordering::SeqCst) == 0 {
            return ready(Err(HandlerOutcome::retry_after(Duration::from_secs(30))));
        }
        ready(Ok(()))
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
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("delayed", DelayedRetry).build());
        });
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
        .settled(HandlerOutcome::retry_after(Duration::from_secs(30)));
    assert_eq!(seen.load(Ordering::SeqCst), 1);

    // Advancing past the delay fires the redelivery and drives it to settle.
    tb.advance(Duration::from_secs(30)).await.unwrap();
    tb.broker::<MemoryBroker>()
        .subscriber("delayed")
        .assert_called(2)
        .settled(HandlerOutcome::ack());
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}
// --8<-- [end:retry_after]
