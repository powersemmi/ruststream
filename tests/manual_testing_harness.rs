//! The macro-free counterpart of `tests/testing_harness.rs`: the fail-fast panic path and the
//! delayed-redelivery path, driven through hand-written definitions.
//!
//! The harness asserts on what the definition registered, so only the declaration side changes;
//! the two test bodies read exactly as they do with the attribute.
#![cfg(all(feature = "testing", feature = "memory", feature = "json"))]

use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{
    AllOpen, Declared, Decoded, Handler, PublishError, Settle, SubscriberBuilder, SubscriberDef,
    forms,
};
use ruststream::testing::{TestApp, TestError};
use ruststream::{CallerName, MessageHeaders, NoHeaders, OutgoingDestination};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
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

impl<State: Send + Sync> Handler<Order, (), State> for HandleOrders {
    fn handle(
        &self,
        order: &Order,
        _ctx: &mut Context<'_, (), State>,
    ) -> impl Future<Output = Settle> + Send {
        // The panic belongs inside the future: the dispatcher's unwind guard wraps what `handle`
        // returns, not the call that builds it.
        let id = order.id;
        async move {
            assert!(id != 0, "boom on id 0");
            HandlerResult::Ack.into()
        }
    }
}

impl Declared for HandleOrders {
    type Form = forms::Subscribing;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("orders"))
    }
}

impl SubscriberDef for HandleOrders {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("orders")
    }

    fn into_handler(self) -> Self {
        self
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fail_fast_panic_shuts_down_and_blocks_further_publishes() {
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(HandleOrders));
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
/// A handler bound to one state type: naming `Counter` in the `Handler` impl is what the
/// attribute's `ctx: &mut Context<'_, (), Counter>` parameter declares.
struct DelayedRetry;

impl Handler<Order, (), Counter> for DelayedRetry {
    fn handle(
        &self,
        _order: &Order,
        ctx: &mut Context<'_, (), Counter>,
    ) -> impl Future<Output = Settle> + Send {
        ready(if ctx.state().seen.fetch_add(1, Ordering::SeqCst) == 0 {
            HandlerResult::retry_after(Duration::from_secs(30)).into()
        } else {
            HandlerResult::Ack.into()
        })
    }
}

impl Declared for DelayedRetry {
    type Form = forms::Subscribing;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("delayed"))
    }
}

impl SubscriberDef for DelayedRetry {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("delayed")
    }

    fn into_handler(self) -> Self {
        self
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
        .with_broker(MemoryBroker::new(), |b| b.include(DelayedRetry));
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
        .settled(HandlerResult::NackAfter {
            delay: Duration::from_secs(30),
        });
    assert_eq!(seen.load(Ordering::SeqCst), 1);

    // Advancing past the delay fires the redelivery and drives it to settle.
    tb.advance(Duration::from_secs(30)).await.unwrap();
    tb.broker::<MemoryBroker>()
        .subscriber("delayed")
        .assert_called(2)
        .settled(HandlerResult::Ack);
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}
// --8<-- [end:retry_after]
