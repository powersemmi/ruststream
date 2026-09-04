//! The macro-free counterpart of `tests/app_start.rs`: the background-start handle
//! (`RustStream::start` -> `RunningApp`) driven by a hand-written subscriber definition.
//!
//! `start()` and `shutdown()` are plain methods on the app, so the run machinery is reached the
//! same way with the attribute off; only the definition changes.
#![cfg(all(feature = "memory", feature = "json"))]

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time::timeout;

/// What the service carries; the run machinery, not the payload, is the subject.
#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct Order {
    id: u32,
}

// The `Outgoing` derive by hand: no declared name, so the test names the destination itself.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// `notify_one` stores a permit, so the handler may fire before the test awaits.
static SEEN: Notify = Notify::const_new();

/// Signals the test that a delivery arrived; the run machinery, not the body, is the subject.
struct Observe;

impl Handle<Order> for Observe {
    fn handle(
        &self,
        _order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        SEEN.notify_one();
        ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_resolves_running_and_shutdown_completes() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .with_broker(broker, |b| {
            b.include(subscriber("started.orders", Observe).build());
        });

    // --8<-- [start:handle]
    // `start` resolves only once subscriptions are open, so one publish is guaranteed to land.
    let running = app.start().await.expect("startup failed");
    publisher
        .message(&Order { id: 1 })
        .to("started.orders")
        .publish()
        .await
        .expect("publish failed");
    timeout(Duration::from_secs(5), SEEN.notified())
        .await
        .expect("handler never saw the message");

    running.shutdown().await.expect("graceful shutdown failed");
    // --8<-- [end:handle]
}
