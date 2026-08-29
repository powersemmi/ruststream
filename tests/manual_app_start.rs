//! The macro-free counterpart of `tests/app_start.rs`: the background-start handle
//! (`RustStream::start` -> `RunningApp`) driven by a hand-written subscriber definition.
//!
//! `start()` and `shutdown()` are plain methods on the app, so the run machinery is reached the
//! same way with the attribute off; only the definition changes.
#![cfg(all(feature = "memory", feature = "json"))]

mod common;

use std::future::{Future, ready};
use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::prelude::*;
use ruststream::runtime::{
    AllOpen, Declared, Decoded, Handler, Settle, SubscriberBuilder, SubscriberDef, forms,
};
use tokio::sync::Notify;
use tokio::time::timeout;

use common::{Order, order_bytes};

// `notify_one` stores a permit, so the handler may fire before the test awaits.
static SEEN: Notify = Notify::const_new();

/// Signals the test that a delivery arrived; the run machinery, not the body, is the subject.
struct Observe;

impl<State: Send + Sync> Handler<Order, (), State> for Observe {
    fn handle(
        &self,
        _order: &Order,
        _ctx: &mut Context<'_, (), State>,
    ) -> impl Future<Output = Settle> + Send {
        SEEN.notify_one();
        ready(HandlerResult::Ack.into())
    }
}

impl Declared for Observe {
    type Form = forms::Subscribing;
    type Settings = SubscriberBuilder<Self, Name, AllOpen>;

    fn declare(self) -> Self::Settings {
        SubscriberBuilder::new(self, Name::new("started.orders"))
    }
}

impl SubscriberDef for Observe {
    type Input = Decoded<Order>;
    type Context = ();
    type Handler = Self;
    type Source = Name;

    fn source(&self) -> Self::Source {
        Name::new("started.orders")
    }

    fn into_handler(self) -> Self {
        self
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_resolves_running_and_shutdown_completes() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .with_broker(broker, |b| b.include(Observe));

    // --8<-- [start:handle]
    // `start` resolves only once subscriptions are open, so one publish is guaranteed to land.
    let running = app.start().await.expect("startup failed");
    publisher
        .raw(&order_bytes(1))
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
