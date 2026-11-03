//! `ruststream::time::sleep` runs on the app's clock: the harness moves it with `advance`, the
//! way it moves every timer the framework arms.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

use crate::common;

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use common::Order;
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::testing::TestApp;
use ruststream::{subscriber, time};

/// How long the reminder waits after the order is acknowledged.
const REMINDER: Duration = Duration::from_secs(60);

/// One step short of the reminder.
const SHORT_OF_REMINDER: Duration = Duration::from_secs(59);

/// A handler that schedules a reminder once the order is acknowledged; the reminder sleeps on
/// the app's clock first.
#[subscriber("orders")]
async fn remind_later(_order: &Order, ctx: &mut Context<'_, (), Arc<AtomicU32>>) -> HandlerOutcome {
    let reminded = Arc::clone(ctx.state());
    ctx.after_ack(async move {
        time::sleep(REMINDER).await;
        reminded.fetch_add(1, Ordering::SeqCst);
    });
    HandlerOutcome::ack()
}

#[tokio::test(start_paused = true)]
async fn a_service_sleep_follows_the_harness_clock() {
    let reminded = Arc::new(AtomicU32::new(0));
    let state = Arc::clone(&reminded);
    let app = RustStream::new(AppInfo::new("reminders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(state))
        .with_broker(MemoryBroker::new(), |b| b.include(remind_later));
    let tb = TestApp::start(app).await.expect("startup failed");

    tb.message(&Order { id: 1 })
        .to("orders")
        .publish()
        .await
        .expect("publish");
    tb.advance(SHORT_OF_REMINDER).await.expect("settle");
    assert_eq!(
        reminded.load(Ordering::SeqCst),
        0,
        "the reminder must not fire before its sleep is over",
    );

    tb.advance(Duration::from_secs(1)).await.expect("settle");
    tb.drain().await;
    assert_eq!(
        reminded.load(Ordering::SeqCst),
        1,
        "advancing past the sleep must fire the reminder",
    );
}
