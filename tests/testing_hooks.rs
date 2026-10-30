//! Hooks on the harness's handles: a test acts at the moment a subscription is called or a message
//! is published, instead of reading the record once the publish returned.
#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::error::Error;
use std::future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use ruststream::memory::prelude::*;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::time;

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "audit")]
struct Note {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "bulk")]
struct Line {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Note)]
struct Audit;

/// Answers every order with a receipt, and files a copy of it through the audit slot.
#[subscriber("orders", reply)]
async fn accept(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> Receipt {
    let _ = audit.message(&Note { id: order.id }).publish().await;
    Receipt { id: order.id }
}

/// Takes what arrived as one batch.
#[subscriber("bulk")]
async fn take_bulk(lines: &[Line]) -> HandlerOutcome {
    let _ = lines.len();
    HandlerOutcome::ack()
}

/// The app `main` would run.
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(accept)
            .out_reply(Publish)
            .out(Audit, Publish)
            .build();
        b.include(take_bulk.batch(nonzero!(8)));
    })
}

/// Counts the calls of `orders` through a hook, and checks inside it that the record it is handed
/// already holds the call that triggered it.
fn count_calls(tb: &TestApp<()>) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .on_call(async move |orders| {
            let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
            orders.assert_called(n).settled(HandlerOutcome::ack());
        });
    calls
}

/// Counts the publishes to `receipts` through a hook, keeping the last one it saw.
fn count_receipts(tb: &TestApp<()>) -> (Arc<AtomicUsize>, Arc<Mutex<Vec<u64>>>) {
    let publishes = Arc::new(AtomicUsize::new(0));
    let last = Arc::new(Mutex::new(Vec::new()));
    let (seen, kept) = (Arc::clone(&publishes), Arc::clone(&last));
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .on_publish(async move |receipts| {
            let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
            let decoded = receipts.assert_called(n).decoded();
            kept.lock().expect("poisoned").push(decoded[n - 1].id);
        });
    (publishes, last)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hook_counts_the_calls_as_they_happen() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let calls = count_calls(&tb);

    for id in 1..=3 {
        tb.broker::<MemoryBroker>()
            .message(&Order { id })
            .publish()
            .await?;
        // The publish returned after the hook ran, so the count is already there.
        assert_eq!(calls.load(Ordering::SeqCst), usize::try_from(id)?);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hook_counts_the_publishes_the_service_makes() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let (publishes, last) = count_receipts(&tb);

    for id in [4, 9] {
        tb.broker::<MemoryBroker>()
            .message(&Order { id })
            .publish()
            .await?;
    }

    assert_eq!(publishes.load(Ordering::SeqCst), 2);
    assert_eq!(*last.lock().expect("poisoned"), [4, 9]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hook_sees_the_publishes_of_the_test_itself() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let orders = Arc::new(Mutex::new(Vec::new()));
    let kept = Arc::clone(&orders);
    tb.broker::<MemoryBroker>()
        .published::<Order>("orders")
        .on_publish(async move |published| {
            let last = published.decoded().pop().map(|order| order.id);
            kept.lock().expect("poisoned").extend(last);
        });

    tb.broker::<MemoryBroker>()
        .message(&Order { id: 2 })
        .publish()
        .await?;

    assert_eq!(*orders.lock().expect("poisoned"), [2]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hook_on_an_out_slot_counts_its_publishes() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let audited = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&audited);
    tb.out::<Audit>().on_publish(async move |audit| {
        let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
        audit.assert_called(n);
    });

    for id in [1, 2] {
        tb.broker::<MemoryBroker>()
            .message(&Order { id })
            .publish()
            .await?;
    }

    assert_eq!(audited.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_calls_the_hook_once_per_batch() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let batches = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&batches);
    tb.broker::<MemoryBroker>()
        .subscriber("bulk")
        .on_call(async move |bulk| {
            let n = seen.fetch_add(1, Ordering::SeqCst) + 1;
            bulk.assert_batch_sizes(&vec![1; n]);
        });

    for id in [1, 2] {
        tb.broker::<MemoryBroker>()
            .message(&Line { id })
            .publish()
            .await?;
    }

    assert_eq!(batches.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hooks_on_one_handle_run_in_the_order_they_were_registered() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let order = Arc::new(Mutex::new(Vec::new()));
    for hook in ["first", "second", "third"] {
        let order = Arc::clone(&order);
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .on_call(async move |_| order.lock().expect("poisoned").push(hook));
    }

    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1 })
        .publish()
        .await?;

    assert_eq!(
        *order.lock().expect("poisoned"),
        ["first", "second", "third"]
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hook_applies_from_its_registration_and_to_its_own_handle() -> Result<(), Box<dyn Error>>
{
    let tb = TestApp::start(app()).await?;
    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1 })
        .publish()
        .await?;

    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .on_call(async move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
        });
    let elsewhere = Arc::new(AtomicUsize::new(0));
    let other = Arc::clone(&elsewhere);
    tb.broker::<MemoryBroker>()
        .published::<Order>("refunds")
        .on_publish(async move |_| {
            other.fetch_add(1, Ordering::SeqCst);
        });

    tb.broker::<MemoryBroker>()
        .message(&Order { id: 2 })
        .publish()
        .await?;

    // The call before the registration did not run the hook, and nothing reached `refunds`.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(elsewhere.load(Ordering::SeqCst), 0);
    Ok(())
}

/// A hook that awaits holds the publish until it is done, timers included: the hook's sleep runs
/// on the test's clock.
#[tokio::test(start_paused = true)]
async fn the_publish_waits_for_a_hook_that_awaits() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let done = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&done);
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .on_call(async move |_| {
            time::sleep(Duration::from_secs(60)).await;
            seen.fetch_add(1, Ordering::SeqCst);
        });
    let started = time::Instant::now();

    tb.broker::<MemoryBroker>()
        .message(&Order { id: 1 })
        .publish()
        .await?;

    assert_eq!(done.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() >= Duration::from_secs(60));
    Ok(())
}

/// What a hook that never completes shows the test: when it started, and when its future was
/// dropped.
#[derive(Default)]
struct Hang {
    entered: Notify,
    dropping: Notify,
    /// When set, the drop of the hook's future holds until the test meets it here.
    hold: Option<Barrier>,
    dropped: AtomicBool,
    gone: Notify,
}

/// Held by the hook's future, so it is dropped with it.
struct Held(Arc<Hang>);

impl Drop for Held {
    fn drop(&mut self) {
        self.0.dropping.notify_one();
        if let Some(hold) = &self.0.hold {
            hold.wait();
        }
        self.0.dropped.store(true, Ordering::SeqCst);
        self.0.gone.notify_one();
    }
}

/// The body of a hook that never completes.
async fn hang(state: Arc<Hang>) {
    let _held = Held(Arc::clone(&state));
    state.entered.notify_one();
    future::pending::<()>().await;
}

/// Registers a hook that never completes on the calls of `orders`.
fn hang_on_call(tb: &TestApp<()>, state: Hang) -> Arc<Hang> {
    let state = Arc::new(state);
    let hook = Arc::clone(&state);
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .on_call(async move |_| hang(Arc::clone(&hook)).await);
    state
}

/// Publishes an order and goes on once the hook it triggers has started: the publish itself waits
/// for the hook, which never ends, and is dropped here.
async fn publish_until_hung(tb: &TestApp<()>, state: &Hang) {
    tokio::select! {
        sent = tb.broker::<MemoryBroker>().message(&Order { id: 1 }).publish() => {
            panic!("the publish returned while its hook still waits: {sent:?}");
        }
        () = state.entered.notified() => {}
    }
}

/// A hook that never completes is stopped with the handler the shutdown timeout aborted, and
/// `shutdown` returns only once the hook is gone.
#[tokio::test(start_paused = true)]
async fn shutdown_stops_a_hook_whose_handler_it_aborted() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app().shutdown_timeout(Duration::from_secs(5))).await?;
    let state = hang_on_call(
        &tb,
        Hang {
            hold: Some(Barrier::new(2)),
            ..Hang::default()
        },
    );
    publish_until_hung(&tb, &state).await;

    let shutdown = tb.shutdown();
    tokio::pin!(shutdown);
    // The hook is being dropped and is held there: the shutdown has nothing left to wait for
    // but the hook, so the paused clock moves on only while the shutdown waits for it.
    tokio::select! {
        biased;
        stopped = &mut shutdown => panic!("shutdown returned before the hook was dropped: {stopped:?}"),
        () = state.dropping.notified() => {}
    }
    tokio::select! {
        biased;
        stopped = &mut shutdown => panic!("shutdown returned while the hook was dropping: {stopped:?}"),
        () = time::sleep(Duration::from_secs(60)) => {}
    }
    if let Some(hold) = &state.hold {
        hold.wait();
    }
    shutdown.await?;

    assert!(
        state.dropped.load(Ordering::SeqCst),
        "the hook still runs after shutdown returned",
    );
    Ok(())
}

/// A hook stops when the publish waiting for it is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hook_stops_with_the_publish_that_waits_for_it() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let state = Arc::new(Hang::default());
    let hook = Arc::clone(&state);
    tb.broker::<MemoryBroker>()
        .published::<Order>("orders")
        .on_publish(async move |_| hang(Arc::clone(&hook)).await);

    publish_until_hung(&tb, &state).await;

    // The bound only turns a hook that is never dropped into a failure instead of a hang.
    time::timeout(Duration::from_secs(10), state.gone.notified())
        .await
        .map_err(|_| "the hook still runs after the publish waiting for it was dropped")?;
    Ok(())
}

/// A harness dropped without a shutdown stops the hooks still running.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_harness_stops_its_hooks() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start(app()).await?;
    let state = hang_on_call(&tb, Hang::default());
    publish_until_hung(&tb, &state).await;

    drop(tb);

    time::timeout(Duration::from_secs(10), state.gone.notified())
        .await
        .map_err(|_| "the hook still runs after the harness was dropped")?;
    Ok(())
}

/// A failed assertion inside a hook fails the test from the publish that drove the event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "subscriber \"orders\" was called 1 times, expected 2")]
async fn a_failed_assertion_in_a_hook_fails_the_publish() {
    let tb = TestApp::start(app()).await.unwrap();
    tb.broker::<MemoryBroker>()
        .subscriber("orders")
        .on_call(async move |orders| {
            orders.assert_called(2);
        });

    let _ = tb
        .broker::<MemoryBroker>()
        .message(&Order { id: 1 })
        .publish()
        .await;
}

/// Against a live stand the same hooks count the same events: the harness records them in the
/// runtime, not in the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hooks_count_calls_and_publishes_live() -> Result<(), Box<dyn Error>> {
    let tb = TestApp::start_live(app()).await?;
    let calls = count_calls(&tb);
    let (publishes, last) = count_receipts(&tb);

    for id in [3, 5] {
        tb.broker::<MemoryBroker>()
            .message(&Order { id })
            .publish()
            .await?;
    }

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(publishes.load(Ordering::SeqCst), 2);
    assert_eq!(*last.lock().expect("poisoned"), [3, 5]);
    tb.shutdown().await?;
    Ok(())
}
