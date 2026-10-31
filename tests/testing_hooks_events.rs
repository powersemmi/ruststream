//! Which event a hook answers for: a call's hooks run after that call settled, a hook reads the
//! record as it stood at its own event, and a hook's failure fails the test however it ends.
//!
//! Two deliveries of one subscription run at once here (`workers(2)`), and each scenario orders
//! them with notifications, so the interleaving that tells a right answer from a wrong one is the
//! only one that can happen.
#![cfg(all(
    feature = "testing",
    feature = "memory",
    feature = "json",
    feature = "macros"
))]

use std::convert::Infallible;
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ruststream::memory::prelude::*;
use ruststream::runtime::Identity;
use ruststream::testing::TestApp;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc};

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
struct Order {
    id: u64,
}

#[derive(Debug, Outgoing, Serialize, Deserialize, PartialEq)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

/// What lets the second delivery go: the first one's hook has started.
#[derive(Default)]
struct Gates {
    first_hooked: Notify,
}

/// Sends the first order to the dead letter destination; the second waits for the first's hook.
#[subscriber("settling", workers(2))]
async fn settle(order: &Order, ctx: &mut Context<'_, (), Arc<Gates>>) -> HandlerOutcome {
    if order.id == 1 {
        return HandlerOutcome::retry();
    }
    ctx.state().first_hooked.notified().await;
    HandlerOutcome::ack()
}

/// Acknowledges the first order at once; the second waits for the first's hook.
#[subscriber("calls", workers(2))]
async fn call(order: &Order, ctx: &mut Context<'_, (), Arc<Gates>>) -> HandlerOutcome {
    if order.id != 1 {
        ctx.state().first_hooked.notified().await;
    }
    HandlerOutcome::ack()
}

/// Answers the first order at once; the second waits for the first's hook.
#[subscriber("replies", workers(2), reply)]
async fn reply(order: &Order, ctx: &mut Context<'_, (), Arc<Gates>>) -> Receipt {
    if order.id != 1 {
        ctx.state().first_hooked.notified().await;
    }
    Receipt { id: order.id }
}

#[subscriber("watched")]
async fn watch(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

/// The app `main` would run.
fn app(broker: MemoryBroker) -> RustStream<Identity, Arc<Gates>> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Arc::new(Gates::default())))
        .with_broker(broker, |b| {
            b.include(settle)
                .dead_letter("settling.dead")
                .include(call)
                .include(reply)
                .out_reply(Publish)
                .include(watch)
        })
}

/// Starts the app in process with `gates` as its state.
async fn start(gates: &Arc<Gates>) -> Result<TestApp<Arc<Gates>>, Box<dyn Error>> {
    let gates = Arc::clone(gates);
    Ok(TestApp::with_state(app(MemoryBroker::new()), move |_| gates).await?)
}

/// Publishes orders 1 and 2 to `name` at once.
async fn publish_both(tb: &TestApp<Arc<Gates>>, name: &str) -> Result<(), Box<dyn Error>> {
    let broker = tb.broker::<MemoryBroker>();
    let first = Order { id: 1 };
    let second = Order { id: 2 };
    let (one, two) = tokio::join!(
        broker.message(&first).to(name).publish(),
        broker.message(&second).to(name).publish(),
    );
    one?;
    two?;
    Ok(())
}

/// The first order's settlement publishes its dead letter, and that publish's hook holds it until
/// the second order's call hook ran. The first order's own call hook therefore runs only after
/// the dead letter is through, never from the second order's dispatch while the first is still
/// settling.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_hook_runs_after_its_own_settlement() -> Result<(), Box<dyn Error>> {
    let gates = Arc::new(Gates::default());
    let tb = start(&gates).await?;
    let second_hooked = Arc::new(Notify::new());
    let dead_lettered = Arc::new(AtomicBool::new(false));

    let (release, done) = (Arc::clone(&gates), Arc::clone(&dead_lettered));
    let wait = Arc::clone(&second_hooked);
    tb.broker::<MemoryBroker>()
        .published::<Order>("settling.dead")
        .on_publish(async move |_| {
            release.first_hooked.notify_one();
            wait.notified().await;
            done.store(true, Ordering::SeqCst);
        });
    let calls = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&calls);
    tb.broker::<MemoryBroker>()
        .subscriber("settling")
        .on_call(async move |settling| {
            let id = settling.received::<Order>().pop().map(|order| order.id);
            seen.lock()
                .expect("poisoned")
                .push((id, dead_lettered.load(Ordering::SeqCst)));
            if id == Some(2) {
                second_hooked.notify_one();
            }
        });

    publish_both(&tb, "settling").await?;

    assert_eq!(
        *calls.lock().expect("poisoned"),
        [(Some(2), false), (Some(1), true)]
    );
    Ok(())
}

/// The first call's hook waits until the second call is recorded and its hook ran, then reads the
/// record: it sees its own call, not the one recorded after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_hook_reads_the_record_as_it_stood_at_its_call() -> Result<(), Box<dyn Error>> {
    let gates = Arc::new(Gates::default());
    let tb = start(&gates).await?;
    let hooked = Arc::new(AtomicUsize::new(0));
    let second_hooked = Arc::new(Notify::new());
    let seen = Arc::new(Mutex::new(Vec::new()));

    let kept = Arc::clone(&seen);
    tb.broker::<MemoryBroker>()
        .subscriber("calls")
        .on_call(async move |calls| {
            if hooked.fetch_add(1, Ordering::SeqCst) == 0 {
                gates.first_hooked.notify_one();
                second_hooked.notified().await;
            } else {
                second_hooked.notify_one();
            }
            let ids: Vec<u64> = calls.received::<Order>().iter().map(|o| o.id).collect();
            kept.lock().expect("poisoned").push(ids);
        });

    publish_both(&tb, "calls").await?;

    let mut seen = seen.lock().expect("poisoned").clone();
    seen.sort_by_key(Vec::len);
    assert_eq!(seen, [vec![1], vec![1, 2]]);
    Ok(())
}

/// The first receipt's hooks run in turn: the first waits until the second receipt is published
/// and hooked, the next one reads the record. It sees the receipt that triggered it as the last
/// one, not the receipt published while it waited.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_publish_hook_reads_the_record_as_it_stood_at_its_publish() -> Result<(), Box<dyn Error>>
{
    let gates = Arc::new(Gates::default());
    let tb = start(&gates).await?;
    let hooked = Arc::new(AtomicUsize::new(0));
    let second_hooked = Arc::new(Notify::new());
    let seen = Arc::new(Mutex::new(Vec::new()));

    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .on_publish(async move |_| {
            if hooked.fetch_add(1, Ordering::SeqCst) == 0 {
                gates.first_hooked.notify_one();
                second_hooked.notified().await;
            } else {
                second_hooked.notify_one();
            }
        });
    let kept = Arc::clone(&seen);
    tb.broker::<MemoryBroker>()
        .published::<Receipt>("receipts")
        .on_publish(async move |receipts| {
            let ids: Vec<u64> = receipts.decoded().iter().map(|r| r.id).collect();
            kept.lock().expect("poisoned").push(ids);
        });

    publish_both(&tb, "replies").await?;

    let mut seen = seen.lock().expect("poisoned").clone();
    seen.sort_by_key(Vec::len);
    assert_eq!(seen, [vec![1], vec![1, 2]]);
    Ok(())
}

/// Starts the app against the in-memory broker as a running stand, with a hook on `watched` that
/// fails its first call and reports every later one, and has a producer outside the harness
/// publish two orders. Returns once the second call's hook reported, which is after the first
/// call's hook failed: the subscription runs its calls one after the other.
async fn first_hook_failed_outside_the_harness() -> Result<TestApp<Arc<Gates>>, Box<dyn Error>> {
    let broker = MemoryBroker::new();
    let producer = broker.publisher();
    let tb = TestApp::start_live(app(broker)).await?;
    let (report, mut reported) = mpsc::channel(1);
    let hooked = Arc::new(AtomicUsize::new(0));
    tb.broker::<MemoryBroker>()
        .subscriber("watched")
        .on_call(async move |_| {
            assert_ne!(
                hooked.fetch_add(1, Ordering::SeqCst),
                0,
                "the first call failed its check"
            );
            let _ = report.send(()).await;
        });

    for id in [1, 2] {
        producer
            .message(&Order { id })
            .to("watched")
            .publish()
            .await?;
    }
    reported
        .recv()
        .await
        .ok_or("the second call was not hooked")?;
    Ok(tb)
}

/// A test that never publishes through the harness still fails on the hook's failure: shutdown
/// raises it once the handlers are done.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the first call failed its check")]
async fn shutdown_raises_a_hook_failure() {
    let tb = first_hook_failed_outside_the_harness().await.unwrap();
    let _ = tb.shutdown().await;
}

/// A test that ends without a shutdown fails on the hook's failure too: the harness raises it as
/// it is dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "the first call failed its check")]
async fn dropping_the_harness_raises_a_hook_failure() {
    let tb = first_hook_failed_outside_the_harness().await.unwrap();
    drop(tb);
}
