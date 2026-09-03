//! Integration tests for the declarative settings a subscriber carries: the ones the attribute
//! fixes, the ones the mount site fills in through the builder, and the four source forms.
//!
//! Apps come up through `start()`, which resolves only after subscriptions are open, so each
//! message is published exactly once; the tests wait on the handlers' recorded state.
#![cfg(feature = "macros")]

mod common;

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Order, order_bytes, wait_for};
use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySource};
use ruststream::runtime::{
    AppInfo, FailurePolicies, FailurePolicy, HandlerOutcome, PublishExt, Router, RustStream,
    SubscriberSettings,
};
use ruststream::{Deserialized, nonzero, subscriber};

/// The payload view the raw batch body below takes, one element per delivery in the page.
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

static NAMED: Mutex<Vec<u32>> = Mutex::new(Vec::new());

/// The shortest source form: the by-name source with its value left to the mount site.
#[subscriber]
async fn audit(order: &Order) -> HandlerOutcome {
    NAMED.lock().unwrap().push(order.id);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bare_attribute_is_named_at_the_mount_site() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    // The name is a value the service only knows here, which is the whole point of the form.
    let subject = format!("audit-{}", 7);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(broker, |b| b.include(audit.name(subject.clone())));
    let running = app.start().await.expect("startup failed");

    publisher
        .raw(&order_bytes(11))
        .to(&*subject)
        .publish()
        .await
        .expect("publish failed");
    wait_for(|| !NAMED.lock().unwrap().is_empty(), Duration::from_secs(5)).await;
    assert_eq!(NAMED.lock().unwrap().as_slice(), [11]);
    running.shutdown().await.expect("shutdown failed");
}

static KIND: Mutex<Vec<u32>> = Mutex::new(Vec::new());

/// A named kind carrying only what it needs to exist: the value arrives through the builder,
/// which constructs it through the kind's own from-name constructor.
#[subscriber(MemorySource)]
async fn record(order: &Order) -> HandlerOutcome {
    KIND.lock().unwrap().push(order.id);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_named_kind_is_built_from_the_name_the_mount_site_gives() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        // `map_source` is the hook a broker's own settings trait layers on; the identity
        // transform pins that it composes between the name and the mount.
        b.include(record.name("record-kind").map_source(|source| source));
    });
    let running = app.start().await.expect("startup failed");

    publisher
        .raw(&order_bytes(3))
        .to("record-kind")
        .publish()
        .await
        .expect("publish failed");
    wait_for(|| !KIND.lock().unwrap().is_empty(), Duration::from_secs(5)).await;
    assert_eq!(KIND.lock().unwrap().as_slice(), [3]);
    running.shutdown().await.expect("shutdown failed");
}

static CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// The worker policy is left open by the attribute and named at the mount site.
#[subscriber("workers-from-builder")]
async fn parallel(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    let in_flight = CONCURRENT.fetch_add(1, Ordering::SeqCst) + 1;
    PEAK.fetch_max(in_flight, Ordering::SeqCst);
    // Yield so a second delivery can overlap this one when the pool allows it.
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    CONCURRENT.fetch_sub(1, Ordering::SeqCst);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_builder_supplies_the_worker_policy() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(broker, |b| b.include(parallel.workers(nonzero!(4))));
    let running = app.start().await.expect("startup failed");

    for id in 0..4u32 {
        publisher
            .raw(&order_bytes(id))
            .to("workers-from-builder")
            .publish()
            .await
            .expect("publish failed");
    }
    wait_for(|| PEAK.load(Ordering::SeqCst) > 1, Duration::from_secs(5)).await;
    running.shutdown().await.expect("shutdown failed");
}

static SKIPPED: Mutex<Vec<u32>> = Mutex::new(Vec::new());

/// The failure policies are left open by the attribute and named at the mount site.
#[subscriber("failures-from-builder")]
async fn tolerant(order: &Order) -> HandlerOutcome {
    SKIPPED.lock().unwrap().push(order.id);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_builder_supplies_the_failure_policies() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(tolerant.on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip)));
    });
    let running = app.start().await.expect("startup failed");

    // The undecodable payload is skipped by the mount-site policy; the next one still arrives.
    publisher
        .raw(b"not json")
        .to("failures-from-builder")
        .publish()
        .await
        .expect("publish failed");
    publisher
        .raw(&order_bytes(9))
        .to("failures-from-builder")
        .publish()
        .await
        .expect("publish failed");
    wait_for(
        || !SKIPPED.lock().unwrap().is_empty(),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(SKIPPED.lock().unwrap().as_slice(), [9]);
    running.shutdown().await.expect("shutdown failed");
}

static REPLAYED: Mutex<Vec<u32>> = Mutex::new(Vec::new());

/// The start position is left open by the attribute and named at the mount site.
#[subscriber(MemorySource)]
async fn replay(order: &Order) -> HandlerOutcome {
    REPLAYED.lock().unwrap().push(order.id);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_builder_supplies_the_start_position() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    // Published before the service exists: only a subscription opened at the start sees it.
    publisher
        .raw(&order_bytes(42))
        .to("replay")
        .publish()
        .await
        .expect("publish failed");

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(replay.name("replay").start_at(MemoryPosition::start()));
    });
    let running = app.start().await.expect("startup failed");

    wait_for(
        || !REPLAYED.lock().unwrap().is_empty(),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(REPLAYED.lock().unwrap().as_slice(), [42]);
    running.shutdown().await.expect("shutdown failed");
}

static BUFFERED: Mutex<Vec<Vec<u32>>> = Mutex::new(Vec::new());

/// A batch shape read off the signature; where the batches come from is settled at the mount.
#[subscriber]
async fn correlate(orders: &[Order]) -> HandlerOutcome {
    BUFFERED
        .lock()
        .unwrap()
        .push(orders.iter().map(|o| o.id).collect());
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_builder_supplies_the_buffer() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(
            correlate
                .name("correlate")
                .buffered(nonzero!(8), Duration::from_millis(5)),
        );
    });
    let running = app.start().await.expect("startup failed");

    for id in 0..3u32 {
        publisher
            .raw(&order_bytes(id))
            .to("correlate")
            .publish()
            .await
            .expect("publish failed");
    }
    wait_for(
        || BUFFERED.lock().unwrap().iter().map(Vec::len).sum::<usize>() >= 3,
        Duration::from_secs(5),
    )
    .await;
    let flattened: Vec<u32> = BUFFERED.lock().unwrap().iter().flatten().copied().collect();
    assert_eq!(flattened, vec![0, 1, 2]);
    running.shutdown().await.expect("shutdown failed");
}

static FRAMES: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

/// A batch of payloads: the typed batch without the decode step, borrowed from the batch's own
/// messages.
#[subscriber("frames")]
async fn ingest(frames: &[Frame<'_>]) -> HandlerOutcome {
    FRAMES
        .lock()
        .unwrap()
        .extend(frames.iter().map(|f| f.0.to_vec()));
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_raw_batch_handler_borrows_the_payloads() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let app =
        RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| b.include(ingest));
    let running = app.start().await.expect("startup failed");

    for frame in [b"one".as_slice(), b"two".as_slice()] {
        publisher
            .raw(frame)
            .to("frames")
            .publish()
            .await
            .expect("publish failed");
    }
    wait_for(|| FRAMES.lock().unwrap().len() >= 2, Duration::from_secs(5)).await;
    assert_eq!(
        FRAMES.lock().unwrap().as_slice(),
        [b"one".to_vec(), b"two".to_vec()],
    );
    running.shutdown().await.expect("shutdown failed");
}

static ROUTED: Mutex<Vec<u32>> = Mutex::new(Vec::new());

/// The same surface on the router: `include` takes the settings builder there too.
#[subscriber]
async fn routed(order: &Order) -> HandlerOutcome {
    ROUTED.lock().unwrap().push(order.id);
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_router_mounts_the_settings_builder() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let routes = Router::<MemoryBroker>::new().include(routed.name("routed").workers(nonzero!(2)));
    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .with_broker(broker, |b| b.include_router(routes));
    let running = app.start().await.expect("startup failed");

    publisher
        .raw(&order_bytes(5))
        .to("routed")
        .publish()
        .await
        .expect("publish failed");
    wait_for(
        || !ROUTED.lock().unwrap().is_empty(),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(ROUTED.lock().unwrap().as_slice(), [5]);
    running.shutdown().await.expect("shutdown failed");
}
