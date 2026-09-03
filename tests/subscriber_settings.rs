//! Integration tests for the declarative settings a subscriber carries: the ones the attribute
//! fixes, the ones the mount site fills in through the builder, and the four source forms.
//!
//! Apps come up through `start()`, which resolves only after subscriptions are open, so each
//! message is published exactly once; the tests wait on the handlers' recorded state.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "testing"
))]

mod common;

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{Order, Wire, wait_for};
use ruststream::memory::{MemoryBroker, MemoryPosition, MemorySource};
use ruststream::runtime::{
    AppInfo, FailurePolicies, FailurePolicy, HandlerOutcome, PublishExt, Router, RustStream,
    SubscriberSettings,
};
use ruststream::testing::TestApp;
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
        .message(&Order { id: 11 })
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
        .message(&Order { id: 3 })
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
            .message(&Order { id })
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
        .message(&Wire::of(b"not json"))
        .to("failures-from-builder")
        .publish()
        .await
        .expect("publish failed");
    publisher
        .message(&Order { id: 9 })
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
        .message(&Order { id: 42 })
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
            .message(&Order { id })
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

/// The same page shape, taking the broker's own batches instead of the framework's buffer: the
/// mount site caps how much of one page reaches the body at a time. The body records nothing;
/// the harness reports both the page the broker delivered and the slices the body was handed.
#[subscriber]
async fn paginate(orders: &[Order]) -> HandlerOutcome {
    let _ = orders.len();
    HandlerOutcome::ack()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_builder_supplies_the_page_cap() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    // The whole run is in the log before the subscription opens, so the opening replay hands the
    // subscription one native page of three: nothing but the cap can split it. The harness
    // injects after startup and settles per message, so the entries are published here.
    for id in 0..3u32 {
        publisher
            .message(&Order { id })
            .to("paginate")
            .publish()
            .await
            .expect("publish failed");
    }

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        b.include(
            paginate
                .name("paginate")
                .start_at(MemoryPosition::start())
                .batch(nonzero!(2)),
        );
    });
    let tb = TestApp::start(app).await.expect("startup failed");
    tb.settle().await.expect("the replayed page settles");

    let subscriber = tb.broker::<MemoryBroker>();
    let subscriber = subscriber.subscriber("paginate");
    assert_eq!(
        subscriber.received::<Order>(),
        [Order { id: 0 }, Order { id: 1 }, Order { id: 2 }],
    );
    subscriber
        // One page arrived, carrying every replayed entry in publish order...
        .assert_called_once()
        // ... and the cap is what the body saw it through.
        .assert_page_sizes(&[2, 1])
        .settled(HandlerOutcome::ack());
    tb.shutdown().await.expect("shutdown failed");
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
            .message(&Wire::of(frame))
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
        .message(&Order { id: 5 })
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

/// The per-registration codec, the top rung of the codec ladder: a scope decoding with CBOR, and
/// registrations that name JSON for themselves.
///
/// One case per input kind the override resolves against - a decoded body, a body paired with its
/// typed header contract, and a self-deserializing one. The last reads no codec at all, so naming
/// one there changes nothing and the delivery's bytes still arrive untouched; the first two would
/// fail to decode the JSON payloads below if the scope's CBOR had won.
#[cfg(feature = "cbor")]
mod codec_override {
    use std::sync::Mutex;
    use std::time::Duration;

    use ruststream::codec::{CborCodec, JsonCodec};
    use ruststream::memory::MemoryBroker;
    use ruststream::runtime::{
        AppInfo, HandlerOutcome, Message, PublishExt, RustStream, SubscriberSettings,
    };
    use ruststream::{Outgoing, subscriber};
    use serde::{Deserialize, Serialize};

    use super::{Frame, Order, Wire, wait_for};

    /// The contract the paired case reads off the delivery's headers.
    #[derive(Serialize, Deserialize, Debug, PartialEq, schemars::JsonSchema)]
    struct Meta {
        shard: u8,
    }

    /// The outgoing side of the paired case: an [`Order`] body declaring the contract the
    /// subscriber pairs it with, so the publish is asked for the headers it must carry.
    #[derive(Serialize, Outgoing)]
    #[outgoing(headers = Meta)]
    struct PairedOrder {
        id: u32,
    }

    static DECODED: Mutex<Vec<u32>> = Mutex::new(Vec::new());
    static PAIRED: Mutex<Vec<(u32, u8)>> = Mutex::new(Vec::new());
    static PROVIDED: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

    #[subscriber]
    async fn decoded(order: &Order) -> HandlerOutcome {
        DECODED.lock().unwrap().push(order.id);
        HandlerOutcome::ack()
    }

    #[subscriber]
    async fn paired(order: &Message<Meta, Order>) -> HandlerOutcome {
        PAIRED
            .lock()
            .unwrap()
            .push((order.body.id, order.headers.shard));
        HandlerOutcome::ack()
    }

    /// A self-deserializing body with a reply, which is the shape that resolves a codec for a
    /// byte input: the plain self-deserializing mount reads none at all.
    #[subscriber(publish("codec-provided-out"))]
    async fn provided(frame: &Frame<'_>) -> Order {
        PROVIDED.lock().unwrap().push(frame.0.to_vec());
        Order { id: 0 }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_builder_overrides_the_scope_codec_per_registration() {
        let broker = MemoryBroker::new();
        let publisher = broker.publisher();

        let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker_codec(
            broker,
            CborCodec,
            |b| {
                b.include(decoded.name("codec-decoded").codec(JsonCodec));
                b.include(paired.name("codec-paired").codec(JsonCodec));
                b.include(provided.name("codec-provided").codec(JsonCodec));
            },
        );
        let running = app.start().await.expect("startup failed");

        publisher
            .message(&Order { id: 1 })
            .to("codec-decoded")
            .publish()
            .await
            .expect("publish failed");
        publisher
            .message(&PairedOrder { id: 2 })
            .to("codec-paired")
            .with_headers(&Meta { shard: 7 })
            .publish()
            .await
            .expect("publish failed");
        publisher
            .message(&Wire::of(b"\x00\xffnot json"))
            .to("codec-provided")
            .publish()
            .await
            .expect("publish failed");

        wait_for(
            || {
                !DECODED.lock().unwrap().is_empty()
                    && !PAIRED.lock().unwrap().is_empty()
                    && !PROVIDED.lock().unwrap().is_empty()
            },
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(DECODED.lock().unwrap().as_slice(), [1]);
        assert_eq!(PAIRED.lock().unwrap().as_slice(), [(2, 7)]);
        assert_eq!(
            PROVIDED.lock().unwrap().as_slice(),
            [b"\x00\xffnot json".to_vec()],
        );
        running.shutdown().await.expect("shutdown failed");
    }
}
