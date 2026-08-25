//! The publishing handler's failure paths, end to end over the memory broker: a decode failure
//! settled by the per-subscriber policy, and a reply the publisher rejects. Both are diagnosed
//! by a warning, so the test binary installs a capturing subscriber - a warning's field values
//! are only evaluated while someone listens, and an unexplained failure is half a failure.
#![cfg(all(
    feature = "macros",
    feature = "memory",
    feature = "json",
    feature = "logging"
))]

use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Once};
use std::time::Duration;

use futures::StreamExt;
use ruststream::memory::{ConnectedMemoryBroker, MemoryBroker, MemoryPublish, MemoryPublisher};
use ruststream::runtime::{AppInfo, PublishExt, RustStream, RustStreamError, TypedPublisher};
use ruststream::{
    IncomingMessage, OutgoingMessage, PairError, PublishPolicy, Publisher, Subscriber, subscriber,
};
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber as TracingSubscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};

/// Every warning this binary emitted, one string of `field=value` pairs per event.
static EVENTS: LazyLock<Arc<Mutex<Vec<String>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(Vec::new())));

/// Flattens an event's fields into `name=value` pairs.
struct Grab(Vec<String>);

impl Visit for Grab {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.push(format!("{}={value:?}", field.name()));
    }
}

/// A layer that keeps the warnings around for the assertions.
struct Capture(Arc<Mutex<Vec<String>>>);

impl<S: TracingSubscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        if *event.metadata().level() > Level::WARN {
            return;
        }
        let mut grab = Grab(Vec::new());
        event.record(&mut grab);
        self.0.lock().unwrap().push(grab.0.join(" "));
    }
}

/// Installs the capture once for the whole binary (a global subscriber, because the dispatch
/// loop runs on runtime threads the test does not own).
fn capture_logs() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        let capture = Capture(Arc::clone(&EVENTS));
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(capture))
            .expect("this binary installs no other global subscriber");
    });
}

/// Whether any captured warning carries `needle`.
fn logged(needle: &str) -> bool {
    EVENTS
        .lock()
        .unwrap()
        .iter()
        .any(|event| event.contains(needle))
}

#[derive(Debug, Serialize, Deserialize)]
struct Order {
    id: u32,
}

fn order_bytes(id: u32) -> Vec<u8> {
    serde_json::to_vec(&Order { id }).expect("an order serializes")
}

/// The reply publisher's refusal.
#[derive(Debug)]
struct Rejected;

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the reply publisher rejected the message")
    }
}

impl StdError for Rejected {}

/// Rejects the first reply, then forwards to the broker: the redelivery the failure asks for
/// must be able to succeed, so the whole failure-then-retry path is observable.
struct FailsOnce {
    armed: AtomicBool,
    inner: MemoryPublisher,
}

impl Publisher for FailsOnce {
    type Error = Rejected;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(Rejected);
        }
        self.inner.publish(msg).await.map_err(|_| Rejected)
    }
}

/// The declaration half: pure policy, paired at startup like any broker's.
struct FailsOncePolicy;

impl PublishPolicy<ConnectedMemoryBroker> for FailsOncePolicy {
    type Live = FailsOnce;

    async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
        Ok(FailsOnce {
            armed: AtomicBool::new(true),
            inner: MemoryPublish.pair(connected).await?,
        })
    }
}

/// A publishing handler whose decode failure is declared fatal.
#[subscriber("pubff", publish("pubff.out"), on_failure(decode = fail_fast))]
async fn pubff(order: &Order) -> u32 {
    order.id
}

/// A publishing handler whose reply leaves through the publisher that fails once.
#[subscriber("flaky", publish("flaky.out"))]
async fn flaky(order: &Order) -> u32 {
    order.id
}

/// `decode = fail_fast` on a publishing handler tears the service down, and the warning names
/// the subscription and the input type so the operator can find the offending producer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fail_fast_decode_failure_tears_the_service_down_and_says_why() {
    capture_logs();

    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let app = RustStream::new(AppInfo::new("pubff", "0.1.0")).with_broker(broker, |b| {
        b.include(pubff);
    });

    let running = app.start().await.expect("startup failed");
    publisher
        .raw(b"not json")
        .to("pubff")
        .publish()
        .await
        .expect("publish failed");

    tokio::time::timeout(Duration::from_secs(5), running.stopping())
        .await
        .expect("a fail-fast decode failure must tear the service down");
    let result = running.shutdown().await;

    assert!(
        matches!(result, Err(RustStreamError::Dispatch(_))),
        "the run must report the failure, got {result:?}",
    );
    assert!(
        logged("codec decode failed"),
        "the decode failure must be diagnosed: {:?}",
        EVENTS.lock().unwrap(),
    );
    assert!(
        logged("subscription=pubff"),
        "the diagnostic must name the subscription: {:?}",
        EVENTS.lock().unwrap(),
    );
}

/// A reply the publisher rejects nacks the delivery with requeue instead of losing the reply:
/// the redelivered message publishes it, and the warning names the reply destination.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_reply_publish_retries_the_delivery() {
    capture_logs();

    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    let mut replies = broker.subscribe("flaky.out");
    let app = RustStream::new(AppInfo::new("flaky", "0.1.0")).with_broker(broker, |b| {
        b.include(flaky)
            .publisher(TypedPublisher::new(FailsOncePolicy));
    });

    let running = app.start().await.expect("startup failed");
    publisher
        .raw(&order_bytes(7))
        .to("flaky")
        .publish()
        .await
        .expect("publish failed");

    let mut stream = std::pin::pin!(replies.stream());
    let reply = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the retried delivery must publish the reply")
        .expect("delivery missing")
        .expect("memory subscriber never errors");
    assert_eq!(
        reply.payload(),
        b"7",
        "the reply survives the failed attempt"
    );
    reply.ack().await.expect("ack failed");

    assert!(
        logged("reply publish failed"),
        "the failed publish must be diagnosed: {:?}",
        EVENTS.lock().unwrap(),
    );
    assert!(
        logged("reply=flaky.out"),
        "the diagnostic must name the reply destination: {:?}",
        EVENTS.lock().unwrap(),
    );

    running.shutdown().await.expect("shutdown failed");
}
