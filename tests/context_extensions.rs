//! Integration tests for the per-delivery `Context` extensions: isolation across deliveries, a
//! broker-contributed extension (via the `IncomingMessage` seam) reaching the handler, a
//! middleware-written extension reaching a downstream handler, and `ctx.state()` still reaching
//! app state. All use the in-memory broker.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::memory::{MemoryBroker, MemoryMessage, MemorySubscriber};
use ruststream::runtime::{
    AppInfo, Context, Handler, HandlerExt, HandlerMetadata, HandlerResult, Layer, RustStream,
    Settle,
};
use ruststream::{AckError, Extensions, Headers, IncomingMessage, OutgoingMessage, Publisher};
use tokio::sync::Notify;

/// A broker-supplied per-delivery value, the kind a real broker would stash (an offset, a commit
/// token, a reply-to handle).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct DeliveryTag(u32);

/// Wraps a `MemoryMessage` and contributes a `DeliveryTag` through the `IncomingMessage`
/// extensions seam, standing in for a broker that attaches native per-delivery metadata.
struct TaggedMessage {
    inner: MemoryMessage,
    tag: u32,
}

impl IncomingMessage for TaggedMessage {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &Headers {
        self.inner.headers()
    }

    fn extensions(&self) -> Extensions {
        let mut ext = Extensions::new();
        ext.insert(DeliveryTag(self.tag));
        ext
    }

    async fn ack(self) -> Result<(), AckError> {
        self.inner.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.inner.nack(requeue).await
    }
}

/// A subscriber that yields `TaggedMessage`s, numbering each delivery so the contributed tag
/// differs per message (used to prove isolation as well as delivery).
struct TaggedSubscriber {
    inner: MemorySubscriber,
    next_tag: u32,
}

impl ruststream::Subscriber for TaggedSubscriber {
    type Message = TaggedMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.inner.stream().map(|item| {
            self.next_tag += 1;
            item.map(|inner| TaggedMessage {
                inner,
                tag: self.next_tag,
            })
        })
    }
}

async fn wait_for(mut cond: impl FnMut() -> bool, timeout: Duration) {
    let result = tokio::time::timeout(timeout, async {
        while !cond() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(result.is_ok(), "condition not met within {timeout:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_contributed_extension_reaches_handler() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let seen: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = Arc::clone(&seen);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        let subscriber = TaggedSubscriber {
            inner: b.broker().subscribe("orders"),
            next_tag: 0,
        };
        b.handle(
            subscriber,
            move |_msg: &TaggedMessage, ctx: &mut Context| {
                // The broker-contributed extension is readable from the handler's context.
                let tag = ctx.get::<DeliveryTag>().copied();
                let seen = Arc::clone(&seen_clone);
                async move {
                    if let Some(DeliveryTag(n)) = tag {
                        seen.lock().expect("poisoned").push(n);
                    }
                    HandlerResult::Ack
                }
            },
            HandlerMetadata::raw("orders"),
        );
    });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    publisher
        .publish(OutgoingMessage::new("orders", b"a"))
        .await
        .unwrap();
    publisher
        .publish(OutgoingMessage::new("orders", b"b"))
        .await
        .unwrap();

    wait_for(
        || seen.lock().expect("poisoned").len() >= 2,
        Duration::from_secs(5),
    )
    .await;

    // Each delivery sees its own tag: the seam delivers a fresh value per message, and one
    // delivery's value never leaks into the next.
    assert_eq!(*seen.lock().expect("poisoned"), vec![1, 2]);

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

/// A layer that writes a per-delivery extension before the inner handler runs, then asserts the
/// extension it wrote is gone on the next delivery (proving per-delivery isolation through the
/// dispatch loop, not just within one `Extensions` value).
struct StampLayer {
    counter: Arc<std::sync::atomic::AtomicU32>,
}

struct StampHandler<H> {
    inner: H,
    counter: Arc<std::sync::atomic::AtomicU32>,
}

impl<H> Layer<H> for StampLayer {
    type Handler = StampHandler<H>;

    fn layer(&self, inner: H) -> StampHandler<H> {
        StampHandler {
            inner,
            counter: Arc::clone(&self.counter),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Stamp(u32);

impl<M, H> Handler<M> for StampHandler<H>
where
    M: Sync,
    H: Handler<M>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_>) -> Settle {
        // No value should survive from a previous delivery: the dispatch loop builds a fresh
        // context each time.
        assert!(
            ctx.get::<Stamp>().is_none(),
            "extension leaked across deliveries"
        );
        let n = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ctx.insert(Stamp(n));
        self.inner.handle(msg, ctx).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn middleware_written_extension_reaches_downstream_handler_and_is_isolated() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let seen: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = Arc::clone(&seen);
    let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));

    let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |b| {
        let subscriber = b.broker().subscribe("orders");
        let handler = {
            let layer = StampLayer {
                counter: Arc::clone(&counter),
            };
            (move |_msg: &MemoryMessage, ctx: &mut Context| {
                // The layer ran first and wrote a per-delivery Stamp; the downstream handler reads
                // it back from the same context.
                let stamp = ctx.get::<Stamp>().copied();
                let seen = Arc::clone(&seen_clone);
                async move {
                    if let Some(Stamp(n)) = stamp {
                        seen.lock().expect("poisoned").push(n);
                    }
                    HandlerResult::Ack
                }
            })
            .with(layer)
        };
        b.handle(subscriber, handler, HandlerMetadata::raw("orders"));
    });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    publisher
        .publish(OutgoingMessage::new("orders", b"a"))
        .await
        .unwrap();
    publisher
        .publish(OutgoingMessage::new("orders", b"b"))
        .await
        .unwrap();

    wait_for(
        || seen.lock().expect("poisoned").len() >= 2,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(*seen.lock().expect("poisoned"), vec![0, 1]);

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}

struct AppPrefix(String);

/// What the handler observed: the app-state prefix (through `state()`) and the per-delivery `u8`
/// extension (through `get`), which is empty here.
type Observed = Option<(Option<String>, Option<u8>)>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_still_reaches_app_state_separately_from_extensions() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let seen: Arc<Mutex<Observed>> = Arc::new(Mutex::new(None));
    let seen_clone = Arc::clone(&seen);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .insert_state(AppPrefix("svc".to_owned()))
        .with_broker(broker, |b| {
            let subscriber = b.broker().subscribe("orders");
            b.handle(
                subscriber,
                move |_msg: &MemoryMessage, ctx: &mut Context| {
                    // App state through state(); the per-delivery map is empty for this type.
                    let prefix = ctx.state().get::<AppPrefix>().map(|p| p.0.clone());
                    // The same accessor name on the per-delivery map sees a different (empty) store.
                    let ext = ctx.get::<u8>().copied();
                    let seen = Arc::clone(&seen_clone);
                    async move {
                        *seen.lock().expect("poisoned") = Some((prefix, ext));
                        HandlerResult::Ack
                    }
                },
                HandlerMetadata::raw("orders"),
            );
        });

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let run = tokio::spawn(app.run_until(async move { shutdown_signal.notified().await }));

    publisher
        .publish(OutgoingMessage::new("orders", b"x"))
        .await
        .unwrap();

    wait_for(
        || seen.lock().expect("poisoned").is_some(),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        *seen.lock().expect("poisoned"),
        Some((Some("svc".to_owned()), None)),
    );

    shutdown.notify_one();
    run.await.unwrap().unwrap();
}
