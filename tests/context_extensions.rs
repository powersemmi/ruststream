//! Integration tests for the typed per-delivery `Context`: a broker-contributed field (built from
//! the message via `BuildContext`) reaching the handler by key, a middleware-written scratch value
//! reaching a downstream handler and being isolated per delivery, and `ctx.state()` still reaching
//! app state. All use the in-memory broker with hand-written handlers (which can name a context
//! type; macro handlers use the default `()` context).

mod common;

use common::{BackgroundRun, Wire, wait_for};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{Stream, StreamExt};
use ruststream::memory::{MemoryBroker, MemoryMessage, MemorySubscriber};
use ruststream::runtime::{
    AppInfo, Context, Handler, HandlerExt, HandlerMetadata, HandlerOutcome, Layer, PublishExt,
    RustStream,
};
use ruststream::{AckError, BuildContext, Field, FieldMut, HeaderMap, IncomingMessage};

/// A broker that attaches native per-delivery metadata: `TaggedMessage` carries a tag, and the
/// `TagContext` reads it off the message via `BuildContext`, standing in for an offset / commit
/// token / reply-to handle a real broker would expose.
struct TaggedMessage {
    inner: MemoryMessage,
    tag: u32,
}

impl IncomingMessage for TaggedMessage {
    fn payload(&self) -> &[u8] {
        self.inner.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.inner.headers()
    }

    async fn ack(self) -> Result<(), AckError> {
        self.inner.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.inner.nack(requeue).await
    }
}

/// The broker's typed per-delivery context, built from the message.
struct TagContext {
    tag: u32,
}

impl BuildContext<TaggedMessage> for TagContext {
    fn build(msg: &TaggedMessage) -> Self {
        Self { tag: msg.tag }
    }
}

/// The compile-time key reading the tag out of [`TagContext`].
#[derive(Clone, Copy)]
struct Tag;

impl Field<TagContext> for Tag {
    type Value<'a> = u32;
    fn get(self, cx: &TagContext) -> u32 {
        cx.tag
    }
}

/// A subscriber that yields `TaggedMessage`s, numbering each delivery so the contributed tag
/// differs per message (proving delivery as well as per-delivery freshness).
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_contributed_field_reaches_handler_by_key() {
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
            move |_msg: &TaggedMessage, ctx: &mut Context<'_, TagContext>| {
                // The broker-contributed field is read off the typed context by key.
                let tag = ctx.context(Tag);
                let seen = Arc::clone(&seen_clone);
                async move {
                    seen.lock().expect("poisoned").push(tag);
                    HandlerOutcome::ack()
                }
            },
            HandlerMetadata::raw("orders"),
        );
    });

    let run = BackgroundRun::spawn(app);

    publisher
        .message(&Wire::of(b"a"))
        .to("orders")
        .publish()
        .await
        .unwrap();
    publisher
        .message(&Wire::of(b"b"))
        .to("orders")
        .publish()
        .await
        .unwrap();

    wait_for(
        || seen.lock().expect("poisoned").len() >= 2,
        Duration::from_secs(5),
    )
    .await;

    // Each delivery is built a fresh context from its own message, so each sees its own tag.
    assert_eq!(*seen.lock().expect("poisoned"), vec![1, 2]);

    run.stop().await;
}

/// A per-delivery scratch context a middleware writes and a downstream handler reads.
#[derive(Default)]
struct Scratch {
    stamp: Option<u32>,
}

// Reads nothing off the message: each delivery starts from a fresh default, which is what makes
// the value isolated per delivery.
impl<M: ?Sized> BuildContext<M> for Scratch {
    fn build(_msg: &M) -> Self {
        Self::default()
    }
}

/// The key reading / writing the middleware stamp.
#[derive(Clone, Copy)]
struct Stamp;

impl Field<Scratch> for Stamp {
    type Value<'a> = Option<&'a u32>;
    fn get(self, cx: &Scratch) -> Option<&u32> {
        cx.stamp.as_ref()
    }
}

impl FieldMut<Scratch> for Stamp {
    type Owned = u32;
    fn set(self, cx: &mut Scratch, value: u32) {
        cx.stamp = Some(value);
    }
}

/// A layer that writes the scratch stamp before the inner handler runs, asserting first that no
/// stamp survived from a previous delivery (proving per-delivery isolation through the dispatch
/// loop).
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

impl<M, H> Handler<M, Scratch> for StampHandler<H>
where
    M: Sync,
    H: Handler<M, Scratch>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, Scratch>) -> HandlerOutcome {
        // No value should survive from a previous delivery: the dispatch loop builds a fresh
        // context each time.
        assert!(
            ctx.context(Stamp).is_none(),
            "scratch leaked across deliveries"
        );
        let n = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ctx.set(Stamp, n);
        self.inner.handle(msg, ctx).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn middleware_written_scratch_reaches_downstream_handler_and_is_isolated() {
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
            (move |_msg: &MemoryMessage, ctx: &mut Context<'_, Scratch>| {
                // The layer ran first and wrote a per-delivery stamp; the downstream handler reads
                // it back from the same context by key.
                let stamp = ctx.context(Stamp).copied();
                let seen = Arc::clone(&seen_clone);
                async move {
                    if let Some(n) = stamp {
                        seen.lock().expect("poisoned").push(n);
                    }
                    HandlerOutcome::ack()
                }
            })
            .with(layer)
        };
        b.handle(subscriber, handler, HandlerMetadata::raw("orders"));
    });

    let run = BackgroundRun::spawn(app);

    publisher
        .message(&Wire::of(b"a"))
        .to("orders")
        .publish()
        .await
        .unwrap();
    publisher
        .message(&Wire::of(b"b"))
        .to("orders")
        .publish()
        .await
        .unwrap();

    wait_for(
        || seen.lock().expect("poisoned").len() >= 2,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(*seen.lock().expect("poisoned"), vec![0, 1]);

    run.stop().await;
}

struct AppPrefix(String);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_reaches_app_state_independently_of_the_delivery_context() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let seen_clone = Arc::clone(&seen);

    let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(AppPrefix("svc".to_owned())))
        .with_broker(broker, |b| {
            let subscriber = b.broker().subscribe("orders");
            b.handle(
                subscriber,
                move |_msg: &MemoryMessage, ctx: &mut Context<'_, (), AppPrefix>| {
                    // The typed app state through state(), independent of the per-delivery context.
                    let prefix = Some(ctx.state().0.clone());
                    let seen = Arc::clone(&seen_clone);
                    async move {
                        *seen.lock().expect("poisoned") = prefix;
                        HandlerOutcome::ack()
                    }
                },
                HandlerMetadata::raw("orders"),
            );
        });

    let run = BackgroundRun::spawn(app);

    publisher
        .message(&Wire::of(b"x"))
        .to("orders")
        .publish()
        .await
        .unwrap();

    wait_for(
        || seen.lock().expect("poisoned").is_some(),
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(*seen.lock().expect("poisoned"), Some("svc".to_owned()));

    run.stop().await;
}
