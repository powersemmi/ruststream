//! Where `threads(n)` runs a delivery: on a dedicated thread of the subscription's own, from the
//! handler's first poll to its end, with the app's runtime reached only on purpose.
//!
//! The subject is thread placement, which the test harness does not reproduce (under `TestApp`
//! every subscription runs on the test's runtime), so these tests start the app on a runtime
//! whose threads carry a name of their own and read what the handler reports through the broker:
//! each handler replies with the names of the threads its steps ran on.
#![cfg(all(feature = "memory", feature = "macros", feature = "json"))]

mod common;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::future::{Future, ready};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use common::Order;
use futures::{Stream, StreamExt};
use ruststream::memory::prelude::*;
use ruststream::memory::{MemoryMessage, MemoryPublisher};
use ruststream::{
    AckError, AddressedCopies, HeaderMap, IncomingMessage, Outgoing, RedeliveryAddress,
    RedeliveryAddressed, Subscribe, Subscriber, SubscriptionSource,
};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Runtime};
use tokio::time::timeout;

/// What the app runtime's threads are named: no other thread in the process carries it.
const APP_THREADS: &str = "the-app-runtime";

/// How long a test waits for the replies it expects.
const DEADLINE: Duration = Duration::from_secs(10);

/// The runtime the app runs on: multi-threaded, as `#[ruststream::app]` builds by default.
fn app_runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(APP_THREADS)
        .enable_all()
        .build()
        .expect("the app runtime")
}

fn here() -> String {
    thread::current().name().unwrap_or_default().to_owned()
}

/// The threads each step of one delivery ran on.
#[derive(Debug, Clone, Outgoing, Serialize, Deserialize, schemars::JsonSchema)]
struct Trace {
    id: u32,
    start: String,
    after_timer: String,
    spawned: String,
    on_main: String,
}

#[subscriber("jobs", threads(2), publish("traces"))]
async fn traced(job: &Order, Ctx(main): Ctx<MainRuntime>) -> Trace {
    let start = here();
    tokio::time::sleep(Duration::from_millis(1)).await;
    let after_timer = here();
    // A plain spawn stays where the handler runs; the explicit route goes to the app's runtime.
    let spawned = tokio::spawn(async { here() }).await.unwrap_or_default();
    let on_main = main.spawn(async { here() }).await.unwrap_or_default();
    Trace {
        id: job.id,
        start,
        after_timer,
        spawned,
        on_main,
    }
}

/// Reads `count` JSON payloads off `stream`, acking each.
async fn read<Model, Messages>(stream: &mut Messages, count: usize) -> Vec<Model>
where
    Model: for<'de> Deserialize<'de>,
    Messages: Stream<Item = Result<MemoryMessage, Infallible>> + Unpin,
{
    let mut models = Vec::with_capacity(count);
    for _ in 0..count {
        let msg = timeout(DEADLINE, stream.next())
            .await
            .expect("a reply within the deadline")
            .expect("the stream stays open")
            .expect("a memory subscriber never errors");
        models.push(serde_json::from_slice(msg.payload()).expect("a JSON reply"));
        msg.ack().await.expect("ack");
    }
    models
}

#[test]
fn a_delivery_is_handled_on_a_dedicated_thread_alone() {
    const JOBS: u32 = 6;
    let broker = MemoryBroker::new();
    let traces = app_runtime().block_on(async {
        let mut replies = broker.subscribe("traces");
        let mut replies = pin!(replies.stream());
        let app =
            RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(broker.clone(), |b| {
                b.include(traced).out_reply(Publish);
            });
        let running = app.start().await.expect("startup");
        let publisher = broker.publisher();
        for id in 0..JOBS {
            publisher
                .message(&Order { id })
                .to("jobs")
                .publish()
                .await
                .expect("publish");
        }
        let traces: Vec<Trace> = read(&mut replies, JOBS as usize).await;
        running.shutdown().await.expect("shutdown");
        traces
    });
    let mut threads = HashSet::new();
    for trace in &traces {
        assert!(
            !trace.start.starts_with(APP_THREADS),
            "the handler ran on the app's runtime: {trace:?}"
        );
        assert_eq!(
            trace.after_timer, trace.start,
            "moved across a timer: {trace:?}"
        );
        assert_eq!(
            trace.spawned, trace.start,
            "a plain spawn left the thread: {trace:?}"
        );
        assert!(
            trace.on_main.starts_with(APP_THREADS),
            "the main-runtime task ran elsewhere: {trace:?}"
        );
        threads.insert(trace.start.clone());
    }
    assert!(
        threads.len() <= 2,
        "more threads than declared: {threads:?}"
    );
}

/// Replies with the thread a keyed delivery ran on.
#[derive(Debug, Clone, Outgoing, Serialize, Deserialize, schemars::JsonSchema)]
struct Placed {
    id: u32,
    thread: String,
}

#[subscriber("keyed", threads(3, by_key), publish("placed"))]
async fn placed(job: &Order) -> Placed {
    // Enough of a pause for the lanes to interleave.
    tokio::task::yield_now().await;
    Placed {
        id: job.id,
        thread: here(),
    }
}

#[test]
fn a_key_stays_on_one_thread_and_in_order() {
    const PER_KEY: u32 = 20;
    let keys = ["alpha", "beta", "gamma", "delta"];
    let broker = MemoryBroker::new();
    let seen: Vec<Placed> = app_runtime().block_on(async {
        let mut replies = broker.subscribe("placed");
        let mut replies = pin!(replies.stream());
        let app =
            RustStream::new(AppInfo::new("threads", "0.1.0")).with_broker(broker.clone(), |b| {
                b.include(placed).out_reply(Publish);
            });
        let running = app.start().await.expect("startup");
        let publisher = broker.publisher();
        for sequence in 0..PER_KEY {
            for (band, key) in (0u32..).zip(keys) {
                let mut headers = HeaderMap::new();
                headers.insert("partition-key", key);
                publisher
                    .message(&Order {
                        id: band * 1000 + sequence,
                    })
                    .with_headers(headers)
                    .to("keyed")
                    .publish()
                    .await
                    .expect("publish");
            }
        }
        let seen = read(&mut replies, (PER_KEY as usize) * keys.len()).await;
        running.shutdown().await.expect("shutdown");
        seen
    });
    let mut bands: HashMap<u32, Vec<&Placed>> = HashMap::new();
    for reply in &seen {
        assert!(
            !reply.thread.starts_with(APP_THREADS),
            "a keyed delivery ran on the app's runtime: {reply:?}"
        );
        bands.entry(reply.id / 1000).or_default().push(reply);
    }
    for (band, replies) in bands {
        let threads: HashSet<&str> = replies.iter().map(|reply| reply.thread.as_str()).collect();
        assert_eq!(
            threads.len(),
            1,
            "key {band} moved between threads: {threads:?}"
        );
        let ids: Vec<u32> = replies.iter().map(|reply| reply.id).collect();
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "key {band} out of order: {ids:?}"
        );
    }
}

/// What the continuation tests share with their handler: where a continuation reports the thread
/// it ran on.
struct Report {
    publisher: MemoryPublisher,
}

/// Registers an `and_after` continuation and an `after` hook, each reporting its thread.
#[subscriber("continued", threads(1))]
async fn continued(job: &Order, ctx: &mut Context<'_, (), Arc<Report>>) -> HandlerOutcome {
    let id = job.id;
    let publisher = ctx.state().publisher.clone();
    ctx.after(HandlerOutcome::ack()).then(async move {
        let reply = Placed { id, thread: here() };
        let _ = publisher
            .message(&reply)
            .to("continued.done")
            .publish()
            .await;
    });
    let publisher = ctx.state().publisher.clone();
    HandlerOutcome::ack().and_after(async move {
        let reply = Placed {
            id: id + 1000,
            thread: here(),
        };
        let _ = publisher
            .message(&reply)
            .to("continued.done")
            .publish()
            .await;
    })
}

/// Work that outlives the delivery runs on the app's runtime: a continuation must not wait
/// behind the thread's computation, nor die with the thread's runtime.
#[test]
fn continuations_run_on_the_app_runtime() {
    let broker = MemoryBroker::new();
    let seen: Vec<Placed> = app_runtime().block_on(async {
        let mut replies = broker.subscribe("continued.done");
        let mut replies = pin!(replies.stream());
        let report = Arc::new(Report {
            publisher: broker.publisher(),
        });
        let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(report))
            .with_broker(broker.clone(), |b| {
                b.include(continued);
            });
        let running = app.start().await.expect("startup");
        broker
            .publisher()
            .message(&Order { id: 1 })
            .to("continued")
            .publish()
            .await
            .expect("publish");
        let seen = read(&mut replies, 2).await;
        running.shutdown().await.expect("shutdown");
        seen
    });
    for reply in &seen {
        assert!(
            reply.thread.starts_with(APP_THREADS),
            "a continuation ran off the app's runtime: {reply:?}"
        );
    }
}

/// A subscription whose deliveries report no native delayed redelivery, so a `retry_after` takes
/// the runtime's own copy path; the copy goes to `copies`, where the test reads it.
#[derive(Debug, Clone)]
struct CopiedSubscription;

impl CopiedSubscription {
    const fn new() -> Self {
        Self
    }
}

impl<C: Subscribe> SubscriptionSource<C> for CopiedSubscription {
    type Subscriber = UnsettledSubscriber<C::Subscriber>;
    type Copies = AddressedCopies;

    fn name(&self) -> &str {
        "retried"
    }

    async fn subscribe(self, connected: &C) -> Result<Self::Subscriber, C::Error> {
        Ok(UnsettledSubscriber(connected.subscribe("retried").await?))
    }
}

impl<C: Subscribe> RedeliveryAddressed<C> for CopiedSubscription {
    fn redelivery_address(
        &self,
        _connected: &C,
    ) -> impl Future<Output = Result<RedeliveryAddress, C::Error>> + Send {
        ready(Ok(RedeliveryAddress::new("copies")))
    }
}

/// The broker's subscriber with its native delayed redelivery taken away.
struct UnsettledSubscriber<S>(S);

impl<S: Subscriber> Subscriber for UnsettledSubscriber<S> {
    type Message = UnsettledMessage<S::Message>;
    type Error = S::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_ {
        self.0.stream().map(|item| item.map(UnsettledMessage))
    }
}

/// A delivery that settles like the broker's own but keeps the trait default for
/// [`IncomingMessage::supports_nack_after`].
struct UnsettledMessage<M>(M);

impl<M: IncomingMessage> IncomingMessage for UnsettledMessage<M> {
    fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    fn headers(&self) -> &HeaderMap {
        self.0.headers()
    }

    async fn ack(self) -> Result<(), AckError> {
        self.0.ack().await
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        self.0.nack(requeue).await
    }
}

/// How long the computing delivery holds its thread at most.
const COMPUTATION: Duration = Duration::from_secs(5);

/// Defers order 1; computes on order 2 until the test says the copy of order 1 arrived, or for
/// [`COMPUTATION`] at most.
#[subscriber(CopiedSubscription::new(), threads(1))]
async fn retried(order: &Order, ctx: &mut Context<'_, (), Arc<AtomicBool>>) -> HandlerOutcome {
    if order.id == 1 {
        return HandlerOutcome::retry_after(Duration::from_millis(50));
    }
    let copied = ctx.state();
    let started = std::time::Instant::now();
    // Holds the thread the way a computation does: its runtime turns no timer meanwhile.
    while !copied.load(Ordering::SeqCst) && started.elapsed() < COMPUTATION {
        thread::sleep(Duration::from_millis(1));
    }
    HandlerOutcome::ack()
}

/// The delayed copy of a `retry_after` leaves on time while the delivery's thread computes: its
/// timer runs on the app's runtime, not behind the computation.
#[test]
fn a_delayed_retry_copy_does_not_wait_for_the_thread() {
    let broker = MemoryBroker::new();
    let copied = Arc::new(AtomicBool::new(false));
    let arrived = app_runtime().block_on(async {
        let mut copies = broker.subscribe("copies");
        let mut copies = pin!(copies.stream());
        let state = Arc::clone(&copied);
        let app = RustStream::new(AppInfo::new("threads", "0.1.0"))
            .on_startup(async move |()| Ok::<_, Infallible>(state))
            .with_broker(broker.clone(), |b| {
                b.include(retried).out_retry(Publish);
            });
        let running = app.start().await.expect("startup");
        let publisher = broker.publisher();
        for id in [1, 2] {
            publisher
                .message(&Order { id })
                .to("retried")
                .publish()
                .await
                .expect("publish");
        }
        // Well under the computation: a copy that waited for the thread arrives after it.
        let arrived = timeout(COMPUTATION / 2, copies.next()).await.is_ok();
        copied.store(true, Ordering::SeqCst);
        running.shutdown().await.expect("shutdown");
        arrived
    });
    assert!(
        arrived,
        "the retry copy waited for the thread's computation to end"
    );
}
