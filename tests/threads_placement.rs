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
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use common::Order;
use futures::{Stream, StreamExt};
use ruststream::memory::MemoryMessage;
use ruststream::memory::prelude::*;
use ruststream::{HeaderMap, IncomingMessage, Outgoing, Subscriber};
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
