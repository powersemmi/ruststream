//! Batch routes of a device service written with the attributes, on the build without `alloc`. The
//! app builds with no `recursion_limit` attribute, so a batch mount that nests the futures of an
//! app deeper than the default limit fails this build, and a batch published to each route is
//! handled.

// The app and its handlers are not `Send`: the executor polls them on its own thread.
#![allow(clippy::future_not_send)]
#![cfg(not(ruststream_alloc))]

use core::future::poll_fn;
use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use core::task::Poll;
use core::time::Duration;
use std::thread;

use embassy_executor::{Executor, Spawner};
use ruststream::codec::PostcardCodec;
use ruststream::memory::local::{
    LocalMemoryBroker, LocalMemoryBus, LocalMemoryPublish, LocalMemorySource,
};
use ruststream::runtime::{
    EmbeddedApp, Failures, HandlerOutcome, MountSteps, Mounts, Outcomes, SubscriberSettings,
};
use ruststream::{Broker, Deserialized, Outgoing, OutgoingMessage, Publisher, nonzero, subscriber};
use serde::{Deserialize, Serialize};

/// The board's bus: each subscription holds four messages.
type Bus = LocalMemoryBroker<4>;

/// The board's bus is a `static`: this test's own.
static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();

/// A job as its bytes.
#[derive(Deserialized)]
struct Job<'a>(&'a [u8]);

/// The jobs each route saw.
static SEEN: [AtomicU32; 8] = [const { AtomicU32::new(0) }; 8];

/// Set when the driver is done.
static DONE: AtomicBool = AtomicBool::new(false);

/// One outcome for the whole batch.
#[subscriber(LocalMemorySource::new("uniform"))]
async fn on_uniform(jobs: &[Job<'_>]) -> HandlerOutcome {
    SEEN[0].fetch_add(
        jobs.iter().map(|job| u32::from(!job.0.is_empty())).sum(),
        Ordering::Relaxed,
    );
    HandlerOutcome::ack()
}

/// One outcome per element.
#[subscriber(LocalMemorySource::new("each"))]
async fn on_each(jobs: &[Job<'_>]) -> Outcomes<HandlerOutcome, 3> {
    SEEN[1].fetch_add(
        jobs.iter().map(|job| u32::from(!job.0.is_empty())).sum(),
        Ordering::Relaxed,
    );
    jobs.iter().map(|_| HandlerOutcome::ack()).collect()
}

/// One retry for the whole batch, kept in the mount's store.
#[subscriber(LocalMemorySource::new("retried"))]
async fn on_retried(jobs: &[Job<'_>]) -> HandlerOutcome {
    SEEN[2].fetch_add(u32::from(!jobs.is_empty()), Ordering::Relaxed);
    HandlerOutcome::retry_after(Duration::from_secs(60))
}

/// One retry for the whole batch, with an `.error` for the copies the store cannot publish.
#[subscriber(LocalMemorySource::new("errored"))]
async fn on_errored(jobs: &[Job<'_>]) -> HandlerOutcome {
    SEEN[5].fetch_add(u32::from(!jobs.is_empty()), Ordering::Relaxed);
    HandlerOutcome::retry_after(Duration::from_secs(60)).error(async move |failures: &Failures| {
        SEEN[5].fetch_add(
            u32::try_from(failures.len()).unwrap_or(0),
            Ordering::Relaxed,
        );
    })
}

/// One retry per element under a cap, the last attempt sent to a dead-letter destination.
#[subscriber(LocalMemorySource::new("capped"))]
async fn on_capped(jobs: &[Job<'_>]) -> Outcomes<HandlerOutcome, 3> {
    SEEN[3].fetch_add(u32::from(!jobs.is_empty()), Ordering::Relaxed);
    jobs.iter()
        .map(|_| HandlerOutcome::retry_after(Duration::from_secs(60)))
        .collect()
}

/// A continuation for the whole batch.
#[subscriber(LocalMemorySource::new("then"))]
async fn on_then(jobs: &[Job<'_>]) -> HandlerOutcome {
    SEEN[4].fetch_add(u32::from(!jobs.is_empty()), Ordering::Relaxed);
    HandlerOutcome::ack().and_after(async {
        SEEN[4].fetch_add(1, Ordering::Relaxed);
    })
}

/// A reading, decoded by the codec.
#[derive(Deserialize)]
struct Reading {
    celsius: u32,
}

/// The answer to a reading.
#[derive(Outgoing, Serialize)]
#[outgoing(name = "answers")]
struct Answer {
    celsius: u32,
}

/// One answer for each reading of the batch.
#[subscriber(LocalMemorySource::new("replied"), reply)]
async fn on_replied(readings: &[Reading]) -> Outcomes<Answer, 3> {
    SEEN[6].fetch_add(u32::from(!readings.is_empty()), Ordering::Relaxed);
    readings
        .iter()
        .map(|reading| Answer {
            celsius: reading.celsius,
        })
        .collect()
}

/// One answer for each reading, or one retry for the whole batch with an `.error` for the copies
/// the store cannot publish.
#[subscriber(LocalMemorySource::new("guarded"), reply)]
async fn on_guarded(readings: &[Reading]) -> Result<Outcomes<Answer, 3>, HandlerOutcome> {
    SEEN[7].fetch_add(u32::from(!readings.is_empty()), Ordering::Relaxed);
    if readings.iter().any(|reading| reading.celsius == 0) {
        return Err(HandlerOutcome::retry_after(Duration::from_secs(60)).error(
            async move |failures: &Failures| {
                SEEN[7].fetch_add(
                    u32::try_from(failures.len()).unwrap_or(0),
                    Ordering::Relaxed,
                );
            },
        ));
    }
    Ok(readings
        .iter()
        .map(|reading| Answer {
            celsius: reading.celsius,
        })
        .collect())
}

#[ruststream::app(runtime = embassy)]
fn batch_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    EmbeddedApp::new().with_broker(bus, |b| {
        b.include(on_uniform.batch::<3>())
            .include(on_each.batch::<3>())
            .include(on_retried.batch::<3>())
            .retry_capacity::<3>()
            .include(on_errored.batch::<3>())
            .retry_capacity::<3>()
            .include(on_capped.batch::<3>())
            .max_attempts(nonzero!(2u32))
            .dead_letter("capped.dead")
            .retry_capacity::<3>()
            .include(on_then.batch::<3>())
            .with_codec(PostcardCodec)
            .include(on_replied.batch::<3>())
            .out_reply(LocalMemoryPublish)
            .codec(PostcardCodec)
            .include(on_guarded.batch::<3>())
            .out_reply(LocalMemoryPublish)
            .codec(PostcardCodec)
            .retry_capacity::<3>()
    })
}

/// Lets every task the last step woke run once.
async fn yield_once() {
    let mut yielded = false;
    poll_fn(|context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

#[embassy_executor::task]
async fn drive(spawner: Spawner) {
    spawner.spawn(batch_app::task(Bus::new(&BUS)).expect("the app task is free"));
    for _ in 0..8 {
        yield_once().await;
    }
    let connected = Bus::new(&BUS).connect().await.expect("the broker connects");
    for subject in [
        "uniform", "each", "retried", "errored", "capped", "then", "replied", "guarded",
    ] {
        connected
            .publisher()
            .publish(OutgoingMessage::new(subject, b"x"), None)
            .await
            .expect("the broker takes the message");
    }
    for _ in 0..8 {
        yield_once().await;
    }
    batch_app::CONTROL.stop();
    for _ in 0..8 {
        yield_once().await;
    }
    DONE.store(true, Ordering::Relaxed);
}

/// The stack the executor runs on: the stores and the fans of the mounts come on top of the
/// batch's frames, which the build without optimisation lays out in full.
const STACK: usize = 8 * 1024 * 1024;

#[test]
fn batch_routes_build_under_the_default_recursion_limit_and_run() {
    thread::Builder::new()
        .stack_size(STACK)
        .spawn(|| {
            let executor = Box::leak(Box::new(Executor::new()));
            executor.run_until(
                |spawner: Spawner| spawner.spawn(drive(spawner).expect("the driver task is free")),
                || DONE.load(Ordering::Relaxed),
            );
        })
        .expect("the executor thread starts")
        .join()
        .expect("the executor thread finishes");
    assert_eq!(
        SEEN[0].load(Ordering::Relaxed),
        1,
        "the uniform route's jobs"
    );
    assert_eq!(
        SEEN[1].load(Ordering::Relaxed),
        1,
        "the per-element route's jobs"
    );
    assert_eq!(
        SEEN[2].load(Ordering::Relaxed),
        1,
        "the retried route's jobs"
    );
    assert_eq!(
        SEEN[5].load(Ordering::Relaxed),
        1,
        "the route with an `.error`: its jobs, and no failure"
    );
    assert_eq!(
        SEEN[3].load(Ordering::Relaxed),
        1,
        "the capped route's jobs"
    );
    assert_eq!(
        SEEN[4].load(Ordering::Relaxed),
        2,
        "the continuation route's jobs and its continuation"
    );
    assert_eq!(
        SEEN[6].load(Ordering::Relaxed),
        1,
        "the replying route's readings"
    );
    assert_eq!(
        SEEN[7].load(Ordering::Relaxed),
        1,
        "the replying route with a store and an `.error`: its readings, and no failure"
    );
}
