//! An app of batch routes, written with the attributes as a device crate writes them. It builds
//! without a `recursion_limit` attribute, so a batch mount that nests the futures of an app deeper
//! than the default limit fails this build, and every route gets its own batches.

// A thread-mode embassy task is never sent anywhere.
#![allow(clippy::future_not_send)]

// The scenario drives a broker that has no in-process mode. With `testing` on, the embedded app
// asks every broker for one, so it builds in the embassy-alone legs.
#[cfg(not(feature = "testing"))]
mod scenarios {
    use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use core::time::Duration;

    use embassy_executor::{Executor, Spawner};
    use embassy_futures::yield_now;
    use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
    use ruststream::runtime::{
        EmbeddedApp, Failures, HandlerOutcome, MountSteps, Mounts, Out, SubscriberSettings,
    };
    use ruststream::{
        Broker, Deserialized, OutSlot, Outgoing, OutgoingMessage, Publisher, Serialized, nonzero,
        subscriber,
    };
    use ruststream_core::__alloc::bytes::BytesMut;
    use serde::{Deserialize, Serialize};
    use static_cell::StaticCell;

    /// Executor rounds a step yields: a round polls every task woken before it began.
    const SETTLE_ROUNDS: usize = 8;

    /// The board's bus: each subscription holds four messages.
    type Bus = LocalMemoryBroker<4>;

    /// A job as its bytes.
    #[derive(Deserialized)]
    struct Job<'a>(&'a [u8]);

    /// A reading, decoded by the codec.
    #[derive(Deserialize)]
    struct Reading {
        celsius: u32,
    }

    /// The jobs each route saw.
    static SEEN: [AtomicU32; 10] = [const { AtomicU32::new(0) }; 10];

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
    async fn on_each(jobs: &[Job<'_>]) -> Vec<HandlerOutcome> {
        SEEN[1].fetch_add(u32::from(!jobs.is_empty()), Ordering::Relaxed);
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
        SEEN[6].fetch_add(u32::from(!jobs.is_empty()), Ordering::Relaxed);
        HandlerOutcome::retry_after(Duration::from_secs(60)).error(
            async move |failures: Failures| {
                SEEN[6].fetch_add(
                    u32::try_from(failures.into_iter().count()).unwrap_or(0),
                    Ordering::Relaxed,
                );
            },
        )
    }

    /// One retry per element under a cap, the last attempt sent to a dead-letter destination.
    #[subscriber(LocalMemorySource::new("capped"))]
    async fn on_capped(jobs: &[Job<'_>]) -> Vec<HandlerOutcome> {
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

    /// Readings the codec decodes, with a store of retries.
    #[subscriber(LocalMemorySource::new("decoded"))]
    async fn on_decoded(readings: &[Reading]) -> HandlerOutcome {
        SEEN[5].fetch_add(
            readings.iter().map(|reading| reading.celsius.min(1)).sum(),
            Ordering::Relaxed,
        );
        HandlerOutcome::retry_after(Duration::from_secs(60))
    }

    /// The answer to a reading.
    #[derive(Outgoing, Serialize)]
    #[outgoing(name = "answers")]
    struct Answer {
        celsius: u32,
    }

    /// One answer for each reading of the batch.
    #[subscriber(LocalMemorySource::new("replied"), reply)]
    async fn on_replied(readings: &[Reading]) -> Vec<Answer> {
        SEEN[7].fetch_add(u32::from(!readings.is_empty()), Ordering::Relaxed);
        readings
            .iter()
            .map(|reading| Answer {
                celsius: reading.celsius,
            })
            .collect()
    }

    /// One answer for each reading, or one retry for the whole batch with an `.error` for the
    /// copies the store cannot publish.
    #[subscriber(LocalMemorySource::new("guarded"), reply)]
    async fn on_guarded(readings: &[Reading]) -> Result<Vec<Answer>, HandlerOutcome> {
        SEEN[8].fetch_add(u32::from(!readings.is_empty()), Ordering::Relaxed);
        if readings.iter().any(|reading| reading.celsius == 0) {
            return Err(HandlerOutcome::retry_after(Duration::from_secs(60)).error(
                async move |failures: Failures| {
                    SEEN[8].fetch_add(
                        u32::try_from(failures.into_iter().count()).unwrap_or(0),
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

    /// The entry the audited route writes for its batch.
    #[derive(Outgoing, Serialized)]
    #[outgoing(name = "audit-log")]
    struct Entry([u8; 1]);

    /// The slot audit entries leave through.
    #[derive(OutSlot)]
    #[publishes(Entry)]
    struct Audit;

    /// A batch of readings that publishes an entry through an `Out` slot.
    #[subscriber(LocalMemorySource::new("audited"))]
    async fn on_audited(
        readings: &[Reading],
        Out(audit): Out<impl Publisher, Audit>,
    ) -> HandlerOutcome {
        SEEN[9].fetch_add(u32::from(!readings.is_empty()), Ordering::Relaxed);
        let _ = audit.message(&Entry([1])).publish().await;
        HandlerOutcome::ack()
    }

    #[ruststream::app(runtime = embassy)]
    fn batch_app(bus: Bus) -> EmbeddedApp<impl Mounts> {
        EmbeddedApp::new().with_broker(bus, |b| {
            b.include(on_uniform.batch(nonzero!(3)))
                .include(on_each.batch(nonzero!(3)))
                .include(on_retried.batch(nonzero!(3)))
                .retry_capacity::<3>()
                .include(on_errored.batch(nonzero!(3)))
                .retry_capacity::<3>()
                .include(on_capped.batch(nonzero!(3)))
                .max_attempts(nonzero!(2u32))
                .dead_letter("capped.dead")
                .retry_capacity::<3>()
                .include(on_then.batch(nonzero!(3)))
                .include(on_decoded.batch(nonzero!(3)))
                .retry_capacity::<3>()
                .include(on_replied.batch(nonzero!(3)))
                .include(on_guarded.batch(nonzero!(3)))
                .retry_capacity::<3>()
                .include(on_audited.batch(nonzero!(3)))
                .out(Audit, LocalMemoryPublish)
        })
    }

    /// Lets every task the last step woke run until it parks again.
    async fn settle() {
        for _ in 0..SETTLE_ROUNDS {
            yield_now().await;
        }
    }

    /// Publishes one byte to `subject` on `broker`.
    async fn publish(broker: &Bus, subject: &str) {
        let connected = broker.clone().connect().await.expect("the broker connects");
        let body = if ["decoded", "replied", "guarded", "audited"].contains(&subject) {
            &br#"{"celsius":1}"#[..]
        } else {
            &b"x"[..]
        };
        connected
            .publisher()
            .publish(
                OutgoingMessage::produced(subject, BytesMut::from(body)),
                None,
            )
            .await
            .expect("the broker takes the message");
    }

    #[embassy_executor::task]
    async fn drive(spawner: Spawner, bus: Bus) {
        spawner.spawn(batch_app::task(bus.clone()).expect("the app task is free"));
        settle().await;
        for subject in [
            "uniform", "each", "retried", "errored", "capped", "then", "decoded", "replied",
            "guarded", "audited",
        ] {
            publish(&bus, subject).await;
        }
        settle().await;
        batch_app::CONTROL.stop();
        settle().await;
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
            SEEN[6].load(Ordering::Relaxed),
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
            SEEN[5].load(Ordering::Relaxed),
            1,
            "the decoded route's readings"
        );
        assert_eq!(
            SEEN[7].load(Ordering::Relaxed),
            1,
            "the replying route's readings"
        );
        assert_eq!(
            SEEN[8].load(Ordering::Relaxed),
            1,
            "the replying route with a store and an `.error`: its readings, and no failure"
        );
        assert_eq!(
            SEEN[9].load(Ordering::Relaxed),
            1,
            "the route with an `Out` slot: its jobs"
        );
        println!("test batch routes build under the default recursion limit and run ... ok");
        DONE.store(true, Ordering::Relaxed);
    }

    static DONE: AtomicBool = AtomicBool::new(false);

    pub(super) fn main() {
        static EXECUTOR: StaticCell<Executor> = StaticCell::new();
        let bus = Bus::new();
        EXECUTOR.init(Executor::new()).run_until(
            |spawner: Spawner| {
                spawner.spawn(drive(spawner, bus).expect("the driver task is free"));
            },
            || DONE.load(Ordering::Relaxed),
        );
    }
}

fn main() {
    #[cfg(not(feature = "testing"))]
    scenarios::main();
}
