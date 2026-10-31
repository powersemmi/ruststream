//! Whole embedded apps on embassy's executor: brokers connected, routes opened and joined in the
//! app's one task, health reported through the control, and the brokers shut down at the end.
//!
//! Each scenario spawns an app task over its own statics and a driver step publishes, reads what
//! the app did, and stops it. The executor runs until the last scenario passes; a failed check
//! panics out of it and fails the binary.

// A thread-mode embassy task is never sent anywhere, and the apps hold brokers that are not
// `Send` on purpose.
#![allow(clippy::future_not_send)]

// The scenarios drive brokers of their own that have no in-process mode. With `testing` on, the
// embedded app asks every broker for one, so they build in the embassy-alone legs.
#[cfg(not(feature = "testing"))]
mod scenarios {

    use core::cell::{Cell, RefCell};
    use core::future::ready;
    use core::sync::atomic::{AtomicU32, Ordering};
    use std::alloc::System;
    use std::error::Error;
    use std::fmt;
    use std::sync::Arc;

    use bytes::BytesMut;
    use embassy_executor::{Executor, Spawner};
    use embassy_futures::yield_now;
    use ruststream_core::{Broker, ConnectedBroker, LocalPublisher, OutgoingMessage};
    use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
    use ruststream_runtime::{
        Context, Deserialized, FailurePolicies, FailurePolicy, HandlerOutcome, Input, LocalHandle,
        SoloDeserialized, SubscriberSettings, local_subscriber,
    };
    use ruststream_runtime_embassy::frames::{
        Frame, FrameBroker, FrameSource, Frames, Settlements,
    };
    use ruststream_runtime_embassy::{Control, EmbeddedApp, Health, MountSteps, Mounts, RunError};
    use static_cell::StaticCell;
    use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

    // Counts every allocation of the process, so a scenario reads how many its deliveries made.
    #[global_allocator]
    static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

    /// Executor rounds a step yields: a round polls every task woken before it began.
    const SETTLE_ROUNDS: usize = 8;

    /// The local bus of the scenarios: each subscription holds four messages.
    type Bus = LocalMemoryBroker<4>;

    /// A reading: the frame's bytes, decoded by no codec. An empty frame does not decode.
    struct Reading<'a>(#[allow(dead_code)] &'a [u8]);

    /// Why a reading did not decode.
    #[derive(Debug)]
    struct EmptyReading;

    impl fmt::Display for EmptyReading {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an empty reading")
        }
    }

    impl Error for EmptyReading {}

    impl Deserialized for Reading<'_> {
        type Output<'a> = Reading<'a>;
        type Error = EmptyReading;

        fn from_payload(payload: &[u8]) -> Result<Reading<'_>, EmptyReading> {
            if payload.is_empty() {
                Err(EmptyReading)
            } else {
                Ok(Reading(payload))
            }
        }
    }

    impl Input for Reading<'_> {
        type Axis = SoloDeserialized<Reading<'static>>;
    }

    /// Counts the readings it handles.
    struct Count {
        seen: &'static AtomicU32,
    }

    impl LocalHandle<Reading<'_>> for Count {
        fn handle(
            &self,
            _reading: &Reading<'_>,
            _outs: &(),
            _ctx: &mut Context<'_>,
        ) -> impl Future<Output = Result<(), HandlerOutcome>> {
            self.seen.fetch_add(1, Ordering::Relaxed);
            ready(Ok(()))
        }
    }

    /// What the scenarios keep in statics.
    struct Stand {
        frames: Frames,
        settled: Settlements,
        bus: Bus,
        seen_frames: AtomicU32,
        seen_bus: AtomicU32,
        two: Control,
        failing: Control,
        seen_failing: AtomicU32,
        hooked: Control,
        ending_bus: Bus,
        ending: Control,
        alone: Control,
        calls: RefCell<Vec<&'static str>>,
        health: RefCell<Vec<Health>>,
        outcome: RefCell<Option<Result<(), RunError>>>,
        done: Cell<bool>,
    }

    /// An app with two brokers: the frame stand-in and the local bus.
    fn two_brokers(stand: &'static Stand) -> EmbeddedApp<impl Mounts> {
        EmbeddedApp::new()
            .with_broker(FrameBroker::new(&stand.frames, &stand.settled), |b| {
                b.include(
                    local_subscriber(
                        FrameSource::new("frames"),
                        Count {
                            seen: &stand.seen_frames,
                        },
                    )
                    .build(),
                )
            })
            .with_broker(stand.bus.clone(), |b| {
                b.include(
                    local_subscriber(
                        LocalMemorySource::new("bus/readings"),
                        Count {
                            seen: &stand.seen_bus,
                        },
                    )
                    .build(),
                )
            })
    }

    /// An app whose subscription fails the app on a reading that does not decode.
    fn fails_fast(stand: &'static Stand) -> EmbeddedApp<impl Mounts> {
        EmbeddedApp::new().with_broker(FrameBroker::new(&stand.frames, &stand.settled), |b| {
            b.include(
                local_subscriber(
                    FrameSource::new("frames"),
                    Count {
                        seen: &stand.seen_failing,
                    },
                )
                .on_failure(FailurePolicies::default().with_decode(FailurePolicy::FailFast))
                .build(),
            )
        })
    }

    /// An app with a hook in each phase, recording the order they ran in.
    fn hooked(stand: &'static Stand) -> EmbeddedApp<impl Mounts> {
        let calls = &stand.calls;
        EmbeddedApp::new()
            .with_state(7u32)
            .with_broker(stand.bus.clone(), |b| b)
            .after_startup(async move |state: Arc<u32>| {
                assert_eq!(*state, 7, "the hook receives the app's state");
                calls.borrow_mut().push("after_startup");
                Ok::<(), fmt::Error>(())
            })
            .on_shutdown(async move |_| {
                calls.borrow_mut().push("on_shutdown");
                Ok::<(), fmt::Error>(())
            })
            .after_shutdown(async move |_| {
                calls.borrow_mut().push("after_shutdown");
                Ok::<(), fmt::Error>(())
            })
    }

    /// An app of hooks alone: no broker to serve, and a shutdown hook that fails.
    fn hooks_alone(stand: &'static Stand) -> EmbeddedApp<impl Mounts> {
        let calls = &stand.calls;
        EmbeddedApp::new()
            .after_startup(async move |_| {
                calls.borrow_mut().push("after_startup");
                Ok::<(), fmt::Error>(())
            })
            .on_shutdown(async move |_| {
                calls.borrow_mut().push("on_shutdown");
                Err::<(), fmt::Error>(fmt::Error)
            })
            .after_shutdown(async move |_| {
                calls.borrow_mut().push("after_shutdown");
                Ok::<(), fmt::Error>(())
            })
    }

    /// An app with two subscriptions on a bus of its own, which the scenario shuts down under it:
    /// one read in order, one by a pool of workers.
    fn ending(stand: &'static Stand) -> EmbeddedApp<impl Mounts> {
        let count = || Count {
            seen: &stand.seen_bus,
        };
        EmbeddedApp::new().with_broker(stand.ending_bus.clone(), |b| {
            b.include(local_subscriber(LocalMemorySource::new("bus/ending"), count()).build())
                .include(
                    local_subscriber(LocalMemorySource::new("bus/ending-pool"), count()).build(),
                )
                .pool::<2>()
        })
    }

    // A device holds the app's future in its static task and awaits it in place, as this task does:
    // nothing moves it. Two brokers' loops make it larger than the lint's limit on some targets.
    #[allow(clippy::large_futures)]
    #[embassy_executor::task]
    async fn run_two(stand: &'static Stand) {
        let outcome = two_brokers(stand).run(&stand.two).await;
        *stand.outcome.borrow_mut() = Some(outcome);
    }

    #[embassy_executor::task]
    async fn run_failing(stand: &'static Stand) {
        let outcome = fails_fast(stand).run(&stand.failing).await;
        *stand.outcome.borrow_mut() = Some(outcome);
    }

    #[embassy_executor::task]
    async fn run_hooked(stand: &'static Stand) {
        let outcome = hooked(stand).run(&stand.hooked).await;
        *stand.outcome.borrow_mut() = Some(outcome);
    }

    #[embassy_executor::task]
    async fn run_alone(stand: &'static Stand) {
        let outcome = hooks_alone(stand).run(&stand.alone).await;
        *stand.outcome.borrow_mut() = Some(outcome);
    }

    // As `run_two`: the pool's lanes make the app's future larger than the lint's limit.
    #[allow(clippy::large_futures)]
    #[embassy_executor::task]
    async fn run_ending(stand: &'static Stand) {
        let outcome = ending(stand).run(&stand.ending).await;
        *stand.outcome.borrow_mut() = Some(outcome);
    }

    /// Records every health the control moves through until it stops or fails.
    #[embassy_executor::task(pool_size = 4)]
    async fn watch(control: &'static Control, stand: &'static Stand) {
        // Read when the watcher is spawned, before the app's task first runs.
        let mut seen = Health::Starting;
        while !matches!(seen, Health::Stopped | Health::Failed) {
            seen = control.changed(seen).await;
            stand.health.borrow_mut().push(seen);
        }
    }

    /// Lets every task the last step woke run until it parks again.
    async fn settle() {
        for _ in 0..SETTLE_ROUNDS {
            yield_now().await;
        }
    }

    /// Reports one passed check the way the test harness does.
    fn passed(check: &str) {
        println!("test {check} ... ok");
    }

    fn frame(payload: &[u8]) -> Frame {
        Frame::new(payload).expect("the payload fits a frame")
    }

    /// Takes the outcome the last app's task reported.
    fn outcome(stand: &Stand) -> Result<(), RunError> {
        stand
            .outcome
            .borrow_mut()
            .take()
            .expect("the app's run returned")
    }

    async fn check_two_brokers(spawner: Spawner, stand: &'static Stand) {
        stand.health.borrow_mut().push(stand.two.health());
        spawner.spawn(watch(&stand.two, stand).expect("a watcher task is free"));
        spawner.spawn(run_two(stand).expect("the app task is free"));
        settle().await;
        let running = stand.two.health();

        let connected = stand.bus.clone().connect().await.expect("the bus connects");
        let publisher = connected.publisher();
        let delivery_path = Region::new(ALLOCATOR);
        stand.frames.send(frame(b"reading")).await;
        settle().await;
        let frame_allocations = delivery_path.change().allocations;
        let payload = BytesMut::from(&b"reading"[..]);
        publisher
            .publish(OutgoingMessage::produced("bus/readings", payload), None)
            .await
            .expect("the bus takes the reading");
        settle().await;
        let seen = (
            stand.seen_frames.load(Ordering::Relaxed),
            stand.seen_bus.load(Ordering::Relaxed),
        );

        stand.two.stop();
        settle().await;
        let health = stand.health.borrow_mut().drain(..).collect::<Vec<_>>();

        assert_eq!(
            running,
            Health::Running,
            "the app runs once every route opened"
        );
        passed("the app reports Running once every subscription is open");
        assert_eq!(
            seen,
            (1, 1),
            "readings handled on the frames and on the bus"
        );
        passed("one app runs the routes of two brokers");
        assert_eq!(
            frame_allocations, 0,
            "allocations of one delivery through the app"
        );
        passed("a delivery through the app allocates nothing");
        assert_eq!(
            health,
            [
                Health::Starting,
                Health::Running,
                Health::ShuttingDown,
                Health::Stopped
            ],
            "the health the app moved through"
        );
        assert!(outcome(stand).is_ok(), "a stopped app returns Ok");
        passed("a stop moves the app through ShuttingDown to Stopped");
    }

    async fn check_fail_fast(spawner: Spawner, stand: &'static Stand) {
        stand.health.borrow_mut().push(stand.failing.health());
        spawner.spawn(watch(&stand.failing, stand).expect("a watcher task is free"));
        spawner.spawn(run_failing(stand).expect("the app task is free"));
        settle().await;
        stand.frames.send(frame(b"reading")).await;
        settle().await;
        stand.frames.send(frame(b"")).await;
        settle().await;
        let health = stand.health.borrow_mut().drain(..).collect::<Vec<_>>();

        assert_eq!(
            stand.seen_failing.load(Ordering::Relaxed),
            1,
            "readings handled before the failure"
        );
        assert_eq!(
            health,
            [Health::Starting, Health::Running, Health::Failed],
            "the health the failing app moved through"
        );
        assert!(
            matches!(outcome(stand), Err(RunError::Failed)),
            "a failed app returns the failure"
        );
        passed("a fail-fast ends the app with its health Failed");
    }

    async fn check_hooks(spawner: Spawner, stand: &'static Stand) {
        stand.health.borrow_mut().push(stand.hooked.health());
        spawner.spawn(watch(&stand.hooked, stand).expect("a watcher task is free"));
        spawner.spawn(run_hooked(stand).expect("the app task is free"));
        settle().await;
        stand.hooked.stop();
        settle().await;
        let calls = stand.calls.borrow().clone();
        stand.health.borrow_mut().clear();

        assert_eq!(
            calls,
            ["after_startup", "on_shutdown", "after_shutdown"],
            "the hooks in the order they ran"
        );
        assert!(outcome(stand).is_ok(), "the hooked app stops cleanly");
        passed("the hooks run in the order of the app's life, with its state");
    }

    async fn check_hooks_alone(spawner: Spawner, stand: &'static Stand) {
        stand.calls.borrow_mut().clear();
        spawner.spawn(run_alone(stand).expect("the app task is free"));
        settle().await;
        let calls = stand.calls.borrow().clone();

        assert_eq!(
            calls,
            ["after_startup", "on_shutdown", "after_shutdown"],
            "the hooks in the order they ran, past the one that failed"
        );
        assert_eq!(
            stand.alone.health(),
            Health::Stopped,
            "an app with nothing to serve ends once it started"
        );
        assert!(
            outcome(stand).is_ok(),
            "a failing shutdown hook is logged, not returned"
        );
        passed("an app of hooks alone runs them through and ends, past a failing shutdown hook");
    }

    async fn check_stream_end(spawner: Spawner, stand: &'static Stand) {
        stand.health.borrow_mut().push(stand.ending.health());
        spawner.spawn(watch(&stand.ending, stand).expect("a watcher task is free"));
        spawner.spawn(run_ending(stand).expect("the app task is free"));
        settle().await;
        let ended_while_running = stand.ending.stream_ended();
        let connected = stand
            .ending_bus
            .clone()
            .connect()
            .await
            .expect("the bus connects");
        connected.shutdown().await.expect("the bus shuts down");
        settle().await;
        let health = stand.health.borrow_mut().drain(..).collect::<Vec<_>>();

        assert!(
            !ended_while_running,
            "a stream reported ended while the bus ran"
        );
        assert!(
            stand.ending.stream_ended(),
            "the control reports the stream that ended"
        );
        assert_eq!(
            health,
            [Health::Starting, Health::Running, Health::Stopped],
            "the health of the app whose stream ended"
        );
        assert!(
            outcome(stand).is_ok(),
            "an app whose streams ended returns Ok"
        );
        passed(
            "an app whose every stream ended, in order or in a pool, stops on its own and says so",
        );
    }

    #[embassy_executor::task]
    async fn drive(spawner: Spawner, stand: &'static Stand) {
        check_two_brokers(spawner, stand).await;
        check_fail_fast(spawner, stand).await;
        check_hooks(spawner, stand).await;
        check_hooks_alone(spawner, stand).await;
        check_stream_end(spawner, stand).await;
        stand.done.set(true);
    }

    pub(super) fn main() {
        static STAND: StaticCell<Stand> = StaticCell::new();
        static EXECUTOR: StaticCell<Executor> = StaticCell::new();

        let stand: &'static Stand = STAND.init(Stand {
            frames: Frames::new(),
            settled: Settlements::new(),
            bus: Bus::new(),
            seen_frames: AtomicU32::new(0),
            seen_bus: AtomicU32::new(0),
            two: Control::new(),
            failing: Control::new(),
            seen_failing: AtomicU32::new(0),
            hooked: Control::new(),
            ending_bus: Bus::new(),
            ending: Control::new(),
            alone: Control::new(),
            calls: RefCell::new(Vec::new()),
            health: RefCell::new(Vec::new()),
            outcome: RefCell::new(None),
            done: Cell::new(false),
        });
        EXECUTOR.init(Executor::new()).run_until(
            |spawner: Spawner| {
                spawner.spawn(drive(spawner, stand).expect("the driver task is free"));
            },
            || stand.done.get(),
        );
    }
}

fn main() {
    #[cfg(not(feature = "testing"))]
    scenarios::main();
}
