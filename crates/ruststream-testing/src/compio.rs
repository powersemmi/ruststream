//! The [`TestApp`] harness of the compio runner, over the test's own compio runtime.
//!
//! It runs a service's production app in process, every broker connected in process, and moves the
//! clock the app reads through the runner's virtual clock.
//!
//! The facade re-exports it as `ruststream::testing::compio::TestApp` when the build enables the
//! `compio` runtime.

use std::any::{TypeId, type_name};
use std::fmt;
use std::future::poll_fn;
use std::pin::{Pin, pin};
use std::task::Poll;
use std::time::Duration;

use futures::future::{self, Either};
use ruststream_runtime::ConnectedLifecycle;
use ruststream_runtime::OutSlot;
use ruststream_runtime::testing::{Coordinator, PublishedAssertions, TestError, Within};
use ruststream_runtime_compio::testing::{Quiescence, Runnable, VirtualClock, armed_timers};
use ruststream_runtime_compio::{
    App, InProcessError, InProcessStart, RunningApp, RustStreamError, StateSource, yield_now,
};

mod brokers;
mod handle;

pub use brokers::TestBrokers;
use brokers::registration_of;
pub use handle::{BrokerHandle, InjectSink};

/// The default cap on dispatched deliveries before a step gives up driving a reaction to
/// quiescence: a guard against a handler that requeues forever.
const MAX_STEPS: usize = 10_000;

/// A test harness around a compio service's production app.
///
/// Takes the app `main` builds, unchanged. [`start`](Self::start) connects each of its brokers
/// through the broker's [`InProcess`](ruststream_runtime::testing::InProcess) transition instead
/// of `connect`, runs the app as the only core of the test's own compio runtime, and then drives
/// input through a broker addressed by its production type. A publish returns once the app has
/// handled everything it set off, up to what waits on the clock.
///
/// The harness holds the runner's virtual clock from `start` until it is dropped: the app's
/// retries, batch deadlines and `ruststream::time::sleep` wait until [`advance`](Self::advance)
/// moves the clock. Work that waits on compio's own timers or on I/O is not advanced: with nothing
/// else to run and no timer of the clock armed, `settle` returns [`TestError::Stalled`]. One
/// `TestApp` runs at a time on a thread.
///
/// # Examples
///
/// ```
/// use compio::runtime::Runtime;
/// use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
/// use ruststream::runtime::compio::{AppInfo, RustStream};
/// use ruststream::runtime::HandlerOutcome;
/// use ruststream::testing::compio::TestApp;
/// use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
///
/// type Bus = LocalMemoryBroker<4>;
///
/// /// A temperature frame, as the sensor sends its bytes.
/// #[derive(Deserialized)]
/// struct Reading<'a>(&'a [u8]);
///
/// #[subscriber(LocalMemorySource::new("sensors/temp"))]
/// async fn on_reading(reading: &Reading<'_>) -> HandlerOutcome {
///     let _ = reading.0;
///     HandlerOutcome::ack()
/// }
///
/// /// The app `main` runs, and the one the test hands the harness.
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("sensors", "0.1.0"))
///         .with_broker(Bus::new(), |b| b.include(on_reading))
/// }
///
/// /// A frame as the test's sensor sends it.
/// #[derive(Outgoing, Serialized)]
/// #[outgoing(name = "sensors/temp")]
/// struct Sent(&'static [u8]);
///
/// async fn reads_the_temperature() -> Result<(), Box<dyn std::error::Error>> {
///     let tb = TestApp::start(app()).await?;
///     tb.broker::<Bus>().message(&Sent(b"21")).publish().await?;
///
///     tb.broker::<Bus>()
///         .subscriber("sensors/temp")
///         .assert_called_once()
///         .with_raw(b"21")
///         .settled(HandlerOutcome::ack());
///     tb.shutdown().await?;
///     Ok(())
/// }
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// #     Runtime::new()?.block_on(reads_the_temperature())
/// # }
/// ```
pub struct TestApp {
    running: RunningApp,
    coordinator: Coordinator,
    clock: VirtualClock,
    quiescence: Quiescence,
}

impl fmt::Debug for TestApp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestApp")
            .field("brokers", &self.running.connected().count())
            .finish_non_exhaustive()
    }
}

impl TestApp {
    /// Starts the service's `app` in process: the app's real `on_startup`, each broker's
    /// in-process connect, the subscriptions, then `after_startup`, in the production order.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NoTransport`] if a broker of the app has no in-process mode,
    /// [`TestError::Startup`] if a lifecycle hook fails, [`TestError::Connect`] if a broker fails
    /// to connect in process, or [`TestError::Subscribe`] if a subscription fails to open.
    ///
    /// # Panics
    ///
    /// Panics if another `TestApp` is alive on this thread: the clock is one per thread.
    ///
    /// # Examples
    ///
    /// See [`TestApp`].
    pub async fn start<A: App>(app: A) -> Result<Self, TestError> {
        Self::run(app, StateSource::App).await
    }

    /// Starts the service's `app` in process with a state the test builds in place of the one
    /// the app's `on_startup` produces.
    ///
    /// `build` runs once every broker is connected, before any subscription delivers, and
    /// receives the brokers so the state can hold what it needs of them
    /// ([`TestBrokers::broker`]).
    ///
    /// # Errors
    ///
    /// As [`start`](Self::start), except that the app's `on_startup` does not run.
    ///
    /// # Panics
    ///
    /// As [`start`](Self::start).
    pub async fn with_state<A, Build>(app: A, build: Build) -> Result<Self, TestError>
    where
        A: App,
        Build: FnOnce(&TestBrokers<'_>) -> A::State + Send + 'static,
    {
        let build = Box::new(move |connected: &[&(dyn ConnectedLifecycle + 'static)]| {
            build(&TestBrokers { connected })
        });
        Self::run(app, StateSource::Built(build)).await
    }

    async fn run<A: App>(app: A, state: StateSource<A::State>) -> Result<Self, TestError> {
        // Taken before the app starts, so every task of its startup is followed.
        let quiescence = Quiescence::take();
        let clock = VirtualClock::take();
        let coordinator = Coordinator::in_place(MAX_STEPS);
        let start = InProcessStart::new(coordinator.clone(), registration_of, state);
        let running = app.start_in_process(start).await.map_err(startup_error)?;
        Ok(Self {
            running,
            coordinator,
            clock,
            quiescence,
        })
    }

    /// Lets the app handle everything already delivered to it, up to what waits on the clock.
    ///
    /// A publish settles on its own; this is for work the test set off another way.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NotQuiescent`] if the app keeps producing work.
    pub async fn settle(&self) -> Result<(), TestError> {
        settled(&self.coordinator, &self.quiescence.reader()).await
    }

    /// Moves the virtual clock forward by `by` and lets the app handle what fell due: a retry
    /// copy, a batch deadline, a handler that slept on `ruststream::time::sleep`.
    ///
    /// # Errors
    ///
    /// As [`settle`](Self::settle).
    pub async fn advance(&self, by: Duration) -> Result<(), TestError> {
        self.clock.advance(by);
        self.settle().await
    }

    /// Stops the app the way a signal stops it: the `on_shutdown` hooks, the subscriptions, the
    /// handlers and continuations in flight, every broker in reverse order, and the
    /// `after_shutdown` hooks.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::Failed`] with the runner's error if the app stopped on a failure
    /// before or during the shutdown, or a broker failed to shut down.
    pub async fn shutdown(self) -> Result<(), TestError> {
        let Self {
            running,
            coordinator,
            clock,
            quiescence,
        } = self;
        let stopped = {
            let runnable = quiescence.reader();
            let mut shutdown = pin!(running.shutdown());
            // Work the app holds that waits on the clock runs to its end: the clock moves from
            // one armed deadline to the next until the app has stopped.
            loop {
                let idle = pin!(idle(&runnable));
                match future::select(shutdown.as_mut(), idle).await {
                    Either::Left((stopped, _)) => break stopped,
                    Either::Right(((), _)) => {
                        if !clock.advance_to_next() {
                            return Err(TestError::Stalled);
                        }
                    }
                }
            }
        };
        drop(clock);
        coordinator.hooks().raise_failure();
        stopped.map_err(|err| TestError::Failed(Box::new(err)))
    }

    /// Addresses the broker of type `B`, the production type the app was built with.
    ///
    /// # Panics
    ///
    /// Panics if the app has no broker of type `B`, or more than one.
    #[must_use]
    pub fn broker<B: ruststream_core::Broker + 'static>(&self) -> BrokerHandle<'_> {
        let mut matches = self
            .running
            .connected()
            .enumerate()
            .filter(|(_, (_, connected))| connected.broker_type() == TypeId::of::<B>());
        let (scope_id, (_, connected)) = matches
            .next()
            .unwrap_or_else(|| panic!("the app has no broker of type {}", type_name::<B>()));
        assert!(
            matches.next().is_none(),
            "the app has more than one broker of type {}",
            type_name::<B>(),
        );
        let broker = registration_of(TypeId::of::<B>())
            .and_then(|registration| registration.resolve(connected.as_any()))
            .expect("an in-process broker connected through its registration");
        BrokerHandle {
            coordinator: &self.coordinator,
            runnable: self.quiescence.reader(),
            scope_id,
            broker,
            name: connected.name(),
        }
    }

    /// Asserts on what was published through the [`Out`](ruststream_runtime::Out) slot marked
    /// `M`: exactly the messages the handlers sent through that slot, with their destinations
    /// and headers, across all brokers.
    #[must_use]
    pub fn out<M: OutSlot>(&self) -> PublishedAssertions<()> {
        PublishedAssertions::captured(&self.coordinator, M::NAME, Within::ALL)
    }
}

/// Drives the reaction to a standstill: everything delivered is handled, and what remains waits on
/// the virtual clock.
///
/// The app is settled once no task of its runtime is runnable and either nothing is in flight or a
/// timer of the virtual clock waits: the work that remains is the clock's to release. Nothing
/// runnable, a delivery in flight and no timer armed is [`TestError::Stalled`]: the delivery waits
/// on something neither the app nor the clock wakes (a handler that sleeps on `compio::time`
/// directly, whose timer `advance` does not move, ends here too).
///
/// compio exposes no signal that its runtime is idle, so the runner follows its own tasks
/// ([`Quiescence`]). The harness yields first, which lets every task woken before the yield run,
/// and reads the count after.
pub(crate) async fn settled(
    coordinator: &Coordinator,
    runnable: &Runnable,
) -> Result<(), TestError> {
    // The step budget counts from here; `drive` begins it again only when it is made anew.
    coordinator.begin_step();
    let mut drive: Option<Pin<Box<_>>> = None;
    let settled = loop {
        yield_now().await;
        // The budget is read before `drive` is: both measure from the same step, so `drive` would
        // report the same exhaustion, with however many deliveries ran in this turn on top.
        if coordinator.exhausted() {
            break Err(TestError::NotQuiescent {
                processed: MAX_STEPS,
            });
        }
        let drive_now = drive.get_or_insert_with(|| Box::pin(coordinator.drive()));
        // `drive` is ready exactly when nothing is in flight; its error is the exhaustion above.
        let nothing_in_flight = poll_fn(|cx| Poll::Ready(drive_now.as_mut().poll(cx)))
            .await
            .is_ready();
        if nothing_in_flight {
            // Read again at the next look: work may have been set off since.
            drive = None;
        }
        if runnable.count() > 0 {
            continue;
        }
        break if nothing_in_flight || armed_timers() > 0 {
            Ok(())
        } else {
            Err(TestError::Stalled)
        };
    };
    coordinator.hooks().raise_failure();
    settled
}

/// Resolves once no task of the app's runtime is runnable, after the harness has yielded to them.
async fn idle(runnable: &Runnable) {
    loop {
        yield_now().await;
        if runnable.count() == 0 {
            return;
        }
    }
}

/// The harness's error for an app that stopped while it started.
fn startup_error(err: RustStreamError) -> TestError {
    match err {
        RustStreamError::Connect(source) => match source.downcast::<InProcessError>() {
            Ok(failure) => match *failure {
                InProcessError::NoTransport(broker) => TestError::NoTransport(broker),
                InProcessError::Connect { broker, source } => TestError::Connect { broker, source },
                other => TestError::Startup(Box::new(other)),
            },
            Err(source) => TestError::Startup(source),
        },
        RustStreamError::Startup(source) => TestError::Startup(source),
        RustStreamError::Subscribe(source) => TestError::Subscribe(source),
        other => TestError::Startup(Box::new(other)),
    }
}
