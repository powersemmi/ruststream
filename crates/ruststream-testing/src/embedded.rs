//! The [`TestApp`] harness of the embedded runtime: it runs a device service's production
//! [`EmbeddedApp`] on the host, every broker connected in process, and moves the clock the app
//! reads through embassy-time.
//!
//! The facade re-exports it as `ruststream::testing::TestApp` when the build selects the
//! `embassy` runtime.

// The app and its brokers are not `Send`: the harness polls them inside the test's own future.
#![allow(clippy::future_not_send)]

use std::any::type_name;
use std::cell::RefCell;
use std::fmt;
use std::future::poll_fn;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use ruststream_runtime::OutSlot;
use ruststream_runtime::testing::{
    Coordinator, InProcessRun, LocalInProcess, PublishedAssertions, TestError, Within,
};
use ruststream_runtime_embassy::{Control, EmbeddedApp, Health, Mounts, RunError};

mod brokers;
mod clock;
mod handle;

pub use brokers::TestBrokers;
use clock::ClockLease;
pub use handle::{BrokerHandle, InjectSink};

/// How many times one step polls the app before it gives up on a reaction that keeps waking
/// itself: a guard against a handler that requeues forever.
const MAX_POLLS: usize = 10_000;

/// The app's run, boxed: the harness holds it across the test's steps and polls it in each.
type AppRun = Pin<Box<dyn Future<Output = Result<(), RunError>>>>;

/// The hooks of one event the app recorded, which the harness polls beside the app.
type HookRun = Pin<Box<dyn Future<Output = ()>>>;

/// A test harness around a device service's production app, on the host.
///
/// Takes the [`EmbeddedApp`] the device's `app` function builds, unchanged, with the brokers the
/// test hands that function. [`start`](Self::start) connects each broker through its
/// [`LocalInProcess`] transition instead of `connect`, so the same connected forms carry the
/// routes with no hardware. The test then drives input through a broker, addressed by its
/// production type, and reads what the handlers saw and what the app published.
///
/// The app runs inside the test's own future: each step (a publish, [`settle`](Self::settle),
/// [`advance`](Self::advance), [`shutdown`](Self::shutdown)) polls it until it has nothing left
/// to do without the clock moving. A handler that waits on embassy-time's clock waits until the
/// test advances it, or until `shutdown` lets it run to its end.
///
/// The hooks a test registers on its assertions
/// ([`on_call`](ruststream_runtime::testing::SubscriberAssertions::on_call),
/// [`on_publish`](PublishedAssertions::on_publish)) run in the same steps: the dispatch that
/// recorded the event waits for them, and the step polls them beside the app, so a hook needs no
/// thread and may wait on the clock. A panic inside a hook, a failed assertion included, is raised
/// again from the step that ran it.
///
/// [`with_state`](Self::with_state) starts the app with a state the test builds, and
/// [`out`](Self::out) reads what went through an `Out` slot.
///
/// The harness is the test build's embassy-time driver: the clock starts at zero and moves only
/// when the test moves it. A process has one driver, so the harness holds the clock from `start`
/// until it is dropped: tests that build a `TestApp` run one at a time in a process, and one test
/// runs one `TestApp` at a time. embassy-time's own `mock-driver` is another driver of the same
/// clock, and a test build links only one of the two; a device build keeps the driver of its
/// board.
///
/// # Examples
///
/// ```
/// use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
/// use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
/// use ruststream::testing::TestApp;
/// use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
///
/// /// The board's bus: each subscription holds four messages.
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
/// /// The app the device's task runs, and the one the test hands the harness.
/// #[ruststream::app]
/// fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| b.include(on_reading))
/// }
///
/// /// A frame as the test's sensor sends it.
/// #[derive(Outgoing, Serialized)]
/// #[outgoing(name = "sensors/temp")]
/// struct Sent(&'static [u8]);
///
/// async fn reads_the_temperature() -> Result<(), Box<dyn std::error::Error>> {
///     let tb = TestApp::start(app(Bus::new())).await?;
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
/// #     embassy_futures::block_on(reads_the_temperature())
/// # }
/// ```
pub struct TestApp {
    run: RefCell<Option<AppRun>>,
    /// The hooks of events a dispatch of the app waits on, polled in the same steps as the app.
    hooks: RefCell<Vec<HookRun>>,
    ended: RefCell<Option<Result<(), RunError>>>,
    control: Rc<Control>,
    record: Rc<InProcessRun>,
    waker: Arc<AppWaker>,
    clock: ClockLease,
}

impl fmt::Debug for TestApp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestApp")
            .field("brokers", &self.record.brokers().len())
            .field("health", &self.control.health())
            .finish_non_exhaustive()
    }
}

/// The waker the harness polls the app with: it notes that something woke the app, so a step
/// polls again until nothing does.
#[derive(Default)]
struct AppWaker {
    woken: AtomicBool,
}

impl Wake for AppWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
    }
}

impl TestApp {
    /// Starts the device service's `app` in process: each broker connects through its
    /// [`LocalInProcess`] transition, every subscription opens, and the `after_startup` hooks
    /// run, in the production order.
    ///
    /// A connection or a subscription that waits on the clock is waited for: the clock moves
    /// from one armed deadline to the next until the app is running, as time would pass on the
    /// board. An `after_startup` hook that waits on the clock stays queued like a handler does,
    /// and finishes on [`advance`](Self::advance) or [`shutdown`](Self::shutdown).
    ///
    /// # Errors
    ///
    /// Returns [`TestError::Connect`] if a broker fails to connect in process,
    /// [`TestError::Subscribe`] if a subscription fails to open, [`TestError::Startup`] if an
    /// `after_startup` hook fails, [`TestError::Stalled`] if the startup waits on something that
    /// neither the app nor the clock wakes, or [`TestError::NotQuiescent`] if the app keeps
    /// waking itself.
    ///
    /// # Panics
    ///
    /// Panics if another `TestApp` is alive on this thread: the clock is one per process.
    ///
    /// # Examples
    ///
    /// See [`TestApp`].
    pub async fn start<M: Mounts + 'static>(app: EmbeddedApp<M>) -> Result<Self, TestError> {
        Self::run(app, |state, _brokers| state).await
    }

    /// Starts the device service's `app` in process with a `state` the test builds in place of
    /// the one the app was built with, as [`start`](Self::start) starts it otherwise.
    ///
    /// `build` runs once every broker is connected, before any subscription delivers, and
    /// receives the brokers so the state can hold what it needs of them
    /// ([`TestBrokers::broker`]). The app's own state is dropped unread: a value read from the
    /// board's flash, a driver of its hardware, whatever the test replaces with a stand-in.
    ///
    /// The app function names its state in its type, `EmbeddedApp<impl Mounts<State = ..>>`,
    /// for the test to build one.
    ///
    /// # Errors
    ///
    /// As [`start`](Self::start).
    ///
    /// # Panics
    ///
    /// As [`start`](Self::start).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
    /// use ruststream::runtime::{Context, EmbeddedApp, HandlerOutcome, Mounts};
    /// use ruststream::testing::TestApp;
    /// use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
    ///
    /// type Bus = LocalMemoryBroker<4>;
    ///
    /// /// What the board's handlers read: the level from which the pump runs.
    /// struct Limits {
    ///     start_pump: u8,
    /// }
    ///
    /// impl Limits {
    ///     /// The limits the board keeps in its flash.
    ///     fn from_flash() -> Self {
    ///         Self { start_pump: 200 }
    ///     }
    /// }
    ///
    /// /// A tank's level, one byte.
    /// #[derive(Deserialized)]
    /// struct Level<'a>(&'a [u8]);
    ///
    /// /// Acknowledges a level below the limit, and leaves one at or above it to the pump's
    /// /// subscription by dropping it here.
    /// #[subscriber(LocalMemorySource::new("tank/level"))]
    /// async fn watch(level: &Level<'_>, ctx: &mut Context<'_, (), Limits>) -> HandlerOutcome {
    ///     if level.0.first().copied().unwrap_or(0) < ctx.state().start_pump {
    ///         HandlerOutcome::ack()
    ///     } else {
    ///         HandlerOutcome::drop()
    ///     }
    /// }
    ///
    /// /// The app names its state in its type, for a test to hand it one.
    /// fn app(bus: Bus) -> EmbeddedApp<impl Mounts<State = Limits>> {
    ///     EmbeddedApp::new()
    ///         .with_state(Limits::from_flash())
    ///         .with_broker(bus, |b| b.include(watch))
    /// }
    ///
    /// /// A level as the test's sensor sends it.
    /// #[derive(Outgoing, Serialized)]
    /// #[outgoing(name = "tank/level")]
    /// struct Sent(&'static [u8]);
    ///
    /// async fn a_low_limit_starts_the_pump() -> Result<(), Box<dyn std::error::Error>> {
    ///     let tb = TestApp::with_state(app(Bus::new()), |_brokers| Limits { start_pump: 10 })
    ///         .await?;
    ///     tb.broker::<Bus>().message(&Sent(&[50])).publish().await?;
    ///
    ///     tb.broker::<Bus>()
    ///         .subscriber("tank/level")
    ///         .assert_called_once()
    ///         .settled(HandlerOutcome::drop());
    ///     tb.shutdown().await?;
    ///     Ok(())
    /// }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #     embassy_futures::block_on(a_low_limit_starts_the_pump())
    /// # }
    /// ```
    pub async fn with_state<M, Build>(app: EmbeddedApp<M>, build: Build) -> Result<Self, TestError>
    where
        M: Mounts + 'static,
        Build: FnOnce(&TestBrokers<'_>) -> M::State + 'static,
    {
        Self::run(app, move |_app_state, brokers| build(brokers)).await
    }

    /// Runs `app` in process, its state the one `state` makes of the app's own once every broker
    /// is connected, and lets it start.
    async fn run<M, Make>(app: EmbeddedApp<M>, state: Make) -> Result<Self, TestError>
    where
        M: Mounts + 'static,
        Make: FnOnce(M::State, &TestBrokers<'_>) -> M::State + 'static,
    {
        let clock = ClockLease::take();
        let record = Rc::new(InProcessRun::new(Coordinator::in_place(MAX_POLLS)));
        let control = Rc::new(Control::new());
        let run: AppRun = {
            let record = Rc::clone(&record);
            let control = Rc::clone(&control);
            Box::pin(async move {
                let swap = |own| state(own, &TestBrokers::of(&record.brokers()));
                app.run_in_process(&control, &record, swap).await
            })
        };
        let tb = Self {
            run: RefCell::new(Some(run)),
            hooks: RefCell::new(Vec::new()),
            ended: RefCell::new(None),
            control,
            record,
            waker: Arc::default(),
            clock,
        };
        tb.await_running().await?;
        match tb.ended.borrow_mut().take() {
            None | Some(Ok(())) => {}
            Some(Err(err)) => return Err(startup_error(err)),
        }
        Ok(tb)
    }

    /// Lets the app handle everything already delivered to it, up to what waits on the clock.
    ///
    /// A publish settles on its own; this is for work the test set off another way.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::NotQuiescent`] if the app keeps waking itself.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ruststream::memory::local::LocalMemoryBroker;
    /// # use ruststream::runtime::{EmbeddedApp, Mounts};
    /// use ruststream::testing::TestApp;
    ///
    /// # fn app(bus: LocalMemoryBroker<4>) -> EmbeddedApp<impl Mounts> {
    /// #     EmbeddedApp::new().with_broker(bus, |b| b)
    /// # }
    /// async fn settles(bus: LocalMemoryBroker<4>) -> Result<(), Box<dyn std::error::Error>> {
    ///     let tb = TestApp::start(app(bus)).await?;
    ///     tb.settle().await?;
    ///     tb.shutdown().await?;
    ///     Ok(())
    /// }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #     embassy_futures::block_on(settles(LocalMemoryBroker::new()))
    /// # }
    /// ```
    pub async fn settle(&self) -> Result<(), TestError> {
        self.drive().await
    }

    /// Moves embassy-time's clock forward by `by` and lets the app handle what fell due: a
    /// handler that waited on a timer, a retry copy, a step of a periodic task.
    ///
    /// # Errors
    ///
    /// As [`settle`](Self::settle).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use embassy_time::Timer;
    /// use ruststream::memory::local::{LocalMemoryBroker, LocalMemorySource};
    /// use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts};
    /// use ruststream::testing::TestApp;
    /// use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
    ///
    /// type Bus = LocalMemoryBroker<4>;
    ///
    /// /// A request to open the valve.
    /// #[derive(Deserialized)]
    /// struct Open<'a>(&'a [u8]);
    ///
    /// /// Holds the valve open for five seconds, then acknowledges.
    /// #[subscriber(LocalMemorySource::new("valve/open"))]
    /// async fn on_open(open: &Open<'_>) -> HandlerOutcome {
    ///     let _ = open.0;
    ///     Timer::after_secs(5).await;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    ///     EmbeddedApp::new().with_broker(bus, |b| b.include(on_open))
    /// }
    ///
    /// /// The request as the test sends it.
    /// #[derive(Outgoing, Serialized)]
    /// #[outgoing(name = "valve/open")]
    /// struct Sent(&'static [u8]);
    ///
    /// async fn closes_after_the_delay() -> Result<(), Box<dyn std::error::Error>> {
    ///     let tb = TestApp::start(app(Bus::new())).await?;
    ///     tb.broker::<Bus>().message(&Sent(b"open")).publish().await?;
    ///     tb.broker::<Bus>().subscriber("valve/open").assert_not_called();
    ///
    ///     tb.advance(Duration::from_secs(5)).await?;
    ///     tb.broker::<Bus>().subscriber("valve/open").assert_called_once();
    ///     tb.shutdown().await?;
    ///     Ok(())
    /// }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #     embassy_futures::block_on(closes_after_the_delay())
    /// # }
    /// ```
    pub async fn advance(&self, by: Duration) -> Result<(), TestError> {
        self.clock.advance(by);
        self.drive().await
    }

    /// Stops the app the way its control stops it on a device: every subscription finishes what
    /// it holds, the shutdown hooks run, and every broker shuts down.
    ///
    /// Work the app holds that waits on the clock runs to its end: the clock moves from one
    /// armed deadline to the next until the app has stopped, as time would pass on the board.
    ///
    /// # Errors
    ///
    /// Returns [`TestError::Failed`] with the app's own [`RunError`] if it stopped on one before
    /// or during the shutdown (a subscription failed under a fail-fast policy, say),
    /// [`TestError::Stalled`] if work it holds waits on something that neither the app nor the
    /// clock wakes, or [`TestError::NotQuiescent`] if it keeps waking itself.
    ///
    /// # Examples
    ///
    /// See [`TestApp`].
    pub async fn shutdown(self) -> Result<(), TestError> {
        self.control.stop();
        // Each round lets the app and the queued hooks run until they wait on the clock, then
        // moves the clock to the deadline they wait for; the round limit ends a stop that a
        // periodic timer never lets come. A hook still waiting holds its checks, so the shutdown
        // returns only once the app has ended and every hook has finished.
        for _ in 0..MAX_POLLS {
            self.drive().await?;
            if self.run.borrow().is_none() && self.hooks.borrow().is_empty() {
                return self
                    .ended
                    .take()
                    .unwrap_or(Ok(()))
                    .map_err(|err| TestError::Failed(Box::new(err)));
            }
            if !self.clock.advance_to_next() {
                return Err(TestError::Stalled);
            }
        }
        Err(TestError::NotQuiescent {
            processed: MAX_POLLS,
        })
    }

    /// Addresses the broker of type `B`, the production type the app was built with.
    ///
    /// # Panics
    ///
    /// Panics if the app has no broker of type `B`, or more than one.
    ///
    /// # Examples
    ///
    /// See [`TestApp`].
    #[must_use]
    pub fn broker<B: LocalInProcess + 'static>(&self) -> BrokerHandle<'_> {
        let brokers = self.record.brokers();
        let mut matches = brokers.iter().filter(|broker| broker.is::<B>());
        let first = matches
            .next()
            .unwrap_or_else(|| panic!("the app has no broker of type {}", type_name::<B>()));
        assert!(
            matches.next().is_none(),
            "the app has more than one broker of type {}",
            type_name::<B>(),
        );
        BrokerHandle::new(self, first.clone())
    }

    /// Runs the startup to its end: until the app is running, or has stopped on a startup error.
    /// What the startup waits on the clock for is waited out, deadline by deadline.
    async fn await_running(&self) -> Result<(), TestError> {
        for _ in 0..MAX_POLLS {
            self.drive().await?;
            if self.run.borrow().is_none() || self.control.health() != Health::Starting {
                return Ok(());
            }
            if !self.clock.advance_to_next() {
                return Err(TestError::Stalled);
            }
        }
        Err(TestError::NotQuiescent {
            processed: MAX_POLLS,
        })
    }

    /// Asserts on what was published through the [`Out`](ruststream_runtime::Out) slot marked
    /// `M`: exactly the messages the handlers sent through that slot, with their destinations
    /// and headers, across all brokers.
    ///
    /// A hook registered on the view with
    /// [`on_publish`](PublishedAssertions::on_publish) runs at each publish through the slot,
    /// inside the step that drove it.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
    /// use ruststream::runtime::{EmbeddedApp, HandlerOutcome, Mounts, Out};
    /// use ruststream::testing::TestApp;
    /// use ruststream::{Deserialized, OutSlot, Outgoing, Publisher, Serialized, subscriber};
    ///
    /// type Bus = LocalMemoryBroker<4>;
    ///
    /// /// A tank's level, as the sensor sends its bytes.
    /// #[derive(Deserialized)]
    /// struct Level<'a>(&'a [u8]);
    ///
    /// /// The alarm the tank raises, carrying the level that set it off.
    /// #[derive(Outgoing, Serialized)]
    /// #[outgoing(name = "alarms")]
    /// struct Alarm(Vec<u8>);
    ///
    /// /// The slot alarms leave through.
    /// #[derive(OutSlot)]
    /// #[publishes(Alarm)]
    /// struct Alarms;
    ///
    /// #[subscriber(LocalMemorySource::new("tank/level"))]
    /// async fn watch(level: &Level<'_>, Out(alarms): Out<impl Publisher, Alarms>) -> HandlerOutcome {
    ///     match alarms.message(&Alarm(level.0.to_vec())).publish().await {
    ///         Ok(()) => HandlerOutcome::ack(),
    ///         Err(_) => HandlerOutcome::retry(),
    ///     }
    /// }
    ///
    /// fn app(bus: Bus) -> EmbeddedApp<impl Mounts> {
    ///     EmbeddedApp::new().with_broker(bus, |b| b.include(watch).out(Alarms, LocalMemoryPublish))
    /// }
    ///
    /// /// A level as the test's sensor sends it.
    /// #[derive(Outgoing, Serialized)]
    /// #[outgoing(name = "tank/level")]
    /// struct Sent(&'static [u8]);
    ///
    /// async fn raises_the_alarm() -> Result<(), Box<dyn std::error::Error>> {
    ///     let tb = TestApp::start(app(Bus::new())).await?;
    ///     tb.broker::<Bus>().message(&Sent(b"full")).publish().await?;
    ///
    ///     tb.out::<Alarms>().assert_called_once().with_raw(b"full");
    ///     tb.shutdown().await?;
    ///     Ok(())
    /// }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #     embassy_futures::block_on(raises_the_alarm())
    /// # }
    /// ```
    #[must_use]
    pub fn out<M: OutSlot>(&self) -> PublishedAssertions<()> {
        PublishedAssertions::captured(self.coordinator(), M::NAME, Within::ALL)
    }

    /// The coordinator the run records into.
    pub(crate) fn coordinator(&self) -> &Coordinator {
        self.record.coordinator()
    }

    /// Starts the hooks of the events the app recorded since the last poll, and polls every hook
    /// still running.
    fn poll_hooks(&self, cx: &mut Context<'_>) {
        let mut hooks = self.hooks.borrow_mut();
        let coordinator = self.coordinator();
        hooks.extend(
            coordinator
                .hooks()
                .take_pending()
                .into_iter()
                .map(|pending| pending.into_future(coordinator.clone())),
        );
        hooks.retain_mut(|hook| hook.as_mut().poll(cx).is_pending());
    }

    /// Polls the app once, unless it already ended, and keeps how it ended.
    fn poll_app(&self, cx: &mut Context<'_>) {
        let mut slot = self.run.borrow_mut();
        if let Some(run) = slot.as_mut()
            && let Poll::Ready(ended) = run.as_mut().poll(cx)
        {
            *slot = None;
            *self.ended.borrow_mut() = Some(ended);
        }
    }

    /// Polls the app until nothing wakes it: everything delivered is handled, and what remains
    /// waits on the clock.
    async fn drive(&self) -> Result<(), TestError> {
        let driven = poll_fn(move |_cx| {
            let waker = Waker::from(Arc::clone(&self.waker));
            let mut cx = Context::from_waker(&waker);
            for _ in 0..MAX_POLLS {
                self.waker.woken.store(false, Ordering::Release);
                self.poll_app(&mut cx);
                // A hook that finishes lets the dispatch waiting on it go on, which wakes the app;
                // the hooks of the test's own publish run here too, the app stopped or not.
                self.poll_hooks(&mut cx);
                if !self.waker.woken.load(Ordering::Acquire) {
                    return Poll::Ready(Ok(()));
                }
            }
            Poll::Ready(Err(TestError::NotQuiescent {
                processed: MAX_POLLS,
            }))
        })
        .await;
        // The hooks ran inside the polls above; a failure one of them left fails this step.
        self.coordinator().hooks().raise_failure();
        driven
    }
}

/// The harness's error for an app that stopped while it started.
fn startup_error(err: RunError) -> TestError {
    match err {
        RunError::Connect { broker, source } => TestError::Connect {
            broker: broker.to_owned(),
            source,
        },
        RunError::Subscribe { source, .. } => TestError::Subscribe(source),
        RunError::Startup { source } => TestError::Startup(source),
        other => TestError::Startup(Box::new(other)),
    }
}
