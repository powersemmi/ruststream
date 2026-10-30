//! The app's control over embassy-sync: the shutdown device code asks for and the health it
//! reads.

use core::cell::RefCell;
use core::future::poll_fn;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use core::task::{Context, Poll};

use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::waitqueue::MultiWakerRegistration;
use ruststream_runtime::{Halt, ShutdownWait};
use tracing::error;

/// How many tasks may wait on one control at once: the app's task and the device's own.
///
/// A registration past the last slot wakes every waiter to make room, so a slot per waiting
/// task keeps a wait from waking the others.
const WAITERS: usize = 4;

/// Where the app is in its life, as device code reads it.
///
/// The runner moves it forward only: `Starting`, `Running`, `ShuttingDown`, then `Stopped`, or
/// `Failed` from any state before `Stopped`. `Failed` is final.
///
/// # Examples
///
/// A device task that lights a fault LED once the app fails:
///
/// ```
/// use ruststream_runtime_embassy::{Control, Health};
///
/// static CONTROL: Control = Control::new();
///
/// async fn fault_led(mut light: impl FnMut()) {
///     let mut seen = CONTROL.health();
///     while seen != Health::Failed {
///         seen = CONTROL.changed(seen).await;
///     }
///     light();
/// }
/// # embassy_futures::block_on(async {
/// # let lit = core::cell::Cell::new(false);
/// # let led = core::pin::pin!(fault_led(|| lit.set(true)));
/// # use ruststream_runtime::Halt;
/// # (&CONTROL).fail("readings", "the broker went away");
/// # led.await;
/// # assert!(lit.get());
/// # });
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Health {
    /// The app is connecting its brokers and opening its subscriptions.
    Starting,
    /// Every subscription is open and delivering.
    Running,
    /// A stop was asked for; the subscriptions are finishing what they hold.
    ShuttingDown,
    /// The app stopped cleanly.
    Stopped,
    /// A subscription failed under a fail-fast policy and stopped the app.
    Failed,
}

impl Health {
    const fn encode(self) -> u8 {
        match self {
            Self::Starting => 0,
            Self::Running => 1,
            Self::ShuttingDown => 2,
            Self::Stopped => 3,
            Self::Failed => 4,
        }
    }

    const fn decode(raw: u8) -> Self {
        match raw {
            0 => Self::Starting,
            1 => Self::Running,
            2 => Self::ShuttingDown,
            3 => Self::Stopped,
            _ => Self::Failed,
        }
    }
}

/// The app's control: the stop device code asks for and the [`Health`] it reads.
///
/// It is a plain value with a `const` constructor, so a firmware keeps it in a `static` and
/// hands the app and its own tasks a `&'static` to it. Reading the health and checking for a stop
/// are one relaxed load each, and nothing in it touches a heap.
///
/// # Examples
///
/// A button task that stops the app, beside a task that waits for it to stop:
///
/// ```
/// use ruststream_runtime_embassy::{Control, Health};
///
/// static CONTROL: Control = Control::new();
///
/// async fn on_button() {
///     CONTROL.stop();
/// }
///
/// async fn power_down(mut off: impl FnMut()) {
///     let mut seen = CONTROL.health();
///     while !matches!(seen, Health::Stopped | Health::Failed) {
///         seen = CONTROL.changed(seen).await;
///     }
///     off();
/// }
/// # embassy_futures::block_on(async {
/// # on_button().await;
/// # assert_eq!(CONTROL.health(), Health::ShuttingDown);
/// # });
/// ```
#[derive(Debug)]
pub struct Control {
    stopped: AtomicBool,
    health: AtomicU8,
    ended: AtomicBool,
    waiters: Mutex<CriticalSectionRawMutex, RefCell<MultiWakerRegistration<WAITERS>>>,
}

impl Control {
    /// A control whose app is starting.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
            health: AtomicU8::new(Health::Starting.encode()),
            ended: AtomicBool::new(false),
            waiters: Mutex::new(RefCell::new(MultiWakerRegistration::new())),
        }
    }

    /// Asks the app to stop: each subscription finishes the delivery it holds and ends.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.advance(Health::ShuttingDown);
    }

    /// The app's health now.
    #[must_use]
    pub fn health(&self) -> Health {
        Health::decode(self.health.load(Ordering::Relaxed))
    }

    /// Waits until the health differs from `seen`, and returns it.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: dropping the wait loses nothing, the health is read again on the next call.
    pub async fn changed(&self, seen: Health) -> Health {
        poll_fn(|cx| {
            // Registered before the state is read, so a change between the two still wakes.
            self.register(cx);
            let now = self.health();
            if now == seen {
                Poll::Pending
            } else {
                Poll::Ready(now)
            }
        })
        .await
    }

    /// Whether a subscription's stream ended while the app ran.
    #[must_use]
    pub fn stream_ended(&self) -> bool {
        self.ended.load(Ordering::Relaxed)
    }

    /// The runner reports every subscription open.
    pub(crate) fn running(&self) {
        self.advance(Health::Running);
    }

    /// The runner reports every subscription ended.
    pub(crate) fn stopped(&self) {
        self.advance(Health::Stopped);
    }

    /// The runner fails the app itself: a startup that did not complete, or a failing hook.
    pub(crate) fn fail_app(&self, subscription: &str, reason: &str) {
        Halt::fail(&self, subscription, reason);
    }

    /// Whether the app failed.
    pub(crate) fn has_failed(&self) -> bool {
        self.health() == Health::Failed
    }

    /// Moves the health forward to `next`: never back, and never out of `Failed` or `Stopped`.
    fn advance(&self, next: Health) {
        // One writer moves the state at a time (the app's task, or device code asking for a
        // stop), and a critical section orders them, so a load and a store suffice where a
        // compare-and-swap is not available.
        self.waiters.lock(|waiters| {
            let now = self.health();
            let forward = match now {
                Health::Failed | Health::Stopped => false,
                _ => next == Health::Failed || next.encode() > now.encode(),
            };
            if forward {
                self.health.store(next.encode(), Ordering::Relaxed);
            }
            waiters.borrow_mut().wake();
        });
    }

    fn register(&self, cx: &Context<'_>) {
        self.waiters
            .lock(|waiters| waiters.borrow_mut().register(cx.waker()));
    }
}

impl Default for Control {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownWait for &Control {
    type Wait<'a>
        = Stopping<'a>
    where
        Self: 'a;

    fn is_cancelled(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    fn cancelled(&self) -> Stopping<'_> {
        Stopping { control: self }
    }
}

impl Halt for &Control {
    fn fail(&self, subscription: &str, reason: &str) {
        error!(target: "ruststream::embassy", subscription, reason, "the app failed");
        self.stopped.store(true, Ordering::Relaxed);
        self.advance(Health::Failed);
    }

    fn stream_ended(&self, _subscription: &str) {
        self.ended.store(true, Ordering::Relaxed);
    }
}

/// The wait a parked subscription keeps on its app's stop.
#[derive(Debug)]
#[must_use = "a wait does nothing unless polled"]
pub struct Stopping<'a> {
    control: &'a Control,
}

impl Future for Stopping<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // Registered before the flag is read, so a stop between the two still wakes this task.
        self.control.register(cx);
        if self.control.stopped.load(Ordering::Relaxed) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// The app's shutdown as its loops read it: the stop flag of the app's [`Control`], and the
/// failure a loop reports through it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LoopSignal<'a> {
    control: &'a Control,
}

impl<'a> LoopSignal<'a> {
    /// The loops' view of `control`.
    pub(crate) const fn new(control: &'a Control) -> Self {
        Self { control }
    }
}

impl LoopSignal<'_> {
    /// The app's own wait for the stop: it registers the task's waker with the control, so a
    /// stop wakes the task once and every loop of it reads the flag on that poll.
    pub(crate) const fn watch(&self) -> Stopping<'_> {
        Stopping {
            control: self.control,
        }
    }
}

// A loop's wait reads the flag and registers nothing: every loop runs in the app's one task, and
// the app's own wait keeps that task registered. A registration per loop would take a critical
// section per idle loop on every wake.
impl ShutdownWait for LoopSignal<'_> {
    type Wait<'a>
        = Flag<'a>
    where
        Self: 'a;

    fn is_cancelled(&self) -> bool {
        (&self.control).is_cancelled()
    }

    fn cancelled(&self) -> Flag<'_> {
        Flag {
            control: self.control,
        }
    }
}

/// A loop's wait for the stop: ready once the flag is set, woken by the app's own wait.
#[derive(Debug)]
#[must_use = "a wait does nothing unless polled"]
pub(crate) struct Flag<'a> {
    control: &'a Control,
}

impl Future for Flag<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.control.stopped.load(Ordering::Relaxed) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Halt for LoopSignal<'_> {
    fn fail(&self, subscription: &str, reason: &str) {
        (&self.control).fail(subscription, reason);
    }

    fn stream_ended(&self, subscription: &str) {
        (&self.control).stream_ended(subscription);
    }
}

#[cfg(test)]
mod tests {
    use alloc::sync::Arc;
    use alloc::task::Wake;
    use core::pin::pin;
    use core::sync::atomic::{AtomicU32, Ordering};
    use core::task::{Context, Poll, Waker};

    use embassy_futures::block_on;

    use ruststream_runtime::{Halt, ShutdownWait};

    use super::{Control, Health, LoopSignal};

    /// A waker that counts its wakes.
    struct Wakes(AtomicU32);

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn a_loops_wait_for_the_stop_registers_nothing_and_sees_the_flag() {
        let control = Control::new();
        let counted = Arc::new(Wakes(AtomicU32::new(0)));
        let waker = Waker::from(Arc::clone(&counted));
        let mut cx = Context::from_waker(&waker);
        let signal = LoopSignal::new(&control);
        let mut wait = pin!(signal.cancelled());
        assert_eq!(wait.as_mut().poll(&mut cx), Poll::Pending);
        control.stop();
        assert_eq!(
            counted.0.load(Ordering::Relaxed),
            0,
            "wakes of a loop's own wait: the app's wait wakes the task"
        );
        assert_eq!(wait.as_mut().poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn a_new_control_reports_the_app_starting() {
        let control = Control::new();
        assert_eq!(control.health(), Health::Starting);
    }

    #[test]
    fn the_runner_moves_health_through_the_lifecycle() {
        let control = Control::new();
        control.running();
        assert_eq!(control.health(), Health::Running);
        control.stop();
        assert_eq!(control.health(), Health::ShuttingDown);
        control.stopped();
        assert_eq!(control.health(), Health::Stopped);
    }

    #[test]
    fn a_failure_is_final_and_stops_the_app() {
        let control = Control::new();
        control.running();
        (&control).fail("readings", "the broker went away");
        assert_eq!(control.health(), Health::Failed);
        assert!((&control).is_cancelled());
        control.stopped();
        assert_eq!(control.health(), Health::Failed);
    }

    #[test]
    fn device_code_waits_for_the_next_state() {
        let control = Control::new();
        let mut changed = pin!(control.changed(Health::Starting));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(changed.as_mut().poll(&mut cx), Poll::Pending);
        control.running();
        assert_eq!(changed.as_mut().poll(&mut cx), Poll::Ready(Health::Running));
        assert_eq!(block_on(control.changed(Health::Starting)), Health::Running);
    }
}
