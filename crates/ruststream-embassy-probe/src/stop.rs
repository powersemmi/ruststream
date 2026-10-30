//! The runner's shutdown over embassy-sync.

use core::cell::RefCell;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll};

use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::waitqueue::MultiWakerRegistration;
use ruststream_runtime::{Halt, ShutdownWait};

/// How many parked loops one shutdown wakes without waking them all early.
const WAITERS: usize = 4;

/// The app's shutdown: a flag every loop reads, and the wakers of the loops parked on it.
///
/// It lives in a `static`, so it is signalled from any task, executor or interrupt. The wait
/// list is a fixed array behind a critical section; a loop beyond its capacity makes it wake
/// every waiter, and each re-registers. A loop holds it by reference: `&Stop` is the runner's
/// shutdown handle, and it records what the loops report to the app (a failure, a stream that
/// ended).
#[derive(Debug)]
pub struct Stop {
    stopped: AtomicBool,
    failed: AtomicBool,
    ended: AtomicBool,
    waiters: Mutex<CriticalSectionRawMutex, RefCell<MultiWakerRegistration<WAITERS>>>,
}

impl Stop {
    /// A shutdown not yet signalled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            ended: AtomicBool::new(false),
            waiters: Mutex::new(RefCell::new(MultiWakerRegistration::new())),
        }
    }

    /// Signals shutdown and wakes every parked loop.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.waiters.lock(|waiters| waiters.borrow_mut().wake());
    }

    /// Whether a loop reported a failure that stops the service.
    #[must_use]
    pub fn failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }

    /// Whether a loop reported that its subscription's stream ended.
    #[must_use]
    pub fn stream_ended(&self) -> bool {
        self.ended.load(Ordering::Relaxed)
    }
}

impl Default for Stop {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownWait for &Stop {
    type Wait<'a>
        = Stopping<'a>
    where
        Self: 'a;

    fn is_cancelled(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    fn cancelled(&self) -> Stopping<'_> {
        Stopping { stop: self }
    }
}

impl Halt for &Stop {
    fn fail(&self, _subscription: &str, _reason: &str) {
        self.failed.store(true, Ordering::Relaxed);
        self.stop();
    }

    fn stream_ended(&self, _subscription: &str) {
        self.ended.store(true, Ordering::Relaxed);
    }
}

/// The future [`cancelled`](ShutdownWait::cancelled) of a `&Stop` returns: resolves once shutdown is
/// signalled.
#[derive(Debug)]
#[must_use = "a wait does nothing unless polled"]
pub struct Stopping<'a> {
    stop: &'a Stop,
}

impl Future for Stopping<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // Registered before the flag is read, so a signal between the two still wakes this task.
        self.stop
            .waiters
            .lock(|waiters| waiters.borrow_mut().register(cx.waker()));
        if self.stop.stopped.load(Ordering::Relaxed) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
