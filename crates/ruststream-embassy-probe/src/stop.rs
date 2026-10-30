//! The runner's shutdown over embassy-sync.

use core::cell::RefCell;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll};

use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::waitqueue::MultiWakerRegistration;
use ruststream_runtime::ShutdownWait;

/// How many parked loops one shutdown wakes without waking them all early.
const WAITERS: usize = 4;

/// The app's shutdown: a flag every loop reads, and the wakers of the loops parked on it.
///
/// It lives in a `static`, so it is signalled from any task, executor or interrupt. The wait
/// list is a fixed array behind a critical section; a loop beyond its capacity makes it wake
/// every waiter, and each re-registers.
#[derive(Debug)]
pub struct Stop {
    stopped: AtomicBool,
    waiters: Mutex<CriticalSectionRawMutex, RefCell<MultiWakerRegistration<WAITERS>>>,
}

impl Stop {
    /// A shutdown not yet signalled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
            waiters: Mutex::new(RefCell::new(MultiWakerRegistration::new())),
        }
    }

    /// Signals shutdown and wakes every parked loop.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.waiters.lock(|waiters| waiters.borrow_mut().wake());
    }
}

impl Default for Stop {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownWait for Stop {
    type Wait<'a> = Stopping<'a>;

    fn is_cancelled(&self) -> bool {
        self.stopped.load(Ordering::Relaxed)
    }

    fn cancelled(&self) -> Stopping<'_> {
        Stopping { stop: self }
    }
}

/// The future [`Stop::cancelled`](ShutdownWait::cancelled) returns: resolves once shutdown is
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
        if self.stop.is_cancelled() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
