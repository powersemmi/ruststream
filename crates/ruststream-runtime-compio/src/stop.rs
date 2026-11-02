//! The app's shutdown signal: the flag a running loop reads, the wait a parked one parks on, and
//! the failure that raised it.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use event_listener::{Event, EventListener};
use ruststream_runtime::{Halt, ShutdownWait, SubscriptionEnd};
use tracing::{debug, error};

/// What every dispatch loop stops on, in the two forms a loop asks for, plus the reason where a
/// failure raised it.
///
/// A turn asks twice: for the flag before it touches the stream, and for the wait where the stream
/// has nothing. The flag is what a busy subscription pays for shutdown, one relaxed load; the wait
/// is polled only when the subscription has nothing ready, and a loop builds it once and keeps it
/// pinned. Cloning shares all of it, which is how any core tears the whole service down.
///
/// Cost: [`is_cancelled`](ShutdownWait::is_cancelled) is one relaxed load. The first
/// [`cancelled`](ShutdownWait::cancelled) poll of a wait makes one `event-listener` listener (one
/// allocation, the `Event`'s own list node), and raising the signal is one store, one fence and a
/// walk over the registered listeners; none of it is on the delivery path.
///
/// # Examples
///
/// A loop that serves until the app stops:
///
/// ```
/// use std::pin::pin;
///
/// use compio::runtime::Runtime;
/// use futures::future::{self, Either};
/// use ruststream_runtime::ShutdownWait;
/// use ruststream_runtime_compio::Stop;
///
/// async fn serve(stop: &Stop, mut next: impl AsyncFnMut()) {
///     let mut stopping = pin!(stop.cancelled());
///     while !stop.is_cancelled() {
///         match future::select(pin!(next()), stopping.as_mut()).await {
///             Either::Left(((), _)) => {}
///             Either::Right(((), _)) => break,
///         }
///     }
/// }
///
/// # fn main() -> std::io::Result<()> {
/// Runtime::new()?.block_on(async {
///     let stop = Stop::default();
///     let mut served = 0;
///     serve(&stop, async || {
///         served += 1;
///         if served == 3 {
///             stop.cancel();
///         }
///     })
///     .await;
///     assert_eq!(served, 3);
/// });
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct Stop(Arc<State>);

#[derive(Debug, Default)]
struct State {
    cancelled: AtomicBool,
    stopping: AtomicBool,
    failure: Mutex<Option<String>>,
    parked: Event,
    on_end: SubscriptionEnd,
}

impl Stop {
    /// A signal nobody has raised yet, where a stream ending outside shutdown does what `on_end`
    /// says.
    #[must_use]
    pub fn new(on_end: SubscriptionEnd) -> Self {
        Self(Arc::new(State {
            on_end,
            ..State::default()
        }))
    }

    /// Marks the orderly teardown as begun while the loops still run: a stream the shutdown hooks
    /// end is part of the shutdown.
    pub fn begin(&self) {
        self.0.stopping.store(true, Ordering::Relaxed);
    }

    /// Raises the signal for every holder of this handle, with no failure behind it: the orderly
    /// teardown.
    pub fn cancel(&self) {
        // The flag is set before the waiters are woken, so a holder that observes the wake
        // observes the flag. A loop that reads the flag late loses nothing: the wait is the
        // authoritative exit, and `Event::notify` fences against a wait registering at the same
        // moment.
        self.0.cancelled.store(true, Ordering::Release);
        self.0.parked.notify(usize::MAX);
    }

    /// The first recorded failure, without taking it.
    #[must_use]
    pub fn failure(&self) -> Option<String> {
        self.0.failure.lock().ok()?.clone()
    }
}

impl ShutdownWait for Stop {
    type Wait<'a> = Cancelled<'a>;

    #[inline]
    fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Relaxed)
    }

    fn cancelled(&self) -> Cancelled<'_> {
        Cancelled {
            state: &self.0,
            listener: None,
        }
    }
}

impl Halt for Stop {
    fn fail(&self, subscription: &str, reason: &str) {
        error!(
            target: "ruststream::dispatch",
            subscription = %subscription,
            reason = %reason,
            "fail-fast: a dispatch failure is tearing the service down",
        );
        // Held only long enough to record the first failure, never across an await.
        if let Ok(mut slot) = self.0.failure.lock() {
            slot.get_or_insert_with(|| format!("{subscription}: {reason}"));
        }
        self.cancel();
    }

    fn stream_ended(&self, subscription: &str) {
        if self.is_cancelled() || self.0.stopping.load(Ordering::Relaxed) {
            return;
        }
        if self.0.on_end == SubscriptionEnd::FailFast {
            self.fail(subscription, "the subscription's stream ended");
        } else {
            // `Log`, and any end the enum may name later, is logged and the service goes on.
            debug!(
                target: "ruststream::dispatch",
                subscription = %subscription,
                "subscriber stream ended",
            );
        }
    }
}

/// The wait [`Stop`] hands a parked loop: ready once the signal is raised.
///
/// # Cancel safety
///
/// Cancel-safe: dropping it between polls loses nothing, and a signal raised while no wait exists
/// is still seen by [`is_cancelled`](ShutdownWait::is_cancelled) and by the next wait.
#[derive(Debug)]
pub struct Cancelled<'stop> {
    state: &'stop State,
    listener: Option<EventListener>,
}

impl Future for Cancelled<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        loop {
            if this.state.cancelled.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            match &mut this.listener {
                // Registered first and the flag read again after: a signal raised in between is
                // seen by the read, one raised later wakes the listener.
                None => this.listener = Some(this.state.parked.listen()),
                Some(listener) => match Pin::new(listener).poll(cx) {
                    Poll::Ready(()) => this.listener = None,
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}
