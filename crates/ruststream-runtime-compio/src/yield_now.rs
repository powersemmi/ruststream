//! The cooperative yield of the compio runner.

use std::pin::Pin;
use std::task::{Context, Poll};

/// Gives the other tasks of the core a turn, then continues.
///
/// compio has no budget of its own and no `yield_now`: a subscription whose handler never waits
/// would keep the core inside one poll, and the continuations, hooks and retry timers on that
/// core would wait for the whole burst. A subscription's loop and each lane of `workers` or
/// `threads` yield after a run of 128 deliveries.
///
/// Cost: one `Pending` that wakes its own task, which compio's executor queues behind the tasks
/// already ready (a slab push, no allocation); a `Ready` task would pay nothing.
///
/// # Examples
///
/// ```
/// use compio::runtime::Runtime;
/// use ruststream_runtime_compio::yield_now;
///
/// # fn main() -> std::io::Result<()> {
/// Runtime::new()?.block_on(async {
///     for _delivery in 0..3 {
///         // The handler's work, then a turn for the others.
///         yield_now().await;
///     }
/// });
/// # Ok(())
/// # }
/// ```
pub const fn yield_now() -> YieldNow {
    YieldNow { yielded: false }
}

/// The future [`yield_now`] returns.
#[derive(Debug)]
#[must_use = "a yield takes effect only when awaited"]
pub struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }
        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
