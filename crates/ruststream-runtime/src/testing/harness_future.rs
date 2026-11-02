//! The future of the harness's own work that has to wait.

use core::pin::Pin;
use core::task::{Context, Poll};

use super::harness_section;

/// Polls `bookkeeping` with every poll in a [`harness_section`]: the harness's own work that has
/// to wait (its hooks) runs where the failing allocator lets its allocations through.
#[doc(hidden)]
pub fn harness_future<F: Future>(bookkeeping: F) -> HarnessFuture<F> {
    HarnessFuture { bookkeeping }
}

pin_project_lite::pin_project! {
    /// See [`harness_future`].
    #[doc(hidden)]
    pub struct HarnessFuture<F> {
        #[pin]
        bookkeeping: F,
    }
}

impl<F: Future> Future for HarnessFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let bookkeeping = self.project().bookkeeping;
        harness_section(|| bookkeeping.poll(cx))
    }
}
