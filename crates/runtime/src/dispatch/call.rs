//! A call made on the first poll of the future that holds it.

use core::pin::Pin;
use core::task::{Context, Poll, ready};

use pin_project_lite::pin_project;

/// A call that returns a future: what [`CallOnPoll`] makes on its first poll.
///
/// The future's type is the call's own projection rather than a parameter of its own, so a future
/// that holds the call names one type, and the lifetimes the call borrows are the future's.
pub(crate) trait MakeFuture {
    /// The future the call returns.
    type Future: Future;

    /// Makes the call.
    fn make(self) -> Self::Future;
}

impl<Call, Fut> MakeFuture for Call
where
    Call: FnOnce() -> Fut,
    Fut: Future,
{
    type Future = Fut;

    #[inline]
    fn make(self) -> Fut {
        self()
    }
}

pin_project! {
    /// A call made on the first poll of this future, and the future the call returned, polled
    /// where it lies.
    ///
    /// The call runs inside whatever the poll runs under: the catch of a panic, the harness's
    /// task-local scope. A dispatch pins it in its own frame and hands those combinators a
    /// reference to it, so the handler's future is laid out one level below the dispatch's
    /// frame however many of them the poll passes through: a reference nests no type the
    /// compiler lays out, which keeps a device app's future inside the default recursion limit.
    #[project = CallOnPollProj]
    #[project_replace = CallOnPollReplace]
    pub(crate) enum CallOnPoll<Call: MakeFuture> {
        /// Not polled yet: the call, still to be made.
        Waiting { call: Call },
        /// Made: the future the call returned.
        Running { #[pin] fut: Call::Future },
        /// The future ended, and was dropped where it lay.
        Done,
    }
}

impl<Call: MakeFuture> CallOnPoll<Call> {
    /// `call`, to be made on the first poll.
    // The `FnOnce` bound is what lets a closure that borrows its captures mutably hand out a
    // future borrowing them: a closure's kind is inferred from the bound it is passed to.
    pub(crate) const fn new<Fut>(call: Call) -> Self
    where
        Call: FnOnce() -> Fut,
        Fut: Future,
    {
        Self::Waiting { call }
    }
}

impl<Call: MakeFuture> Future for CallOnPoll<Call> {
    type Output = <Call::Future as Future>::Output;

    // Why a poll past the end panics: a future polled again after it returned `Ready` breaks the
    // `Future` contract, and the dispatch that owns this one never does.
    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let CallOnPollProj::Waiting { .. } = self.as_mut().project() {
            let CallOnPollReplace::Waiting { call } = self.as_mut().project_replace(Self::Done)
            else {
                unreachable!("the future was waiting for its call a moment ago");
            };
            self.set(Self::Running { fut: call.make() });
        }
        let CallOnPollProj::Running { fut } = self.as_mut().project() else {
            panic!("a call's future was polled after it ended");
        };
        let output = ready!(fut.poll(cx));
        self.set(Self::Done);
        Poll::Ready(output)
    }
}

#[cfg(test)]
mod tests {
    use core::cell::Cell;
    use core::panic::AssertUnwindSafe;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};

    use futures::FutureExt;
    use futures::future::poll_fn;

    use super::CallOnPoll;

    /// Counts its drops, so a test reads when the future the call returned went away.
    struct Counted<'a>(&'a Cell<u32>);

    impl Drop for Counted<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn the_call_is_made_on_the_first_poll_and_its_future_is_dropped_where_it_ends() {
        let calls = Cell::new(0);
        let drops = Cell::new(0);
        let mut polls = 0;
        let mut call = pin!(CallOnPoll::new(|| {
            calls.set(calls.get() + 1);
            let counted = Counted(&drops);
            poll_fn(move |_| {
                let _held = &counted;
                polls += 1;
                if polls < 2 {
                    Poll::Pending
                } else {
                    Poll::Ready(7)
                }
            })
        }));
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(calls.get(), 0, "building the future makes no call");
        assert_eq!(call.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(calls.get(), 1, "the first poll makes the call");
        assert_eq!(drops.get(), 0, "the call's future is held while it runs");
        assert_eq!(call.as_mut().poll(&mut cx), Poll::Ready(7));
        assert_eq!(calls.get(), 1, "the call is made once");
        assert_eq!(
            drops.get(),
            1,
            "the call's future is dropped where it ended"
        );
    }

    #[test]
    fn a_panic_while_the_call_builds_its_future_is_caught_by_the_poll_around_it() {
        let call = pin!(CallOnPoll::new(|| -> core::future::Ready<u32> {
            panic!("the call refused to build its future")
        }));
        let mut caught = pin!(AssertUnwindSafe(call).catch_unwind());
        let mut cx = Context::from_waker(Waker::noop());

        let Poll::Ready(outcome) = caught.as_mut().poll(&mut cx) else {
            panic!("a panicking call ends the poll");
        };
        assert!(
            outcome.is_err(),
            "the panic reaches the catch around the poll"
        );
    }
}
