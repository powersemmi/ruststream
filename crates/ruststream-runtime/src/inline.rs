//! The room a continuation lives in on a device without a heap.
//!
//! [`InlineFuture`] holds an `async` block in place, in a fixed number of bytes, behind one
//! vtable. Nothing is allocated: building one moves the future into the room, polling it is one
//! indirect call, and dropping it runs the future's own drop.

use core::fmt;
use core::pin::Pin;
use core::task::{Context, Poll};

use dyn_utils::DynObject;
use dyn_utils::storage::{Raw, Storage};
use pin_project_lite::pin_project;

/// The room of a continuation (`and_after`, a `.then`, a `.error` closure), in bytes. A capture
/// that does not fit is a compile error that names the capture and this room.
#[doc(hidden)]
pub const CONTINUATION_ROOM: usize = 128;

/// The room of `BYTES` bytes, aligned as a pointer.
#[doc(hidden)]
pub type Room<const BYTES: usize> = Raw<BYTES>;

pin_project! {
    /// A future of no output, stored in the room `Store` names (`Room<128>` is the continuation
    /// room). It lives as long as `'work`: a continuation is `'static`, and the reaction of an
    /// `.error` closure lives as long as the failures it borrows.
    #[doc(hidden)]
    pub struct InlineFuture<'work, Store: Storage = Room<CONTINUATION_ROOM>> {
        #[pin]
        object: DynObject<dyn Future<Output = ()> + 'work, Store>,
    }
}

/// The continuation a delivery leaves behind, as the runner receives it.
#[doc(hidden)]
pub type Continuation = InlineFuture<'static>;

impl<'work, const ROOM: usize> InlineFuture<'work, Room<ROOM>> {
    /// Moves `work` into the room.
    ///
    /// A `Work` larger than `ROOM`, or aligned more than a pointer, is a compile error that
    /// names the size found and the room. The check runs when the code is built, not on
    /// `cargo check`.
    pub fn new<Work>(work: Work) -> Self
    where
        Work: Future<Output = ()> + 'work,
    {
        const {
            assert!(
                align_of::<Work>() <= align_of::<usize>(),
                "a continuation may not be aligned more than a pointer"
            );
            // Too large a capture overflows this subtraction, and the error prints both numbers:
            // the room and the size of the capture.
            let _spare = ROOM - size_of::<Work>();
        }
        Self {
            object: DynObject::new(work),
        }
    }

    /// Whether a `Work` fits the room: its size and its alignment.
    #[must_use]
    pub const fn fits<Work>() -> bool {
        Raw::<ROOM>::can_store::<Work>()
    }
}

impl<Store: Storage> fmt::Debug for InlineFuture<'_, Store> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InlineFuture").finish_non_exhaustive()
    }
}

impl<Store: Storage> Future for InlineFuture<'_, Store> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.project().object.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use alloc::rc::Rc;
    use core::cell::Cell;
    use core::future::ready;
    use core::pin::{Pin, pin};
    use core::task::{Context, Poll, Waker};

    use allocation_counter::measure;

    use super::{CONTINUATION_ROOM, Continuation, InlineFuture, Room};

    /// Pending on its first poll, ready on the second.
    #[derive(Default)]
    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                return Poll::Ready(());
            }
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    #[test]
    fn a_future_runs_to_completion_across_a_suspension() {
        let steps = Rc::new(Cell::new(0));
        let seen = Rc::clone(&steps);
        let mut work = pin!(InlineFuture::<Room<CONTINUATION_ROOM>>::new(async move {
            seen.set(1);
            YieldOnce::default().await;
            seen.set(2);
        }));
        let mut cx = Context::from_waker(Waker::noop());

        assert!(work.as_mut().poll(&mut cx).is_pending());
        assert_eq!(steps.get(), 1);
        assert!(work.as_mut().poll(&mut cx).is_ready());
        assert_eq!(steps.get(), 2);
    }

    /// Counts its drops.
    struct Guard(Rc<Cell<u32>>);

    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn a_capture_is_dropped_once_whether_or_not_the_future_ran() {
        let drops = Rc::new(Cell::new(0));

        let unpolled = InlineFuture::<Room<CONTINUATION_ROOM>>::new({
            let guard = Guard(Rc::clone(&drops));
            async move {
                let _guard = guard;
            }
        });
        drop(unpolled);
        assert_eq!(drops.get(), 1);

        let mut ran = pin!(InlineFuture::<Room<CONTINUATION_ROOM>>::new({
            let guard = Guard(Rc::clone(&drops));
            async move {
                let _guard = guard;
            }
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(ran.as_mut().poll(&mut cx).is_ready());
        assert_eq!(drops.get(), 2);
    }

    #[test]
    fn building_and_polling_allocates_nothing() {
        let counted = measure(|| {
            let mut work = pin!(Continuation::new(async {
                YieldOnce::default().await;
                ready(()).await;
            }));
            let mut cx = Context::from_waker(Waker::noop());
            while work.as_mut().poll(&mut cx).is_pending() {}
        });
        assert_eq!(counted.count_total, 0);
    }

    #[test]
    fn a_future_may_borrow_what_outlives_it() {
        let seen = Cell::new(0);
        let mut work = pin!(InlineFuture::<Room<CONTINUATION_ROOM>>::new(async {
            YieldOnce::default().await;
            seen.set(seen.get() + 1);
        }));
        let mut cx = Context::from_waker(Waker::noop());

        assert!(work.as_mut().poll(&mut cx).is_pending());
        assert!(work.as_mut().poll(&mut cx).is_ready());
        assert_eq!(seen.get(), 1);
    }

    #[test]
    fn the_room_takes_a_capture_up_to_its_size_and_refuses_a_larger_one() {
        assert!(InlineFuture::<Room<CONTINUATION_ROOM>>::fits::<[u8; 128]>());
        assert!(!InlineFuture::<Room<CONTINUATION_ROOM>>::fits::<[u8; 129]>());
        assert!(!InlineFuture::<Room<CONTINUATION_ROOM>>::fits::<u128>());
        assert!(InlineFuture::<Room<256>>::fits::<[u8; 129]>());
    }
}
