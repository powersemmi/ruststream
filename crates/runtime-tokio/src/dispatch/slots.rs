//! The slots of a `threads(n)` batch subscription: the loop fills them, a thread settles one and
//! gives it back.
//!
//! The loop allocates a slot when it has none to fill, up to the most that can be out at once (a
//! full ring and a batch in hand per thread, and the one being filled), so a subscription's slots
//! are allocated as it ramps up and never again. When all of them are out, the loop waits for a
//! thread to give one back, which is the back-pressure a full ring applies to deliveries.

use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crossbeam_queue::ArrayQueue;
use futures::Stream;
use futures::task::AtomicWaker;
use ruststream_core::{BatchSlot, BatchStream, BatchVec};

/// The slots the threads have given back, waiting to be filled again.
pub(super) struct Stock<Slot> {
    returned: ArrayQueue<Slot>,
    waker: AtomicWaker,
}

impl<Slot> Stock<Slot> {
    /// A stock for a subscription that has at most `most` slots.
    pub(super) fn new(most: usize) -> Arc<Self> {
        Arc::new(Self {
            returned: ArrayQueue::new(most),
            waker: AtomicWaker::new(),
        })
    }

    /// A thread is done with `slot`, now empty: the loop may fill it again.
    pub(super) fn give(&self, slot: Slot) {
        // The queue holds as many as the loop ever allocates, so there is always room.
        let _ = self.returned.push(slot);
        self.waker.wake();
    }
}

/// The batches of a subscription as a stream of filled slots.
pub(super) struct Filled<Fill, Slot: BatchSlot> {
    stream: Pin<Box<Fill>>,
    stock: Arc<Stock<Slot>>,
    size: NonZeroUsize,
    most: usize,
    made: usize,
    /// The slot being filled, which a pending fill keeps its messages in.
    current: Option<Slot>,
    /// Where a failed fill's leftovers go to be dropped, trading storage with the slot.
    spare: BatchVec<Slot::Message>,
}

// The slot is only ever moved, never pinned in place.
impl<Fill, Slot: BatchSlot> Unpin for Filled<Fill, Slot> {}

impl<Fill, Slot: BatchSlot> Filled<Fill, Slot> {
    /// Fills slots of `size` from `stream`, taking them from `stock` and allocating up to `most`.
    pub(super) fn new(
        stream: Fill,
        stock: Arc<Stock<Slot>>,
        size: NonZeroUsize,
        most: usize,
    ) -> Self {
        Self {
            stream: Box::pin(stream),
            stock,
            size,
            most,
            made: 0,
            current: None,
            spare: BatchVec::with_capacity(size.get()),
        }
    }
}

impl<Fill, Slot> Stream for Filled<Fill, Slot>
where
    Fill: BatchStream<Slot>,
    Slot: BatchSlot,
{
    type Item = Result<Slot, Fill::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.current.is_none() {
            this.current = this.stock.returned.pop();
            if this.current.is_none() && this.made < this.most {
                this.made += 1;
                this.current = Some(Slot::with_capacity(this.size));
            }
            if this.current.is_none() {
                this.stock.waker.register(cx.waker());
                // A slot given between the pop and the registration is found here.
                this.current = this.stock.returned.pop();
            }
        }
        let Some(slot) = this.current.as_mut() else {
            return Poll::Pending;
        };
        match this.stream.as_mut().poll_fill(cx, slot) {
            Poll::Ready(Some(Ok(()))) => Poll::Ready(this.current.take().map(Ok)),
            Poll::Ready(Some(Err(err))) => {
                // A failed fill leaves the slot empty for the next batch.
                slot.hand_over(&mut this.spare);
                this.spare.clear();
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
