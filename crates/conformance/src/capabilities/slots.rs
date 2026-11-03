//! The lent slot as the checks read it: batch streams as ordinary streams of batches, holding
//! the broker to the slot rules of [`BatchStream::poll_fill`] on every fill.
//!
//! A check drives the stream the way the runtime does: it lends one slot, drains it after each
//! fill and lends the same one again. A broker that keeps messages in the slot it was handed
//! back, grows it past the size, or ends the subscription with messages still in the slot fails here.

use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use ruststream_core::{BatchSlot, BatchStream};

/// A batch stream that reuses one slot and yields each batch as a `Vec` of its messages.
pub(super) struct Reused<'s, Fill, Slot> {
    stream: Pin<&'s mut Fill>,
    slot: Slot,
    size: NonZeroUsize,
}

impl<'s, Fill, Slot: BatchSlot> Reused<'s, Fill, Slot> {
    /// Reads `stream`, which was opened at `size`.
    pub(super) fn new(stream: Pin<&'s mut Fill>, size: NonZeroUsize) -> Self {
        Self {
            stream,
            slot: Slot::with_capacity(size),
            size,
        }
    }
}

// The slot is only moved, never pinned in place.
impl<Fill, Slot> Unpin for Reused<'_, Fill, Slot> {}

impl<Fill, Slot> Stream for Reused<'_, Fill, Slot>
where
    Fill: BatchStream<Slot>,
    Slot: BatchSlot,
{
    type Item = Result<Vec<Slot::Message>, Fill::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        match this.stream.as_mut().poll_fill(cx, &mut this.slot) {
            Poll::Ready(Some(Ok(()))) => {
                let batch: Vec<_> = this.slot.drain().collect();
                assert!(
                    batch.len() <= this.size.get(),
                    "a batch must never carry more than the size it was opened with: got {}, asked {}",
                    batch.len(),
                    this.size,
                );
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(err))) => {
                assert_eq!(
                    this.slot.drain().count(),
                    0,
                    "a failed fill must leave the slot empty, so the next batch starts clean",
                );
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                assert_eq!(
                    this.slot.drain().count(),
                    0,
                    "a subscription that ended must have handed its partial batch over first and leave the slot empty",
                );
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A batch stream that yields each filled slot whole, for a check that reads the slot itself
/// (the values a batch lends) before it drains it. Each fill gets a fresh slot.
pub(super) struct Handed<'s, Fill, Slot> {
    stream: Pin<&'s mut Fill>,
    size: NonZeroUsize,
    /// The slot being filled, which a pending fill keeps its messages in.
    current: Option<Slot>,
}

impl<'s, Fill, Slot> Handed<'s, Fill, Slot> {
    /// Reads `stream`, which was opened at `size`.
    pub(super) fn new(stream: Pin<&'s mut Fill>, size: NonZeroUsize) -> Self {
        Self {
            stream,
            size,
            current: None,
        }
    }
}

impl<Fill, Slot> Unpin for Handed<'_, Fill, Slot> {}

impl<Fill, Slot> Stream for Handed<'_, Fill, Slot>
where
    Fill: BatchStream<Slot>,
    Slot: BatchSlot,
{
    type Item = Result<Slot, Fill::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let slot = this
            .current
            .get_or_insert_with(|| Slot::with_capacity(this.size));
        match this.stream.as_mut().poll_fill(cx, slot) {
            Poll::Ready(Some(Ok(()))) => Poll::Ready(this.current.take().map(Ok)),
            Poll::Ready(Some(Err(err))) => {
                this.current = None;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
