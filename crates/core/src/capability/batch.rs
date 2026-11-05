//! Where a batch is collected: the slot the runtime lends a batching subscriber, and the stream
//! that fills it.

#[cfg(feature = "alloc")]
use alloc::vec::Vec;
#[cfg(feature = "alloc")]
use core::mem;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};

use crate::MaybeSend;

/// The most messages one batch holds on a build without `alloc`: the room of a [`BatchVec`]
/// there.
pub const BATCH_CAPACITY: usize = 16;

/// Where the runtime keeps the messages of one batch while it handles them, which
/// [`BatchSlot::hand_over`] moves a batch into. A `Vec` with `alloc`.
#[cfg(feature = "alloc")]
pub type BatchVec<T> = Vec<T>;

/// Where the runtime keeps the messages of one batch while it handles them, which
/// [`BatchSlot::hand_over`] moves a batch into. A fixed buffer of [`BATCH_CAPACITY`] messages
/// without `alloc`.
#[cfg(not(feature = "alloc"))]
pub type BatchVec<T> = heapless::Vec<T, BATCH_CAPACITY>;

/// The storage one batch is collected in, which the runtime owns and reuses.
///
/// The runtime allocates one per worker when the subscription starts and lends it to the broker
/// to fill. Once a batch is in, the slot hands it over to the runtime's own buffer for the batch
/// and is lent again, empty, for the next one. A `Vec` slot hands over by trading storage with
/// that buffer, so a delivery stays where the broker put it, whatever its size.
///
/// A broker whose batches are a `Vec` of its messages names `Vec<..>` as its
/// [`BatchSubscriber::Batch`](crate::BatchSubscriber::Batch) and needs nothing more. A broker with
/// a batch type of its own (the rows a database page carries beside the deliveries that settle
/// them) implements this for it.
///
/// # Examples
///
/// A page of rows read through a database driver, with the claims that settle them in a second
/// vector. The page hands its claims over by trading that vector, and keeps the rows' storage for
/// the next page:
///
/// ```
/// # #[cfg(feature = "std")]
/// # mod demo {
/// use std::convert::Infallible;
/// use std::num::NonZeroUsize;
/// use std::pin::Pin;
/// use std::task::{Context, Poll};
///
/// use ruststream::{BatchSlot, BatchStream};
///
/// /// One claim on a row of the queue table.
/// pub struct Claim(u64);
///
/// /// One fetched page.
/// pub struct Page {
///     rows: Vec<String>,
///     claims: Vec<Claim>,
/// }
///
/// impl BatchSlot for Page {
///     type Message = Claim;
///
///     fn with_capacity(size: NonZeroUsize) -> Self {
///         Self {
///             rows: Vec::with_capacity(size.get()),
///             claims: Vec::with_capacity(size.get()),
///         }
///     }
///
///     fn hand_over(&mut self, taken: &mut Vec<Claim>) {
///         self.rows.clear();
///         self.claims.hand_over(taken);
///     }
/// }
///
/// /// The rows the driver read, waiting to be claimed.
/// pub struct Fetched {
///     waiting: Vec<(u64, String)>,
///     size: NonZeroUsize,
/// }
///
/// impl BatchStream<Page> for Fetched {
///     type Error = Infallible;
///
///     fn poll_fill(
///         self: Pin<&mut Self>,
///         _cx: &mut Context<'_>,
///         page: &mut Page,
///     ) -> Poll<Option<Result<(), Infallible>>> {
///         let this = self.get_mut();
///         if this.waiting.is_empty() {
///             return Poll::Ready(None);
///         }
///         let room = this.size.get().min(this.waiting.len());
///         for (id, recipient) in this.waiting.drain(..room) {
///             page.rows.push(recipient);
///             page.claims.push(Claim(id));
///         }
///         Poll::Ready(Some(Ok(())))
///     }
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a lent batch slot",
    label = "a `BatchSubscriber::Batch` is a slot the runtime lends and reuses",
    note = "implement `BatchSlot` for the batch type, or use `Vec<..>` of the subscriber's messages"
)]
pub trait BatchSlot: MaybeSend {
    /// What the slot holds: the subscriber's own message.
    type Message;

    /// Allocates a slot with room for `size` messages. Called once per worker, as the
    /// subscription starts.
    #[must_use]
    fn with_capacity(size: NonZeroUsize) -> Self;

    /// Moves the batch into `taken`, in order, and leaves the slot empty and ready to be filled
    /// again, with room for a batch.
    ///
    /// The runtime calls this once per batch, with its own buffer for the batch, empty and with
    /// room for the batch size. A slot that keeps its messages in a `Vec` trades that vector with
    /// `taken` (`Vec<..>`'s own hand-over does it), so no message moves; a slot that builds its
    /// messages from what it holds pushes each into `taken` as it builds it. Without `alloc`,
    /// [`BatchVec`] holds its messages inline, so a fixed slot moves each message into it once.
    fn hand_over(&mut self, taken: &mut BatchVec<Self::Message>);
}

// A message is as large as its broker makes it, so it is never copied here: an empty `taken`
// (the runtime's case) trades storage with the slot, and a full one takes every message in one
// bulk move instead of one move per message.
#[cfg(feature = "alloc")]
impl<Message: MaybeSend> BatchSlot for Vec<Message> {
    type Message = Message;

    fn with_capacity(size: NonZeroUsize) -> Self {
        Self::with_capacity(size.get())
    }

    fn hand_over(&mut self, taken: &mut Self) {
        if taken.is_empty() {
            mem::swap(self, taken);
        } else {
            taken.append(self);
        }
    }
}

/// The stream of batches a [`BatchSubscriber`](crate::BatchSubscriber) collects into slots the
/// runtime lends it.
///
/// A poll that returns `Poll::Pending` may leave messages in the slot: the collection resumes
/// from them on the next poll, so the state of a half-filled batch lives in the slot and is
/// cancel-safe between polls. A partial batch is handed to the handler before the subscription
/// ends: the end is reported on a poll that finds the slot empty, because the runtime drops the
/// slot with the stream.
pub trait BatchStream<Slot> {
    /// What a failed fill reports.
    type Error;

    /// Fills the empty `slot` with at most `size` messages, the size the stream was opened at.
    ///
    /// - `Ready(Some(Ok(())))`: the slot holds a batch of at least one message.
    /// - `Ready(Some(Err(_)))`: the fill failed and the slot is empty. The next poll starts a new
    ///   batch.
    /// - `Ready(None)`: the subscription ended and the slot is empty. Messages collected before the
    ///   end go out first as a batch, and the end is reported on the next poll.
    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Slot,
    ) -> Poll<Option<Result<(), Self::Error>>>;
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use alloc::vec;
    use alloc::vec::Vec;
    use core::num::NonZeroUsize;

    use super::BatchSlot;

    #[test]
    fn a_vec_slot_has_room_for_the_batch_size() {
        let slot = <Vec<u32> as BatchSlot>::with_capacity(NonZeroUsize::new(64).unwrap());
        assert!(slot.capacity() >= 64);
        assert_eq!(slot.len(), 0);
    }

    #[test]
    fn handing_a_vec_slot_over_trades_storage_so_no_message_moves() {
        let mut slot = <Vec<u32> as BatchSlot>::with_capacity(NonZeroUsize::new(8).unwrap());
        slot.extend([3, 1, 2]);
        let filled = slot.as_ptr();
        let mut taken = Vec::with_capacity(8);
        let spare = taken.as_ptr();

        slot.hand_over(&mut taken);

        assert_eq!(taken, [3, 1, 2]);
        assert_eq!(taken.as_ptr(), filled);
        assert_eq!(slot.len(), 0);
        assert_eq!(slot.as_ptr(), spare);
        assert!(slot.capacity() >= 8);
    }

    #[test]
    fn handing_a_vec_slot_over_to_a_batch_in_progress_appends_after_it() {
        let mut slot = <Vec<u32> as BatchSlot>::with_capacity(NonZeroUsize::new(8).unwrap());
        slot.extend([3, 1]);
        let room = slot.capacity();
        let mut taken = vec![9];

        slot.hand_over(&mut taken);

        assert_eq!(taken, [9, 3, 1]);
        assert_eq!(slot.len(), 0);
        assert_eq!(slot.capacity(), room);
    }
}
