//! Where a batch is collected: the slot the runtime lends a batching subscriber, and the stream
//! that fills it.

#[cfg(feature = "alloc")]
use alloc::vec::Vec;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};

use crate::MaybeSend;

/// The storage one batch is collected in, which the runtime owns and reuses.
///
/// The runtime allocates one per worker when the subscription starts, lends it to the broker to
/// fill, drains it while it settles the batch, and lends it again for the next one.
///
/// A broker whose batches are a `Vec` of its messages names `Vec<..>` as its
/// [`BatchSubscriber::Batch`](crate::BatchSubscriber::Batch) and needs nothing more. A broker with
/// a batch type of its own (the rows a database page carries beside the deliveries that settle
/// them) implements this for it.
///
/// # Examples
///
/// A page of rows read through a database driver, with the claims that settle them in a second
/// vector. The slot keeps both vectors for the next page:
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use ruststream::BatchSlot;
///
/// /// One claim on a row of the queue table.
/// struct Claim(u64);
///
/// /// One fetched page.
/// struct Page {
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
///     fn drain(&mut self) -> impl Iterator<Item = Claim> + '_ {
///         self.rows.clear();
///         self.claims.drain(..)
///     }
/// }
///
/// let mut page = Page::with_capacity(NonZeroUsize::MIN);
/// page.rows.push("ops@example.com".to_owned());
/// page.claims.push(Claim(7));
/// assert_eq!(page.drain().map(|claim| claim.0).sum::<u64>(), 7);
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

    /// Takes the messages out and keeps the storage, so the next batch is collected into it
    /// without allocating.
    fn drain(&mut self) -> impl Iterator<Item = Self::Message> + '_;
}

#[cfg(feature = "alloc")]
impl<Message: MaybeSend> BatchSlot for Vec<Message> {
    type Message = Message;

    fn with_capacity(size: NonZeroUsize) -> Self {
        Self::with_capacity(size.get())
    }

    fn drain(&mut self) -> impl Iterator<Item = Message> + '_ {
        Self::drain(self, ..)
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
    fn draining_a_vec_slot_yields_its_messages_in_order_and_keeps_the_storage() {
        let mut slot = <Vec<u32> as BatchSlot>::with_capacity(NonZeroUsize::new(8).unwrap());
        slot.extend([3, 1, 2]);
        let room = slot.capacity();

        let drained: Vec<u32> = BatchSlot::drain(&mut slot).collect();

        assert_eq!(drained, [3, 1, 2]);
        assert_eq!(slot.len(), 0);
        assert_eq!(slot.capacity(), room);
    }
}
