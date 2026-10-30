//! A worker's one-slot mailbox, and the join that runs a constant set of workers, each reading
//! its own mailbox.

// The workers hold deliveries that are not `Send`, on the app's one task.
#![allow(clippy::future_not_send)]

use core::array;

use embassy_futures::join::join_array;
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::Channel;

/// What a feeder hands one worker: the next delivery, or `None` once the lane has ended.
pub(crate) struct Mailbox<Message> {
    slot: Channel<NoopRawMutex, Option<Message>, 1>,
}

impl<Message> Mailbox<Message> {
    /// An empty mailbox.
    pub(crate) const fn new() -> Self {
        Self {
            slot: Channel::new(),
        }
    }

    /// Hands the worker `item`, waiting while the slot still holds the previous one.
    pub(crate) async fn send(&self, item: Option<Message>) {
        self.slot.send(item).await;
    }

    /// The next item the feeder hands over.
    pub(crate) async fn receive(&self) -> Option<Message> {
        self.slot.receive().await
    }
}

/// Runs one worker per mailbox, joined: `worker(index, mailbox)` builds each.
pub(crate) async fn join_workers<'a, const COUNT: usize, Message, Worker, Work>(
    mailboxes: &'a [Mailbox<Message>; COUNT],
    worker: Worker,
) where
    Worker: Fn(usize, &'a Mailbox<Message>) -> Work,
    Work: Future<Output = ()>,
{
    join_array(array::from_fn::<_, COUNT, _>(|index| {
        worker(index, &mailboxes[index])
    }))
    .await;
}
