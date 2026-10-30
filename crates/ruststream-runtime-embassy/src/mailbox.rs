//! A worker's one-slot mailbox, and the join that runs a constant set of workers, each reading
//! its own mailbox.
//!
//! A worker waiting on an empty mailbox is parked: the join skips it until the feeder hands it
//! something, so a delivery to one worker does not poll the others. The feeder runs in the same
//! task, so the flag is a plain cell and handing over costs one store.

// The workers hold deliveries that are not `Send`, on the app's one task.
#![allow(clippy::future_not_send)]

use core::array;
use core::cell::Cell;
use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Context, Poll};

use embassy_futures::join::join_array;
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::Channel;
use pin_project_lite::pin_project;

/// What a feeder hands one worker: the next delivery, or `None` once the lane has ended.
pub(crate) struct Mailbox<Message> {
    slot: Channel<NoopRawMutex, Option<Message>, 1>,
    /// Set while the worker waits on an empty slot; cleared when the feeder fills it.
    parked: Cell<bool>,
}

impl<Message> Mailbox<Message> {
    /// An empty mailbox.
    pub(crate) const fn new() -> Self {
        Self {
            slot: Channel::new(),
            parked: Cell::new(false),
        }
    }

    /// Hands the worker `item`, waiting while the slot still holds the previous one.
    pub(crate) async fn send(&self, item: Option<Message>) {
        self.slot.send(item).await;
        self.parked.set(false);
    }

    /// The next item the feeder hands over; the worker parks while the slot is empty.
    pub(crate) fn receive(&self) -> impl Future<Output = Option<Message>> + '_ {
        poll_fn(|cx| {
            let next = self.slot.poll_receive(cx);
            self.parked.set(next.is_pending());
            next
        })
    }
}

pin_project! {
    /// A worker the join polls only while its mailbox is not parked.
    struct Gated<'a, Message, Work> {
        mailbox: &'a Mailbox<Message>,
        #[pin]
        work: Work,
    }
}

impl<Message, Work: Future<Output = ()>> Future for Gated<'_, Message, Work> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        if this.mailbox.parked.get() {
            // The slot's wait registered the task's waker when the worker parked, and the
            // feeder clears the flag in this task before the join polls the worker again.
            return Poll::Pending;
        }
        this.work.poll(cx)
    }
}

/// Runs one worker per mailbox, joined: `worker(index, mailbox)` builds each, and a worker
/// parked on its empty mailbox is not polled.
pub(crate) async fn join_workers<'a, const COUNT: usize, Message, Worker, Work>(
    mailboxes: &'a [Mailbox<Message>; COUNT],
    worker: Worker,
) where
    Worker: Fn(usize, &'a Mailbox<Message>) -> Work,
    Work: Future<Output = ()>,
{
    join_array(array::from_fn::<_, COUNT, _>(|index| Gated {
        mailbox: &mailboxes[index],
        work: worker(index, &mailboxes[index]),
    }))
    .await;
}

#[cfg(test)]
mod tests {
    use core::cell::Cell;
    use core::future::poll_fn;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};

    use super::{Mailbox, join_workers};

    /// A worker that counts how often it is polled and reads its mailbox until the lane ends.
    async fn counted(mailbox: &Mailbox<u32>, polls: &Cell<u32>) {
        let mut next = pin!(async { while mailbox.receive().await.is_some() {} });
        poll_fn(|cx| {
            polls.set(polls.get() + 1);
            next.as_mut().poll(cx)
        })
        .await;
    }

    #[test]
    fn a_worker_with_an_empty_mailbox_is_not_polled_while_another_works() {
        let mailboxes: [Mailbox<u32>; 3] = [Mailbox::new(), Mailbox::new(), Mailbox::new()];
        let polls = [Cell::new(0), Cell::new(0), Cell::new(0)];
        let mut workers = pin!(join_workers(&mailboxes, |index, mailbox| counted(
            mailbox,
            &polls[index]
        )));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(workers.as_mut().poll(&mut cx).is_pending());
        for delivery in 0..10 {
            let mut send = pin!(mailboxes[0].send(Some(delivery)));
            assert!(send.as_mut().poll(&mut cx).is_ready(), "the slot is free");
            assert!(workers.as_mut().poll(&mut cx).is_pending());
        }
        assert_eq!(polls[0].get(), 11, "polls of the busy worker");
        assert_eq!(
            [polls[1].get(), polls[2].get()],
            [1, 1],
            "polls of the idle workers: once, when they first parked"
        );
        for mailbox in &mailboxes {
            assert!(pin!(mailbox.send(None)).poll(&mut cx).is_ready());
        }
        assert_eq!(workers.as_mut().poll(&mut cx), Poll::Ready(()));
    }
}
