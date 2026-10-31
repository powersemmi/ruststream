//! The state one local bus holds: its subscriptions, each with the channel its deliveries wait in,
//! and the fan-out a publish runs.

use alloc::rc::Rc;
use core::cell::{Cell, RefCell};
use core::mem;
use core::num::NonZeroU64;
use core::task::{Context, Poll};

#[cfg(feature = "testing")]
use core::cell::OnceCell;

use bytes::Bytes;
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::Channel;
use embassy_sync::waitqueue::WakerRegistration;
use ruststream_core::HeaderMap;
#[cfg(feature = "testing")]
use ruststream_core::RawMessage;
use ruststream_runtime::RETRY_COUNT_HEADER;
#[cfg(feature = "testing")]
use ruststream_runtime::testing::Coordinator;

use super::LocalMemoryError;
use super::alloc_prelude::*;

/// One message as a subscription holds it: the payload and the headers the publish handed over,
/// and how many times it has been delivered.
#[derive(Clone)]
pub(super) struct Delivery {
    pub(super) payload: Bytes,
    pub(super) headers: HeaderMap,
    pub(super) deliveries: NonZeroU64,
}

/// How many times the bus has delivered a message that is published now: the first time, unless
/// the runtime published it as the copy of a delivery it retried later. The bus has no delay of
/// its own, so the runtime's copies carry the retry, and the copy continues the count its header
/// holds rather than starting over. A header that is not a count is a first delivery.
fn delivery_count_of(headers: &HeaderMap) -> NonZeroU64 {
    headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .map_or(NonZeroU64::MIN, |retried| {
            NonZeroU64::MIN.saturating_add(retried)
        })
}

/// Where a subscription stands.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Standing {
    /// Its subscriber reads it.
    Open,
    /// Its subscriber was dropped: a requeue lands nowhere, as on a server whose consumer left.
    Unsubscribed,
    /// The broker shut down: the stream ends once the queue is drained, and a requeue is refused.
    ShutDown,
}

/// One subscription: the channel its deliveries wait in and the count of those in its handler's
/// hands.
///
/// A delivery in hand keeps its place in the capacity, so a requeue always finds room in the
/// channel and is never lost.
pub(super) struct Subscription<const CAP: usize> {
    name: Box<str>,
    queue: Channel<NoopRawMutex, Delivery, CAP>,
    in_hand: Cell<usize>,
    standing: Cell<Standing>,
    // The channel wakes a reader when a delivery arrives; this wakes it when the bus shuts down.
    closing: RefCell<WakerRegistration>,
    // Wakes a publisher waiting for room when a place is released.
    room: RefCell<WakerRegistration>,
    #[cfg(feature = "testing")]
    pub(super) harness: Rc<Harness>,
}

impl<const CAP: usize> Subscription<CAP> {
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    fn has_room(&self) -> bool {
        self.queue.len() + self.in_hand.get() < CAP
    }

    /// Puts `delivery` in the queue, which [`has_room`](Self::has_room) or a delivery in hand has
    /// reserved a place for.
    fn enqueue(&self, delivery: Delivery) {
        if self.queue.try_send(delivery).is_err() {
            // Every enqueue is preceded by the room check, or comes from a delivery in hand whose
            // place it takes back, in the same synchronous step.
            unreachable!("a subscription's channel was full where its capacity had room");
        }
        #[cfg(feature = "testing")]
        self.harness.enqueued();
    }

    /// The next delivery, in publish order, or the end once the bus shut down and the queue is
    /// drained.
    pub(super) fn poll_next(&self, cx: &mut Context<'_>) -> Poll<Option<Delivery>> {
        // A reaction that never settles would hand the app's one future a delivery on every poll,
        // so the poll would never return to the test: hold the deliveries back instead, and the
        // test's step reports it.
        #[cfg(feature = "testing")]
        if self.standing.get() == Standing::Open && self.harness.exhausted() {
            self.closing.borrow_mut().register(cx.waker());
            return Poll::Pending;
        }
        match self.queue.poll_receive(cx) {
            Poll::Ready(delivery) => {
                self.in_hand.set(self.in_hand.get() + 1);
                Poll::Ready(Some(delivery))
            }
            Poll::Pending if self.standing.get() == Standing::ShutDown => Poll::Ready(None),
            Poll::Pending => {
                self.closing.borrow_mut().register(cx.waker());
                Poll::Pending
            }
        }
    }

    /// The next delivery already waiting, without registering a waker.
    pub(super) fn try_next(&self) -> Option<Delivery> {
        let delivery = self.queue.try_receive().ok()?;
        self.in_hand.set(self.in_hand.get() + 1);
        Some(delivery)
    }

    /// Releases the place of a delivery that left its handler's hands.
    pub(super) fn settle(&self) {
        self.in_hand.set(self.in_hand.get() - 1);
        self.room.borrow_mut().wake();
        #[cfg(feature = "testing")]
        self.harness.consumed();
    }

    /// Puts a delivery in hand back at the end of the queue, before its place is released.
    pub(super) fn requeue(&self, delivery: Delivery) -> Result<(), LocalMemoryError> {
        match self.standing.get() {
            Standing::Open => {
                self.enqueue(delivery);
                Ok(())
            }
            Standing::Unsubscribed => Ok(()),
            Standing::ShutDown => Err(LocalMemoryError::ShutDown),
        }
    }

    /// Marks the subscription as left by its subscriber and releases what waits in its queue.
    pub(super) fn unsubscribe(&self) {
        if self.standing.get() == Standing::Open {
            self.standing.set(Standing::Unsubscribed);
        }
        while self.queue.try_receive().is_ok() {
            #[cfg(feature = "testing")]
            self.harness.consumed();
        }
    }

    fn shut_down(&self) {
        self.standing.set(Standing::ShutDown);
        self.closing.borrow_mut().wake();
    }
}

/// The bus a broker, its connected form and its publishers share.
pub(super) struct Bus<const CAP: usize> {
    live: Cell<bool>,
    subscriptions: RefCell<Vec<Rc<Subscription<CAP>>>>,
    #[cfg(feature = "testing")]
    harness: Rc<Harness>,
}

impl<const CAP: usize> Bus<CAP> {
    pub(super) fn new() -> Self {
        Self {
            live: Cell::new(false),
            subscriptions: RefCell::new(Vec::new()),
            #[cfg(feature = "testing")]
            harness: Rc::default(),
        }
    }

    pub(super) fn connect(&self) {
        self.live.set(true);
    }

    /// Opens a subscription to `name`.
    pub(super) fn subscribe(&self, name: &str) -> Result<Rc<Subscription<CAP>>, LocalMemoryError> {
        if !self.live.get() {
            return Err(LocalMemoryError::ShutDown);
        }
        let subscription = Rc::new(Subscription {
            name: name.into(),
            queue: Channel::new(),
            in_hand: Cell::new(0),
            standing: Cell::new(Standing::Open),
            closing: RefCell::new(WakerRegistration::new()),
            room: RefCell::new(WakerRegistration::new()),
            #[cfg(feature = "testing")]
            harness: Rc::clone(&self.harness),
        });
        self.subscriptions
            .borrow_mut()
            .push(Rc::clone(&subscription));
        Ok(subscription)
    }

    pub(super) fn unsubscribe(&self, subscription: &Rc<Subscription<CAP>>) {
        self.subscriptions
            .borrow_mut()
            .retain(|open| !Rc::ptr_eq(open, subscription));
        subscription.unsubscribe();
    }

    /// Closes every subscription and refuses publishes until the next connect. Returns how many
    /// subscriptions it closed.
    pub(super) fn shut_down(&self) -> usize {
        self.live.set(false);
        let closed = mem::take(&mut *self.subscriptions.borrow_mut());
        for subscription in &closed {
            subscription.shut_down();
        }
        closed.len()
    }

    /// Ready once a publish to `name` would be accepted; otherwise registers `cx`'s waker with a
    /// subscription that is full, woken when it releases a place.
    pub(super) fn poll_room(&self, name: &str, cx: &mut Context<'_>) -> Poll<()> {
        for subscription in self.subscriptions.borrow().iter() {
            if subscription.name() == name && !subscription.has_room() {
                subscription.room.borrow_mut().register(cx.waker());
                return Poll::Pending;
            }
        }
        Poll::Ready(())
    }

    /// Hands a publish to every subscription of `name`, or to none of them.
    ///
    /// The payload and the headers move into the last subscription's delivery, so a publish that
    /// one subscription reads allocates nothing here; each further subscription costs a clone of
    /// both.
    pub(super) fn fanout(
        &self,
        name: &str,
        payload: Bytes,
        headers: HeaderMap,
    ) -> Result<(), LocalMemoryError> {
        if !self.live.get() {
            return Err(LocalMemoryError::ShutDown);
        }
        let subscriptions = self.subscriptions.borrow();
        let reading = || subscriptions.iter().filter(|open| open.name() == name);
        // All or nothing: a publish refused by one subscription reaches no other, so the caller
        // can publish it again without doubling it anywhere.
        if reading().any(|subscription| !subscription.has_room()) {
            return Err(LocalMemoryError::Full {
                name: String::from(name),
                capacity: CAP,
            });
        }
        #[cfg(feature = "testing")]
        self.harness.record(name, &payload, &headers);
        let deliveries = delivery_count_of(&headers);
        let mut delivery = Some(Delivery {
            payload,
            headers,
            deliveries,
        });
        let mut reading = reading().peekable();
        while let Some(subscription) = reading.next() {
            let copy = if reading.peek().is_some() {
                delivery.clone()
            } else {
                delivery.take()
            };
            if let Some(copy) = copy {
                subscription.enqueue(copy);
            }
        }
        Ok(())
    }

    #[cfg(feature = "testing")]
    pub(super) fn harness(&self) -> &Harness {
        &self.harness
    }
}

/// What the test harness reads off a bus: every publish, and the coordinator that counts the
/// deliveries in flight.
#[cfg(feature = "testing")]
#[derive(Default)]
pub(super) struct Harness {
    log: RefCell<Vec<RawMessage>>,
    coordinator: OnceCell<Coordinator>,
}

#[cfg(feature = "testing")]
impl Harness {
    pub(super) fn install(&self, coordinator: Coordinator) {
        // A second install is ignored: the run keeps counting on the first coordinator.
        let _ = self.coordinator.set(coordinator);
    }

    fn record(&self, name: &str, payload: &Bytes, headers: &HeaderMap) {
        self.log
            .borrow_mut()
            .push(RawMessage::new(name, payload.clone()).with_headers(headers.clone()));
    }

    pub(super) fn published(&self, name: &str) -> Vec<RawMessage> {
        self.log
            .borrow()
            .iter()
            .filter(|message| message.name() == name)
            .cloned()
            .collect()
    }

    fn enqueued(&self) {
        if let Some(coordinator) = self.coordinator.get() {
            coordinator.enqueued();
        }
    }

    fn exhausted(&self) -> bool {
        self.coordinator.get().is_some_and(Coordinator::exhausted)
    }

    fn consumed(&self) {
        if let Some(coordinator) = self.coordinator.get() {
            coordinator.consumed();
        }
    }
}
