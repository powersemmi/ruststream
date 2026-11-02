//! The state one local bus holds: its subscriptions, each with the channel its deliveries wait in,
//! and the fan-out a publish runs.

use core::mem;
use core::num::NonZeroU64;
use core::task::{Context, Poll};

use bytes::Bytes;
use embassy_sync::channel::Channel;
#[cfg(feature = "testing")]
use embassy_sync::once_lock::OnceLock;
use embassy_sync::waitqueue::WakerRegistration;
#[cfg(feature = "testing")]
use ruststream_core::RawMessage;
use ruststream_core::{BytesMut, HeaderMap, OutgoingMessage, PublishMessageError};
use ruststream_runtime::RETRY_COUNT_HEADER;
#[cfg(feature = "testing")]
use ruststream_runtime::testing::Coordinator;
#[cfg(test)]
use std::sync::Mutex;

use super::LocalMemoryError;
use super::alloc_prelude::*;
use super::share::{Guarded, RawLock, Share, Shared, Sharing};

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
    queue: Channel<RawLock, Delivery, CAP>,
    state: Guarded<SubscriptionState>,
    #[cfg(feature = "testing")]
    pub(super) harness: Shared<Harness>,
    // Lets a test run another thread's step at a point inside a critical section.
    #[cfg(test)]
    pauses: Mutex<Option<PauseHook>>,
}

/// What a test runs when a subscription reaches a [`Pause`].
#[cfg(test)]
type PauseHook = Box<dyn Fn(Pause) + Send + Sync>;

/// The points of a subscription's critical sections a test can pause at.
#[cfg(test)]
#[derive(Clone, Copy)]
enum Pause {
    /// A delivery left the queue and is not yet counted in hand.
    AfterDequeue,
    /// A requeue read the subscription as open and has not yet enqueued.
    AfterStanding,
}

/// What a subscription changes as deliveries come and go.
struct SubscriptionState {
    in_hand: usize,
    standing: Standing,
    // The channel wakes a reader when a delivery arrives; this wakes it when the bus shuts down.
    closing: WakerRegistration,
}

impl<const CAP: usize> Subscription<CAP> {
    pub(super) fn name(&self) -> &str {
        &self.name
    }

    #[cfg(test)]
    fn pause(&self, at: Pause) {
        if let Some(hook) = self.pauses.lock().unwrap().as_ref() {
            hook(at);
        }
    }

    fn has_room(&self) -> bool {
        // One section for both counts: a receiver that moves a delivery from the queue to its
        // handler's hands changes them together, so the sum is never read between the two.
        self.state
            .with(|state| self.queue.len() + state.in_hand < CAP)
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
        if self.state.with(|state| state.standing) == Standing::Open && self.harness.exhausted() {
            self.state.with(|state| state.closing.register(cx.waker()));
            return Poll::Pending;
        }
        // Taking the delivery and counting it in hand is one section: a publisher on another
        // thread must not see the place free between the two and fill it, or the delivery's
        // requeue would find the channel full.
        self.state.with(|state| match self.queue.poll_receive(cx) {
            Poll::Ready(delivery) => {
                #[cfg(test)]
                self.pause(Pause::AfterDequeue);
                state.in_hand += 1;
                Poll::Ready(Some(delivery))
            }
            Poll::Pending => {
                if state.standing == Standing::ShutDown {
                    Poll::Ready(None)
                } else {
                    state.closing.register(cx.waker());
                    Poll::Pending
                }
            }
        })
    }

    /// Whether the next poll of the subscription's stream could return `Ready`: a delivery waits,
    /// or the bus shut down and the stream ends.
    ///
    /// Under the test harness the subscription may hold deliveries back while the harness is
    /// exhausted; the answer is then `true` all the same, which costs one poll that answers
    /// `Pending`.
    pub(super) fn may_be_ready(&self) -> bool {
        !self.queue.is_empty() || self.state.with(|state| state.standing) == Standing::ShutDown
    }

    /// The next delivery already waiting, without registering a waker.
    pub(super) fn try_next(&self) -> Option<Delivery> {
        self.state.with(|state| {
            let delivery = self.queue.try_receive().ok()?;
            #[cfg(test)]
            self.pause(Pause::AfterDequeue);
            state.in_hand += 1;
            Some(delivery)
        })
    }

    /// Releases the place of a delivery that left its handler's hands.
    pub(super) fn settle(&self) {
        self.state.with(|state| state.in_hand -= 1);
        #[cfg(feature = "testing")]
        self.harness.consumed();
    }

    /// Puts a delivery in hand back at the end of the queue, before its place is released.
    pub(super) fn requeue(&self, delivery: Delivery) -> Result<(), LocalMemoryError> {
        // The standing is read and the delivery enqueued in one section, so a subscriber that
        // leaves on another thread drains the queue either before the check (the requeue lands
        // nowhere) or after the enqueue (the drain releases it), never between.
        self.state.with(|state| match state.standing {
            Standing::Open => {
                #[cfg(test)]
                self.pause(Pause::AfterStanding);
                self.enqueue(delivery);
                Ok(())
            }
            Standing::Unsubscribed => Ok(()),
            Standing::ShutDown => Err(LocalMemoryError::ShutDown),
        })
    }

    /// Marks the subscription as left by its subscriber and releases what waits in its queue.
    pub(super) fn unsubscribe(&self) {
        self.state.with(|state| {
            if state.standing == Standing::Open {
                state.standing = Standing::Unsubscribed;
            }
            while self.queue.try_receive().is_ok() {
                #[cfg(feature = "testing")]
                self.harness.consumed();
            }
        });
    }

    fn shut_down(&self) {
        self.state.with(|state| {
            state.standing = Standing::ShutDown;
            state.closing.wake();
        });
    }
}

/// The bus a broker, its connected form and its publishers share.
pub(super) struct Bus<const CAP: usize> {
    state: Guarded<BusState<CAP>>,
    #[cfg(feature = "testing")]
    harness: Shared<Harness>,
}

/// Whether the bus accepts publishes, and the subscriptions it hands them to.
struct BusState<const CAP: usize> {
    live: bool,
    subscriptions: Vec<Shared<Subscription<CAP>>>,
}

impl<const CAP: usize> Bus<CAP> {
    pub(super) fn new() -> Self {
        Self {
            state: Guarded::new(BusState {
                live: false,
                subscriptions: Vec::new(),
            }),
            #[cfg(feature = "testing")]
            harness: Sharing::new(Harness::default()),
        }
    }

    pub(super) fn connect(&self) {
        self.state.with(|state| state.live = true);
    }

    /// Opens a subscription to `name`.
    pub(super) fn subscribe(
        &self,
        name: &str,
    ) -> Result<Shared<Subscription<CAP>>, LocalMemoryError> {
        self.state.with(|state| {
            if !state.live {
                return Err(LocalMemoryError::ShutDown);
            }
            let subscription = Sharing::new(Subscription {
                name: name.into(),
                queue: Channel::new(),
                state: Guarded::new(SubscriptionState {
                    in_hand: 0,
                    standing: Standing::Open,
                    closing: WakerRegistration::new(),
                }),
                #[cfg(feature = "testing")]
                harness: Clone::clone(&self.harness),
                #[cfg(test)]
                pauses: Mutex::new(None),
            });
            state.subscriptions.push(Clone::clone(&subscription));
            Ok(subscription)
        })
    }

    pub(super) fn unsubscribe(&self, subscription: &Shared<Subscription<CAP>>) {
        self.state.with(|state| {
            state
                .subscriptions
                .retain(|open| !Sharing::same(open, subscription));
        });
        subscription.unsubscribe();
    }

    /// Closes every subscription and refuses publishes until the next connect. Returns how many
    /// subscriptions it closed.
    pub(super) fn shut_down(&self) -> usize {
        let closed = self.state.with(|state| {
            state.live = false;
            mem::take(&mut state.subscriptions)
        });
        for subscription in &closed {
            subscription.shut_down();
        }
        closed.len()
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
    ) -> Result<(), Box<Unsent>> {
        self.state.with(move |state| {
            if !state.live {
                return Err(Unsent::boxed(LocalMemoryError::ShutDown, payload, headers));
            }
            let reading = || {
                state
                    .subscriptions
                    .iter()
                    .filter(|open| open.name() == name)
            };
            // All or nothing: a publish refused by one subscription reaches no other, so the
            // caller can publish it again without doubling it anywhere.
            if reading().any(|subscription| !subscription.has_room()) {
                let error = LocalMemoryError::Full {
                    name: String::from(name),
                    capacity: CAP,
                };
                return Err(Unsent::boxed(error, payload, headers));
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
        })
    }

    #[cfg(feature = "testing")]
    pub(super) fn harness(&self) -> &Harness {
        &self.harness
    }
}

/// The message a refused publish did not send, with the error that refused it.
///
/// Boxed so that the success path of a publish stays pointer-sized.
pub(super) struct Unsent {
    pub(super) error: LocalMemoryError,
    payload: Bytes,
    headers: HeaderMap,
}

impl Unsent {
    fn boxed(error: LocalMemoryError, payload: Bytes, headers: HeaderMap) -> Box<Self> {
        Box::new(Self {
            error,
            payload,
            headers,
        })
    }

    /// The refusal as a publisher reports it, with the message handed back in the buffer it came
    /// in.
    pub(super) fn returned(
        self,
        name: &str,
    ) -> PublishMessageError<LocalMemoryError, OutgoingMessage<'_, BytesMut>> {
        let Self {
            error,
            payload,
            headers,
        } = self;
        // The bus refused before sharing the block, so it is still unique and the buffer comes
        // back without a copy.
        let buffer = payload
            .try_into_mut()
            .unwrap_or_else(|shared| BytesMut::from(shared.as_ref()));
        PublishMessageError::with_message(
            error,
            OutgoingMessage::with_payload(name, buffer).with_headers(headers),
        )
    }
}

/// What the test harness reads off a bus: every publish, and the coordinator that counts the
/// deliveries in flight.
#[cfg(feature = "testing")]
pub(super) struct Harness {
    log: Guarded<Vec<RawMessage>>,
    coordinator: OnceLock<Coordinator>,
}

#[cfg(feature = "testing")]
impl Default for Harness {
    fn default() -> Self {
        Self {
            log: Guarded::default(),
            coordinator: OnceLock::new(),
        }
    }
}

#[cfg(feature = "testing")]
impl Harness {
    pub(super) fn install(&self, coordinator: Coordinator) {
        // A second install is ignored: the run keeps counting on the first coordinator.
        let _ = self.coordinator.init(coordinator);
    }

    fn record(&self, name: &str, payload: &Bytes, headers: &HeaderMap) {
        let message = RawMessage::new(name, payload.clone()).with_headers(headers.clone());
        self.log.with(|log| log.push(message));
    }

    pub(super) fn published(&self, name: &str) -> Vec<RawMessage> {
        self.log.with(|log| {
            log.iter()
                .filter(|message| message.name() == name)
                .cloned()
                .collect()
        })
    }

    fn enqueued(&self) {
        if let Some(coordinator) = self.coordinator.try_get() {
            coordinator.enqueued();
        }
    }

    fn exhausted(&self) -> bool {
        self.coordinator
            .try_get()
            .is_some_and(Coordinator::exhausted)
    }

    fn consumed(&self) {
        if let Some(coordinator) = self.coordinator.try_get() {
            coordinator.consumed();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc::channel;
    use std::thread;
    use std::time::Duration;

    use super::*;

    /// Whether the bus took the publish, or the error that refused it.
    fn publish(bus: &Bus<1>, payload: &'static [u8]) -> Result<(), LocalMemoryError> {
        bus.fanout("a", Bytes::from_static(payload), HeaderMap::new())
            .map_err(|unsent| unsent.error)
    }

    /// Runs `other` on a thread when the subscription reaches `at`, and gives it a moment to
    /// finish while the subscription is paused there. A step that has to wait for the pause to
    /// end is still waiting when the moment is over, and the pause carries on.
    fn run_at<Out: Send + 'static>(
        subscription: &Subscription<1>,
        at: Pause,
        other: impl FnOnce() -> Out + Send + 'static,
    ) -> thread::JoinHandle<Out> {
        let (go, started) = channel();
        let (finished, done) = channel();
        let thread = thread::spawn(move || {
            started.recv().unwrap();
            let out = other();
            let _ = finished.send(());
            out
        });
        let go = Mutex::new(Some(go));
        let done = Mutex::new(done);
        *subscription.pauses.lock().unwrap() = Some(Box::new(move |reached| {
            let go = if mem::discriminant(&reached) == mem::discriminant(&at) {
                go.lock().unwrap().take()
            } else {
                None
            };
            if let Some(go) = go {
                go.send(()).unwrap();
                let _ = done
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_millis(100));
            }
        }));
        thread
    }

    fn open() -> (Arc<Bus<1>>, Shared<Subscription<1>>) {
        let bus = Arc::new(Bus::<1>::new());
        bus.connect();
        let subscription = bus.subscribe("a").unwrap();
        (bus, subscription)
    }

    #[test]
    fn a_publish_cannot_fill_the_place_of_a_delivery_a_receiver_is_taking() {
        let (bus, subscription) = open();
        publish(&bus, b"first").unwrap();
        let publisher = Arc::clone(&bus);
        let other = run_at(&subscription, Pause::AfterDequeue, move || {
            publish(&publisher, b"second")
        });

        let taken = subscription.try_next().unwrap();
        let second = other.join().unwrap();

        assert!(matches!(second, Err(LocalMemoryError::Full { .. })));
        subscription.requeue(taken).unwrap();
        assert!(subscription.try_next().is_some());
    }

    #[test]
    fn a_requeue_is_not_stranded_by_a_subscriber_that_leaves_meanwhile() {
        let (bus, subscription) = open();
        publish(&bus, b"first").unwrap();
        let taken = subscription.try_next().unwrap();
        let leaver = Arc::clone(&bus);
        let leaving = Shared::clone(&subscription);
        let other = run_at(&subscription, Pause::AfterStanding, move || {
            leaver.unsubscribe(&leaving);
        });

        subscription.requeue(taken).unwrap();
        other.join().unwrap();

        assert!(subscription.try_next().is_none());
    }

    fn open_four() -> (Bus<4>, Shared<Subscription<4>>) {
        let bus = Bus::<4>::new();
        bus.connect();
        let subscription = bus.subscribe("cmd").unwrap();
        (bus, subscription)
    }

    fn publish_open(bus: &Bus<4>) {
        let sent = bus.fanout("cmd", Bytes::from_static(b"open"), HeaderMap::new());
        assert!(sent.is_ok());
    }

    #[test]
    fn an_empty_subscription_is_not_ready() {
        let (_bus, subscription) = open_four();
        assert!(!subscription.may_be_ready());
    }

    #[test]
    fn a_published_delivery_makes_it_ready_until_it_is_taken() {
        let (bus, subscription) = open_four();
        publish_open(&bus);
        assert!(subscription.may_be_ready());
        let _taken = subscription.try_next().unwrap();
        assert!(!subscription.may_be_ready());
    }

    #[test]
    fn a_shut_down_makes_it_ready_so_the_end_of_the_stream_is_seen() {
        let (bus, subscription) = open_four();
        bus.shut_down();
        assert!(subscription.may_be_ready());
    }

    #[test]
    fn a_subscription_its_subscriber_left_is_not_ready() {
        let (bus, subscription) = open_four();
        publish_open(&bus);
        bus.unsubscribe(&subscription);
        assert!(!subscription.may_be_ready());
    }
}
