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

use super::LocalMemoryError;
use super::alloc_prelude::*;
use super::share::{Guarded, RawLock, Shared};

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

    fn has_room(&self) -> bool {
        self.queue.len() + self.state.with(|state| state.in_hand) < CAP
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
        match self.queue.poll_receive(cx) {
            Poll::Ready(delivery) => {
                self.state.with(|state| state.in_hand += 1);
                Poll::Ready(Some(delivery))
            }
            Poll::Pending => self.state.with(|state| {
                if state.standing == Standing::ShutDown {
                    Poll::Ready(None)
                } else {
                    state.closing.register(cx.waker());
                    Poll::Pending
                }
            }),
        }
    }

    /// The next delivery already waiting, without registering a waker.
    pub(super) fn try_next(&self) -> Option<Delivery> {
        let delivery = self.queue.try_receive().ok()?;
        self.state.with(|state| state.in_hand += 1);
        Some(delivery)
    }

    /// Releases the place of a delivery that left its handler's hands.
    pub(super) fn settle(&self) {
        self.state.with(|state| state.in_hand -= 1);
        #[cfg(feature = "testing")]
        self.harness.consumed();
    }

    /// Puts a delivery in hand back at the end of the queue, before its place is released.
    pub(super) fn requeue(&self, delivery: Delivery) -> Result<(), LocalMemoryError> {
        match self.state.with(|state| state.standing) {
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
        self.state.with(|state| {
            if state.standing == Standing::Open {
                state.standing = Standing::Unsubscribed;
            }
        });
        while self.queue.try_receive().is_ok() {
            #[cfg(feature = "testing")]
            self.harness.consumed();
        }
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
            harness: Shared::default(),
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
            let subscription = Shared::new(Subscription {
                name: name.into(),
                queue: Channel::new(),
                state: Guarded::new(SubscriptionState {
                    in_hand: 0,
                    standing: Standing::Open,
                    closing: WakerRegistration::new(),
                }),
                #[cfg(feature = "testing")]
                harness: Shared::clone(&self.harness),
            });
            state.subscriptions.push(Shared::clone(&subscription));
            Ok(subscription)
        })
    }

    pub(super) fn unsubscribe(&self, subscription: &Shared<Subscription<CAP>>) {
        self.state.with(|state| {
            state
                .subscriptions
                .retain(|open| !Shared::ptr_eq(open, subscription));
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
