//! The state one fixed local bus holds: its subscriptions, each with the places its deliveries lie
//! in and the deque of those waiting, and the fan-out a publish runs. Every size is a const
//! generic, so the bus is one value of known size that a board declares as a `static`.

use core::cell::RefCell;
use core::fmt;
use core::num::NonZeroU64;
use core::task::{Context, Poll};

use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::waitqueue::WakerRegistration;
use heapless::{Deque, String as FixedString, Vec as FixedVec};
use ruststream_core::{FixedBuf, Headers};
use ruststream_runtime::RETRY_COUNT_HEADER;
use static_cell::ConstStaticCell;

use super::LocalMemoryError;
#[cfg(feature = "testing")]
use super::testing::Harness;

/// The longest subscription name the bus copies, in bytes. A name that is a literal of the program
/// is held whole and has no limit.
pub(super) const NAME_BYTES: usize = 64;

/// One message as a subscription holds it: the payload and the headers the publish handed over,
/// and how many times it has been delivered.
pub(super) struct Delivery<const PAYLOAD: usize> {
    pub(super) payload: FixedBuf<PAYLOAD>,
    pub(super) headers: Headers,
    pub(super) deliveries: NonZeroU64,
}

impl<const PAYLOAD: usize> Delivery<PAYLOAD> {
    /// A place before its first publish.
    const EMPTY: Self = Self {
        payload: FixedBuf::new(),
        headers: Headers::new(),
        deliveries: NonZeroU64::MIN,
    };

    /// Writes a publish into the place: the payload, which the caller checked fits, and the
    /// headers' used entries and bytes alone.
    fn write(&mut self, payload: &[u8], headers: &Headers, deliveries: NonZeroU64) {
        self.payload.clear();
        if self.payload.extend_from_slice(payload).is_err() {
            unreachable!("a publish checks that its payload fits before it is written");
        }
        self.headers.clone_from(headers);
        self.deliveries = deliveries;
    }
}

/// The place one delivery lies in, from the publish that writes it to the settlement that frees
/// it: a part of the bus's `static`, handed between a subscription's free places, its queue and
/// the message that holds it, so a delivery is never copied as it travels.
pub(super) type Place<const PAYLOAD: usize> = &'static mut Delivery<PAYLOAD>;

/// How many times the bus has delivered a message that is published now: the first time, unless
/// the runtime published it as the copy of a delivery it retried later. The bus has no delay of
/// its own, so the runtime's copies carry the retry, and the copy continues the count its header
/// holds rather than starting over. A header that is not a count is a first delivery.
fn delivery_count_of(headers: &Headers) -> NonZeroU64 {
    headers
        .get_str(RETRY_COUNT_HEADER)
        .and_then(|count| count.parse::<u64>().ok())
        .map_or(NonZeroU64::MIN, |retried| {
            NonZeroU64::MIN.saturating_add(retried)
        })
}

/// The name a subscription reads: a literal of the program, held whole, or a built name copied
/// into the bus.
pub(super) enum SlotName {
    Static(&'static str),
    Built(FixedString<NAME_BYTES>),
}

impl SlotName {
    /// A name built at run time, copied into the bus; `None` past [`NAME_BYTES`].
    pub(super) fn built(name: &str) -> Option<Self> {
        FixedString::try_from(name).ok().map(Self::Built)
    }

    fn as_str(&self) -> &str {
        match self {
            Self::Static(name) => name,
            Self::Built(name) => name,
        }
    }
}

/// Where a subscription stands.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Standing {
    /// No subscriber holds the slot.
    Free,
    /// Its subscriber reads it.
    Open,
    /// The bus shut down: the stream ends once the queue is drained, and a requeue is refused. The
    /// slot stays held until its subscriber is dropped.
    ShutDown,
}

/// One subscription: its places, the deque of the deliveries waiting in them, and the places free
/// for the next publish. A place in its handler's hands is in neither, so a requeue always finds
/// room in the deque and is never lost.
struct Slot<const MESSAGES: usize, const PAYLOAD: usize> {
    // Counts the subscribers the slot has had, so a delivery that outlives its subscriber finds the
    // slot taken by another and does not touch it.
    generation: u32,
    standing: Standing,
    name: Option<SlotName>,
    queue: Deque<Place<PAYLOAD>, MESSAGES>,
    free: FixedVec<Place<PAYLOAD>, MESSAGES>,
    // Woken when a delivery arrives and when the bus shuts down.
    reader: WakerRegistration,
}

impl<const MESSAGES: usize, const PAYLOAD: usize> Slot<MESSAGES, PAYLOAD> {
    const fn new() -> Self {
        Self {
            generation: 0,
            standing: Standing::Free,
            name: None,
            queue: Deque::new(),
            free: FixedVec::new(),
            reader: WakerRegistration::new(),
        }
    }

    #[inline]
    fn reads(&self, name: &str) -> bool {
        self.standing == Standing::Open
            && self.name.as_ref().is_some_and(|own| own.as_str() == name)
    }

    fn has_room(&self) -> bool {
        !self.free.is_empty()
    }

    /// Puts the delivery in `place` in the queue: a place of this slot's own, so the deque, which
    /// holds as many as the slot has, always has room for it.
    fn enqueue(&mut self, place: Place<PAYLOAD>) {
        if self.queue.push_back(place).is_err() {
            unreachable!("a subscription's deque holds every place of the subscription");
        }
        self.reader.wake();
    }

    /// Gives `place` back to the slot it belongs to.
    fn release(&mut self, place: Place<PAYLOAD>) {
        if self.free.push(place).is_err() {
            unreachable!("a subscription's free list holds every place of the subscription");
        }
    }
}

/// Which slot a subscriber holds, and which of the slot's subscribers it was.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Held {
    index: usize,
    generation: u32,
}

/// Whether the bus accepts publishes, and the subscriptions it hands them to.
struct BusState<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> {
    live: bool,
    // Whether the slots hold their places: taken once, at the first connect.
    placed: bool,
    // The slots a subscriber holds, so a publish looks at those alone and not at every slot of
    // the bus.
    held: FixedVec<usize, SUBSCRIPTIONS>,
    slots: [Slot<MESSAGES, PAYLOAD>; SUBSCRIPTIONS],
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    BusState<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    /// The slot `held` names, unless its subscriber has gone and another holds it now.
    fn slot(&mut self, held: Held) -> Option<&mut Slot<MESSAGES, PAYLOAD>> {
        self.slots
            .get_mut(held.index)
            .filter(|slot| slot.generation == held.generation && slot.standing != Standing::Free)
    }
}

/// The bus a broker, its connected form and its publishers share: a value of fixed size that a
/// board declares as a `static`, so no allocator is needed.
///
/// `MESSAGES` is how many messages each subscription holds, waiting or in its handler's hands;
/// `PAYLOAD` the largest payload in bytes; `SUBSCRIPTIONS` how many subscriptions are open at
/// once.
///
/// Each subscription has `MESSAGES` places in the bus, and a delivery lies in its place from the
/// publish to its settlement: the message a handler reads holds the place, so moving it moves a
/// reference, never the payload and the headers. Settling the message frees the place; a delivery
/// whose subscriber left keeps its place until it is settled.
///
/// The state sits behind a critical section, so the bus is `Sync`; the board's binary supplies
/// the critical-section implementation. The first connect hands each subscription its places
/// through a compare-and-swap; on a target without one (a Cortex-M0+) the firmware gives
/// `portable-atomic` its `critical-section` feature.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::prelude::*;
///
/// /// Two subscriptions at a time, each holding up to four messages of up to 16 bytes.
/// static BUS: LocalMemoryBus<4, 16, 2> = LocalMemoryBus::new();
///
/// let opened = embassy_futures::block_on(async {
///     let connected = LocalMemoryBroker::new(&BUS).connect().await?;
///     let _readings = connected.subscribe("sensors/temp").await?;
///     let _commands = connected.subscribe("actuator/cmd").await?;
///     // Both places are taken, so a third subscription is refused.
///     connected.subscribe("sensors/humidity").await.map(drop)
/// });
/// assert_eq!(opened, Err(LocalMemoryError::NoFreeSubscription { capacity: 2 }));
/// ```
pub struct LocalMemoryBus<
    const MESSAGES: usize,
    const PAYLOAD: usize = 64,
    const SUBSCRIPTIONS: usize = 8,
> {
    state: Mutex<CriticalSectionRawMutex, RefCell<BusState<MESSAGES, PAYLOAD, SUBSCRIPTIONS>>>,
    // Where each subscription's deliveries lie: `MESSAGES` places per subscription, each handed to
    // its slot once as a `&'static mut`.
    places: [[ConstStaticCell<Delivery<PAYLOAD>>; MESSAGES]; SUBSCRIPTIONS],
    #[cfg(feature = "testing")]
    harness: Harness,
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    /// An empty bus.
    ///
    /// A bus that holds no message, no byte of payload or no subscription does not build: every
    /// publish to it would be refused.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream_memory::local::prelude::*;
    ///
    /// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
    ///
    /// let connected = embassy_futures::block_on(LocalMemoryBroker::new(&BUS).connect());
    /// assert!(connected.is_ok());
    /// ```
    ///
    /// A bus that holds nothing is a build error:
    ///
    /// ```compile_fail,E0080
    /// use ruststream_memory::local::LocalMemoryBus;
    ///
    /// static BUS: LocalMemoryBus<0> = LocalMemoryBus::new();
    /// ```
    #[must_use]
    pub const fn new() -> Self {
        const {
            assert!(
                MESSAGES > 0 && PAYLOAD > 0 && SUBSCRIPTIONS > 0,
                "a LocalMemoryBus holds at least one message of at least one byte in at least \
                 one subscription: give MESSAGES, PAYLOAD and SUBSCRIPTIONS values above zero",
            );
        }
        Self {
            state: Mutex::new(RefCell::new(BusState {
                live: false,
                placed: false,
                held: FixedVec::new(),
                slots: [const { Slot::new() }; SUBSCRIPTIONS],
            })),
            places: [const { [const { ConstStaticCell::new(Delivery::EMPTY) }; MESSAGES] };
                SUBSCRIPTIONS],
            #[cfg(feature = "testing")]
            harness: Harness::new(),
        }
    }

    fn with<Out>(
        &self,
        access: impl FnOnce(&mut BusState<MESSAGES, PAYLOAD, SUBSCRIPTIONS>) -> Out,
    ) -> Out {
        self.state.lock(|cell| access(&mut cell.borrow_mut()))
    }

    /// Opens the bus to publishes; the first connect hands each subscription its places.
    pub(super) fn connect(&'static self) {
        self.with(|state| {
            if !state.placed {
                for (slot, places) in state.slots.iter_mut().zip(&self.places) {
                    for place in places {
                        // Taken here alone, under the bus's lock and once: `placed` is set below.
                        let Some(place) = place.try_take() else {
                            unreachable!("a place of the bus is taken once, at its first connect");
                        };
                        slot.release(place);
                    }
                }
                state.placed = true;
            }
            state.live = true;
        });
    }

    /// Opens a subscription to `name`.
    pub(super) fn subscribe(&self, name: SlotName) -> Result<Held, LocalMemoryError> {
        self.with(|state| {
            if !state.live {
                return Err(LocalMemoryError::ShutDown);
            }
            let free = state
                .slots
                .iter_mut()
                .enumerate()
                .find(|(_, slot)| slot.standing == Standing::Free);
            let Some((index, slot)) = free else {
                return Err(LocalMemoryError::NoFreeSubscription {
                    capacity: SUBSCRIPTIONS,
                });
            };
            slot.generation = slot.generation.wrapping_add(1);
            slot.standing = Standing::Open;
            slot.name = Some(name);
            if state.held.push(index).is_err() {
                unreachable!("the bus lists as many held slots as it has");
            }
            Ok(Held {
                index,
                generation: slot.generation,
            })
        })
    }

    /// Frees the slot of a subscriber that left and releases what waits in its queue. A delivery
    /// still in its handler's hands finds the slot gone and settles into nothing.
    pub(super) fn unsubscribe(&self, held: Held) {
        self.with(|state| {
            if let Some(slot) = state.slot(held) {
                #[cfg(feature = "testing")]
                for _ in 0..slot.queue.len() {
                    self.harness.consumed();
                }
                while let Some(place) = slot.queue.pop_front() {
                    slot.release(place);
                }
                slot.name = None;
                slot.standing = Standing::Free;
                slot.reader = WakerRegistration::new();
                state.held.retain(|&index| index != held.index);
            }
        });
    }

    /// Closes every subscription and refuses publishes until the next connect. Returns how many
    /// subscriptions it closed.
    pub(super) fn shut_down(&self) -> usize {
        self.with(|state| {
            state.live = false;
            let mut closed = 0;
            for slot in &mut state.slots {
                if slot.standing == Standing::Open {
                    slot.standing = Standing::ShutDown;
                    slot.reader.wake();
                    closed += 1;
                }
            }
            closed
        })
    }

    /// The next delivery, in publish order, or the end once the bus shut down and the queue is
    /// drained.
    pub(super) fn poll_next(&self, held: Held, cx: &Context<'_>) -> Poll<Option<Place<PAYLOAD>>> {
        // A reaction that never settles would hand the app's one future a delivery on every poll,
        // so the poll would never return to the test: the deliveries are held back instead, and
        // the test's step reports it.
        #[cfg(feature = "testing")]
        let held_back = self.harness.exhausted();
        self.with(|state| {
            let Some(slot) = state.slot(held) else {
                return Poll::Ready(None);
            };
            #[cfg(feature = "testing")]
            if held_back && slot.standing == Standing::Open {
                slot.reader.register(cx.waker());
                return Poll::Pending;
            }
            if let Some(place) = slot.queue.pop_front() {
                Poll::Ready(Some(place))
            } else if slot.standing == Standing::ShutDown {
                Poll::Ready(None)
            } else {
                slot.reader.register(cx.waker());
                Poll::Pending
            }
        })
    }

    /// The next delivery already waiting, without registering a waker.
    pub(super) fn try_next(&self, held: Held) -> Option<Place<PAYLOAD>> {
        self.with(|state| state.slot(held)?.queue.pop_front())
    }

    /// Whether the next poll of the subscription's stream could return `Ready`: a delivery waits,
    /// or the bus shut down and the stream ends.
    pub(super) fn may_be_ready(&self, held: Held) -> bool {
        self.with(|state| {
            state
                .slot(held)
                .is_none_or(|slot| !slot.queue.is_empty() || slot.standing == Standing::ShutDown)
        })
    }

    /// Frees the place of a delivery that left its handler's hands. The place belongs to its slot
    /// whoever holds the slot now, so it goes back there even after its subscriber left.
    pub(super) fn settle(&self, held: Held, place: Place<PAYLOAD>) {
        self.with(|state| state.slots[held.index].release(place));
        // A delivery whose subscriber left is released all the same.
        #[cfg(feature = "testing")]
        self.harness.consumed();
    }

    /// Puts a delivery in hand back at the end of the queue, one delivery more. A subscriber that
    /// left takes nothing, as on a server whose consumer is gone, and the place goes back to its
    /// slot.
    pub(super) fn requeue(
        &self,
        held: Held,
        place: Place<PAYLOAD>,
    ) -> Result<(), LocalMemoryError> {
        let outcome = self.with(|state| {
            let live = state.live;
            let refused = match state.slot(held) {
                Some(slot) if slot.standing == Standing::Open => {
                    place.deliveries = place.deliveries.saturating_add(1);
                    slot.enqueue(place);
                    #[cfg(feature = "testing")]
                    self.harness.enqueued();
                    return Ok(());
                }
                // A bus shut down refuses a requeue even when the subscriber has left: the
                // settlement must not report success on a dead bus.
                Some(_) => Err(LocalMemoryError::ShutDown),
                None if !live => Err(LocalMemoryError::ShutDown),
                None => Ok(()),
            };
            // What is not requeued is released: its place goes back to its slot.
            state.slots[held.index].release(place);
            refused
        });
        // The delivery left its handler's hands either way.
        #[cfg(feature = "testing")]
        self.harness.consumed();
        outcome
    }

    /// Hands a publish to every subscription of `name`, or to none of them. The bus copies what it
    /// keeps, so a refusal leaves the message with the caller, whole.
    pub(super) fn fanout(
        &self,
        name: &str,
        payload: &[u8],
        headers: &Headers,
    ) -> Result<(), LocalMemoryError> {
        self.with(|state| {
            if !state.live {
                return Err(LocalMemoryError::ShutDown);
            }
            // The slots that read `name`, each found once. All or nothing: a publish refused by
            // one subscription reaches no other, so the caller can publish it again without
            // doubling it anywhere.
            let mut readers = FixedVec::<usize, SUBSCRIPTIONS>::new();
            for &index in &state.held {
                let slot = &state.slots[index];
                if !slot.reads(name) {
                    continue;
                }
                if !slot.has_room() {
                    return Err(LocalMemoryError::Full { capacity: MESSAGES });
                }
                if readers.push(index).is_err() {
                    unreachable!("the bus lists each held slot once");
                }
            }
            if readers.is_empty() {
                #[cfg(feature = "testing")]
                self.harness.record(name, payload, headers);
                return Ok(());
            }
            if payload.len() > PAYLOAD {
                return Err(LocalMemoryError::PayloadTooLarge {
                    size: payload.len(),
                    capacity: PAYLOAD,
                });
            }
            #[cfg(feature = "testing")]
            self.harness.record(name, payload, headers);
            let deliveries = delivery_count_of(headers);
            // Each reader's delivery is written where it lies.
            for &index in &readers {
                let slot = &mut state.slots[index];
                let Some(place) = slot.free.pop() else {
                    unreachable!("every reader had a free place a moment ago");
                };
                place.write(payload, headers, deliveries);
                slot.enqueue(place);
                #[cfg(feature = "testing")]
                self.harness.enqueued();
            }
            Ok(())
        })
    }
}

#[cfg(feature = "testing")]
impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize>
    LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    pub(super) const fn harness(&self) -> &Harness {
        &self.harness
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> fmt::Debug
    for LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalMemoryBus")
            .field("messages", &MESSAGES)
            .field("payload", &PAYLOAD)
            .field("subscriptions", &SUBSCRIPTIONS)
            .finish_non_exhaustive()
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> Default
    for LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn default() -> Self {
        Self::new()
    }
}
