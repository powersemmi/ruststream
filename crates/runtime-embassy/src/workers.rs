//! The mount steps of the embedded runner: a constant pool of workers, constant keyed workers, and
//! a constant capacity for the delayed copies of `retry_after`.
//!
//! Each step fixes its count in the registration's type, so a count the runner cannot honour
//! does not compile. The workers are futures joined inside the app's task: the subscription's
//! loop hands each delivery to a worker over a one-slot channel, and nothing is allocated per
//! delivery.

// The workers and their channels hold deliveries that are not `Send`, on the app's one task.
#![allow(clippy::future_not_send)]

use core::array;
use core::cell::Cell;
use core::future::poll_fn;
use core::hash::Hasher as _;
use core::marker::PhantomData;
use core::pin::{Pin, pin};
use core::task::Poll;
use ruststream_core::MaybeSendSync;
use ruststream_core::{Broker, PlaceRoom, Places, Unbounded};
#[cfg(not(ruststream_alloc))]
use ruststream_core::{Capped, PAYLOAD_CAPACITY};

use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::Channel;
use futures::Stream;
use futures::future::{Either, join, select};
use hash32::{FnvHasher, Hasher as _};
#[cfg(ruststream_alloc)]
use ruststream_core::__alloc::bytes::BytesMut;
#[cfg(not(ruststream_alloc))]
use ruststream_core::FixedBuf;
use ruststream_core::diag::error;
use ruststream_core::{BuildContext, IncomingMessage, Settle, Subscriber};
use ruststream_runtime::{
    BatchWidth, Delivery, DeliverySlot, Detach, DispatchFailure, Dispatched, EncodeRoom, Halt,
    Handler, HeadersAtOnce, HostDetach, KeepCopies, Keeping, LocalPipeline, NoCopies,
    OneTaskDispatch, OneTaskPlanned, OneTaskRoute, Parks, PrepareCopy, PrepareKept, RefusalRoom,
    ReplyRoom, RetryCopies, RetryDestinationsDeclared, Router, RouterCommit, RouterWith,
    ShutdownWait, dispatch, idle,
};

use crate::batch_plan::Holds;
use crate::copies::CopyStore;
use crate::hooks::{AppState, borrowed};
#[cfg(not(ruststream_alloc))]
use crate::host::WorkerHost;
use crate::mailbox::{Mailbox, join_workers};

/// What a plan names in its type and holds no value of: the room, the encode buffer, the batch
/// size of its route and the places its refusals keep their messages in.
type Shape<Capacity, Encoding, Size, Refusals> =
    PhantomData<(Capacity, Encoding, fn() -> Size, Refusals)>;

/// The plan of one registration on the embedded runner: its workers, the room it keeps for retry
/// copies ([`NoRoom`] for none), and the places a refused publish of its keeps the message in.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct EmbeddedPlan<
    WorkerSet,
    Capacity,
    Encoding = Encode<DEFAULT_ENCODE_CAPACITY>,
    Size = (),
    Refusals = OnePlace,
> {
    workers: WorkerSet,
    room: Shape<Capacity, Encoding, Size, Refusals>,
}

impl<WorkerSet, Capacity, Encoding, Size, Refusals>
    EmbeddedPlan<WorkerSet, Capacity, Encoding, Size, Refusals>
{
    /// The same plan with no room for retry copies.
    pub(crate) fn without_room(self) -> EmbeddedPlan<WorkerSet, NoRoom, Encoding, Size, Refusals> {
        EmbeddedPlan {
            workers: self.workers,
            room: PhantomData,
        }
    }
}

/// The place a registration keeps for the message of a refused publish when its mount names no
/// count: one, since a refused message is read out of its place as the publish returns.
/// Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct OnePlace;

/// `N` places for the messages a registration's refused publishes hand back, as
/// `.refusal_capacity::<N>()` names them. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Refusals<const N: usize>;

/// The room of places a plan keeps. Machinery; never named directly.
#[doc(hidden)]
pub trait RefusalCount {
    /// The room.
    type Room: RefusalRoom;
}

impl RefusalCount for OnePlace {
    type Room = PlaceRoom<1>;
}

impl<const N: usize> RefusalCount for Refusals<N> {
    type Room = PlaceRoom<N>;
}

/// The room, in bytes, of a worker's encode buffer when a registration names none.
pub const DEFAULT_ENCODE_CAPACITY: usize = 256;

/// An encode buffer of `CAPACITY` bytes for each worker of a registration.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Encode<const CAPACITY: usize>;

/// A reply room an `encode_capacity` of `Self` fits in: any size where the reply publisher only
/// reads the bytes, and up to the payload room where it keeps them.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` asks for more room than the reply publisher can carry (`{Room}`)",
    label = "this `encode_capacity` is past the reply publisher's payload room",
    note = "a publisher that keeps its payload (`Take`) is handed a buffer of the payload room, \
            so a worker's extra room would go unused: name a count within it, or use a publisher \
            that reads the bytes (`Lend`)"
)]
pub trait Within<Room> {}

impl<const CAPACITY: usize> Within<Unbounded> for Encode<CAPACITY> {}

#[cfg(not(ruststream_alloc))]
macro_rules! within_payload_room {
    ($($capacity:literal)*) => {$(
        impl Within<Capped<{ PAYLOAD_CAPACITY }>> for Encode<$capacity> {}
    )*};
}

#[cfg(not(ruststream_alloc))]
within_payload_room!(
    1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32
    33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61
    62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90
    91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114
    115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136
    137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158
    159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180
    181 182 183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202
    203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224
    225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246
    247 248 249 250 251 252 253 254 255 256
);

// The list above ends where the room does.
#[cfg(not(ruststream_alloc))]
const _: () = assert!(
    PAYLOAD_CAPACITY == 256,
    "the sizes `within_payload_room` lists end at the payload room"
);

/// What the workers of a registration encode their replies into.
#[doc(hidden)]
pub trait EncodeSize {
    /// The buffer one worker owns.
    type Room: EncodeRoom;

    /// A new, empty buffer for one worker.
    fn room() -> Self::Room;
}

#[cfg(ruststream_alloc)]
impl<const CAPACITY: usize> EncodeSize for Encode<CAPACITY> {
    type Room = BytesMut;

    // A buffer that grows, so the capacity is the room reserved up front.
    fn room() -> BytesMut {
        BytesMut::with_capacity(CAPACITY)
    }
}

#[cfg(not(ruststream_alloc))]
impl<const CAPACITY: usize> EncodeSize for Encode<CAPACITY> {
    type Room = FixedBuf<CAPACITY>;

    fn room() -> FixedBuf<CAPACITY> {
        FixedBuf::new()
    }
}

/// Room for `CAPACITY` retry copies.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Room<const CAPACITY: usize>;

/// No room for retry copies: `retry_after` requeues the delivery.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NoRoom;

/// One delivery at a time.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct OneWorker;

/// `WORKERS` deliveries at once, each to the next free worker.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Pool<const WORKERS: usize>;

/// `WORKERS` sequential workers, a delivery's worker chosen by its partition key.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Keyed<const WORKERS: usize>;

/// How a plan's workers run the opened route.
#[doc(hidden)]
pub trait WorkerSet {
    /// Runs the route on these workers until `signal` stops it; a refused publish of a delivery
    /// keeps its message in one of `places`.
    #[allow(clippy::too_many_arguments)]
    fn run<Sub, Body, Cx, State, Signal, Host, Park, Enc>(
        self,
        route: OneTaskRoute<Sub, Body, Cx>,
        signal: Signal,
        state: &AppState<State>,
        host: &Host,
        parked: Park,
        places: Places<'_>,
    ) -> impl Future<Output = ()>
    where
        Enc: EncodeSize,
        Park: Parks,
        Sub: Subscriber,
        Body: Handler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: HostDetach<Kept: PrepareKept<Cx>, Retry: RetryCopies<Cx>>;
}

#[cfg(ruststream_alloc)]
impl WorkerSet for OneWorker {
    fn run<Sub, Body, Cx, State, Signal, Host, Park, Enc>(
        self,
        route: OneTaskRoute<Sub, Body, Cx>,
        signal: Signal,
        state: &AppState<State>,
        host: &Host,
        parked: Park,
        places: Places<'_>,
    ) -> impl Future<Output = ()>
    where
        Enc: EncodeSize,
        Park: Parks,
        Sub: Subscriber,
        Body: Handler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: HostDetach<Kept: PrepareKept<Cx>, Retry: RetryCopies<Cx>>,
    {
        route.run_sequential(signal, state, host, parked, Enc::room(), places)
    }
}

#[cfg(not(ruststream_alloc))]
impl WorkerSet for OneWorker {
    async fn run<Sub, Body, Cx, State, Signal, Host, Park, Enc>(
        self,
        route: OneTaskRoute<Sub, Body, Cx>,
        signal: Signal,
        state: &AppState<State>,
        host: &Host,
        parked: Park,
        places: Places<'_>,
    ) where
        Enc: EncodeSize,
        Park: Parks,
        Sub: Subscriber,
        Body: Handler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: HostDetach<Kept: PrepareKept<Cx>, Retry: RetryCopies<Cx>>,
    {
        // The worker's own continuation slot, beside its deliveries.
        let worker = WorkerHost::new(host);
        // Pinned in this frame, so the join beside the slot's driver holds a reference to the
        // loop and the app's future nests no deeper for the slot than for this frame.
        let run = pin!(route.run_sequential(
            signal,
            state,
            &worker,
            worker.parks(parked),
            Enc::room(),
            places
        ));
        worker.around(run).await;
    }
}

impl<const WORKERS: usize> WorkerSet for Pool<WORKERS> {
    fn run<Sub, Body, Cx, State, Signal, Host, Park, Enc>(
        self,
        route: OneTaskRoute<Sub, Body, Cx>,
        signal: Signal,
        state: &AppState<State>,
        host: &Host,
        parked: Park,
        places: Places<'_>,
    ) -> impl Future<Output = ()>
    where
        Enc: EncodeSize,
        Park: Parks,
        Sub: Subscriber,
        Body: Handler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: HostDetach<Kept: PrepareKept<Cx>, Retry: RetryCopies<Cx>>,
    {
        run_workers::<WORKERS, _, _, _, _, _, _, _, Enc>(
            route, signal, state, host, false, parked, places,
        )
    }
}

impl<const WORKERS: usize> WorkerSet for Keyed<WORKERS> {
    fn run<Sub, Body, Cx, State, Signal, Host, Park, Enc>(
        self,
        route: OneTaskRoute<Sub, Body, Cx>,
        signal: Signal,
        state: &AppState<State>,
        host: &Host,
        parked: Park,
        places: Places<'_>,
    ) -> impl Future<Output = ()>
    where
        Enc: EncodeSize,
        Park: Parks,
        Sub: Subscriber,
        Body: Handler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: HostDetach<Kept: PrepareKept<Cx>, Retry: RetryCopies<Cx>>,
    {
        run_workers::<WORKERS, _, _, _, _, _, _, _, Enc>(
            route, signal, state, host, true, parked, places,
        )
    }
}

impl<L, Enc, Refused> OneTaskDispatch for EmbeddedPlan<L, NoRoom, Enc, (), Refused>
where
    L: WorkerSet,
    Enc: EncodeSize,
    Refused: RefusalCount,
{
    type Copies = NoCopies;

    // The frame owns the registration's places, which its workers lend each delivery.
    async fn run<Sub, Body, Cx, State, Signal, Host, Path>(
        self,
        route: OneTaskRoute<Sub, Body, Cx>,
        _path: Option<Path>,
        signal: Signal,
        state: &AppState<State>,
        host: &Host,
        parked: &Cell<bool>,
    ) where
        Sub: Subscriber,
        Body: Handler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
        Path: PrepareCopy<Cx>,
    {
        let room = Refused::Room::default();
        self.workers
            .run::<_, _, _, _, _, _, _, Enc>(route, signal, state, host, parked, room.places())
            .await;
    }
}

impl<L, Enc, Refused, const CAPACITY: usize> OneTaskDispatch
    for EmbeddedPlan<L, Room<CAPACITY>, Enc, (), Refused>
where
    L: WorkerSet,
    Enc: EncodeSize,
    Refused: RefusalCount,
{
    type Copies = KeepCopies;

    async fn run<Sub, Body, Cx, State, Signal, Host, Path>(
        self,
        route: OneTaskRoute<Sub, Body, Cx>,
        path: Option<Path>,
        signal: Signal,
        state: &AppState<State>,
        host: &Host,
        parked: &Cell<bool>,
    ) where
        Sub: Subscriber,
        Body: Handler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
        Path: PrepareCopy<Cx>,
    {
        // The registration's places, which its workers lend each delivery and its store's copies
        // take from.
        let room = Refused::Room::default();
        let Some(path) = path else {
            self.workers
                .run::<_, _, _, _, _, _, _, Enc>(route, signal, state, host, parked, room.places())
                .await;
            return;
        };
        // The registration's own store: its copies wait in its records, and its driver
        // publishes the last of them after the workers have stopped.
        let store = CopyStore::<Path, CAPACITY>::new(path, &route.name)
            .input(route.input)
            .places(room.places());
        let workers = async {
            let keeping = Keeping(host, &store);
            // The store's driver waits on timers of its own beside the workers, so the join above
            // must not skip this registration while the workers are idle: the flag stays clear.
            self.workers
                .run::<_, _, _, _, _, _, _, Enc>(route, signal, state, &keeping, (), room.places())
                .await;
            store.close();
        };
        join(workers, store.drive(host)).await;
    }
}

/// Runs the route's loop and its `COUNT` workers, joined.
///
/// The loop pulls a delivery and hands it to a worker over the worker's one-slot channel: the
/// next free worker in a pool, the worker its partition key hashes to when `keyed` (a delivery
/// with no key takes the workers in turn). A pool's loop takes a free worker before it pulls, so
/// a delivery waits in the subscription rather than in a channel.
async fn run_workers<const COUNT: usize, Sub, Body, Cx, State, Signal, Host, Park, Enc>(
    route: OneTaskRoute<Sub, Body, Cx>,
    signal: Signal,
    state: &AppState<State>,
    host: &Host,
    keyed: bool,
    group: Park,
    places: Places<'_>,
) where
    Enc: EncodeSize,
    Park: Parks,
    Sub: Subscriber,
    Body: Handler<Sub::Message, Cx, State>,
    Cx: BuildContext<Sub::Message> + MaybeSendSync + 'static,
    Signal: ShutdownWait + Halt + Clone,
    Host: HostDetach<Kept: PrepareKept<Cx>, Retry: RetryCopies<Cx>>,
{
    let OneTaskRoute {
        mut subscriber,
        handler,
        name,
        delivery,
        policies,
        ..
    } = route;
    let failure = DispatchFailure::new(policies, signal.clone());
    let mailboxes: [Mailbox<Sub::Message>; COUNT] = array::from_fn(|_| Mailbox::new());
    let free: Channel<NoopRawMutex, usize, COUNT> = Channel::new();
    if !keyed {
        for index in 0..COUNT {
            // The queue holds every worker once, and none has been taken yet.
            let _ = free.try_send(index);
        }
    }
    let shared = Shared {
        handler: &handler,
        name: &name,
        state: borrowed(state),
        delivery: &delivery,
        host,
        failure: &failure,
        places,
    };
    let workers = join_workers(&mailboxes, |index, mailbox| {
        work::<_, _, _, _, _, _, Enc, COUNT>(mailbox, (!keyed).then_some((&free, index)), &shared)
    });
    // Set while the loop's poll found the stream and the shutdown wait pending, cleared once it
    // hands a delivery over: with every worker idle, the group is idle.
    let feed_idle = Cell::new(false);
    let feed = async {
        let probe = subscriber.ready_hint();
        let mut stream = pin!(subscriber.stream());
        let mut cancelled = pin!(signal.cancelled());
        let mut rotation = 0;
        // A pool's free worker, held until a delivery goes to it: a stream error or a cancelled
        // read hands no delivery over, so the worker stays reserved for the next one.
        let mut held = None;
        loop {
            if signal.is_cancelled() {
                break;
            }
            if !keyed && held.is_none() {
                match select(pin!(free.receive()), cancelled.as_mut()).await {
                    Either::Left((index, _)) => held = Some(index),
                    Either::Right(((), _)) => break,
                }
            }
            // The stream is polled first and the shutdown wait only where it answers `Pending`; a
            // subscription whose probe says nothing is ready is not polled again until it does.
            let mut parked = false;
            let next = poll_fn(|cx| {
                if idle(parked, &probe, &signal) {
                    return Poll::Pending;
                }
                match stream.as_mut().poll_next(cx) {
                    Poll::Ready(next) => {
                        feed_idle.set(false);
                        Poll::Ready(Either::Left(next))
                    }
                    Poll::Pending => {
                        let waiting = cancelled.as_mut().poll(cx).map(Either::Right);
                        parked = waiting.is_pending();
                        feed_idle.set(parked);
                        waiting
                    }
                }
            })
            .await;
            let Either::Left(next) = next else { break };
            match next {
                Some(Ok(msg)) => {
                    let worker = held.take().unwrap_or_else(|| {
                        msg.partition_key().map_or_else(
                            || {
                                rotation = (rotation + 1) % COUNT;
                                rotation
                            },
                            key_index::<COUNT>,
                        )
                    });
                    mailboxes[worker].send(Some(msg)).await;
                }
                Some(Err(err)) => {
                    error!(
                        target: "ruststream::dispatch",
                        subscription = %name,
                        error = %err,
                        "subscriber stream error",
                    );
                }
                None => {
                    signal.stream_ended(&name);
                    break;
                }
            }
        }
        // Each worker finishes the delivery it holds, then reads the end of its worker.
        for mailbox in &mailboxes {
            mailbox.send(None).await;
        }
    };
    recording_idle(pin!(join(feed, workers)), group, &feed_idle, &mailboxes).await;
}

/// Runs `running`, the loop joined with its workers, and records in `group` after every poll that
/// leaves it pending whether the whole group is idle.
///
/// A worker holds a delivery, waits on a timer or on a publish only while its mailbox is not
/// parked, so the group is idle exactly when the loop and every mailbox are: the join above may
/// then skip the group until its probe says a delivery waits.
fn recording_idle<'a, Message, Park, Done, const COUNT: usize>(
    mut running: Pin<&'a mut impl Future<Output = Done>>,
    group: Park,
    feed_idle: &'a Cell<bool>,
    mailboxes: &'a [Mailbox<Message>; COUNT],
) -> impl Future<Output = Done> + 'a
where
    Park: Parks + 'a,
{
    poll_fn(move |cx| {
        let polled = running.as_mut().poll(cx);
        if polled.is_pending() {
            group.park(feed_idle.get() && mailboxes.iter().all(Mailbox::is_parked));
        }
        polled
    })
}

/// What every worker of one subscription borrows from its loop.
struct Shared<'a, Body, Cx, State, Signal, Host> {
    handler: &'a Body,
    name: &'a str,
    state: &'a State,
    delivery: &'a Delivery<Cx>,
    host: &'a Host,
    failure: &'a DispatchFailure<Signal>,
    /// The subscription's places, which every worker lends its deliveries.
    places: Places<'a>,
}

/// One worker: dispatches what its worker hands it until the worker ends, and reports itself free
/// after each delivery when it is a pool's.
async fn work<Message, Body, Cx, State, Signal, Host, Enc, const COUNT: usize>(
    mailbox: &Mailbox<Message>,
    free: Option<(&Channel<NoopRawMutex, usize, COUNT>, usize)>,
    shared: &Shared<'_, Body, Cx, State, Signal, Host>,
) where
    Enc: EncodeSize,
    Message: Settle,
    Body: Handler<Message, Cx, State>,
    Cx: BuildContext<Message> + MaybeSendSync + 'static,
    Signal: Halt,
    Host: HostDetach<Kept: PrepareKept<Cx>, Retry: RetryCopies<Cx>>,
{
    #[cfg(ruststream_alloc)]
    {
        deliveries::<_, _, _, _, _, _, _, Enc, COUNT>(mailbox, free, shared, shared.host, || false)
            .await;
    }
    // The worker's own continuation slot: a delivery whose continuation finds it busy waits here.
    #[cfg(not(ruststream_alloc))]
    {
        let worker = WorkerHost::new(shared.host);
        // A worker whose continuation still runs stays awake: the continuation's timer wakes the
        // app task, and a parked worker would not be polled to drive it.
        worker
            .around(deliveries::<_, _, _, _, _, _, _, Enc, COUNT>(
                mailbox,
                free,
                shared,
                &worker,
                || worker.is_busy(),
            ))
            .await;
    }
}

/// The worker's deliveries, dispatched on `host` until its worker ends.
async fn deliveries<Message, Body, Cx, State, Signal, Host, Lent, Enc, const COUNT: usize>(
    mailbox: &Mailbox<Message>,
    free: Option<(&Channel<NoopRawMutex, usize, COUNT>, usize)>,
    shared: &Shared<'_, Body, Cx, State, Signal, Host>,
    host: &Lent,
    holding: impl Fn() -> bool,
) where
    Enc: EncodeSize,
    Message: Settle,
    Body: Handler<Message, Cx, State>,
    Cx: BuildContext<Message> + MaybeSendSync + 'static,
    Signal: Halt,
    Lent: HostDetach<Kept: PrepareKept<Cx>, Retry: RetryCopies<Cx>>,
{
    // One encode buffer per worker, as the sequential loop keeps one.
    let mut room = Enc::room();
    let encode = room.buffer();
    while let Some(msg) = mailbox.receive(&holding).await {
        let mut slot = DeliverySlot::new(msg);
        dispatch(
            shared.handler,
            &mut slot,
            &mut *encode,
            shared.places,
            shared.name,
            shared.state,
            shared.delivery,
            host,
            shared.failure,
            HeadersAtOnce,
        )
        .await;
        if let Some((free, index)) = free {
            // The queue has room: it holds each index at most once, and this worker's index is
            // not in it while the worker holds a delivery.
            let _ = free.try_send(index);
        }
    }
}

/// The worker a partition key hashes to: 32-bit FNV-1a over the key's bytes, one xor and one
/// multiply per byte, with no state and no allocation.
fn key_index<const WORKERS: usize>(key: &[u8]) -> usize {
    let mut hasher = FnvHasher::default();
    hasher.write(key);
    // A u32 fits a usize on every target the runner builds for.
    hasher.finish32() as usize % WORKERS
}

/// The mount steps of the embedded runner, on a local router's last registration.
///
/// Each count is a constant of the registration's type: a pool of `N` workers, `N` keyed
/// workers, room for `CAP` delayed retries. A step names the registration just added (the
/// preceding `include`), and the steps compose in any order.
///
/// # Examples
///
/// ```
/// use core::future::ready;
///
/// use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// use ruststream_runtime::{Context, Deserialized, HandlerOutcome, Input, Handle, SoloDeserialized, subscriber};
/// use ruststream_runtime_embassy::{EmbeddedApp, MountSteps, Mounts};
///
/// /// A command for the motor, as the radio sends it.
/// struct Command<'a>(&'a [u8]);
///
/// impl Deserialized for Command<'_> {
///     type Output<'a> = Command<'a>;
///     type Error = core::convert::Infallible;
///
///     fn from_payload(payload: &[u8]) -> Result<Command<'_>, Self::Error> {
///         Ok(Command(payload))
///     }
/// }
///
/// impl Input for Command<'_> {
///     type Axis = SoloDeserialized<Command<'static>>;
/// }
///
/// struct Drive;
///
/// impl Handle<Command<'_>> for Drive {
///     fn handle(
///         &self,
///         command: &Command<'_>,
///         _outs: &(),
///         _ctx: &mut Context<'_>,
///     ) -> impl Future<Output = Result<(), HandlerOutcome>> {
///         log::info!("command, {} bytes", command.0.len());
///         ready(Ok(()))
///     }
/// }
///
/// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         // Four commands at once, and room for two delayed retries.
///         b.include(subscriber(LocalMemorySource::new("motor"), Drive).build())
///             .pool::<4>()
///             .retry_capacity::<2>()
///     })
/// }
/// # let _ = app(LocalMemoryBroker::new());
/// ```
///
/// A count of zero does not build, for a pool:
///
/// ```compile_fail
/// # use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// # use ruststream_runtime::{Context, Deserialized, HandlerOutcome, Input, Handle, SoloDeserialized, subscriber};
/// # use ruststream_runtime_embassy::{EmbeddedApp, MountSteps, Mounts};
/// # struct Command;
/// # impl Deserialized for Command { type Output<'a> = Command; type Error = core::convert::Infallible;
/// #     fn from_payload(_: &[u8]) -> Result<Command, Self::Error> { Ok(Command) } }
/// # impl Input for Command { type Axis = SoloDeserialized<Command>; }
/// # struct Drive;
/// # impl Handle<Command> for Drive {
/// #     fn handle(&self, _: &Command, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> { core::future::ready(Ok(())) } }
/// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         b.include(subscriber(LocalMemorySource::new("motor"), Drive).build()).pool::<0>()
///     })
/// }
/// # let _ = app(LocalMemoryBroker::new());
/// ```
///
/// for keyed workers:
///
/// ```compile_fail
/// # use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// # use ruststream_runtime::{Context, Deserialized, HandlerOutcome, Input, Handle, SoloDeserialized, subscriber};
/// # use ruststream_runtime_embassy::{EmbeddedApp, MountSteps, Mounts};
/// # struct Command;
/// # impl Deserialized for Command { type Output<'a> = Command; type Error = core::convert::Infallible;
/// #     fn from_payload(_: &[u8]) -> Result<Command, Self::Error> { Ok(Command) } }
/// # impl Input for Command { type Axis = SoloDeserialized<Command>; }
/// # struct Drive;
/// # impl Handle<Command> for Drive {
/// #     fn handle(&self, _: &Command, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> { core::future::ready(Ok(())) } }
/// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         b.include(subscriber(LocalMemorySource::new("motor"), Drive).build()).keyed::<0>()
///     })
/// }
/// # let _ = app(LocalMemoryBroker::new());
/// ```
///
/// or for the retry room:
///
/// ```compile_fail
/// # use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// # use ruststream_runtime::{Context, Deserialized, HandlerOutcome, Input, Handle, SoloDeserialized, subscriber};
/// # use ruststream_runtime_embassy::{EmbeddedApp, MountSteps, Mounts};
/// # struct Command;
/// # impl Deserialized for Command { type Output<'a> = Command; type Error = core::convert::Infallible;
/// #     fn from_payload(_: &[u8]) -> Result<Command, Self::Error> { Ok(Command) } }
/// # impl Input for Command { type Axis = SoloDeserialized<Command>; }
/// # struct Drive;
/// # impl Handle<Command> for Drive {
/// #     fn handle(&self, _: &Command, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> { core::future::ready(Ok(())) } }
/// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         b.include(subscriber(LocalMemorySource::new("motor"), Drive).build()).retry_capacity::<0>()
///     })
/// }
/// # let _ = app(LocalMemoryBroker::new());
/// ```
///
/// or for the places of refused messages:
///
/// ```compile_fail
/// # use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// # use ruststream_runtime::{Context, Deserialized, HandlerOutcome, Input, Handle, SoloDeserialized, subscriber};
/// # use ruststream_runtime_embassy::{EmbeddedApp, MountSteps, Mounts};
/// # struct Command;
/// # impl Deserialized for Command { type Output<'a> = Command; type Error = core::convert::Infallible;
/// #     fn from_payload(_: &[u8]) -> Result<Command, Self::Error> { Ok(Command) } }
/// # impl Input for Command { type Axis = SoloDeserialized<Command>; }
/// # struct Drive;
/// # impl Handle<Command> for Drive {
/// #     fn handle(&self, _: &Command, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> { core::future::ready(Ok(())) } }
/// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         b.include(subscriber(LocalMemorySource::new("motor"), Drive).build()).refusal_capacity::<0>()
///     })
/// }
/// # let _ = app(LocalMemoryBroker::new());
/// ```
pub trait MountSteps: Sized {
    /// The router once the registration runs on `Next` workers.
    #[doc(hidden)]
    type WithWorkers<Next>;
    /// The router once the registration keeps room for `CAP` delayed retries.
    #[doc(hidden)]
    type WithCapacity<const CAP: usize>;
    /// The router once the registration's workers encode into `N` bytes.
    #[doc(hidden)]
    type WithEncode<const N: usize>;
    /// The router once the registration keeps `N` places for the messages of refused publishes.
    #[doc(hidden)]
    type WithRefusals<const N: usize>;
    /// The room the registration keeps so far.
    #[doc(hidden)]
    type Room;
    /// The registration's batch size as a type: `()` for a single delivery.
    #[doc(hidden)]
    type Batch;
    /// Runs the registration on `workers`.
    #[doc(hidden)]
    fn run_on<Next>(self, workers: Next) -> Self::WithWorkers<Next>;

    /// Keeps room for `CAP` copies of `retry_after`. A delivery that asks for one more finds the
    /// store full: that is a failed publish like any other, with its own error
    /// ([`NoFreeRecordError`](ruststream_runtime::NoFreeRecordError)). The handler's
    /// [`HandlerOutcome::error`](ruststream_runtime::HandlerOutcome::error) continuation gets it
    /// with the copy, and without one a warning names the subscription, the message type and the
    /// capacity and the copy is lost.
    ///
    /// Each copy waits in one of the registration's `CAP` records, its bytes and headers kept as
    /// data, and the registration's own loop publishes it on the clock once it is due, back to
    /// the address the subscription's descriptor reports. A record reuses its buffer from one
    /// copy to the next; a copy still carries the delivery's header map with the retry count in
    /// it, which allocates with the heap form and uses fixed storage without `alloc`. The
    /// descriptor declares `Copies = AddressedCopies` and the broker names a default publish
    /// policy. A registration without this step keeps none, so its
    /// `retry_after` requeues.
    ///
    /// The `.transform(..)` steps after `.out_retry(..)` run over a copy when it is kept, while
    /// the delivery it copies is still at hand to read, and the copy leaves as they left it,
    /// with the publish settings they wrote. They read the destination and do not name one.
    ///
    ///
    /// A batch mount keeps a record for each element its handler retries at once, so the capacity
    /// is at least the batch's size: a smaller one does not compile, and the error names both
    /// numbers. Without `alloc` the capacity of a batch mount is at most 1024.
    fn retry_capacity<const CAP: usize>(self) -> Self::WithCapacity<CAP>
    where
        Room<CAP>: Holds<Self::Batch>;

    /// Gives each worker of the registration `N` bytes to encode a reply into, 256 by default.
    ///
    /// A reply that does not fit is refused with `CodecFailure::BufferFull`, the way any publish
    /// the transport cannot take is: the delivery is acknowledged and the reply reaches the
    /// handler's `.error` continuation. A worker owns its buffer for as long as it runs, so the
    /// registration's footprint grows by `N` bytes per worker.
    fn encode_capacity<const N: usize>(self) -> Self::WithEncode<N>
    where
        Encode<N>: Within<Self::ReplyRoom>;

    /// Keeps `N` places for the messages of the registration's refused publishes, in a build
    /// without `alloc`.
    ///
    /// A publish the runtime makes for a delivery (its reply, a retry or a dead-letter copy) that
    /// the broker refuses hands its message back: the message moves into a free place, and the
    /// runtime reads it out of the place as the publish returns, for the handler's
    /// [`.error`](ruststream_runtime::HandlerOutcome::error) continuation. A publish that goes out
    /// takes no place. By default the registration keeps one place for each
    /// delivery it holds at once: one for a handler that takes one delivery at a time, the batch
    /// size for a batch handler, and the worker count for `.pool::<N>()` or `.keyed::<N>()`.
    ///
    /// When every place is taken, a refused publish comes back without its message: its error
    /// still reaches `.error` and the diagnostics, and the message's bytes and headers do not. A
    /// place is a message's destination, payload and headers in fixed storage, about 700 bytes
    /// of the app. With `alloc` a refused message is boxed and the step changes nothing.
    ///
    /// A publish through an `Out` slot keeps a refused message in a place of the slot's own.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
    /// use ruststream::runtime::{EmbeddedApp, MountSteps, Mounts};
    /// use ruststream::{Deserialized, Outgoing, Serialized, subscriber};
    ///
    /// /// A reading, as its bytes.
    /// #[derive(Deserialized)]
    /// struct Reading<'a>(&'a [u8]);
    ///
    /// /// The answer to a reading: its first byte.
    /// #[derive(Outgoing, Serialized)]
    /// #[outgoing(name = "answers")]
    /// struct Answer([u8; 1]);
    ///
    /// #[subscriber(LocalMemorySource::new("readings"), reply)]
    /// async fn answer(reading: &Reading<'_>) -> Answer {
    ///     Answer([reading.0.first().copied().unwrap_or_default()])
    /// }
    ///
    /// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
    ///     EmbeddedApp::new().with_broker(bus, |b| {
    ///         // Four readings at once, and two places for the answers the bus refuses.
    ///         b.include(answer)
    ///             .out_reply(LocalMemoryPublish)
    ///             .pool::<4>()
    ///             .refusal_capacity::<2>()
    ///     })
    /// }
    /// # let _ = app(LocalMemoryBroker::new());
    /// ```
    fn refusal_capacity<const N: usize>(self) -> Self::WithRefusals<N>;

    /// The room the registration's reply publisher can carry.
    #[doc(hidden)]
    type ReplyRoom;

    /// Runs up to `N` deliveries of the registration at once, each on the next free worker.
    fn pool<const N: usize>(self) -> Self::WithWorkers<Pool<N>> {
        const {
            assert!(
                N > 0,
                "a pool runs at least one worker: give `pool` a count above zero"
            );
        }
        self.run_on(Pool)
    }

    /// Runs the registration on `N` sequential workers, a delivery's worker chosen by its partition
    /// key, so the deliveries of one key stay in order.
    ///
    /// A delivery without a key takes the workers in turn. The key is hashed with 32-bit FNV-1a:
    /// one xor and one multiply per byte.
    fn keyed<const N: usize>(self) -> Self::WithWorkers<Keyed<N>> {
        const {
            assert!(
                N > 0,
                "keyed dispatch runs at least one worker: give `keyed` a count above zero"
            );
        }
        self.run_on(Keyed)
    }
}

/// The plan a registration has so far, as the embedded runner's steps read it.
#[doc(hidden)]
pub trait SoFar {
    /// Its workers.
    type WorkerSet;
    /// Its room for delayed retries.
    type Room;
    /// Its encode buffer.
    type Encoding;
    /// The places its refused publishes keep their messages in.
    type Refusals;

    /// Its workers, for a step that keeps them.
    fn into_workers(self) -> Self::WorkerSet;
}

impl SoFar for () {
    type WorkerSet = OneWorker;
    type Room = NoRoom;
    type Encoding = Encode<DEFAULT_ENCODE_CAPACITY>;
    type Refusals = OnePlace;

    fn into_workers(self) -> OneWorker {
        OneWorker
    }
}

impl<L, R, E, Z, F> SoFar for EmbeddedPlan<L, R, E, Z, F> {
    type WorkerSet = L;
    type Room = R;
    type Encoding = E;
    type Refusals = F;

    fn into_workers(self) -> L {
        self.workers
    }
}

impl<B, Head, Routes, RouteCodec, RouteLayers, Pipe> MountSteps
    for Router<B, (Head, Routes), RouteCodec, RouteLayers, LocalPipeline<Pipe>>
where
    B: Broker,
    Head: OneTaskPlanned<Plan: SoFar, Route: ReplyRoom<B> + BatchWidth>,
{
    type ReplyRoom = <Head::Route as ReplyRoom<B>>::Room;
    type Batch = <Head::Route as BatchWidth>::Size;

    type WithWorkers<Next> = Router<
        B,
        (
            Dispatched<
                Head::Route,
                EmbeddedPlan<
                    Next,
                    <Head::Plan as SoFar>::Room,
                    <Head::Plan as SoFar>::Encoding,
                    <Head::Route as BatchWidth>::Size,
                    <Head::Plan as SoFar>::Refusals,
                >,
            >,
            Routes,
        ),
        RouteCodec,
        RouteLayers,
        LocalPipeline<Pipe>,
    >;
    type WithCapacity<const CAP: usize> = Router<
        B,
        (
            Dispatched<
                Head::Route,
                EmbeddedPlan<
                    <Head::Plan as SoFar>::WorkerSet,
                    Room<CAP>,
                    <Head::Plan as SoFar>::Encoding,
                    <Head::Route as BatchWidth>::Size,
                    <Head::Plan as SoFar>::Refusals,
                >,
            >,
            Routes,
        ),
        RouteCodec,
        RouteLayers,
        LocalPipeline<Pipe>,
    >;
    type WithEncode<const N: usize> = Router<
        B,
        (
            Dispatched<
                Head::Route,
                EmbeddedPlan<
                    <Head::Plan as SoFar>::WorkerSet,
                    <Head::Plan as SoFar>::Room,
                    Encode<N>,
                    <Head::Route as BatchWidth>::Size,
                    <Head::Plan as SoFar>::Refusals,
                >,
            >,
            Routes,
        ),
        RouteCodec,
        RouteLayers,
        LocalPipeline<Pipe>,
    >;
    type WithRefusals<const N: usize> = Router<
        B,
        (
            Dispatched<
                Head::Route,
                EmbeddedPlan<
                    <Head::Plan as SoFar>::WorkerSet,
                    <Head::Plan as SoFar>::Room,
                    <Head::Plan as SoFar>::Encoding,
                    <Head::Route as BatchWidth>::Size,
                    Refusals<N>,
                >,
            >,
            Routes,
        ),
        RouteCodec,
        RouteLayers,
        LocalPipeline<Pipe>,
    >;
    type Room = <Head::Plan as SoFar>::Room;

    fn run_on<Next>(self, workers: Next) -> Self::WithWorkers<Next> {
        self.replan(|_| EmbeddedPlan {
            workers,
            room: PhantomData,
        })
    }

    fn retry_capacity<const CAP: usize>(self) -> Self::WithCapacity<CAP>
    where
        Room<CAP>: Holds<Self::Batch>,
    {
        const {
            assert!(
                CAP > 0,
                "a registration keeps no room by default: give `retry_capacity` a count above zero"
            );
        }
        self.replan(|plan: Head::Plan| EmbeddedPlan {
            workers: plan.into_workers(),
            room: PhantomData,
        })
    }

    fn encode_capacity<const N: usize>(self) -> Self::WithEncode<N>
    where
        Encode<N>: Within<Self::ReplyRoom>,
    {
        const {
            assert!(
                N > 0,
                "a worker encodes into at least one byte: give `encode_capacity` a count above zero"
            );
        }
        self.replan(|plan: Head::Plan| EmbeddedPlan {
            workers: plan.into_workers(),
            room: PhantomData,
        })
    }

    fn refusal_capacity<const N: usize>(self) -> Self::WithRefusals<N> {
        const {
            assert!(
                N > 0,
                "a registration keeps at least one place for refused messages: give \
                 `refusal_capacity` a count above zero"
            );
        }
        self.replan(|plan: Head::Plan| EmbeddedPlan {
            workers: plan.into_workers(),
            room: PhantomData,
        })
    }
}

// A registration with steps of its own (`.out_reply(..)`, `.out(..)`) is committed first, so
// the plan goes on the route the chain built.
impl<Mount, R, Def, Attach, Last> MountSteps for RouterWith<Mount, R, Def, Attach, Last>
where
    Attach: RouterCommit<Mount, R, Def, Out: RetryDestinationsDeclared + MountSteps>,
{
    type WithWorkers<Next> = <Attach::Out as MountSteps>::WithWorkers<Next>;
    type WithCapacity<const CAP: usize> = <Attach::Out as MountSteps>::WithCapacity<CAP>;
    type WithEncode<const N: usize> = <Attach::Out as MountSteps>::WithEncode<N>;
    type WithRefusals<const N: usize> = <Attach::Out as MountSteps>::WithRefusals<N>;
    type Room = <Attach::Out as MountSteps>::Room;
    type Batch = <Attach::Out as MountSteps>::Batch;

    type ReplyRoom = <Attach::Out as MountSteps>::ReplyRoom;

    fn run_on<Next>(self, workers: Next) -> Self::WithWorkers<Next> {
        self.build().run_on(workers)
    }

    fn encode_capacity<const N: usize>(self) -> Self::WithEncode<N>
    where
        Encode<N>: Within<Self::ReplyRoom>,
    {
        self.build().encode_capacity::<N>()
    }

    fn retry_capacity<const CAP: usize>(self) -> Self::WithCapacity<CAP>
    where
        Room<CAP>: Holds<Self::Batch>,
    {
        self.build().retry_capacity::<CAP>()
    }

    fn refusal_capacity<const N: usize>(self) -> Self::WithRefusals<N> {
        self.build().refusal_capacity::<N>()
    }
}
