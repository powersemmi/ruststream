//! The mount steps of the embedded runner: a constant pool of workers, constant keyed lanes, and
//! a constant capacity for the delayed copies of `retry_after`.
//!
//! Each step fixes its count in the registration's type, so a count the runner cannot honour
//! does not compile. The workers are futures joined inside the app's task: the subscription's
//! loop hands each delivery to a worker over a one-slot channel, and nothing is allocated per
//! delivery.

// The workers and their channels hold deliveries that are not `Send`, on the app's one task.
#![allow(clippy::future_not_send)]

use alloc::sync::Arc;
use core::array;
use core::hash::Hasher as _;
use core::marker::PhantomData;
use core::pin::pin;

use bytes::BytesMut;
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::channel::Channel;
use futures::StreamExt;
use futures::future::{Either, join, select};
use hash32::{FnvHasher, Hasher as _};
use ruststream_core::{BuildContext, LocalIncomingMessage, LocalSubscriber};
use ruststream_runtime::{
    Delivery, DeliverySlot, Detach, DispatchFailure, Dispatched, Halt, HeadersAtOnce,
    LocalDispatch, LocalHandler, LocalPipeline, LocalPlanned, LocalRoute, Router, ShutdownWait,
    dispatch,
};
use tracing::error;

use crate::host::Detached;
use crate::mailbox::{Mailbox, join_workers};

/// The plan of one registration on the embedded runner: its lanes and the room it keeps for
/// delayed retries, `0` for none.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct EmbeddedPlan<Lanes, Capacity> {
    lanes: Lanes,
    room: PhantomData<Capacity>,
}

/// Room for `CAPACITY` delayed retries.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Room<const CAPACITY: usize>;

/// One delivery at a time.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct OneLane;

/// `WORKERS` deliveries at once, each to the next free worker.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Pool<const WORKERS: usize>;

/// `LANES` sequential lanes, a delivery's lane chosen by its partition key.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Keyed<const LANES: usize>;

/// How a plan's lanes run the opened route.
#[doc(hidden)]
pub trait Lanes {
    /// Runs the route on these lanes until `signal` stops it.
    fn run<Sub, Body, Cx, State, Signal, Host>(
        self,
        route: LocalRoute<Sub, Body, Cx>,
        signal: Signal,
        state: &Arc<State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Sub: LocalSubscriber,
        Body: LocalHandler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach;
}

impl Lanes for OneLane {
    fn run<Sub, Body, Cx, State, Signal, Host>(
        self,
        route: LocalRoute<Sub, Body, Cx>,
        signal: Signal,
        state: &Arc<State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Sub: LocalSubscriber,
        Body: LocalHandler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        route.run_sequential(signal, state, host)
    }
}

impl<const WORKERS: usize> Lanes for Pool<WORKERS> {
    fn run<Sub, Body, Cx, State, Signal, Host>(
        self,
        route: LocalRoute<Sub, Body, Cx>,
        signal: Signal,
        state: &Arc<State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Sub: LocalSubscriber,
        Body: LocalHandler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        run_lanes::<WORKERS, _, _, _, _, _, _>(route, signal, state, host, false)
    }
}

impl<const LANES: usize> Lanes for Keyed<LANES> {
    fn run<Sub, Body, Cx, State, Signal, Host>(
        self,
        route: LocalRoute<Sub, Body, Cx>,
        signal: Signal,
        state: &Arc<State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Sub: LocalSubscriber,
        Body: LocalHandler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        run_lanes::<LANES, _, _, _, _, _, _>(route, signal, state, host, true)
    }
}

impl<L: Lanes, const CAPACITY: usize> LocalDispatch for EmbeddedPlan<L, Room<CAPACITY>> {
    async fn run<Sub, Body, Cx, State, Signal, Host>(
        self,
        route: LocalRoute<Sub, Body, Cx>,
        signal: Signal,
        state: &Arc<State>,
        host: &Host,
    ) where
        Sub: LocalSubscriber,
        Body: LocalHandler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        if CAPACITY == 0 {
            self.lanes.run(route, signal, state, host).await;
            return;
        }
        // The registration's own store: its delayed copies and continuations run beside its
        // lanes, and it finishes what it holds once the lanes have stopped.
        let store = Detached::with_room(CAPACITY);
        let lanes = async {
            self.lanes.run(route, signal, state, &store).await;
            store.close();
        };
        join(lanes, store.drive()).await;
    }
}

/// Runs the route's loop and its `COUNT` workers, joined.
///
/// The loop pulls a delivery and hands it to a worker over the worker's one-slot channel: the
/// next free worker in a pool, the lane its partition key hashes to when `keyed` (a delivery
/// with no key takes the lanes in turn). A pool's loop takes a free worker before it pulls, so
/// a delivery waits in the subscription rather than in a channel.
async fn run_lanes<const COUNT: usize, Sub, Body, Cx, State, Signal, Host>(
    route: LocalRoute<Sub, Body, Cx>,
    signal: Signal,
    state: &Arc<State>,
    host: &Host,
    keyed: bool,
) where
    Sub: LocalSubscriber,
    Body: LocalHandler<Sub::Message, Cx, State>,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
    Signal: ShutdownWait + Halt + Clone,
    Host: Detach,
{
    let LocalRoute {
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
        state: &**state,
        delivery: &delivery,
        host,
        failure: &failure,
    };
    let workers = join_workers(&mailboxes, |index, mailbox| {
        work(mailbox, (!keyed).then_some((&free, index)), &shared)
    });
    let feed = async {
        let mut stream = pin!(subscriber.stream());
        let mut cancelled = pin!(signal.cancelled());
        let mut rotation = 0;
        loop {
            if signal.is_cancelled() {
                break;
            }
            let held = if keyed {
                None
            } else {
                match select(pin!(free.receive()), cancelled.as_mut()).await {
                    Either::Left((index, _)) => Some(index),
                    Either::Right(((), _)) => break,
                }
            };
            let next = match select(stream.next(), cancelled.as_mut()).await {
                Either::Left((next, _)) => next,
                Either::Right(((), _)) => break,
            };
            match next {
                Some(Ok(msg)) => {
                    let lane = held.unwrap_or_else(|| {
                        msg.partition_key().map_or_else(
                            || {
                                rotation = (rotation + 1) % COUNT;
                                rotation
                            },
                            lane_of::<COUNT>,
                        )
                    });
                    mailboxes[lane].send(Some(msg)).await;
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
        // Each worker finishes the delivery it holds, then reads the end of its lane.
        for mailbox in &mailboxes {
            mailbox.send(None).await;
        }
    };
    join(feed, workers).await;
}

/// What every worker of one subscription borrows from its loop.
struct Shared<'a, Body, Cx, State, Signal, Host> {
    handler: &'a Body,
    name: &'a str,
    state: &'a State,
    delivery: &'a Delivery<Cx>,
    host: &'a Host,
    failure: &'a DispatchFailure<Signal>,
}

/// One worker: dispatches what its lane hands it until the lane ends, and reports itself free
/// after each delivery when it is a pool's.
async fn work<Message, Body, Cx, State, Signal, Host, const COUNT: usize>(
    mailbox: &Mailbox<Message>,
    free: Option<(&Channel<NoopRawMutex, usize, COUNT>, usize)>,
    shared: &Shared<'_, Body, Cx, State, Signal, Host>,
) where
    Message: LocalIncomingMessage,
    Body: LocalHandler<Message, Cx, State>,
    Cx: BuildContext<Message> + Send + Sync + 'static,
    Signal: Halt,
    Host: Detach,
{
    // One encode buffer per worker, as the sequential loop keeps one.
    let mut encode = BytesMut::new();
    while let Some(msg) = mailbox.receive().await {
        let mut slot = DeliverySlot::new(msg);
        dispatch(
            shared.handler,
            &mut slot,
            &mut encode,
            shared.name,
            shared.state,
            shared.delivery,
            shared.host,
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

/// The lane a partition key hashes to: 32-bit FNV-1a over the key's bytes, one xor and one
/// multiply per byte, with no state and no allocation.
fn lane_of<const LANES: usize>(key: &[u8]) -> usize {
    let mut hasher = FnvHasher::default();
    hasher.write(key);
    // A u32 fits a usize on every target the runner builds for.
    hasher.finish32() as usize % LANES
}

/// The mount steps of the embedded runner, on a local router's last registration.
///
/// Each count is a constant of the registration's type: a pool of `N` workers, `N` keyed
/// lanes, room for `CAP` delayed retries. A step names the registration just added (the
/// preceding `include`), and the steps compose in any order.
///
/// # Examples
///
/// ```
/// use core::future::ready;
///
/// use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// use ruststream_runtime::{Context, Deserialized, Handle, HandlerOutcome, Input, SoloDeserialized, subscriber};
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
///     ) -> impl Future<Output = Result<(), HandlerOutcome>> + Send {
///         tracing::info!(bytes = command.0.len(), "command");
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
/// # use ruststream_runtime::{Context, Deserialized, Handle, HandlerOutcome, Input, SoloDeserialized, subscriber};
/// # use ruststream_runtime_embassy::{EmbeddedApp, MountSteps, Mounts};
/// # struct Command;
/// # impl Deserialized for Command { type Output<'a> = Command; type Error = core::convert::Infallible;
/// #     fn from_payload(_: &[u8]) -> Result<Command, Self::Error> { Ok(Command) } }
/// # impl Input for Command { type Axis = SoloDeserialized<Command>; }
/// # struct Drive;
/// # impl Handle<Command> for Drive {
/// #     fn handle(&self, _: &Command, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> + Send { core::future::ready(Ok(())) } }
/// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         b.include(subscriber(LocalMemorySource::new("motor"), Drive).build()).pool::<0>()
///     })
/// }
/// # let _ = app(LocalMemoryBroker::new());
/// ```
///
/// for keyed lanes:
///
/// ```compile_fail
/// # use ruststream_memory::local::{LocalMemoryBroker, LocalMemorySource};
/// # use ruststream_runtime::{Context, Deserialized, Handle, HandlerOutcome, Input, SoloDeserialized, subscriber};
/// # use ruststream_runtime_embassy::{EmbeddedApp, MountSteps, Mounts};
/// # struct Command;
/// # impl Deserialized for Command { type Output<'a> = Command; type Error = core::convert::Infallible;
/// #     fn from_payload(_: &[u8]) -> Result<Command, Self::Error> { Ok(Command) } }
/// # impl Input for Command { type Axis = SoloDeserialized<Command>; }
/// # struct Drive;
/// # impl Handle<Command> for Drive {
/// #     fn handle(&self, _: &Command, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> + Send { core::future::ready(Ok(())) } }
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
/// # use ruststream_runtime::{Context, Deserialized, Handle, HandlerOutcome, Input, SoloDeserialized, subscriber};
/// # use ruststream_runtime_embassy::{EmbeddedApp, MountSteps, Mounts};
/// # struct Command;
/// # impl Deserialized for Command { type Output<'a> = Command; type Error = core::convert::Infallible;
/// #     fn from_payload(_: &[u8]) -> Result<Command, Self::Error> { Ok(Command) } }
/// # impl Input for Command { type Axis = SoloDeserialized<Command>; }
/// # struct Drive;
/// # impl Handle<Command> for Drive {
/// #     fn handle(&self, _: &Command, _: &(), _: &mut Context<'_>)
/// #         -> impl Future<Output = Result<(), HandlerOutcome>> + Send { core::future::ready(Ok(())) } }
/// fn app(bus: LocalMemoryBroker<8>) -> EmbeddedApp<impl Mounts> {
///     EmbeddedApp::new().with_broker(bus, |b| {
///         b.include(subscriber(LocalMemorySource::new("motor"), Drive).build()).retry_capacity::<0>()
///     })
/// }
/// # let _ = app(LocalMemoryBroker::new());
/// ```
pub trait MountSteps: Sized {
    /// The router once the registration runs on `Next` lanes.
    #[doc(hidden)]
    type WithLanes<Next>;
    /// The router once the registration keeps room for `CAP` delayed retries.
    #[doc(hidden)]
    type WithCapacity<const CAP: usize>;

    /// Runs the registration on `lanes`.
    #[doc(hidden)]
    fn lanes<Next>(self, lanes: Next) -> Self::WithLanes<Next>;

    /// Keeps room for `CAP` delayed copies of `retry_after`; a delivery that asks for one more
    /// is requeued at once.
    ///
    /// The copies wait in the app's task, each on the clock. A registration without this step
    /// keeps none, so its `retry_after` requeues.
    fn retry_capacity<const CAP: usize>(self) -> Self::WithCapacity<CAP>;

    /// Runs up to `N` deliveries of the registration at once, each on the next free worker.
    fn pool<const N: usize>(self) -> Self::WithLanes<Pool<N>> {
        const {
            assert!(
                N > 0,
                "a pool runs at least one worker: give `pool` a count above zero"
            );
        }
        self.lanes(Pool)
    }

    /// Runs the registration on `N` sequential lanes, a delivery's lane chosen by its partition
    /// key, so the deliveries of one key stay in order.
    ///
    /// A delivery without a key takes the lanes in turn. The key is hashed with 32-bit FNV-1a:
    /// one xor and one multiply per byte.
    fn keyed<const N: usize>(self) -> Self::WithLanes<Keyed<N>> {
        const {
            assert!(
                N > 0,
                "keyed dispatch runs at least one lane: give `keyed` a count above zero"
            );
        }
        self.lanes(Keyed)
    }
}

/// The plan a registration has so far, as the embedded runner's steps read it.
#[doc(hidden)]
pub trait SoFar {
    /// Its lanes.
    type Lanes;
    /// Its room for delayed retries.
    type Room;

    /// Its lanes, for a step that keeps them.
    fn into_lanes(self) -> Self::Lanes;
}

impl SoFar for () {
    type Lanes = OneLane;
    type Room = Room<0>;

    fn into_lanes(self) -> OneLane {
        OneLane
    }
}

impl<L, R> SoFar for EmbeddedPlan<L, R> {
    type Lanes = L;
    type Room = R;

    fn into_lanes(self) -> L {
        self.lanes
    }
}

impl<B, Head, Routes, RouteCodec, RouteLayers, Pipe> MountSteps
    for Router<B, (Head, Routes), RouteCodec, RouteLayers, LocalPipeline<Pipe>>
where
    Head: LocalPlanned<Plan: SoFar>,
{
    type WithLanes<Next> = Router<
        B,
        (
            Dispatched<Head::Route, EmbeddedPlan<Next, <Head::Plan as SoFar>::Room>>,
            Routes,
        ),
        RouteCodec,
        RouteLayers,
        LocalPipeline<Pipe>,
    >;
    type WithCapacity<const CAP: usize> = Router<
        B,
        (
            Dispatched<Head::Route, EmbeddedPlan<<Head::Plan as SoFar>::Lanes, Room<CAP>>>,
            Routes,
        ),
        RouteCodec,
        RouteLayers,
        LocalPipeline<Pipe>,
    >;

    fn lanes<Next>(self, lanes: Next) -> Self::WithLanes<Next> {
        self.replan(|_| EmbeddedPlan {
            lanes,
            room: PhantomData,
        })
    }

    fn retry_capacity<const CAP: usize>(self) -> Self::WithCapacity<CAP> {
        const {
            assert!(
                CAP > 0,
                "a registration keeps no room by default: give `retry_capacity` a count above zero"
            );
        }
        self.replan(|plan: Head::Plan| EmbeddedPlan {
            lanes: plan.into_lanes(),
            room: PhantomData,
        })
    }
}
