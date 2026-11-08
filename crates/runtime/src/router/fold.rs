//! The typed consumer of a finished router: its route list opened against a connected broker of
//! either family into one value that runs every subscription, with nothing erased.
//!
//! [`RouterDef::mount`](super::RouterDef::mount) pushes each route into a runner's sink, which
//! stores what it is handed and so erases its type. A runner that hosts the whole app in one task
//! (embassy's static tasks name one concrete future) folds the list instead: [`OpenRoutes`]
//! subscribes every route and hands back a list whose type carries each opened subscription, and
//! [`RunRoutes`] joins their loops into one future. Opening allocates (a route's name, its
//! handler), once; the joined future allocates nothing per delivery.
//!
//! A route the fold does not open is a compile error at the runner's call, naming it.

// Written over the contract traits for a runner that polls on one thread: the
// futures are `Send` exactly where the types they run are, and nothing here spawns them.
#![allow(clippy::future_not_send)]

use core::cell::Cell;
use core::future::ready;
use core::num::NonZeroUsize;
use core::pin::Pin;
use core::task::{Context, Poll};
use ruststream_core::MaybeSendSync;

use futures::future::MaybeDone;
use pin_project_lite::pin_project;
use ruststream_core::{EncodeBuf, PayloadBuf, PlaceRoom, Places};

use crate::batch::{BatchHandler, TakenBatch};
use crate::coop::Cooperative;
use crate::dispatch::{Delivery, Gated, HeadersAtOnce, run_batch, run_sequential};
use crate::failure::{DispatchFailure, FailurePolicies};
use crate::handler::Handler;
use crate::host::{Detach, Halt, SendHost, ShutdownWait};
use crate::lifecycle::BoxError;
use crate::middleware::{BlanketLayer, Stack};
use crate::redelivery::ScopeDelivery;
use crate::shared::{StateRef, SubscriptionName, handle_of};
use crate::{BatchSubscriber, Broker, BuildContext, Connected, Subscriber};

use super::builder::Router;
use super::fold_batch::OneTaskBatchRoute;

/// A finished router's routes, opened against the connected broker into one list.
///
/// Implemented by [`Router`] and by every route the fold opens; a runner calls it once at
/// startup and runs what it hands back with [`RunRoutes::run`].
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a route this runner opens",
    note = "a runner that hosts the app in one task opens the `include(def)` of a single-message \
            or batch handler that replies or takes `Out` slots (not both at once)"
)]
pub trait OpenRoutes<B: Broker, State = ()> {
    /// The opened list: every subscription with its handler, ready to run.
    type Opened: RunRoutes<State>;

    /// Subscribes every route against `connected`.
    ///
    /// # Errors
    ///
    /// The first subscription the broker refuses.
    fn open(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
    ) -> impl Future<Output = Result<Self::Opened, BoxError>>;
}

/// An opened route list: runs every subscription it holds in one future until `signal` stops
/// them.
#[doc(hidden)]
pub trait RunRoutes<State> {
    /// What the list's subscriptions record their idleness in: one flag per subscription that
    /// a join can gate, in the list's own shape. It lives in the future that runs the list, above
    /// every join, so a loop borrows its flag and nothing refers to itself.
    type Parks: Default;

    /// Runs each subscription's loop, joined and gated on `parks`; the future ends when every
    /// loop has ended.
    fn run_parked<Signal, Host>(
        self,
        parks: &Self::Parks,
        signal: Signal,
        state: StateRef<'_, State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach;

    /// Runs each subscription's loop, joined; the future ends when every loop has ended. A join
    /// skips a subscription that is idle and whose readiness probe says nothing is ready, without
    /// resuming the subscription's frame.
    fn run<Signal, Host>(
        self,
        signal: Signal,
        state: StateRef<'_, State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Self: Sized,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        async move {
            let parks = Self::Parks::default();
            self.run_parked(&parks, signal, state, host).await;
        }
    }
}

/// One registration of a router's list, opened under the layer stack that wraps its handler:
/// the router's own, inside every router it was merged into.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a route this runner opens",
    note = "a runner that hosts the app in one task opens the `include(def)` of a single-message \
            or batch handler that replies or takes `Out` slots (not both at once)"
)]
pub trait OpenLayered<B: Broker, State> {
    /// The opened registrations, each holding the layer stack it runs under.
    type Opened<Layers: BlanketLayer + Clone>: RunRoutes<State>;

    /// Subscribes every registration against `connected`, keeping `layers` beside each handler.
    ///
    /// # Errors
    ///
    /// See [`OpenRoutes::open`].
    fn open_layered<Layers: BlanketLayer + Clone>(
        self,
        connected: &Connected<B>,
        layers: &Layers,
        scope: &ScopeDelivery,
    ) -> impl Future<Output = Result<Self::Opened<Layers>, BoxError>>;
}

/// One opened single-message subscription: the subscriber, the handler it dispatches to, the
/// layer stack that wraps the handler when it runs, and what each of its deliveries runs under.
#[doc(hidden)]
#[derive(Debug)]
pub struct OpenedRoute<Sub, Body, Cx, Layers> {
    subscriber: Sub,
    handler: Body,
    layers: Layers,
    name: SubscriptionName,
    delivery: Delivery<Cx>,
    policies: FailurePolicies,
}

impl<Sub, Body, Cx, Layers> OpenedRoute<Sub, Body, Cx, Layers> {
    /// An opened subscription, ready to run.
    pub(super) const fn new(
        subscriber: Sub,
        handler: Body,
        layers: Layers,
        name: SubscriptionName,
        delivery: Delivery<Cx>,
        policies: FailurePolicies,
    ) -> Self {
        Self {
            subscriber,
            handler,
            layers,
            name,
            delivery,
            policies,
        }
    }
}

impl<B: Broker, State> OpenLayered<B, State> for () {
    type Opened<Layers: BlanketLayer + Clone> = ();

    fn open_layered<Layers: BlanketLayer + Clone>(
        self,
        _connected: &Connected<B>,
        _layers: &Layers,
        _scope: &ScopeDelivery,
    ) -> impl Future<Output = Result<(), BoxError>> {
        ready(Ok(()))
    }
}

impl<State> RunRoutes<State> for () {
    type Parks = ();

    fn run_parked<Signal, Host>(
        self,
        _parks: &(),
        _signal: Signal,
        _state: StateRef<'_, State>,
        _host: &Host,
    ) -> impl Future<Output = ()>
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        ready(())
    }
}

impl<B, State, Head, Tail> OpenLayered<B, State> for (Head, Tail)
where
    B: Broker,
    Head: OpenLayered<B, State>,
    Tail: OpenLayered<B, State>,
{
    type Opened<Layers: BlanketLayer + Clone> = (Head::Opened<Layers>, Tail::Opened<Layers>);

    async fn open_layered<Layers: BlanketLayer + Clone>(
        self,
        connected: &Connected<B>,
        layers: &Layers,
        scope: &ScopeDelivery,
    ) -> Result<Self::Opened<Layers>, BoxError> {
        // The list is built head first and the router pushes onto the head, so the tail holds
        // the registrations made first: they subscribe first.
        let tail = self.1.open_layered(connected, layers, scope).await?;
        let head = self.0.open_layered(connected, layers, scope).await?;
        Ok((head, tail))
    }
}

impl<State, Head, Tail> RunRoutes<State> for (Head, Tail)
where
    Head: RunRoutes<State>,
    Tail: RunRoutes<State>,
{
    type Parks = (Head::Parks, Tail::Parks);

    fn run_parked<Signal, Host>(
        self,
        parks: &(Head::Parks, Tail::Parks),
        signal: Signal,
        state: StateRef<'_, State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        // A named future, not an `async` block: each pair of the tree is one type level of the
        // app's future, so a device's many routes stay inside the compiler's layout depth.
        Both {
            left: MaybeDone::Future(self.0.run_parked(&parks.0, signal.clone(), state, host)),
            right: MaybeDone::Future(self.1.run_parked(&parks.1, signal, state, host)),
        }
    }
}

pin_project! {
    /// Two route loops polled together; ready once both have ended. Each loop's future is
    /// dropped as soon as it ends.
    struct Both<Left: Future, Right: Future> {
        #[pin]
        left: MaybeDone<Left>,
        #[pin]
        right: MaybeDone<Right>,
    }
}

impl<Left: Future, Right: Future> Future for Both<Left, Right> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        let left = this.left.poll(cx).is_ready();
        let right = this.right.poll(cx).is_ready();
        if left && right {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// A list of registrations re-associated into a balanced tree of pairs, so the joined future of
/// a long list nests as deep as the logarithm of its length rather than its length.
///
/// The list is the router's: built head first, ending in `()`. A pass pairs neighbours (the
/// later registration on the left), and passes repeat until one element remains. Opening a pair
/// opens its right side first, so the tree still subscribes in registration order.
#[doc(hidden)]
pub trait Balance {
    /// The list after one pass: neighbours paired.
    type Paired;
    /// The balanced tree.
    type Tree;

    /// Pairs neighbours once.
    fn pair(self) -> Self::Paired;

    /// Re-associates the list into its balanced tree.
    fn balance(self) -> Self::Tree;
}

impl Balance for () {
    type Paired = ();
    type Tree = ();

    fn pair(self) {}

    fn balance(self) {}
}

impl<Last> Balance for (Last, ()) {
    type Paired = Self;
    type Tree = Last;

    fn pair(self) -> Self {
        self
    }

    fn balance(self) -> Last {
        self.0
    }
}

impl<First, Second, Rest> Balance for (First, (Second, Rest))
where
    Rest: Balance,
    ((First, Second), Rest::Paired): Balance,
{
    type Paired = ((First, Second), Rest::Paired);
    type Tree = <((First, Second), Rest::Paired) as Balance>::Tree;

    fn pair(self) -> Self::Paired {
        let (first, (second, rest)) = self;
        ((first, second), rest.pair())
    }

    fn balance(self) -> Self::Tree {
        self.pair().balance()
    }
}

// A finished router opens its list under its own layer stack.
impl<B, State, Routes, RouteCodec, RouteLayers, RoutePipe> OpenRoutes<B, State>
    for Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>
where
    B: Broker,
    Routes: Balance<Tree: OpenLayered<B, State>>,
    RouteLayers: BlanketLayer + Clone,
{
    type Opened = <Routes::Tree as OpenLayered<B, State>>::Opened<RouteLayers>;

    fn open(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
    ) -> impl Future<Output = Result<Self::Opened, BoxError>> {
        let layers = self.layers;
        let routes = self.routes.balance();
        async move { routes.open_layered(connected, &layers, scope).await }
    }
}

// A merged router is one registration of the router it was merged into: its own stack wraps its
// handlers inside the stack of the router that holds it, as on the multi-thread runner.
impl<B, State, Routes, RouteCodec, RouteLayers, RoutePipe> OpenLayered<B, State>
    for Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>
where
    B: Broker,
    Routes: Balance<Tree: OpenLayered<B, State>>,
    RouteLayers: BlanketLayer + Clone,
{
    type Opened<Layers: BlanketLayer + Clone> =
        <Routes::Tree as OpenLayered<B, State>>::Opened<Stack<RouteLayers, Layers>>;

    fn open_layered<Layers: BlanketLayer + Clone>(
        self,
        connected: &Connected<B>,
        layers: &Layers,
        scope: &ScopeDelivery,
    ) -> impl Future<Output = Result<Self::Opened<Layers>, BoxError>> {
        let composed = Stack::new(self.layers, layers.clone());
        let routes = self.routes.balance();
        async move { routes.open_layered(connected, &composed, scope).await }
    }
}

impl<State, Sub, Body, Cx, Layers> RunRoutes<State> for OpenedRoute<Sub, Body, Cx, Layers>
where
    Sub: Subscriber,
    Sub::Message: 'static,
    State: MaybeSendSync + 'static,
    Body: Handler<Sub::Message, Cx, State> + 'static,
    Cx: BuildContext<Sub::Message> + MaybeSendSync + 'static,
    Layers: BlanketLayer,
{
    type Parks = Cell<bool>;

    fn run_parked<Signal, Host>(
        self,
        parks: &Cell<bool>,
        signal: Signal,
        state: StateRef<'_, State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        // The join's gate asks a probe of its own, taken before the loop takes its.
        let probe = self.subscriber.ready_hint();
        let gate = signal.clone();
        Gated::new(parks, probe, gate, async move {
            let failure = DispatchFailure::new(self.policies, signal.clone());
            // Applied here, where the loop starts: the wrapped handler's type is the layer's own
            // and cannot be named in the opened list.
            let handler =
                BlanketLayer::apply::<Sub::Message, Cx, State, _>(&self.layers, self.handler);
            // The subscription's one place: the loop handles one delivery at a time.
            let room = PlaceRoom::<1>::new();
            run_sequential(
                Cooperative::new(self.subscriber),
                handler,
                signal,
                self.name,
                handle_of(state),
                (&self.delivery, host),
                failure,
                (),
                HeadersAtOnce,
                parks,
                PayloadBuf::new(),
                room.places(),
            )
            .await;
        })
    }
}

/// One opened batch subscription with no plan: the parts the plain batch loop runs.
///
/// A router's layers wrap single-message handlers only, as on the multi-thread runner: a batch
/// handler runs as the registration built it.
#[doc(hidden)]
#[derive(Debug)]
pub struct OpenedBatchRoute<Sub, Body, Cx> {
    route: OneTaskBatchRoute<Sub, Body, Cx>,
}

impl<Sub, Body, Cx> OpenedBatchRoute<Sub, Body, Cx> {
    /// An opened subscription, ready to run.
    pub(super) const fn new(route: OneTaskBatchRoute<Sub, Body, Cx>) -> Self {
        Self { route }
    }
}

impl<State, Sub, Body, Cx> RunRoutes<State> for OpenedBatchRoute<Sub, Body, Cx>
where
    Sub: BatchSubscriber,
    Body: RunsBatches<Sub::Batch, Cx, State>,
{
    type Parks = ();

    // The frame owns the subscription's one place: a refused message is read out of it as the
    // publish returns, so the elements of a batch take it in turn.
    async fn run_parked<Signal, Host>(
        self,
        _parks: &(),
        signal: Signal,
        state: StateRef<'_, State>,
        host: &Host,
    ) where
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        let room = PlaceRoom::<1>::new();
        self.route
            .run_batches(signal, state, host, PayloadBuf::new(), room.places())
            .await;
    }
}

/// A room of places a registration keeps for the messages its refused publishes hand back.
/// Machinery; never named directly.
#[doc(hidden)]
pub trait RefusalRoom: Default {
    /// The places, for a refusal to take one of.
    fn places(&self) -> Places<'_>;
}

impl<const N: usize> RefusalRoom for PlaceRoom<N> {
    fn places(&self) -> Places<'_> {
        Self::places(self)
    }
}

/// A batch handler the fold runs on a one-thread runner of any host: one batch handed to the
/// shared batch body. Machinery; never named directly.
#[doc(hidden)]
pub trait RunsBatches<Batch, C, S> {
    /// The decode buffer one subscription's loop keeps.
    type Scratch: Default;

    /// The batch as the body takes it, which one subscription's loop keeps.
    type Taken;

    /// Allocates the loop's [`Taken`](Self::Taken) for batches of `size`.
    fn taken(size: NonZeroUsize) -> Self::Taken;

    /// Empties `slot` after a failed fill, dropping whatever the broker left in it.
    fn discard_fill(slot: &mut Batch, taken: &mut Self::Taken);

    /// Runs one batch through the shared body.
    #[allow(clippy::too_many_arguments)]
    fn run_one<Signal, Host>(
        &self,
        slot: &mut Batch,
        taken: &mut Self::Taken,
        scratch: &mut Self::Scratch,
        encode: &mut EncodeBuf,
        places: Places<'_>,
        name: &str,
        state: &S,
        delivery: &Delivery<C>,
        host: &Host,
        failure: &DispatchFailure<Signal>,
    ) -> impl Future<Output = ()>
    where
        Signal: Halt,
        Host: SendHost<C>;
}

impl<Batch, C, S, Handler> RunsBatches<Batch, C, S> for Handler
where
    Handler: BatchHandler<Batch, C, S>,
{
    type Scratch = Handler::Scratch;
    type Taken = Handler::Taken;

    fn taken(size: NonZeroUsize) -> Self::Taken {
        Handler::taken(size)
    }

    fn discard_fill(slot: &mut Batch, taken: &mut Handler::Taken) {
        Handler::load(taken, slot);
        taken.discard();
    }

    fn run_one<Signal, Host>(
        &self,
        slot: &mut Batch,
        taken: &mut Handler::Taken,
        scratch: &mut Handler::Scratch,
        encode: &mut EncodeBuf,
        places: Places<'_>,
        name: &str,
        state: &S,
        delivery: &Delivery<C>,
        host: &Host,
        failure: &DispatchFailure<Signal>,
    ) -> impl Future<Output = ()>
    where
        Signal: Halt,
        Host: SendHost<C>,
    {
        run_batch::<Self, Batch, C, S, Signal, Host>(
            self, slot, taken, scratch, encode, places, name, state, delivery, host, failure,
        )
    }
}
