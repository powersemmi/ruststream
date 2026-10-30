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

// Written over the local twins of the contract for a runner that polls on one thread: the
// futures are `Send` exactly where the types they run are, and nothing here spawns them.
#![allow(clippy::future_not_send)]

use alloc::sync::Arc;
use core::future::{poll_fn, ready};
use core::num::NonZeroUsize;
use core::pin::{Pin, pin};
use core::task::{Context, Poll};

use bytes::BytesMut;
use futures::Stream;
use futures::future::MaybeDone;
use pin_project_lite::pin_project;
use tracing::error;

use crate::RetryDeclaration;
use crate::batch::{DeserializedBatch, LocalBatch, LocalSliceHandler};
use crate::dispatch::{Delivery, HeadersAtOnce, Turn, run_batch, run_sequential};
use crate::failure::{DispatchFailure, FailurePolicies};
use crate::handle::Deserialized;
use crate::handler::LocalHandler;
use crate::host::{Halt, HostDetach, LocalDetach, OnOneThread, ShutdownWait};
use crate::lifecycle::BoxError;
use crate::middleware::{LocalBlanketLayer, Stack};
use crate::redelivery::{ScopeDelivery, open_local_subscription};
use crate::{
    BuildBatchContext, BuildContext, LocalBatchSubscriber, LocalBroker, LocalConnected,
    LocalIncomingMessage, LocalSubscriber, LocalSubscriptionSource,
};

use super::builder::Router;
use super::routes::BatchRoute;

/// A finished router's routes, opened against the connected broker into one list.
///
/// Implemented by [`Router`] and by every route the fold opens; a runner calls it once at
/// startup and runs what it hands back with [`RunRoutes::run`].
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a route this runner opens",
    note = "a runner that hosts the app in one task opens plain single-message registrations \
            (`include(def)` of a definition without a reply, an `Out` slot or a batch)"
)]
pub trait OpenRoutes<B: LocalBroker, State = ()> {
    /// The opened list: every subscription with its handler, ready to run.
    type Opened: RunRoutes<State>;

    /// Subscribes every route against `connected`.
    ///
    /// # Errors
    ///
    /// The first subscription the broker refuses.
    fn open(
        self,
        connected: &LocalConnected<B>,
        scope: &ScopeDelivery,
    ) -> impl Future<Output = Result<Self::Opened, BoxError>>;
}

/// An opened route list: runs every subscription it holds in one future until `signal` stops
/// them.
#[doc(hidden)]
pub trait RunRoutes<State> {
    /// Runs each subscription's loop, joined; the future ends when every loop has ended.
    fn run<Signal, Host>(
        self,
        signal: Signal,
        state: &Arc<State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: LocalDetach;
}

/// One registration of a router's list, opened under the layer stack that wraps its handler:
/// the router's own, inside every router it was merged into.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a route this runner opens",
    note = "a runner that hosts the app in one task opens plain single-message registrations \
            (`include(def)` of a definition without a reply, an `Out` slot or a batch)"
)]
pub trait OpenLayered<B: LocalBroker, State> {
    /// The opened registrations, each holding the layer stack it runs under.
    type Opened<Layers: LocalBlanketLayer + Clone>: RunRoutes<State>;

    /// Subscribes every registration against `connected`, keeping `layers` beside each handler.
    ///
    /// # Errors
    ///
    /// See [`OpenRoutes::open`].
    fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        connected: &LocalConnected<B>,
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
    name: Arc<str>,
    delivery: Delivery<Cx>,
    policies: FailurePolicies,
}

impl<Sub, Body, Cx, Layers> OpenedRoute<Sub, Body, Cx, Layers> {
    /// An opened subscription, ready to run.
    pub(super) const fn new(
        subscriber: Sub,
        handler: Body,
        layers: Layers,
        name: Arc<str>,
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

impl<B: LocalBroker, State> OpenLayered<B, State> for () {
    type Opened<Layers: LocalBlanketLayer + Clone> = ();

    fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        _connected: &LocalConnected<B>,
        _layers: &Layers,
        _scope: &ScopeDelivery,
    ) -> impl Future<Output = Result<(), BoxError>> {
        ready(Ok(()))
    }
}

impl<State> RunRoutes<State> for () {
    fn run<Signal, Host>(
        self,
        _signal: Signal,
        _state: &Arc<State>,
        _host: &Host,
    ) -> impl Future<Output = ()>
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: LocalDetach,
    {
        ready(())
    }
}

impl<B, State, Head, Tail> OpenLayered<B, State> for (Head, Tail)
where
    B: LocalBroker,
    Head: OpenLayered<B, State>,
    Tail: OpenLayered<B, State>,
{
    type Opened<Layers: LocalBlanketLayer + Clone> = (Head::Opened<Layers>, Tail::Opened<Layers>);

    async fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        connected: &LocalConnected<B>,
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
    fn run<Signal, Host>(
        self,
        signal: Signal,
        state: &Arc<State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: LocalDetach,
    {
        // A named future, not an `async` block: each pair of the tree is one type level of the
        // app's future, so a device's many routes stay inside the compiler's layout depth.
        Both {
            left: MaybeDone::Future(self.0.run(signal.clone(), state, host)),
            right: MaybeDone::Future(self.1.run(signal, state, host)),
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
    B: LocalBroker,
    Routes: Balance<Tree: OpenLayered<B, State>>,
    RouteLayers: LocalBlanketLayer + Clone,
{
    type Opened = <Routes::Tree as OpenLayered<B, State>>::Opened<RouteLayers>;

    fn open(
        self,
        connected: &LocalConnected<B>,
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
    B: LocalBroker,
    Routes: Balance<Tree: OpenLayered<B, State>>,
    RouteLayers: LocalBlanketLayer + Clone,
{
    type Opened<Layers: LocalBlanketLayer + Clone> =
        <Routes::Tree as OpenLayered<B, State>>::Opened<Stack<RouteLayers, Layers>>;

    fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        connected: &LocalConnected<B>,
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
    Sub: LocalSubscriber,
    Body: LocalHandler<Sub::Message, Cx, State>,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
    Layers: LocalBlanketLayer,
{
    async fn run<Signal, Host>(self, signal: Signal, state: &Arc<State>, host: &Host)
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: LocalDetach,
    {
        let failure = DispatchFailure::new(self.policies, signal.clone());
        let host = OnOneThread(host);
        // Applied here, where the loop starts: the wrapped handler's type is the layer's own
        // and cannot be named in the opened list.
        let handler =
            LocalBlanketLayer::apply::<Sub::Message, Cx, State, _>(&self.layers, self.handler);
        run_sequential(
            self.subscriber,
            handler,
            signal,
            self.name,
            Arc::clone(state),
            (&self.delivery, &host),
            failure,
            (),
            HeadersAtOnce,
        )
        .await;
    }
}

/// One opened batch subscription: the subscriber, the batch handler it dispatches to, the size
/// its batches open at, and what each batch runs under.
///
/// A router's layers wrap single-message handlers only, as on the multi-thread runner: a batch
/// handler runs as the registration built it.
#[doc(hidden)]
#[derive(Debug)]
pub struct OpenedBatchRoute<Sub, Body, Cx> {
    subscriber: Sub,
    handler: Body,
    name: Arc<str>,
    batch_size: NonZeroUsize,
    delivery: Delivery<Cx>,
    policies: FailurePolicies,
}

impl<B, State, Source, M, F, Inner, Cx> OpenLayered<B, State>
    for BatchRoute<Source, DeserializedBatch<M, F, Inner>, Cx>
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
    Source::Subscriber: LocalBatchSubscriber<Message = M>,
    LocalBatch<DeserializedBatch<M, F, Inner>>:
        RunsBatches<<Source::Subscriber as LocalBatchSubscriber>::Batch, Cx, State>,
{
    type Opened<Layers: LocalBlanketLayer + Clone> =
        OpenedBatchRoute<Source::Subscriber, LocalBatch<DeserializedBatch<M, F, Inner>>, Cx>;

    async fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        connected: &LocalConnected<B>,
        _layers: &Layers,
        scope: &ScopeDelivery,
    ) -> Result<Self::Opened<Layers>, BoxError> {
        let (name, subscriber, delivery) = open_local_subscription::<B, _, _>(
            self.source,
            connected,
            scope,
            RetryDeclaration::new(),
        )
        .await?;
        Ok(OpenedBatchRoute {
            subscriber,
            handler: LocalBatch(self.handler),
            name,
            batch_size: self.batch_size,
            delivery,
            policies: self.policies,
        })
    }
}

impl<State, Sub, Body, Cx> RunRoutes<State> for OpenedBatchRoute<Sub, Body, Cx>
where
    Sub: LocalBatchSubscriber,
    Body: RunsBatches<Sub::Batch, Cx, State>,
{
    async fn run<Signal, Host>(mut self, signal: Signal, state: &Arc<State>, host: &Host)
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: LocalDetach,
    {
        let host = OnOneThread(host);
        let failure = DispatchFailure::new(self.policies, signal.clone());
        let mut stream = pin!(self.subscriber.batches(self.batch_size));
        let mut cancelled = pin!(signal.cancelled());
        // One decode buffer and one encode buffer for the whole loop, as on the multi-thread
        // runner's sequential batch loop.
        let mut scratch = Body::Scratch::default();
        let mut encode = BytesMut::new();
        loop {
            if signal.is_cancelled() {
                break;
            }
            // The stream first, the token only where the stream has nothing.
            let pulled = poll_fn(|cx| match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
                Poll::Ready(None) => Poll::Ready(Turn::Ended),
                Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
            })
            .await;
            match pulled {
                Turn::Delivery(Ok(batch)) => {
                    self.handler
                        .run_one(
                            batch,
                            &mut scratch,
                            &mut encode,
                            &self.name,
                            &**state,
                            &self.delivery,
                            &host,
                            &failure,
                        )
                        .await;
                }
                Turn::Delivery(Err(err)) => {
                    error!(
                        target: "ruststream::dispatch",
                        subscription = %self.name,
                        error = %err,
                        "subscriber stream error",
                    );
                }
                Turn::Ended => {
                    signal.stream_ended(&self.name);
                    break;
                }
                Turn::Shutdown => break,
            }
        }
    }
}

/// A batch handler the fold runs on a one-thread runner of any host: one batch handed to the
/// shared batch body. Machinery; never named directly.
#[doc(hidden)]
pub trait RunsBatches<Batch, C, S> {
    /// The decode buffer one subscription's loop keeps.
    type Scratch: Default;

    /// Runs one batch through the shared body.
    #[allow(clippy::too_many_arguments)]
    fn run_one<Signal, Host>(
        &self,
        batch: Batch,
        scratch: &mut Self::Scratch,
        encode: &mut BytesMut,
        name: &str,
        state: &S,
        delivery: &Delivery<C>,
        host: &Host,
        failure: &DispatchFailure<Signal>,
    ) -> impl Future<Output = ()>
    where
        Signal: Halt,
        Host: HostDetach;
}

impl<Batch, M, F, Inner, C, S> RunsBatches<Batch, C, S>
    for LocalBatch<DeserializedBatch<M, F, Inner>>
where
    Batch: IntoIterator<Item = M>,
    M: LocalIncomingMessage,
    F: Deserialized,
    Inner: for<'p> LocalSliceHandler<F::Output<'p>, C, S>,
    C: BuildBatchContext<M> + Send + Sync + 'static,
{
    type Scratch = ();

    fn run_one<Signal, Host>(
        &self,
        batch: Batch,
        scratch: &mut (),
        encode: &mut BytesMut,
        name: &str,
        state: &S,
        delivery: &Delivery<C>,
        host: &Host,
        failure: &DispatchFailure<Signal>,
    ) -> impl Future<Output = ()>
    where
        Signal: Halt,
        Host: HostDetach,
    {
        run_batch::<Self, Batch, C, S, Signal, Host>(
            self, batch, scratch, encode, name, state, delivery, host, failure,
        )
    }
}
