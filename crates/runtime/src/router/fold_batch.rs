//! A batch route of the one-task fold with a dispatch plan of its runner's own: where the copies
//! its elements leave behind wait, fixed in its type at the mount site.
//!
//! The batch twin of [`fold_plan`](super::fold_plan). A batch registration opens into the parts a
//! plan runs ([`OneTaskBatchRoute`]); the plan's [`OneTaskBatchDispatch::run`] runs them, and
//! [`SequentialBatchPlan`] is the plain loop. The step `Router::replan` wraps the registration in
//! [`Dispatched`](super::fold_plan::Dispatched) as it does a single-message one.

// Written over the contract traits for a runner that polls on one thread.
#![allow(clippy::future_not_send)]

use core::any::type_name;
use core::num::NonZeroUsize;
use core::pin::pin;
use core::task::Poll;
use ruststream_core::diag::error;
use ruststream_core::{BatchSlot, BatchStream, MaybeSendSync, PayloadBuf};

use core::future::poll_fn;

use crate::batch::{DeserializedBatch, TypedBatch};
use crate::coop::{Bounded, YIELD_AFTER};
use crate::dispatch::{Delivery, EncodeRoom, Turn};
use crate::failure::{DispatchFailure, FailurePolicies};
use crate::host::{Detach, Halt, SendHost, ShutdownWait};
use crate::input::InputKind;
use crate::lifecycle::BoxError;
use crate::middleware::BlanketLayer;
use crate::redelivery::{
    AppliesDeclaration, DeclaredOn, NoCopies, PairCopies, PrepareCopy, ScopeDelivery, Undeclared,
    open_local_subscription,
};
use crate::shared::{StateRef, SubscriptionName, borrowed};
use crate::{BatchSubscriber, Broker, Connected, RetryDeclaration, SubscriptionSource};

use super::fold::{OpenLayered, RunRoutes, RunsBatches};
use super::fold_plan::{Dispatched, UnboundRetry};
use super::routes::{BatchRoute, DeclaredRoute};
use super::routes_inject::BatchInjectRoute;
use super::routes_publish::BatchPublishingRoute;

/// How an opened batch route runs on a runner that hosts the app in one task.
///
/// A runner implements it for each plan it offers; [`SequentialBatchPlan`] is the plain loop.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a dispatch plan of this runner for a batch",
    note = "a batch mount runs on one worker: `pool` and `keyed` apply to single deliveries"
)]
pub trait OneTaskBatchDispatch {
    /// What the plan keeps of its retry copies: [`NoCopies`], or
    /// [`KeepCopies`](crate::KeepCopies) for a plan with a store of its own.
    type Copies;

    /// Runs the route until `signal` stops it; `path` is the copy path its registration paired
    /// for the plan's kept copies.
    fn run<Sub, Body, Cx, State, Signal, Host, Path>(
        self,
        route: OneTaskBatchRoute<Sub, Body, Cx>,
        path: Option<Path>,
        signal: Signal,
        state: StateRef<'_, State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Sub: BatchSubscriber,
        Body: RunsBatches<Sub::Batch, Cx, State>,
        Cx: MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
        Path: PrepareCopy<Cx> + MaybeSendSync;
}

/// One opened batch route as a plan receives it: the subscriber, the batch handler, the size its
/// batches open at, and what each batch runs under.
///
/// A router's layers wrap single-message handlers only, as on the multi-thread runner: a batch
/// handler runs as the registration built it.
#[doc(hidden)]
#[derive(Debug)]
#[non_exhaustive]
pub struct OneTaskBatchRoute<Sub, Body, Cx> {
    /// The opened subscription.
    pub subscriber: Sub,
    /// The batch handler.
    pub handler: Body,
    /// The subscription's name, for diagnostics.
    pub name: SubscriptionName,
    /// The size its batches open at.
    pub batch_size: NonZeroUsize,
    /// What each batch runs under.
    pub delivery: Delivery<Cx>,
    /// The registration's failure policies.
    pub policies: FailurePolicies,
    /// The name of the type the handler receives per element: what a diagnostic about the
    /// subscription's store of copies calls its messages. Read once when the registration opens.
    pub input: &'static str,
}

impl<Sub, Body, Cx> OneTaskBatchRoute<Sub, Body, Cx> {
    /// Runs the route one batch at a time, on the host the plan lends it, replies encoded into
    /// `room`.
    pub async fn run_batches<State, Signal, Lent, Room>(
        mut self,
        signal: Signal,
        state: StateRef<'_, State>,
        host: &Lent,
        mut room: Room,
    ) where
        Room: EncodeRoom,
        Sub: BatchSubscriber,
        Body: RunsBatches<Sub::Batch, Cx, State>,
        Signal: ShutdownWait + Halt + Clone,
        Lent: SendHost<Cx>,
    {
        let failure = DispatchFailure::new(self.policies, signal.clone());
        // A batch subscriber declares no probe: its worker is polled on every wake, so the bound
        // always yields.
        let mut stream = pin!(Bounded::new(
            self.subscriber.batches(self.batch_size),
            YIELD_AFTER,
            ()
        ));
        let mut cancelled = pin!(signal.cancelled());
        // One slot, one taken batch, one decode buffer and one encode buffer for the whole loop,
        // as on the multi-thread runner's sequential batch loop.
        let mut slot = <Sub::Batch as BatchSlot>::with_capacity(self.batch_size);
        let mut taken = <Body as RunsBatches<Sub::Batch, Cx, State>>::taken(self.batch_size);
        let mut scratch = Body::Scratch::default();
        let encode = room.buffer();
        loop {
            if signal.is_cancelled() {
                break;
            }
            // The stream first, the token only where the stream has nothing.
            let pulled = poll_fn(|cx| match stream.as_mut().poll_fill(cx, &mut slot) {
                Poll::Ready(Some(item)) => Poll::Ready(Turn::Delivery(item)),
                Poll::Ready(None) => Poll::Ready(Turn::Ended),
                Poll::Pending => cancelled.as_mut().poll(cx).map(|()| Turn::Shutdown),
            })
            .await;
            match pulled {
                Turn::Delivery(Ok(())) => {
                    self.handler
                        .run_one(
                            &mut slot,
                            &mut taken,
                            &mut scratch,
                            &mut *encode,
                            &self.name,
                            borrowed(state),
                            &self.delivery,
                            host,
                            &failure,
                        )
                        .await;
                }
                Turn::Delivery(Err(err)) => {
                    // A failed fill leaves the slot empty; taking what a broker left behind and
                    // dropping it keeps that true.
                    Body::discard_fill(&mut slot, &mut taken);
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

/// The plain plan of a batch: one batch at a time, the work it leaves to the runner's host.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct SequentialBatchPlan;

impl OneTaskBatchDispatch for SequentialBatchPlan {
    type Copies = NoCopies;

    // Not an `async fn`: a frame of its own would sit between the caller and the loop for
    // nothing.
    fn run<Sub, Body, Cx, State, Signal, Host, Path>(
        self,
        route: OneTaskBatchRoute<Sub, Body, Cx>,
        _path: Option<Path>,
        signal: Signal,
        state: StateRef<'_, State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Sub: BatchSubscriber,
        Body: RunsBatches<Sub::Batch, Cx, State>,
        Cx: MaybeSendSync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
        Path: PrepareCopy<Cx> + MaybeSendSync,
    {
        route.run_batches(signal, state, host, PayloadBuf::new())
    }
}

/// A batch registration the fold opens into the parts a plan runs: the subscription, its batch
/// handler and what each batch runs under. Every batch registration of a local router is one; its
/// plain opening and its planned one ([`Dispatched`]) both go through it.
#[doc(hidden)]
pub trait OpenBatchRoute<B: Broker, State> {
    /// The descriptor the registration subscribes through.
    type Source;
    /// What the registration declared about its retries: [`Undeclared`], or [`DeclaredOn`] the
    /// descriptor's copy path where the mount site declared a cap or a dead-letter destination.
    type Declaration;
    /// The opened subscription.
    type Sub: BatchSubscriber;
    /// The batch handler.
    type Body: RunsBatches<<Self::Sub as BatchSubscriber>::Batch, Self::Cx, State>;
    /// The broker's typed per-batch context the handler reads.
    type Cx: MaybeSendSync + 'static;

    /// The descriptor, read before the registration opens.
    fn source(&self) -> &Self::Source;

    /// Subscribes against `connected` and builds the handler; `declaration` is what the mount
    /// site declared about the registration's retries, taken into the descriptor first.
    ///
    /// # Errors
    ///
    /// The broker's refusal of the declaration or of the subscription.
    #[allow(clippy::type_complexity)] // the route's own parts, named once here
    fn open_route(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
        declaration: RetryDeclaration,
    ) -> impl Future<Output = Result<OneTaskBatchRoute<Self::Sub, Self::Body, Self::Cx>, BoxError>>;
}

/// Opens a batch registration's subscription and gathers the parts a plan runs.
async fn open_batch<B, Source, Handler, Cx, Size>(
    route: BatchRoute<Source, Handler, Cx, Size>,
    connected: &Connected<B>,
    scope: &ScopeDelivery,
    declaration: RetryDeclaration,
    input: &'static str,
) -> Result<OneTaskBatchRoute<Source::Subscriber, Handler, Cx>, BoxError>
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
{
    let (name, subscriber, delivery) =
        open_local_subscription::<B, _, _>(route.source, connected, scope, declaration).await?;
    Ok(OneTaskBatchRoute {
        subscriber,
        handler: route.handler,
        name,
        batch_size: route.batch_size,
        delivery,
        policies: route.policies,
        input,
    })
}

impl<B, State, Source, M, F, Inner, Cx, Size> OpenBatchRoute<B, State>
    for BatchRoute<Source, DeserializedBatch<M, F, Inner>, Cx, Size>
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Source::Subscriber: BatchSubscriber<Message = M>,
    DeserializedBatch<M, F, Inner>:
        RunsBatches<<Source::Subscriber as BatchSubscriber>::Batch, Cx, State>,
    Cx: MaybeSendSync + 'static,
{
    type Source = Source;
    type Declaration = Undeclared;
    type Sub = Source::Subscriber;
    type Body = DeserializedBatch<M, F, Inner>;
    type Cx = Cx;

    fn source(&self) -> &Source {
        &self.source
    }

    fn open_route(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
        declaration: RetryDeclaration,
    ) -> impl Future<Output = Result<OneTaskBatchRoute<Self::Sub, Self::Body, Cx>, BoxError>> {
        open_batch::<B, _, _, _, _>(self, connected, scope, declaration, type_name::<F>())
    }
}

impl<B, State, Source, M, Input, DecodeCodec, Inner, Cx, Size> OpenBatchRoute<B, State>
    for BatchRoute<Source, TypedBatch<M, Input, DecodeCodec, Inner>, Cx, Size>
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Source::Subscriber: BatchSubscriber<Message = M>,
    TypedBatch<M, Input, DecodeCodec, Inner>:
        RunsBatches<<Source::Subscriber as BatchSubscriber>::Batch, Cx, State>,
    Input: InputKind,
    Cx: MaybeSendSync + 'static,
{
    type Source = Source;
    type Declaration = Undeclared;
    type Sub = Source::Subscriber;
    type Body = TypedBatch<M, Input, DecodeCodec, Inner>;
    type Cx = Cx;

    fn source(&self) -> &Source {
        &self.source
    }

    fn open_route(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
        declaration: RetryDeclaration,
    ) -> impl Future<Output = Result<OneTaskBatchRoute<Self::Sub, Self::Body, Cx>, BoxError>> {
        open_batch::<B, _, _, _, _>(self, connected, scope, declaration, Input::input_label())
    }
}

// A declared registration opens as the one it wraps, with the declaration its mount chain made.
impl<B, State, Route, Dead> OpenBatchRoute<B, State> for DeclaredRoute<Route, Dead>
where
    B: Broker,
    Route: OpenBatchRoute<B, State, Declaration = Undeclared>,
    Route::Source: SubscriptionSource<Connected<B>>,
{
    type Source = Route::Source;
    type Declaration = DeclaredOn<<Route::Source as SubscriptionSource<Connected<B>>>::Copies>;
    type Sub = Route::Sub;
    type Body = Route::Body;
    type Cx = Route::Cx;

    fn source(&self) -> &Self::Source {
        self.route.source()
    }

    fn open_route(
        self,
        connected: &Connected<B>,
        scope: &ScopeDelivery,
        _declaration: RetryDeclaration,
    ) -> impl Future<Output = Result<OneTaskBatchRoute<Self::Sub, Self::Body, Self::Cx>, BoxError>>
    {
        self.route.open_route(connected, scope, self.declaration)
    }
}

/// One opened batch subscription under a plan: the parts the plan runs, and the copy path its
/// registration paired.
#[doc(hidden)]
#[derive(Debug)]
pub struct OpenedBatchDispatch<Sub, Body, Cx, Plan, Path> {
    pub(super) route: OneTaskBatchRoute<Sub, Body, Cx>,
    pub(super) plan: Plan,
    pub(super) path: Option<Path>,
}

impl<State, Sub, Body, Cx, Plan, Path> RunRoutes<State>
    for OpenedBatchDispatch<Sub, Body, Cx, Plan, Path>
where
    Sub: BatchSubscriber,
    Body: RunsBatches<Sub::Batch, Cx, State>,
    Cx: MaybeSendSync + 'static,
    Plan: OneTaskBatchDispatch,
    Path: PrepareCopy<Cx> + MaybeSendSync,
{
    type Parks = ();

    fn run_parked<Signal, Host>(
        self,
        _parks: &(),
        signal: Signal,
        state: StateRef<'_, State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        let Self { route, plan, path } = self;
        plan.run(route, path, signal, state, host)
    }
}

/// Implements the opening of every batch registration that has no plan: the router's layers do
/// not apply to a batch handler, and its copies leave through the descriptor.
macro_rules! open_batch_plain {
    ($([$($param:ident),+] $route:ty),+ $(,)?) => {$(
        impl<B, State, $($param),+> OpenLayered<B, State> for $route
        where
            B: Broker,
            State: MaybeSendSync + 'static,
            $route: OpenBatchRoute<B, State>,
            NoCopies: AppliesDeclaration<<$route as OpenBatchRoute<B, State>>::Declaration>,
        {
            type Opened<Layers: BlanketLayer + Clone> = super::fold::OpenedBatchRoute<
                <$route as OpenBatchRoute<B, State>>::Sub,
                <$route as OpenBatchRoute<B, State>>::Body,
                <$route as OpenBatchRoute<B, State>>::Cx,
            >;

            async fn open_layered<Layers: BlanketLayer + Clone>(
                self,
                connected: &Connected<B>,
                _layers: &Layers,
                scope: &ScopeDelivery,
            ) -> Result<Self::Opened<Layers>, BoxError> {
                let route = self
                    .open_route(connected, scope, RetryDeclaration::new())
                    .await?;
                Ok(super::fold::OpenedBatchRoute::new(route))
            }
        }
    )+};
}

open_batch_plain!(
    [Source, Handler, Cx, Size] BatchRoute<Source, Handler, Cx, Size>,
    [Source, Handler, Cx, Size, Dead] DeclaredRoute<BatchRoute<Source, Handler, Cx, Size>, Dead>,
    [Source, Def, DecodeCodec, Wiring, Extra, Size]
        BatchPublishingRoute<Source, Def, DecodeCodec, Wiring, Extra, Size>,
    [Source, Def, DecodeCodec, Wiring, Extra, Size, Dead]
        DeclaredRoute<BatchPublishingRoute<Source, Def, DecodeCodec, Wiring, Extra, Size>, Dead>,
    [Source, Def, DecodeCodec, Extra, Size]
        BatchInjectRoute<Source, Def, DecodeCodec, Extra, Size>,
    [Source, Def, DecodeCodec, Extra, Size, Dead]
        DeclaredRoute<BatchInjectRoute<Source, Def, DecodeCodec, Extra, Size>, Dead>,
);

// One impl per route type, as for single registrations.
macro_rules! open_batch_dispatched {
    ($([$($param:ident),+] $route:ty),+ $(,)?) => {$(
        impl<B, State, Plan, $($param),+> OpenLayered<B, State> for Dispatched<$route, Plan>
        where
            B: Broker,
            State: MaybeSendSync + 'static,
            $route: OpenBatchRoute<B, State> + UnboundRetry,
            Plan: OneTaskBatchDispatch<
                Copies: PairCopies<B, <$route as OpenBatchRoute<B, State>>::Source>
                    + AppliesDeclaration<<$route as OpenBatchRoute<B, State>>::Declaration>,
            >,
            <Plan::Copies as PairCopies<B, <$route as OpenBatchRoute<B, State>>::Source>>::Path:
                PrepareCopy<<$route as OpenBatchRoute<B, State>>::Cx> + MaybeSendSync,
        {
            type Opened<Layers: BlanketLayer + Clone> = OpenedBatchDispatch<
                <$route as OpenBatchRoute<B, State>>::Sub,
                <$route as OpenBatchRoute<B, State>>::Body,
                <$route as OpenBatchRoute<B, State>>::Cx,
                Plan,
                <Plan::Copies as PairCopies<B, <$route as OpenBatchRoute<B, State>>::Source>>::Path,
            >;

            async fn open_layered<Layers: BlanketLayer + Clone>(
                self,
                connected: &Connected<B>,
                _layers: &Layers,
                scope: &ScopeDelivery,
            ) -> Result<Self::Opened<Layers>, BoxError> {
                let Self { route, plan } = self;
                // Paired before the subscription opens, so a copy path the broker refuses fails
                // the startup before any delivery arrives.
                let path = <Plan::Copies as PairCopies<
                    B,
                    <$route as OpenBatchRoute<B, State>>::Source,
                >>::pair(route.source(), connected)
                .await?;
                Ok(OpenedBatchDispatch {
                    route: route
                        .open_route(connected, scope, RetryDeclaration::new())
                        .await?,
                    plan,
                    path,
                })
            }
        }
    )+};
}

open_batch_dispatched!(
    [Source, Handler, Cx, Size] BatchRoute<Source, Handler, Cx, Size>,
    [Source, Handler, Cx, Size, Dead] DeclaredRoute<BatchRoute<Source, Handler, Cx, Size>, Dead>,
    [Source, Def, DecodeCodec, Wiring, Extra, Size]
        BatchPublishingRoute<Source, Def, DecodeCodec, Wiring, Extra, Size>,
    [Source, Def, DecodeCodec, Wiring, Extra, Size, Dead]
        DeclaredRoute<BatchPublishingRoute<Source, Def, DecodeCodec, Wiring, Extra, Size>, Dead>,
    [Source, Def, DecodeCodec, Extra, Size]
        BatchInjectRoute<Source, Def, DecodeCodec, Extra, Size>,
    [Source, Def, DecodeCodec, Extra, Size, Dead]
        DeclaredRoute<BatchInjectRoute<Source, Def, DecodeCodec, Extra, Size>, Dead>,
);
