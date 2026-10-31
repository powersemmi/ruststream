//! A route of the one-task fold with a dispatch plan of its runner's own: how many deliveries of
//! it run at once, and where the work it leaves behind waits, fixed in its type at the mount
//! site.
//!
//! A runner that hosts the app in one task names its plans as types (a constant worker count, a
//! constant capacity for delayed retries), so a count the runner cannot honour does not compile.
//! The step `Router::replan` wraps the registration just added in [`Dispatched`]; the
//! fold opens it like a plain registration and hands the opened parts to the plan's
//! [`LocalDispatch::run`].

// Written over the local twins of the contract for a runner that polls on one thread.
#![allow(clippy::future_not_send)]

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::marker::PhantomData;

use crate::dispatch::{Delivery, HeadersAtOnce, run_sequential};
use crate::failure::{DispatchFailure, FailurePolicies};
use crate::handler::LocalHandler;
use crate::host::{Halt, HostDetach, LocalDetach, OnOneThread, ShutdownWait};
use crate::input::Materialize;
use crate::lifecycle::BoxError;
use crate::middleware::LocalBlanketLayer;
use crate::redelivery::{CopyPublisher, NoCopies, PairCopies};
use crate::redelivery::{ScopeDelivery, open_local_route};
use crate::typed::{LocalTyped, Typed};
use crate::{
    BuildContext, LocalBroker, LocalConnected, LocalIncomingMessage, LocalSubscriber,
    LocalSubscriptionSource,
};

use super::builder::Router;
use super::fold::{OpenLayered, OpenedRoute, RunRoutes};
use super::local::LocalPipeline;
use super::routes::{RetryDestinationsDeclared, SubscribeRoute};
use super::routes_inject::InjectRoute;
use super::routes_publish::{PublishingRoute, RawReplyRoute};

/// How an opened single-message route runs on a runner that hosts the app in one task.
///
/// A runner implements it for each plan it offers; [`SequentialPlan`] is the plain loop.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a dispatch plan of this runner",
    note = "a plan comes from the runner's own mount steps"
)]
pub trait LocalDispatch {
    /// What the plan keeps of its retry copies: [`NoCopies`], or
    /// [`KeepCopies`](crate::KeepCopies) for a plan with a store of its own, whose
    /// registration pairs the copy path when it opens.
    type Copies;

    /// Runs the route until `signal` stops it; `path` is the copy path its registration paired
    /// for the plan's kept copies.
    fn run<Sub, Body, Cx, State, Signal, Host, Path>(
        self,
        route: LocalRoute<Sub, Body, Cx>,
        path: Option<Path>,
        signal: Signal,
        state: &Arc<State>,
        host: &Host,
    ) -> impl Future<Output = ()>
    where
        Sub: LocalSubscriber,
        Body: LocalHandler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: LocalDetach,
        Path: CopyPublisher;
}

/// One opened route as a plan receives it: the subscriber, the handler with its layers applied,
/// and what each delivery runs under.
#[doc(hidden)]
#[derive(Debug)]
#[non_exhaustive]
pub struct LocalRoute<Sub, Body, Cx> {
    /// The opened subscription.
    pub subscriber: Sub,
    /// The handler, wrapped in the router's layers.
    pub handler: Body,
    /// The subscription's name, for diagnostics.
    pub name: Arc<str>,
    /// What each delivery runs under.
    pub delivery: Delivery<Cx>,
    /// The registration's failure policies.
    pub policies: FailurePolicies,
}

impl<Sub, Body, Cx> LocalRoute<Sub, Body, Cx> {
    /// Runs the route one delivery at a time, on the host the plan lends it: the plan of a route
    /// that declared no workers.
    pub async fn run_sequential<State, Signal, Lent>(
        self,
        signal: Signal,
        state: &Arc<State>,
        host: &Lent,
    ) where
        Sub: LocalSubscriber,
        Body: LocalHandler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Lent: HostDetach,
    {
        let failure = DispatchFailure::new(self.policies, signal.clone());
        run_sequential(
            self.subscriber,
            self.handler,
            signal,
            self.name,
            Arc::clone(state),
            (&self.delivery, host),
            failure,
            (),
            HeadersAtOnce,
        )
        .await;
    }
}

/// The plain plan: one delivery at a time, the work it leaves to the runner's host.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct SequentialPlan;

impl LocalDispatch for SequentialPlan {
    type Copies = NoCopies;

    async fn run<Sub, Body, Cx, State, Signal, Host, Path>(
        self,
        route: LocalRoute<Sub, Body, Cx>,
        _path: Option<Path>,
        signal: Signal,
        state: &Arc<State>,
        host: &Host,
    ) where
        Sub: LocalSubscriber,
        Body: LocalHandler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: LocalDetach,
        Path: CopyPublisher,
    {
        route
            .run_sequential(signal, state, &OnOneThread(host))
            .await;
    }
}

/// A registration with a dispatch plan of its runner's own.
#[doc(hidden)]
#[derive(Debug)]
pub struct Dispatched<Route, Plan> {
    route: Route,
    plan: Plan,
}

/// A [`Dispatched`] route, opened: the parts its plan runs.
#[doc(hidden)]
#[derive(Debug)]
pub struct OpenedDispatch<Sub, Body, Cx, Layers, Plan, Path> {
    route: LocalRoute<Sub, Body, Cx>,
    layers: Layers,
    plan: Plan,
    path: Option<Path>,
}

/// A registration a runner's mount steps give a plan: a plain one, with no plan yet (`()`), or
/// one a step already planned.
#[doc(hidden)]
pub trait LocalPlanned: Sized {
    /// The registration without its plan.
    type Route;
    /// The plan it has: `()` for none.
    type Plan;

    /// Splits the registration from its plan.
    fn into_planned(self) -> (Self::Route, Self::Plan);
}

/// Implements [`LocalPlanned`] for the registrations a mount site adds: none has a plan yet.
macro_rules! impl_planless {
    ($($route:ident<$($param:ident),+>),+ $(,)?) => {$(
        impl<$($param),+> LocalPlanned for $route<$($param),+> {
            type Route = Self;
            type Plan = ();

            fn into_planned(self) -> (Self, ()) {
                (self, ())
            }
        }
    )+};
}

impl_planless!(
    SubscribeRoute<S, H, Cx>,
    PublishingRoute<Source, Def, DecodeCodec, Wiring, Extra>,
    RawReplyRoute<Source, Def, DecodeCodec, Wiring, Extra>,
    InjectRoute<Source, Def, DecodeCodec, Extra>,
);

// A plan changes how deliveries run, never where their retry copies go.
impl<Route: RetryDestinationsDeclared, Plan> RetryDestinationsDeclared for Dispatched<Route, Plan> {}

impl<Route, Plan> LocalPlanned for Dispatched<Route, Plan> {
    type Route = Route;
    type Plan = Plan;

    fn into_planned(self) -> (Route, Plan) {
        (self.route, self.plan)
    }
}

impl<B, Head, Routes, RouteCodec, RouteLayers, Pipe>
    Router<B, (Head, Routes), RouteCodec, RouteLayers, LocalPipeline<Pipe>>
where
    Head: LocalPlanned,
{
    /// Gives the registration just added (the preceding `include` call) the plan `change`
    /// makes of the one it has. A runner's own mount steps call it; the definition's `workers`
    /// value does not apply to a planned registration.
    #[doc(hidden)]
    #[allow(clippy::type_complexity)] // the router's own type, carried
    pub fn replan<Next>(
        self,
        change: impl FnOnce(Head::Plan) -> Next,
    ) -> Router<
        B,
        (Dispatched<Head::Route, Next>, Routes),
        RouteCodec,
        RouteLayers,
        LocalPipeline<Pipe>,
    > {
        let (route, plan) = self.routes.0.into_planned();
        Router {
            routes: (
                Dispatched {
                    route,
                    plan: change(plan),
                },
                self.routes.1,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }
}

/// A registration the fold opens into the parts a plan runs: the subscription, its handler and
/// what each delivery runs under. Every single-message registration of a local router is one;
/// its plain opening and its planned one ([`Dispatched`]) both go through it.
#[doc(hidden)]
pub trait OpenLocalRoute<B: LocalBroker, State> {
    /// The descriptor the registration subscribes through.
    type Source;
    /// The opened subscription.
    type Sub: LocalSubscriber;
    /// The handler each delivery runs.
    type Body: LocalHandler<<Self::Sub as LocalSubscriber>::Message, Self::Cx, State>;
    /// The broker's typed per-delivery context the handler reads.
    type Cx: BuildContext<<Self::Sub as LocalSubscriber>::Message> + Send + Sync + 'static;

    /// The descriptor, read before the registration opens.
    fn source(&self) -> &Self::Source;

    /// Subscribes against `connected` and builds the handler.
    ///
    /// # Errors
    ///
    /// The broker's refusal to subscribe, or to pair a publisher the handler holds.
    #[allow(clippy::type_complexity)] // the route's own parts, named once here
    fn open_route(
        self,
        connected: &LocalConnected<B>,
        scope: &ScopeDelivery,
    ) -> impl Future<Output = Result<LocalRoute<Self::Sub, Self::Body, Self::Cx>, BoxError>>;
}

/// Implements the plain opening of every [`OpenLocalRoute`] registration: no plan, the router's
/// layer stack beside the handler.
macro_rules! open_plain {
    ($([$($param:ident),+] $route:ty),+ $(,)?) => {$(
        impl<B, State, $($param),+> OpenLayered<B, State> for $route
        where
            B: LocalBroker,
            $route: OpenLocalRoute<B, State>,
        {
            type Opened<Layers: LocalBlanketLayer + Clone> = OpenedRoute<
                <$route as OpenLocalRoute<B, State>>::Sub,
                <$route as OpenLocalRoute<B, State>>::Body,
                <$route as OpenLocalRoute<B, State>>::Cx,
                Layers,
            >;

            async fn open_layered<Layers: LocalBlanketLayer + Clone>(
                self,
                connected: &LocalConnected<B>,
                layers: &Layers,
                scope: &ScopeDelivery,
            ) -> Result<Self::Opened<Layers>, BoxError> {
                let route = self.open_route(connected, scope).await?;
                Ok(OpenedRoute::new(
                    route.subscriber,
                    route.handler,
                    layers.clone(),
                    route.name,
                    route.delivery,
                    route.policies,
                ))
            }
        }
    )+};
}

open_plain!(
    [S, H, Cx] SubscribeRoute<S, H, Cx>,
    [Source, Def, DecodeCodec, Wiring, Extra] PublishingRoute<Source, Def, DecodeCodec, Wiring, Extra>,
    [Source, Def, DecodeCodec, Wiring, Extra] RawReplyRoute<Source, Def, DecodeCodec, Wiring, Extra>,
    [Source, Def, DecodeCodec, Extra] InjectRoute<Source, Def, DecodeCodec, Extra>,
);

impl<B, State, Source, M, Input, DecodeCodec, Inner, Cx> OpenLocalRoute<B, State>
    for SubscribeRoute<Source, Typed<M, Input, DecodeCodec, Inner>, Cx>
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
    Source::Subscriber: LocalSubscriber<Message = M>,
    M: LocalIncomingMessage,
    Input: Materialize<DecodeCodec, M>,
    Inner: LocalHandler<Input::Target, Cx, State>,
    Cx: BuildContext<M> + Send + Sync + 'static,
{
    type Source = Source;
    type Sub = Source::Subscriber;
    type Body = LocalTyped<Typed<M, Input, DecodeCodec, Inner>>;
    type Cx = Cx;

    fn source(&self) -> &Source {
        &self.source
    }

    async fn open_route(
        self,
        connected: &LocalConnected<B>,
        scope: &ScopeDelivery,
    ) -> Result<LocalRoute<Self::Sub, Self::Body, Cx>, BoxError> {
        let name: Arc<str> = Arc::from(self.source.name());
        let delivery = open_local_route(scope, &name);
        let subscriber = self
            .source
            .subscribe(connected)
            .await
            .map_err(|err| -> BoxError { Box::new(err) })?;
        Ok(LocalRoute {
            subscriber,
            handler: LocalTyped(self.handler),
            name,
            delivery,
            policies: self.policies,
        })
    }
}

impl<B, State, Route, Plan> OpenLayered<B, State> for Dispatched<Route, Plan>
where
    B: LocalBroker,
    Route: OpenLocalRoute<B, State>,
    Plan: LocalDispatch<Copies: PairCopies<B, Route::Source>>,
{
    type Opened<Layers: LocalBlanketLayer + Clone> = OpenedDispatch<
        Route::Sub,
        Route::Body,
        Route::Cx,
        Layers,
        Plan,
        <Plan::Copies as PairCopies<B, Route::Source>>::Path,
    >;

    async fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        connected: &LocalConnected<B>,
        layers: &Layers,
        scope: &ScopeDelivery,
    ) -> Result<Self::Opened<Layers>, BoxError> {
        let Self { route, plan } = self;
        // Paired before the subscription opens, so a copy path the broker refuses fails the
        // startup before any delivery arrives.
        let path =
            <Plan::Copies as PairCopies<B, Route::Source>>::pair(route.source(), connected).await?;
        Ok(OpenedDispatch {
            route: route.open_route(connected, scope).await?,
            layers: layers.clone(),
            plan,
            path,
        })
    }
}

impl<State, Sub, Body, Cx, Layers, Plan, Path> RunRoutes<State>
    for OpenedDispatch<Sub, Body, Cx, Layers, Plan, Path>
where
    Sub: LocalSubscriber,
    Body: LocalHandler<Sub::Message, Cx, State>,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
    Layers: LocalBlanketLayer,
    Plan: LocalDispatch,
    Path: CopyPublisher,
{
    async fn run<Signal, Host>(self, signal: Signal, state: &Arc<State>, host: &Host)
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: LocalDetach,
    {
        let Self {
            route,
            layers,
            plan,
            path,
        } = self;
        // Applied where the loop starts, as on a plain route: the wrapped handler's type is the
        // layer's own.
        let handler =
            LocalBlanketLayer::apply::<Sub::Message, Cx, State, _>(&layers, route.handler);
        let route = LocalRoute {
            subscriber: route.subscriber,
            handler,
            name: route.name,
            delivery: route.delivery,
            policies: route.policies,
        };
        plan.run(route, path, signal, state, host).await;
    }
}
