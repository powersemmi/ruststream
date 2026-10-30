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
use crate::host::{Halt, LocalDetach, OnOneThread, ShutdownWait};
use crate::input::Materialize;
use crate::lifecycle::BoxError;
use crate::middleware::LocalBlanketLayer;
use crate::typed::{LocalTyped, Typed};
use crate::{
    BuildContext, LocalBroker, LocalConnected, LocalIncomingMessage, LocalSubscriber,
    LocalSubscriptionSource,
};

use super::builder::Router;
use super::fold::{OpenLayered, RunRoutes};
use super::local::LocalPipeline;
use super::routes::SubscribeRoute;

/// How an opened single-message route runs on a runner that hosts the app in one task.
///
/// A runner implements it for each plan it offers; [`SequentialPlan`] is the plain loop.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a dispatch plan of this runner",
    note = "a plan comes from the runner's own mount steps"
)]
pub trait LocalDispatch {
    /// Runs the route until `signal` stops it.
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
        Host: LocalDetach;
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
    /// Runs the route one delivery at a time: the plan of a route that declared no workers.
    pub async fn run_sequential<State, Signal, Host>(
        self,
        signal: Signal,
        state: &Arc<State>,
        host: &Host,
    ) where
        Sub: LocalSubscriber,
        Body: LocalHandler<Sub::Message, Cx, State>,
        Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
        Signal: ShutdownWait + Halt + Clone,
        Host: LocalDetach,
    {
        let failure = DispatchFailure::new(self.policies, signal.clone());
        run_sequential(
            self.subscriber,
            self.handler,
            signal,
            self.name,
            Arc::clone(state),
            (&self.delivery, &OnOneThread(host)),
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
        Host: LocalDetach,
    {
        route.run_sequential(signal, state, host)
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
pub struct OpenedDispatch<Sub, Body, Cx, Layers, Plan> {
    route: LocalRoute<Sub, Body, Cx>,
    layers: Layers,
    plan: Plan,
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

impl<S, H, Cx> LocalPlanned for SubscribeRoute<S, H, Cx> {
    type Route = Self;
    type Plan = ();

    fn into_planned(self) -> (Self, ()) {
        (self, ())
    }
}

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

impl<B, State, Source, M, Input, DecodeCodec, Inner, Cx, Plan> OpenLayered<B, State>
    for Dispatched<SubscribeRoute<Source, Typed<M, Input, DecodeCodec, Inner>, Cx>, Plan>
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
    Source::Subscriber: LocalSubscriber<Message = M>,
    M: LocalIncomingMessage,
    Input: Materialize<DecodeCodec, M>,
    Inner: LocalHandler<Input::Target, Cx, State>,
    Cx: BuildContext<M> + Send + Sync + 'static,
    Plan: LocalDispatch,
{
    type Opened<Layers: LocalBlanketLayer + Clone> = OpenedDispatch<
        Source::Subscriber,
        LocalTyped<Typed<M, Input, DecodeCodec, Inner>>,
        Cx,
        Layers,
        Plan,
    >;

    async fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        connected: &LocalConnected<B>,
        layers: &Layers,
    ) -> Result<Self::Opened<Layers>, BoxError> {
        let Self { route, plan } = self;
        let name: Arc<str> = Arc::from(route.source.name());
        let subscriber = route
            .source
            .subscribe(connected)
            .await
            .map_err(|err| -> BoxError { Box::new(err) })?;
        Ok(OpenedDispatch {
            route: LocalRoute {
                subscriber,
                handler: LocalTyped(route.handler),
                name,
                delivery: Delivery::unscoped(),
                policies: route.policies,
            },
            layers: layers.clone(),
            plan,
        })
    }
}

impl<State, Sub, Body, Cx, Layers, Plan> RunRoutes<State>
    for OpenedDispatch<Sub, Body, Cx, Layers, Plan>
where
    Sub: LocalSubscriber,
    Body: LocalHandler<Sub::Message, Cx, State>,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
    Layers: LocalBlanketLayer,
    Plan: LocalDispatch,
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
        plan.run(route, signal, state, host).await;
    }
}
