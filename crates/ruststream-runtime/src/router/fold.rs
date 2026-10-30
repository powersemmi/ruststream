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

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::future::ready;

use futures::future::join;

use crate::dispatch::{Delivery, HeadersAtOnce, Workers, run_sequential};
use crate::failure::{DispatchFailure, FailurePolicies};
use crate::handler::LocalHandler;
use crate::host::{Detach, Halt, ShutdownWait};
use crate::input::Materialize;
use crate::lifecycle::BoxError;
use crate::middleware::Identity;
use crate::typed::{LocalTyped, Typed};
use crate::{
    BuildContext, Handler, LocalBroker, LocalConnected, LocalIncomingMessage, LocalSubscriber,
    LocalSubscriptionSource,
};

use super::builder::Router;
use super::routes::SubscribeRoute;

/// A finished router's routes, opened against the connected broker into one list.
///
/// Implemented by [`Router`] and by every route the fold opens; a runner calls it once at
/// startup and runs what it hands back with [`RunRoutes::run`].
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a route this runner opens",
    note = "a runner that hosts the app in one task opens plain single-message registrations \
            (`include(def)` of a definition without a reply, an `Out` slot or a batch); the \
            router's own `layer` stack is not applied there"
)]
pub trait OpenRoutes<B: LocalBroker, State = ()> {
    /// The opened list: every subscription with its handler, ready to run.
    type Opened: RunRoutes<State>;

    /// Subscribes every route against `connected`.
    ///
    /// # Errors
    ///
    /// The first subscription the broker refuses, or a route whose dispatch policy this fold
    /// does not run.
    fn open(
        self,
        connected: &LocalConnected<B>,
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
        Host: Detach;
}

/// One opened single-message subscription: the subscriber, the handler it dispatches to, and
/// what each of its deliveries runs under.
#[doc(hidden)]
#[derive(Debug)]
pub struct OpenedRoute<Sub, Body, Cx> {
    subscriber: Sub,
    handler: Body,
    name: Arc<str>,
    delivery: Delivery<Cx>,
    policies: FailurePolicies,
}

impl<B: LocalBroker, State> OpenRoutes<B, State> for () {
    type Opened = ();

    fn open(self, _connected: &LocalConnected<B>) -> impl Future<Output = Result<(), BoxError>> {
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
        Host: Detach,
    {
        ready(())
    }
}

impl<B, State, Head, Tail> OpenRoutes<B, State> for (Head, Tail)
where
    B: LocalBroker,
    Head: OpenRoutes<B, State>,
    Tail: OpenRoutes<B, State>,
{
    type Opened = (Head::Opened, Tail::Opened);

    async fn open(self, connected: &LocalConnected<B>) -> Result<Self::Opened, BoxError> {
        // The list is built head first and the router pushes onto the head, so the tail holds
        // the registrations made first: they subscribe first.
        let tail = self.1.open(connected).await?;
        let head = self.0.open(connected).await?;
        Ok((head, tail))
    }
}

impl<State, Head, Tail> RunRoutes<State> for (Head, Tail)
where
    Head: RunRoutes<State>,
    Tail: RunRoutes<State>,
{
    async fn run<Signal, Host>(self, signal: Signal, state: &Arc<State>, host: &Host)
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
    {
        join(
            self.0.run(signal.clone(), state, host),
            self.1.run(signal, state, host),
        )
        .await;
    }
}

// A merged router is one registration of the router it was merged into; its own layer stack
// would wrap its handlers, which the fold does not apply, so only a router without one opens.
impl<B, State, Routes, RouteCodec, RoutePipe> OpenRoutes<B, State>
    for Router<B, Routes, RouteCodec, Identity, RoutePipe>
where
    B: LocalBroker,
    Routes: OpenRoutes<B, State>,
{
    type Opened = Routes::Opened;

    fn open(
        self,
        connected: &LocalConnected<B>,
    ) -> impl Future<Output = Result<Self::Opened, BoxError>> {
        self.routes.open(connected)
    }
}

impl<B, State, Source, M, Input, DecodeCodec, Inner, Cx> OpenRoutes<B, State>
    for SubscribeRoute<Source, Typed<M, Input, DecodeCodec, Inner>, Cx>
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
    Source::Subscriber: LocalSubscriber<Message = M>,
    M: LocalIncomingMessage,
    Input: Materialize<DecodeCodec, M>,
    Inner: Handler<Input::Target, Cx, State>,
    Cx: BuildContext<M> + Send + Sync + 'static,
{
    type Opened =
        OpenedRoute<Source::Subscriber, LocalTyped<Typed<M, Input, DecodeCodec, Inner>>, Cx>;

    async fn open(self, connected: &LocalConnected<B>) -> Result<Self::Opened, BoxError> {
        let name: Arc<str> = Arc::from(self.source.name());
        // Why a startup refusal and not a compile error: the policy is a value the definition's
        // chain set, so its type does not say whether it is sequential.
        if self.workers != Workers::sequential() {
            return Err(Box::from(alloc::format!(
                "subscription `{name}`: this runner dispatches one delivery at a time, and the \
                 registration asks for workers"
            )));
        }
        let subscriber = self
            .source
            .subscribe(connected)
            .await
            .map_err(|err| -> BoxError { Box::new(err) })?;
        Ok(OpenedRoute {
            subscriber,
            handler: LocalTyped(self.handler),
            name,
            delivery: Delivery::unscoped(),
            policies: self.policies,
        })
    }
}

impl<State, Sub, Body, Cx> RunRoutes<State> for OpenedRoute<Sub, Body, Cx>
where
    Sub: LocalSubscriber,
    Body: LocalHandler<Sub::Message, Cx, State>,
    Cx: BuildContext<Sub::Message> + Send + Sync + 'static,
{
    async fn run<Signal, Host>(self, signal: Signal, state: &Arc<State>, host: &Host)
    where
        Signal: ShutdownWait + Halt + Clone,
        Host: Detach,
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
