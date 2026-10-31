//! The deferred-retry position of a registration the one-task fold opens: `.out_retry(policy)`
//! on a local chain, typed in the route, its copies kept in the store of the mount's plan and
//! published through the policy the mount site named.
//!
//! The multi-thread runner erases the position into a pairing it calls at startup and a publisher
//! it calls per copy. Here nothing is erased: the route carries the policy, and the plan's store
//! holds the publisher it pairs into, so a copy leaves with no dynamic call.

// Written over the local twins of the contract for a runner that polls on one thread.
#![allow(clippy::future_not_send)]

use alloc::borrow::Cow;
use core::marker::PhantomData;

use crate::lifecycle::BoxError;
use crate::metadata::OutgoingKind;
use crate::middleware::LocalBlanketLayer;
use crate::redelivery::{
    AppliesDeclaration, LocalRetrySlot, NoCopies, PairSlot, PublishesCopiesHere, ScopeDelivery,
};
use crate::retry::{RetryOffer, RetryOpen, SettlesDestination};
use crate::{
    DestinationDeclared, LocalBroker, LocalConnected, LocalSubscriptionSource, OutAttachment,
    Reads, Retry, RetryDeclaration,
};

use super::builder::{AttachRetry, Router};
use super::fold::OpenLayered;
use super::fold_plan::{
    Dispatched, LocalDispatch, LocalPlanned, OpenLocalRoute, OpenedDispatch, SequentialPlan,
};
use super::local::LocalPipeline;
use super::routes::SubscribeRoute;
use super::routes::{DeclaredRoute, RetryDestinationsDeclared, RouteMetadata};
use super::routes_inject::InjectRoute;
use super::routes_publish::{PublishingRoute, RawReplyRoute};

/// The subscription descriptor of a registration on a local chain, read while the chain is still
/// being written, before any state type is known. Machinery; never named directly.
#[doc(hidden)]
pub trait LocalRouteSource {
    /// The descriptor the registration subscribes through.
    type Source;
}

/// Implements [`LocalRouteSource`] for the registrations a local mount site adds.
macro_rules! route_source {
    ($([$($param:ident),+] $route:ty => $source:ident),+ $(,)?) => {$(
        impl<$($param),+> LocalRouteSource for $route {
            type Source = $source;
        }
    )+};
}

route_source!(
    [S, H, Cx] SubscribeRoute<S, H, Cx> => S,
    [Source, Def, DecodeCodec, Wiring, Extra]
        PublishingRoute<Source, Def, DecodeCodec, Wiring, Extra> => Source,
    [Source, Def, DecodeCodec, Wiring, Extra]
        RawReplyRoute<Source, Def, DecodeCodec, Wiring, Extra> => Source,
    [Source, Def, DecodeCodec, Extra] InjectRoute<Source, Def, DecodeCodec, Extra> => Source,
);

impl<Route: LocalRouteSource> LocalRouteSource for DeclaredRoute<Route> {
    type Source = Route::Source;
}

/// What the descriptor of a local registration declares about its retry copies.
type LocalCopies<B, Head> =
    <<Head as LocalRouteSource>::Source as LocalSubscriptionSource<LocalConnected<B>>>::Copies;

/// Whether the registration says where its copies go: the descriptor's address, or a `.to(name)`.
type LocalSettled<B, Head, Dest> =
    <<LocalCopies<B, Head> as RetryOffer<Dest>>::Offer as SettlesDestination<Reads>>::Settled;

/// One registration of a local chain with the deferred-retry position bound: the route, and the
/// policy, destination and transforms its kept copies leave through.
#[doc(hidden)]
#[derive(Debug)]
pub struct LocalRetriedRoute<Route, Policy, Stack, Settled> {
    route: Route,
    slot: LocalRetrySlot<Policy, Stack>,
    _settled: PhantomData<fn() -> Settled>,
}

// The position on a local chain: the slot's policy and transforms kept typed in the route. The
// copy carries the delivery's own bytes, so the codec the chain resolved encodes nothing and is
// dropped. The tokio arm reads the router's `PublishPipeline`, which a `LocalPipeline` is not, so
// the two arms are disjoint. The transforms are held to the copy where the registration opens,
// which knows the context they read, so a misfit is reported with its own diagnostic rather than
// as a step the chain lacks.
impl<B, Head, Tail, C, Layers, Pipe, Policy, Transforms, Enc, Dest>
    AttachRetry<OutAttachment<Retry, Policy, Transforms, Enc>, Dest>
    for Router<B, (Head, Tail), C, Layers, LocalPipeline<Pipe>>
where
    B: LocalBroker,
    Head: RetryOpen + RouteMetadata + LocalRouteSource,
    Head::Source: LocalSubscriptionSource<LocalConnected<B>>,
    // Where the broker moves the delivery itself, this is the bound that refuses the position and
    // names the descriptor, as on the multi-thread runner.
    LocalCopies<B, Head>: PublishesCopiesHere<Head::Source> + RetryOffer<Dest>,
    <LocalCopies<B, Head> as RetryOffer<Dest>>::Offer: SettlesDestination<Reads>,
{
    type Out = Router<
        B,
        (
            LocalRetriedRoute<Head, Policy, Transforms, LocalSettled<B, Head, Dest>>,
            Tail,
        ),
        C,
        Layers,
        LocalPipeline<Pipe>,
    >;

    fn attach_retry(
        self,
        slot: OutAttachment<Retry, Policy, Transforms, Enc>,
        destination: Option<Cow<'static, str>>,
    ) -> Self::Out {
        let (policy, stack, _codec) = slot.into_parts();
        let (mut head, tail) = self.routes;
        // A destination named here is a channel of the service, as on the multi-thread runner.
        if let Some(destination) = destination.clone() {
            head.metadata_mut()
                .declare_copy(destination, OutgoingKind::RetryCopy);
        }
        Router {
            routes: (
                LocalRetriedRoute {
                    route: head,
                    slot: LocalRetrySlot {
                        policy,
                        destination,
                        stack,
                    },
                    _settled: PhantomData,
                },
                tail,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }
}

// The registration that bound the position said where its copies go.
impl<Route, Policy, Stack> RetryDestinationsDeclared
    for LocalRetriedRoute<Route, Policy, Stack, DestinationDeclared>
{
}

// A step after the position plans the registration it wraps.
impl<Route: LocalPlanned, Policy, Stack, Settled> LocalPlanned
    for LocalRetriedRoute<Route, Policy, Stack, Settled>
{
    type Route = LocalRetriedRoute<Route::Route, Policy, Stack, Settled>;
    type Plan = Route::Plan;

    fn into_planned(self) -> (Self::Route, Self::Plan) {
        let (route, plan) = self.route.into_planned();
        (
            LocalRetriedRoute {
                route,
                slot: self.slot,
                _settled: PhantomData,
            },
            plan,
        )
    }
}

// Without a plan the copies have nowhere to wait: the registration opens under the plain plan,
// whose store is none, and the bound on its copies names the step to add.
impl<B, State, Route, Policy, Stack, Settled> OpenLayered<B, State>
    for LocalRetriedRoute<Route, Policy, Stack, Settled>
where
    B: LocalBroker,
    Route: OpenLocalRoute<B, State>,
    NoCopies: PairSlot<B, Route::Source, Policy, Stack, Route::Cx>
        + AppliesDeclaration<Route::Declaration>,
{
    type Opened<Layers: LocalBlanketLayer + Clone> = OpenedDispatch<
        Route::Sub,
        Route::Body,
        Route::Cx,
        Layers,
        SequentialPlan,
        <NoCopies as PairSlot<B, Route::Source, Policy, Stack, Route::Cx>>::Path,
    >;

    fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        connected: &LocalConnected<B>,
        layers: &Layers,
        scope: &ScopeDelivery,
    ) -> impl Future<Output = Result<Self::Opened<Layers>, BoxError>> {
        open_retried(self, SequentialPlan, connected, layers, scope)
    }
}

impl<B, State, Route, Policy, Stack, Settled, Plan> OpenLayered<B, State>
    for Dispatched<LocalRetriedRoute<Route, Policy, Stack, Settled>, Plan>
where
    B: LocalBroker,
    Route: OpenLocalRoute<B, State>,
    Plan: LocalDispatch<
        Copies: PairSlot<B, Route::Source, Policy, Stack, Route::Cx>
                    + AppliesDeclaration<Route::Declaration>,
    >,
{
    type Opened<Layers: LocalBlanketLayer + Clone> = OpenedDispatch<
        Route::Sub,
        Route::Body,
        Route::Cx,
        Layers,
        Plan,
        <Plan::Copies as PairSlot<B, Route::Source, Policy, Stack, Route::Cx>>::Path,
    >;

    fn open_layered<Layers: LocalBlanketLayer + Clone>(
        self,
        connected: &LocalConnected<B>,
        layers: &Layers,
        scope: &ScopeDelivery,
    ) -> impl Future<Output = Result<Self::Opened<Layers>, BoxError>> {
        open_retried(self.route, self.plan, connected, layers, scope)
    }
}

/// Opens a registration that bound the position under `plan`: the copy path paired from the
/// mount site's policy, then the subscription.
///
/// # Errors
///
/// The broker's refusal to pair the policy, to say the address, or to subscribe.
#[allow(clippy::type_complexity)] // the opened parts, named once here
async fn open_retried<B, State, Route, Policy, Stack, Settled, Plan, Layers>(
    retried: LocalRetriedRoute<Route, Policy, Stack, Settled>,
    plan: Plan,
    connected: &LocalConnected<B>,
    layers: &Layers,
    scope: &ScopeDelivery,
) -> Result<
    OpenedDispatch<
        Route::Sub,
        Route::Body,
        Route::Cx,
        Layers,
        Plan,
        <Plan::Copies as PairSlot<B, Route::Source, Policy, Stack, Route::Cx>>::Path,
    >,
    BoxError,
>
where
    B: LocalBroker,
    Route: OpenLocalRoute<B, State>,
    Plan: LocalDispatch<Copies: PairSlot<B, Route::Source, Policy, Stack, Route::Cx>>,
    Layers: LocalBlanketLayer + Clone,
{
    let LocalRetriedRoute { route, slot, .. } = retried;
    // Paired before the subscription opens, so a policy the broker refuses fails the startup
    // before any delivery arrives.
    let path = <Plan::Copies as PairSlot<B, Route::Source, Policy, Stack, Route::Cx>>::pair_slot(
        slot,
        route.source(),
        connected,
    )
    .await?;
    Ok(OpenedDispatch {
        route: route
            .open_route(connected, scope, RetryDeclaration::new())
            .await?,
        layers: layers.clone(),
        plan,
        path,
    })
}
