//! The [`Router`] builder: chaining registrations, codecs and layers, and mounting the result.

use alloc::borrow::ToOwned;
use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;

use alloc::borrow::Cow;
use core::num::NonZeroU32;

use crate::{Broker, Connected, PublishPolicy, Publisher, RetryDeclaration, Subscriber};

use crate::dispatch::Workers;
use crate::failure::FailurePolicies;
use crate::handler::Handler;
use crate::metadata::{HandlerMetadata, OutgoingKind, PublishDescription};
use crate::middleware::{BlanketLayer, Identity, Layer, Stack};
use crate::publish::{
    FitsOffer, ForReply, NamesDestination, NarrowToUse, OutPipeline, PublishIdentity,
    PublishPipeline, PublishTransform, Reads,
};
use crate::redelivery::{PublishesCopiesHere, RetryPairing, RetrySetup};
use crate::retry::{
    Absent, DeclareCap, DeclareDeadLetter, DeclareMount, Present, Retry, RetryOffer, RetryOpen,
    RetryStackUse, RouteDeclaring, RoutePosition, SettlesDestination,
};
use crate::slot::{NoReply, OutAttachment, SlotCodec};

use super::MergedRouter;
use super::builders::RouterWith;
use super::mount_sink::MountSink;
use super::routes::{
    BatchRoute, DeclaredRoute, HandleRoute, MountRoute, RetriedRoute, RetryDestinationsDeclared,
    RouteMeta, RouteMetadata, RouteSubscription, RouterDef, RouterHandlers, SubscribeRoute,
};

mod mount;

/// A statically-typed, lazily-bound group of handler registrations, not attached to any broker.
///
/// Build it by chaining (each call consumes the router and returns a new type that carries the
/// added registration), then mount it with
/// [`include_router`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.BrokerScope.html#method.include_router). The registration list `Routes` is
/// an opaque nested tuple, so a builder function returns `impl RouterDef<B>` instead of naming the
/// type.
///
/// The codec parameter `C` is the codec the `include` family decodes with. It starts as `()`, meaning the [`DefaultCodec`](crate::codec::DefaultCodec); switch it
/// for the rest of the chain with [`with_codec`](Self::with_codec).
///
/// The layer parameter `Layers` is the router's own middleware stack, grown with
/// [`layer`](Self::layer). It wraps every handler in the router when the router is mounted, inside
/// the app's global stack.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "json")]
/// # fn build() {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{Context, Handle, HandlerOutcome, Router, RouterDef, subscriber};
///
/// #[derive(serde::Deserialize, schemars::JsonSchema)]
/// struct Event {
///     id: u64,
/// }
///
/// struct Audit;
///
/// impl Handle<Event> for Audit {
///     async fn handle(
///         &self,
///         event: &Event,
///         _outs: &(),
///         _ctx: &mut Context<'_>,
///     ) -> Result<(), HandlerOutcome> {
///         let _ = event.id;
///         Ok(())
///     }
/// }
///
/// fn routes() -> impl RouterDef<MemoryBroker> {
///     Router::<MemoryBroker>::new().include(subscriber("events", Audit).build())
/// }
/// // later: app.with_broker(broker, |b| b.include_router(routes()));
/// # }
/// ```
pub struct Router<B, Routes = (), C = (), Layers = Identity, Pipe = PublishIdentity> {
    pub(super) routes: Routes,
    pub(super) codec: C,
    pub(super) layers: Layers,
    /// The publish pipeline this chain's [`Out`](crate::Out) slots send through, under
    /// each slot's own `.transform(..)` steps.
    ///
    /// A slot's pipeline is part of the instantiated definition's type, so it is fixed when the
    /// slot binds - which is at `include`, before an app exists. A router built on its own
    /// therefore carries [`PublishIdentity`] here and its slots publish with nothing in the way;
    /// the chain a [`BrokerScope`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.BrokerScope.html) drives carries the app's own
    /// pipeline, because there the app is already known. Replies are not affected either way:
    /// their publisher pairs at startup, so they travel the app's pipeline on both surfaces.
    pub(super) pipeline: Pipe,
    pub(super) _broker: PhantomData<fn() -> B>,
}

impl<B: Broker + 'static> Default for Router<B> {
    fn default() -> Self {
        Self {
            routes: (),
            codec: (),
            layers: Identity,
            pipeline: PublishIdentity,
            _broker: PhantomData,
        }
    }
}

impl<B, Routes, C, Layers, Pipe> fmt::Debug for Router<B, Routes, C, Layers, Pipe> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Router").finish_non_exhaustive()
    }
}

impl<B: Broker + 'static> Router<B, ()> {
    /// Creates an empty router decoding with the [`DefaultCodec`](crate::codec::DefaultCodec).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl<B: Broker + 'static, RouteCodec, RouteLayers, RoutePipe>
    Router<B, (), RouteCodec, RouteLayers, RoutePipe>
{
    /// Adds a router-scope middleware layer, wrapping every handler in this router when the
    /// router is mounted. The first layer added runs outermost within the router; the app's
    /// global [`layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.layer) stack wraps outside it.
    ///
    /// The layer must be a [`BlanketLayer`] (it applies to handlers whose concrete types the
    /// router hides), like the app-global stack, and it is declared before the router's first
    /// registration: after one, `.layer(..)` rides that registration instead
    /// ([`Router::layer`](Router::layer) on a registration), exactly as the other steps of the
    /// chain ride the position named before them.
    #[must_use]
    pub fn layer<N>(self, layer: N) -> Router<B, (), RouteCodec, Stack<N, RouteLayers>, RoutePipe> {
        Router {
            routes: self.routes,
            codec: self.codec,
            layers: Stack::new(layer, self.layers),
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }
}

impl<B: Broker + 'static, RouteCodec, RoutePipe> Router<B, (), RouteCodec, Identity, RoutePipe> {
    /// The empty chain a [`BrokerScope`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.BrokerScope.html) drives one registration
    /// through: the scope's codec and its publish pipeline, with no router-scope layers of its
    /// own (the app's stack wraps at the drain, as it does for any router).
    #[doc(hidden)]
    pub const fn for_scope(codec: RouteCodec, pipeline: RoutePipe) -> Self {
        Self {
            routes: (),
            codec,
            layers: Identity,
            pipeline,
            _broker: PhantomData,
        }
    }
}

impl<B: Broker + 'static, Routes, RouteCodec, RouteLayers, RoutePipe>
    Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>
{
    /// Sets the codec that subsequent `include` calls decode with, replacing the default.
    ///
    /// Registrations already in the chain keep the codec they were mounted with, so the codec can
    /// change mid-chain.
    #[must_use]
    pub fn with_codec<C>(self, codec: C) -> Router<B, Routes, C, RouteLayers, RoutePipe> {
        Router {
            routes: self.routes,
            codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// Appends every registration of `other` after this router's own, keeping each router's codec
    /// and layer stack.
    ///
    /// The merged router's handlers stay wrapped by its own layers; this router's layers wrap
    /// around them as well once mounted (scopes nest).
    ///
    /// # Examples
    ///
    /// Two modules' routers mounted as one group:
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use serde::Deserialize;
    ///
    /// # #[derive(Deserialize)]
    /// # struct Order {
    /// #     id: u64,
    /// # }
    /// # #[derive(Deserialize)]
    /// # struct Shipment {
    /// #     order_id: u64,
    /// # }
    /// #[subscriber("orders")]
    /// async fn accept(order: &Order) -> HandlerOutcome {
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// #[subscriber("shipments")]
    /// async fn dispatch(shipment: &Shipment) -> HandlerOutcome {
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn orders() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    ///     Router::new().include(accept)
    /// }
    ///
    /// fn shipping() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    ///     Router::new().include(dispatch)
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("routing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include_router(orders().merge(shipping()));
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn merge<R2, C2, L2, P2>(
        self,
        other: Router<B, R2, C2, L2, P2>,
    ) -> MergedRouter<B, R2, C2, L2, P2, RouteCodec, RouteLayers, RoutePipe, Routes>
    where
        L2: BlanketLayer,
    {
        self.with_route(other)
    }

    /// Attaches `handler` to an already-created `subscriber`. Machinery, not the user path: a
    /// service mounts definitions with [`include`](Router::include) and the value constructors
    /// ([`subscriber`](crate::subscriber), ...).
    ///
    /// The subscriber is created up front (before connect), the handler arrives fully wired (a
    /// decode adapter included, see [`typed`](crate::typed)), and the metadata is the
    /// caller's to assemble. That is the shape the runtime's own dispatch tests and a broker
    /// author holding a hand-built subscriber need, and nothing above it.
    #[allow(clippy::type_complexity)] // the grown chain's own type; an alias would hide the route
    pub fn handle<S, H>(
        self,
        subscriber: S,
        handler: H,
        meta: HandlerMetadata,
    ) -> Router<B, (HandleRoute<S, H>, Routes), RouteCodec, RouteLayers, RoutePipe>
    where
        S: Subscriber + Send + 'static,
        H: Handler<S::Message> + 'static,
    {
        self.with_route(HandleRoute {
            subscriber,
            handler,
            meta,
            policies: FailurePolicies::default(),
        })
    }

    /// Prepends one registration to the chain, carrying the codec, layers and pipeline over: the
    /// shared tail of every step that adds a route.
    fn with_route<Route>(
        self,
        route: Route,
    ) -> Router<B, (Route, Routes), RouteCodec, RouteLayers, RoutePipe> {
        Router {
            routes: (route, self.routes),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }
}

impl<B, S, H, Cx, Routes, RouteCodec, RouteLayers, RoutePipe>
    Router<B, (SubscribeRoute<S, H, Cx>, Routes), RouteCodec, RouteLayers, RoutePipe>
{
    /// Sets the concurrency policy of the registration just added (the preceding `include`
    /// call), replacing its default.
    ///
    /// The functional-path counterpart of the macro's `workers(..)` clause: [`Workers::pool`]
    /// processes up to `n` deliveries of this subscriber concurrently, [`Workers::keyed`]
    /// dispatches over per-key sequential lanes. On an `include`d definition this overrides the
    /// attribute's `workers(..)` clause and the chained
    /// [`workers`](crate::SubscriberSettings::workers) setting alike.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Job {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("jobs")]
    /// async fn work(job: &Job) -> HandlerOutcome {
    ///     tracing::info!(job.id, "done");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn jobs() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    ///     // Four lanes; the jobs of one key stay in order on one lane.
    ///     Router::new()
    ///         .include(work)
    ///         .workers(Workers::keyed(nonzero!(4)))
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include_router(jobs());
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    pub fn workers(mut self, workers: Workers) -> Self {
        self.routes.0.workers = workers;
        self
    }

    /// Wraps the registration just added (the preceding `include` call) with `layer`, outside its
    /// decode step, so the layer sees the raw delivery.
    ///
    /// The per-registration counterpart of the app-wide
    /// [`RustStream::layer`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.RustStream.html#method.layer) and the router-wide
    /// [`layer`](Router::layer) on a fresh router: those two wrap every handler in their scope, so
    /// they take a [`BlanketLayer`], which cannot be written for a layer fixed to one message
    /// type. This one wraps exactly one registration, whose handler type is still concrete here,
    /// so it takes an ordinary [`Layer`] - which is what a [`DynStack`](crate::DynStack)
    /// over the broker's own message type is.
    ///
    /// Which of the two a `.layer(..)` call is follows what the chain named before it, like every
    /// other step: on a router with no registrations yet it is the router's own, and after an
    /// `include` it is that registration's. It repeats: each call wraps what the calls before it
    /// produced, the last one outermost.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::memory::prelude::*;
    /// use ruststream::runtime::layers::TracingLayer;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Job {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("jobs")]
    /// async fn work(job: &Job) -> HandlerOutcome {
    ///     tracing::info!(job.id, "done");
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn jobs() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    ///     // Every handler of this router runs inside the tracing layer's span.
    ///     Router::new().include(work).layer(TracingLayer::default())
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include_router(jobs());
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[must_use]
    // The call site reads `.layer(TracingLayer::default())`, so the layer travels by value like
    // every other builder argument; `Layer::layer` only borrows it.
    #[allow(clippy::needless_pass_by_value, clippy::type_complexity)]
    pub fn layer<N>(
        self,
        layer: N,
    ) -> Router<B, (SubscribeRoute<S, N::Handler, Cx>, Routes), RouteCodec, RouteLayers, RoutePipe>
    where
        N: Layer<H>,
    {
        let SubscribeRoute {
            source,
            handler,
            meta,
            policies,
            workers,
            _context: context,
        } = self.routes.0;
        Router {
            routes: (
                SubscribeRoute {
                    source,
                    handler: layer.layer(handler),
                    meta,
                    policies,
                    workers,
                    _context: context,
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

impl<B, S, H, Cx, Routes, RouteCodec, RouteLayers, RoutePipe>
    Router<B, (BatchRoute<S, H, Cx>, Routes), RouteCodec, RouteLayers, RoutePipe>
{
    /// Sets the concurrency policy of the batch registration just added (the preceding batch
    /// `include` call), replacing its default.
    ///
    /// [`Workers::pool`] keeps up to `n` batches in flight at once. Keyed lanes order single
    /// messages per key and do not apply to batches: a [`Workers::keyed`] policy here behaves
    /// like a plain pool of the same size.
    #[must_use]
    pub fn workers(mut self, workers: Workers) -> Self {
        self.routes.0.workers = workers;
        self
    }
}

impl<B: Broker + 'static, Head, Tail, RouteCodec, RouteLayers, RoutePipe>
    Router<B, (Head, Tail), RouteCodec, RouteLayers, RoutePipe>
{
    /// Names the publish policy of one position of the registration just added (the preceding
    /// `include` call).
    ///
    /// A form that publishes nothing of its own is a finished route by the time `include`
    /// returns, and the one position such a registration still has is the deferred retry - see
    /// [`Retry`](crate::Retry). Naming it opens a mount chain over that route, so the
    /// steps after it are the slot steps and `.build()` hands the router back.
    ///
    /// Name it after that registration's other settings ([`workers`](Self::workers),
    /// [`layer`](Self::layer)): those read the route this call wraps.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use ruststream::memory::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Job {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("jobs")]
    /// async fn work(job: &Job) -> HandlerOutcome {
    ///     tracing::info!(job.id, "done");
    ///     HandlerOutcome::retry_after(Duration::from_secs(30))
    /// }
    ///
    /// fn jobs() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    ///     // The copies a `retry_after` publishes leave through this policy.
    ///     Router::new().include(work).out(Retry, Publish).build()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include_router(jobs());
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    // The unit marker drives inference, so it travels by value to keep the call site
    // `.out(Retry, ..)`, like every other position's.
    #[allow(clippy::needless_pass_by_value)]
    pub fn out<M, Policy>(
        self,
        marker: M,
        policy: Policy,
    ) -> <M as RoutePosition<B, Self, Policy>>::Out
    where
        M: RoutePosition<B, Self, Policy>,
    {
        let _ = marker;
        M::bind(self, policy)
    }

    /// The same call with the marker spelled out: `.out(Retry, policy)`.
    ///
    /// It opens the same mount chain, so the slot steps follow it and `.build()` finishes it.
    pub fn out_retry<Policy>(self, policy: Policy) -> <Retry as RoutePosition<B, Self, Policy>>::Out
    where
        Retry: RoutePosition<B, Self, Policy>,
    {
        self.out(Retry, policy)
    }

    /// Caps the deliveries one message of the registration just added gets, counting the first.
    ///
    /// See [`RouterWith::max_attempts`]: the declaration is the same on every form, and this is
    /// the call for a registration that was a finished route the moment `include` returned. It
    /// opens a mount chain, so `.dead_letter(..)`, `.out_retry(..)` and `.build()` follow it.
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn max_attempts(
        self,
        attempts: NonZeroU32,
    ) -> RouterWith<DeclareMount, Self, (), RouteDeclaring<Present, Absent>>
    where
        Head: RetryOpen,
    {
        RouterWith::new((), (NoReply, ()).declare_cap(attempts), self)
    }

    /// Names where a delivery of the registration just added goes once its attempts run out.
    ///
    /// See [`RouterWith::dead_letter`].
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the position
    pub fn dead_letter(
        self,
        destination: impl Into<Cow<'static, str>>,
    ) -> RouterWith<DeclareMount, Self, (), RouteDeclaring<Absent, Present>>
    where
        Head: RetryOpen,
    {
        RouterWith::new(
            (),
            (NoReply, ()).declare_dead_letter(destination.into()),
            self,
        )
    }
}

impl<B: Broker + 'static, Routes: RouterHandlers, C, Layers, Pipe>
    Router<B, Routes, C, Layers, Pipe>
{
    /// Returns metadata for every registered handler, in registration order.
    #[must_use]
    pub fn handlers(&self) -> Vec<HandlerMetadata> {
        let mut out = Vec::new();
        self.routes.collect_handlers(&mut out);
        out
    }
}

/// The broker a router's registrations mount on, read off the router type.
///
/// A mount chain names its router, not its broker, so this is how `.out(Retry, policy)` reaches
/// the broker the policy pairs against. Machinery; never named directly.
#[doc(hidden)]
pub trait RouterBroker {
    /// The broker every registration of this router mounts on.
    type Broker: Broker + 'static;
}

impl<B: Broker + 'static, Routes, C, Layers, Pipe> RouterBroker
    for Router<B, Routes, C, Layers, Pipe>
{
    type Broker = B;
}

/// The live publisher the deferred-retry position's policy pairs into: what fixes the per-message
/// options its transforms write.
type RetryLive<Policy, B> = <Policy as PublishPolicy<Connected<B>>>::Live;

/// What one registration's retry transforms read: the broker's typed per-delivery context of the
/// delivery being retried.
type RetryCx<Head, B> = <Head as RouteSubscription<B>>::Context;

/// What the position offers a transform on this registration: the naming right only where the
/// descriptor addresses nothing and the mount site named nothing.
type RetryOfferOf<Head, B, Dest> =
    <<Head as RouteSubscription<B>>::Copies as RetryOffer<Dest>>::Offer;

/// The most the mount site's transforms do to the destination, projected against the publisher
/// they run over.
type RetryUse<Transforms, Policy, B, Head> =
    <Transforms as RetryStackUse<RetryLive<Policy, B>, RetryCx<Head, B>>>::Destination;

/// The policy the position pairs: the one the mount site named, narrowed to plain sending where
/// its transforms name the destination.
type RetryPolicy<Transforms, Policy, B, Head> =
    <RetryUse<Transforms, Policy, B, Head> as NarrowToUse<Policy>>::Out;

/// Whether the registration says where its copies go, folded out of what the position offered and
/// what its transforms declared.
type RetrySettled<Head, B, Dest, Transforms, Policy> =
    <RetryOfferOf<Head, B, Dest> as SettlesDestination<RetryUse<Transforms, Policy, B, Head>>>::Settled;

/// The live publisher that narrowed policy pairs into.
type RetryWire<Transforms, Policy, B, Head> =
    <RetryPolicy<Transforms, Policy, B, Head> as PublishPolicy<Connected<B>>>::Live;

/// Wires the deferred-retry position onto the registration a router grew last: the tail of
/// `.out(Retry, policy)` on a mount chain, after the chain committed its registration.
///
/// This is where the position's attachment resolves into what the runtime pairs - the policy and
/// the transform stack that runs against the delivery being retried - and where the two are
/// erased into the pairing the starter takes. It is also where the destination is settled: a
/// descriptor that addresses its own copies, a `.to(name)`, or a transform that names one per
/// delivery, and a registration with none of the three does not compile. Machinery; never named
/// directly.
#[doc(hidden)]
pub trait AttachRetry<Attachment, Dest>: Sized {
    /// The router with that registration's retry position bound.
    type Out;

    /// Wires it.
    fn attach_retry(self, slot: Attachment, destination: Option<Cow<'static, str>>) -> Self::Out;
}

impl<B, Head, Tail, C, Layers, Pipe, Policy, Transforms, Enc, Dest>
    AttachRetry<OutAttachment<Retry, Policy, Transforms, Enc>, Dest>
    for Router<B, (Head, Tail), C, Layers, Pipe>
where
    B: Broker + 'static,
    Head: RetryOpen + RouteMetadata + RouteSubscription<B, Context: Send + Sync + 'static>,
    // The descriptor decides whether there is a publisher of this process's to name at all: where
    // the broker moves the delivery itself, this is the bound that refuses the position and names
    // the descriptor.
    <Head as RouteSubscription<B>>::Copies:
        PublishesCopiesHere<<Head as RouteSubscription<B>>::Source> + RetryOffer<Dest>,
    // And whether the mount site still owes a destination: the offer is `Names` only where
    // nothing has declared one, and then a naming transform is what has to declare it. The answer
    // rides the route, so `.build()` refuses a chain that says nothing while a scope's guard -
    // which commits when the statement ends, before a `.to(name)` could be written - records the
    // registration and lets the startup refusal report it.
    RetryOfferOf<Head, B, Dest>: SettlesDestination<RetryUse<Transforms, Policy, B, Head>>,
    C: Clone,
    Pipe: Clone + OutPipeline<RetryWire<Transforms, Policy, B, Head>> + 'static,
    Enc: SlotCodec<C>,
    Policy: PublishPolicy<Connected<B>>,
    Transforms: RetryStackUse<RetryLive<Policy, B>, RetryCx<Head, B>>
        + PublishTransform<
            ForReply<RetryCx<Head, B>>,
            <RetryWire<Transforms, Policy, B, Head> as Publisher>::Options,
        > + Send
        + Sync
        + 'static,
    // `FitsOffer` is the other half of the same rule: a transform may name the destination only
    // where the position offers the right, so a declared destination and a naming transform are
    // mutually exclusive, as they are on a reply.
    RetryUse<Transforms, Policy, B, Head>: NarrowToUse<Policy>
        + NamesDestination
        + FitsOffer<RetryOfferOf<Head, B, Dest>, Reads, Retry>,
    RetryPolicy<Transforms, Policy, B, Head>:
        PublishPolicy<Connected<B>, Live: Publisher + Send + Sync + 'static> + Send + 'static,
{
    type Out = Router<
        B,
        (
            RetriedRoute<
                Head,
                B,
                RetryCx<Head, B>,
                RetrySettled<Head, B, Dest, Transforms, Policy>,
            >,
            Tail,
        ),
        C,
        Layers,
        Pipe,
    >;

    fn attach_retry(
        self,
        slot: OutAttachment<Retry, Policy, Transforms, Enc>,
        destination: Option<Cow<'static, str>>,
    ) -> Self::Out {
        // The copy carries the delivery's own bytes, so the codec the chain resolved here encodes
        // nothing and is dropped; what is kept is the policy and the transforms.
        let (policy, transforms, _codec) = slot.into_parts();
        let policy = <RetryUse<Transforms, Policy, B, Head> as NarrowToUse<Policy>>::narrow(policy);
        let (mut head, tail) = self.routes;
        // A destination named here is traffic to a channel of the service, so the document
        // reports it the way it reports a dead-letter one. A descriptor that takes its copies
        // back at its own address names nothing and adds no channel: that address is the
        // subscription's, which the document already carries.
        if let Some(destination) = destination.clone() {
            head.metadata_mut()
                .declare_copy(destination, OutgoingKind::RetryCopy);
        }
        Router {
            routes: (
                RetriedRoute::new(
                    head,
                    RetryPairing::new(policy, transforms, self.pipeline.clone()),
                    destination,
                    <RetryUse<Transforms, Policy, B, Head> as NamesDestination>::NAMES,
                ),
                tail,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }
}

/// What one bound slot contributes to the document: what its policy and codec say about each
/// destination its marker declares. Produced by
/// [`OutAttachment::describe`](crate::OutAttachment) at the commit, before wiring
/// consumes the attachment.
pub(super) type SlotDescription = Vec<(Cow<'static, str>, PublishDescription)>;

impl<B, Head, Tail, C, Layers, Pipe> Router<B, (Head, Tail), C, Layers, Pipe>
where
    Head: RouteMetadata,
{
    /// Writes what each bound slot says onto the entries its marker declared, on the route the
    /// commit just added.
    pub(super) fn describing_slots(mut self, slots: &[SlotDescription]) -> Self {
        let meta = self.routes.0.metadata_mut();
        for described in slots {
            meta.describe_slot(described);
        }
        self
    }
}

/// Wires the registration's retry declaration onto the route a router grew last: the tail of
/// `.max_attempts(..)` and `.dead_letter(..)` on a mount chain, after the chain committed its
/// registration.
///
/// The declaration reaches two places from here. The generated document gains a channel the
/// registration publishes to, because a dead-lettered delivery leaves the service; and the route
/// gains the declaration itself, which the runtime hands the subscription descriptor at startup.
/// Machinery; never named directly.
#[doc(hidden)]
pub trait AttachDeclaration: Sized {
    /// The router with that registration's declaration wired.
    type Out;

    /// Wires it.
    fn attach_declaration(self, declaration: RetryDeclaration) -> Self::Out;
}

impl<B, Head, Tail, C, Layers, Pipe> AttachDeclaration for Router<B, (Head, Tail), C, Layers, Pipe>
where
    B: Broker + 'static,
    Head: RetryOpen + RouteMetadata,
{
    type Out = Router<B, (DeclaredRoute<Head>, Tail), C, Layers, Pipe>;

    fn attach_declaration(self, declaration: RetryDeclaration) -> Self::Out {
        let (mut head, tail) = self.routes;
        // A dead-lettered delivery is the registration's input leaving the service, so the
        // document reports it under the input's own schema on the declared channel.
        if let Some(destination) = declaration.dead_letter() {
            head.metadata_mut()
                .declare_copy(destination.to_owned().into(), OutgoingKind::DeadLetter);
        }
        // The declaration itself rides the metadata too: the cap is what the document reports
        // next to the dead-letter channel, and a cap without a destination is still a fact
        // about the registration.
        head.metadata_mut().retry = declaration.clone();
        Router {
            routes: (DeclaredRoute::new(head, declaration), tail),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }
}

// A whole router answers when its registrations do.
impl<B: Broker, Routes: RetryDestinationsDeclared, C, Layers, Pipe> RetryDestinationsDeclared
    for Router<B, Routes, C, Layers, Pipe>
{
}

/// Composes the mount-time global stack (outer) with a router's own layer stack (inner), owned
/// so publishing mounts can carry the composition into their startup pairing closures.
#[derive(Clone)]
struct ComposedBlanket<Outer, Inner> {
    outer: Outer,
    inner: Inner,
}

impl<Outer: BlanketLayer, Inner: BlanketLayer> BlanketLayer for ComposedBlanket<Outer, Inner> {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        self.outer
            .apply::<M, C, S, _>(self.inner.apply::<M, C, S, _>(handler))
    }
}

// A router carries its own publish path, so the one it is mounted with plays no part: whatever
// `RetryPipeline` the caller names, the registrations below take this router's.
impl<B, Routes, C, Layers, Pipe, State, RetryPipeline> RouterDef<B, State, RetryPipeline>
    for Router<B, Routes, C, Layers, Pipe>
where
    B: Broker + 'static,
    Routes: RouterDef<B, State, Pipe>,
    Layers: BlanketLayer + Clone + Send + Sync + 'static,
{
    fn mount<G, PP, Sink>(
        self,
        global: &G,
        pipeline: &PP,
        _retry_pipeline: &RetryPipeline,
        sink: &mut Sink,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
        Sink: MountSink<B, State>,
    {
        let composed = ComposedBlanket {
            outer: global.clone(),
            inner: self.layers,
        };
        // The router's own publish path is what a retry copy travels, named or defaulted, so it
        // travels down beside the app's.
        self.routes.mount(&composed, pipeline, &self.pipeline, sink);
    }
}

impl<B, Routes, C, Layers, Pipe> RouterHandlers for Router<B, Routes, C, Layers, Pipe>
where
    Routes: RouterHandlers,
{
    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>) {
        self.routes.collect_handlers(out);
    }
}

// Lets a whole router be a single registration inside another router's list (`Router::merge`).
impl<B, Routes, C, Layers, Pipe, State, RetryPipeline> MountRoute<B, State, RetryPipeline>
    for Router<B, Routes, C, Layers, Pipe>
where
    B: Broker + 'static,
    Routes: RouterDef<B, State, Pipe>,
    Layers: BlanketLayer + Clone + Send + Sync + 'static,
{
    // A whole router is many registrations, each with its own context; nothing here reads one.
    type Context = ();

    fn mount_one<G, PP, Sink>(
        self,
        global: &G,
        pipeline: &PP,
        // A merged router carries its own publish path down to its own registrations, so what
        // reaches it here only travels on to the `RouterDef` impl, which ignores it in turn.
        retry_pipeline: &RetryPipeline,
        sink: &mut Sink,
        // A whole router is many registrations, and a retry position or declaration belongs to
        // one, so neither binds here: `RetryOpen` is not implemented for a router.
        _setup: RetrySetup<B, ()>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
        Sink: MountSink<B, State>,
    {
        RouterDef::<B, State, RetryPipeline>::mount(self, global, pipeline, retry_pipeline, sink);
    }
}

// Lets a merged router contribute its registrations' metadata to the outer router's `handlers()`.
impl<B, Routes, C, Layers, Pipe> RouteMeta for Router<B, Routes, C, Layers, Pipe>
where
    Routes: RouterHandlers,
{
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        self.routes.collect_handlers(out);
    }
}
